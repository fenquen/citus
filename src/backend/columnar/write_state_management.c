
#include "citus_version.h"

#include "postgres.h"
#include "columnar/columnar.h"


#include <math.h>

#include "miscadmin.h"

#include "access/genam.h"
#include "access/heapam.h"
#include "access/multixact.h"
#include "access/rewriteheap.h"
#include "access/tsmapi.h"

#if PG_VERSION_NUM >= 130000
#include "access/heaptoast.h"
#include "common/hashfn.h"
#else

#include "access/tuptoaster.h"

#endif

#include "access/xact.h"
#include "catalog/catalog.h"
#include "catalog/index.h"
#include "catalog/objectaccess.h"
#include "catalog/pg_am.h"
#include "catalog/pg_trigger.h"
#include "catalog/storage.h"
#include "catalog/storage_xlog.h"
#include "commands/progress.h"
#include "commands/vacuum.h"
#include "executor/executor.h"
#include "nodes/makefuncs.h"
#include "optimizer/plancat.h"
#include "pgstat.h"
#include "storage/bufmgr.h"
#include "storage/bufpage.h"
#include "storage/bufmgr.h"
#include "storage/lmgr.h"
#include "storage/predicate.h"
#include "storage/procarray.h"
#include "storage/smgr.h"
#include "tcop/utility.h"
#include "utils/builtins.h"
#include "utils/pg_rusage.h"
#include "utils/rel.h"
#include "utils/syscache.h"

#include "columnar/columnar_customscan.h"
#include "columnar/columnar_tableam.h"
#include "columnar/columnar_version_compat.h"


/*
 * key是relation->rd_node.relNode
 * Mapping from relfilenode to WriteStateMapEntry. This keeps write state for each relation.
 */
static HTAB *WriteStateMap = NULL;

/* memory context for allocating WriteStateMap & all write states */
static MemoryContext WriteStateContext = NULL;

/*
 * Each member of the subXidWriteState in WriteStateMapEntry. This means that
 * we did some inserts in the subtransaction subXid, and the state of those
 * inserts is stored at columnarWriteState. Those writes can be flushed or unflushed.
 */
typedef struct SubXidWriteState {
    SubTransactionId subXid;
    ColumnarWriteState *columnarWriteState;

    struct SubXidWriteState *next;
} SubXidWriteState;


/*
 * An entry in WriteStateMap.
 */
typedef struct WriteStateMapEntry {
    /* key of the entry */
    Oid relfilenode;

    /*
     * If a table is dropped, we set dropped to true and set dropSubXid to the
     * id of the subtransaction in which the drop happened.
     */
    bool dropped;
    SubTransactionId dropSubXid;

    /*
     * Stack of SubXidWriteState where first element is top of the stack. When
     * inserts happen, we look at top of the stack. If top of stack belongs to
     * current subtransaction, we forward writes to its columnarWriteState. Otherwise,
     * we create a new stack entry for current subtransaction and push it to
     * the stack, and forward writes to that.
     */
    SubXidWriteState *subXidWriteState;
} WriteStateMapEntry;


/*
 * Memory context reset callback so we reset WriteStateMap to NULL at the end
 * of transaction. WriteStateMap is allocated in & WriteStateMap, so its
 * leaked reference can cause memory issues.
 */
static MemoryContextCallback cleanupCallback;

static void CleanupWriteStateMap(void *arg) {
    WriteStateMap = NULL;
    WriteStateContext = NULL;
}


ColumnarWriteState *columnar_init_write_state(Relation relation,
                                              TupleDesc tupleDesc,
                                              SubTransactionId currentSubXid) {
    bool found;

    // this is the first call in current transaction,生成hashMap以oid为key
    if (WriteStateMap == NULL) {
        WriteStateContext = AllocSetContextCreate(TopTransactionContext,
                                                  "Column Store Write State Management Context",
                                                  ALLOCSET_DEFAULT_SIZES);
        HASHCTL info;
        memset(&info, 0, sizeof(info));
        info.keysize = sizeof(Oid);
        info.hash = oid_hash;
        info.entrysize = sizeof(WriteStateMapEntry);
        info.hcxt = WriteStateContext;

        uint32 hashFlags = (HASH_ELEM | HASH_FUNCTION | HASH_CONTEXT);

        // 生成了hashMap
        WriteStateMap = hash_create("column store write state map",
                                    64,
                                    &info,
                                    hashFlags);

        cleanupCallback.arg = NULL;
        cleanupCallback.func = &CleanupWriteStateMap;
        cleanupCallback.next = NULL;

        MemoryContextRegisterResetCallback(WriteStateContext, &cleanupCallback);
    }

    WriteStateMapEntry *writeStateMapEntry = hash_search(WriteStateMap,
                                                         &relation->rd_node.relNode,
                                                         HASH_ENTER,
                                                         &found);
    if (!found) {
        writeStateMapEntry->subXidWriteState = NULL;
        writeStateMapEntry->dropped = false;
    }

    Assert(!writeStateMapEntry->dropped);

    // If top of stack belongs to the current sub transaction, return its columnarWriteState, ...
    if (writeStateMapEntry->subXidWriteState != NULL) {
        if (writeStateMapEntry->subXidWriteState->subXid == currentSubXid) {
            return writeStateMapEntry->subXidWriteState->columnarWriteState;
        }
    }

    // otherwise we need to create a new stack entry for the current sub transaction.
    MemoryContext oldContext = MemoryContextSwitchTo(WriteStateContext);//----------------切换

    ColumnarOptions columnarOptions = {0};
    ReadColumnarOptions(relation->rd_id, &columnarOptions);

    SubXidWriteState *subXidWriteState = palloc0(sizeof(SubXidWriteState));

    subXidWriteState->subXid = currentSubXid;
    subXidWriteState->columnarWriteState = ColumnarBeginWrite(relation->rd_node,
                                                              columnarOptions,
                                                              tupleDesc);

    // 顶掉之前的 让之前的成为next
    subXidWriteState->next = writeStateMapEntry->subXidWriteState;
    writeStateMapEntry->subXidWriteState = subXidWriteState;

    MemoryContextSwitchTo(oldContext);//----------------切回

    return subXidWriteState->columnarWriteState;
}

// Flushes pending writes for given relfilenode in the given subtransaction.
void FlushWriteStateForRelfilenode(Oid relfilenode, SubTransactionId currentSubXid) {
    if (WriteStateMap == NULL) {
        return;
    }

    WriteStateMapEntry *entry = hash_search(WriteStateMap, &relfilenode, HASH_FIND, NULL);

    Assert(!entry || !entry->dropped);

    if (entry && entry->subXidWriteState != NULL) {
        SubXidWriteState *stackEntry = entry->subXidWriteState;
        if (stackEntry->subXid == currentSubXid) {
            ColumnarFlushPendingWrites(stackEntry->columnarWriteState);
        }
    }
}


/*
 * Helper function for FlushWriteStateForAllRels and DiscardWriteStateForAllRels.
 * Pops all of write states for current subtransaction, and depending on "commit"
 * either flushes them or discards them. This also takes into account dropped
 * tables, and either propagates the dropped flag to parent subtransaction or
 * rolls back abort.
 */
static void PopWriteStateForAllRels(SubTransactionId currentSubXid,
                                    SubTransactionId parentSubXid,
                                    bool commit) {
    HASH_SEQ_STATUS hashSeqStatus;
    WriteStateMapEntry *writeStateMapEntry;

    if (WriteStateMap == NULL) {
        return;
    }

    hash_seq_init(&hashSeqStatus, WriteStateMap);

    while ((writeStateMapEntry = hash_seq_search(&hashSeqStatus)) != 0) {
        if (writeStateMapEntry->subXidWriteState == NULL) {
            continue;
        }

        // If the table has been dropped in current sub transaction, either commit the drop or roll it back.
        if (writeStateMapEntry->dropped) {
            if (writeStateMapEntry->dropSubXid == currentSubXid) {
                if (commit) {
                    /* elevate drop to the upper subtransaction */
                    writeStateMapEntry->dropSubXid = parentSubXid;
                } else {
                    /* abort the drop */
                    writeStateMapEntry->dropped = false;
                }
            }
        } else { // Otherwise, commit or discard pending writes.
            SubXidWriteState *subXidWriteState = writeStateMapEntry->subXidWriteState;
            if (subXidWriteState->subXid == currentSubXid) {
                if (commit) {
                    ColumnarEndWrite(subXidWriteState->columnarWriteState);
                }

                writeStateMapEntry->subXidWriteState = subXidWriteState->next;
            }
        }
    }
}


// Called when current subtransaction is committed.
void FlushWriteStateForAllRels(SubTransactionId currentSubXid,
                               SubTransactionId parentSubXid) {
    PopWriteStateForAllRels(currentSubXid, parentSubXid, true);
}


/*
 * Called when current subtransaction is aborted.
 */
void
DiscardWriteStateForAllRels(SubTransactionId currentSubXid, SubTransactionId parentSubXid) {
    PopWriteStateForAllRels(currentSubXid, parentSubXid, false);
}


/*
 * Called when the given relfilenode is dropped.
 */
void
MarkRelfilenodeDropped(Oid relfilenode, SubTransactionId currentSubXid) {
    if (WriteStateMap == NULL) {
        return;
    }

    WriteStateMapEntry *entry = hash_search(WriteStateMap, &relfilenode, HASH_FIND,
                                            NULL);
    if (!entry || entry->dropped) {
        return;
    }

    entry->dropped = true;
    entry->dropSubXid = currentSubXid;
}


// Called when the given relfilenode is dropped in non-transactional TRUNCATE.
void NonTransactionDropWriteState(Oid relfilenode) {
    if (WriteStateMap) {
        hash_search(WriteStateMap, &relfilenode, HASH_REMOVE, false);
    }
}

// true if there are any pending writes in upper transactions.
bool PendingWritesInUpperTransactions(Oid relfilenode,
                                      SubTransactionId currentSubXid) {
    if (WriteStateMap == NULL) {
        return false;
    }

    WriteStateMapEntry *entry = hash_search(WriteStateMap, &relfilenode, HASH_FIND, NULL);

    if (entry && entry->subXidWriteState != NULL) {
        SubXidWriteState *stackEntry = entry->subXidWriteState;

        while (stackEntry != NULL) {
            if (stackEntry->subXid != currentSubXid &&
                ContainsPendingWrites(stackEntry->columnarWriteState)) {
                return true;
            }

            stackEntry = stackEntry->next;
        }
    }

    return false;
}


/*
 * GetWriteContextForDebug exposes WriteStateContext for debugging
 * purposes.
 */
extern MemoryContext
GetWriteContextForDebug(void) {
    return WriteStateContext;
}
