/*-------------------------------------------------------------------------
 *
 * columnar_reader.c
 *
 * This file contains function definitions for reading columnar tables. This
 * includes the logic for reading file level metadata, reading row stripes,
 * and skipping unrelated row chunks and columns.
 *
 * Copyright (c) 2016, Citus Data, Inc.
 *
 * $Id$
 *
 *-------------------------------------------------------------------------
 */


#include "postgres.h"

#include "safe_lib.h"

#include "access/nbtree.h"
#include "access/xact.h"
#include "catalog/pg_am.h"
#include "commands/defrem.h"
#include "distributed/listutils.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "optimizer/optimizer.h"
#include "optimizer/clauses.h"
#include "optimizer/restrictinfo.h"
#include "storage/fd.h"
#include "utils/guc.h"
#include "utils/memutils.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"

#include "columnar/columnar.h"
#include "columnar/columnar_storage.h"
#include "columnar/columnar_tableam.h"
#include "columnar/columnar_version_compat.h"

#define UNEXPECTED_STRIPE_READ_ERR_MSG \
    "attempted to read an unexpected stripe while reading columnar " \
    "table %s, stripe with id=" UINT64_FORMAT " is not flushed"

typedef struct ChunkGroupReadState {
    int64 currentRow;
    int64 rowCount;
    int columnCount;
    List *usedColumnNaturalPosList;  /* borrowed reference */
    ChunkData *chunkData;
} ChunkGroupReadState;

typedef struct StripeReadState {
    int columnCount;
    int64 rowCount;
    int64 currentRow;
    TupleDesc tupleDesc;
    Relation relation;
    int chunkGroupIndex;
    int64 chunkGroupsFiltered;
    MemoryContext stripeReadContext;
    StripeBuffers *stripeBuffers;   /* allocated in stripeReadContext */
    List *usedColumnNaturalPosList;      /* borrowed reference */
    ChunkGroupReadState *chunkGroupReadState; /* owned */
} StripeReadState;

struct ColumnarReadState {
    TupleDesc tupleDesc;
    Relation relation;

    StripeMetadata *currentStripeMetadata; // 会不断的rotate
    StripeReadState *stripeReadState; // 是不是null 意味是不是StripeReadInProgress

    // Integer list of attribute numbers (1-indexed) for columns needed by the query.
    List *usedColumnNaturalPosList;

    List *whereClauseList;
    List *whereClauseVars;

    MemoryContext stripeReadContext;
    int64 chunkGroupsFiltered;

    // Memory context guaranteed to be not freed during scan so we can
    // safely use for any memory allocations regarding ColumnarReadState itself.
    MemoryContext scanContext;

    Snapshot snapshot;
    bool snapshotRegisteredByUs;
};

/* static function declarations */
static MemoryContext CreateStripeReadMemoryContext(void);

static bool ColumnarReadIsCurrentStripe(ColumnarReadState *readState, uint64 rowNumber);

static StripeMetadata *ColumnarReadGetCurrentStripe(ColumnarReadState *readState);

static void ReadStripeRowByRowNumber(ColumnarReadState *readState,
                                     uint64 rowNumber, Datum *columnValues,
                                     bool *columnNulls);

static bool StripeReadIsCurrentChunkGroup(StripeReadState *stripeReadState,
                                          int chunkGroupIndex);

static void ReadChunkGroupRowByRowOffset(ChunkGroupReadState *chunkGroupReadState,
                                         StripeMetadata *stripeMetadata,
                                         uint64 stripeRowOffset, Datum *columnValues,
                                         bool *columnNulls);

static bool StripeReadInProgress(ColumnarReadState *columnarReadState);

static bool HasUnreadStripe(ColumnarReadState *readState);

static StripeReadState *BeginStripeRead(StripeMetadata *stripeMetadata, Relation relation,
                                        TupleDesc tupleDesc, List *usedColumnNaturalPosList,
                                        List *whereClauseList, List *whereClauseVars,
                                        MemoryContext stripeReadContext,
                                        Snapshot snapshot);

static void AdvanceStripeRead(ColumnarReadState *columnarReadState);

static bool SnapshotMightSeeUnflushedStripes(Snapshot snapshot);

static bool ReadStripeNextRow(StripeReadState *stripeReadState, Datum *columnValues,
                              bool *columnNulls);

static ChunkGroupReadState *BeginChunkGroupRead(StripeBuffers *stripeBuffers, int
chunkGroupIndex,
                                                TupleDesc tupleDesc,
                                                List *usedColumnNaturalPosList,
                                                MemoryContext cxt);

static void EndChunkGroupRead(ChunkGroupReadState *chunkGroupReadState);

static bool ReadChunkGroupNextRow(ChunkGroupReadState *chunkGroupReadState,
                                  Datum *columnValues,
                                  bool *columnNulls);

static StripeBuffers *LoadFilteredStripeBuffers(Relation relation,
                                                StripeMetadata *stripeMetadata,
                                                TupleDesc tupleDesc,
                                                List *usedColumnNaturalPosList,
                                                List *whereClauseList,
                                                List *whereClauseVars,
                                                int64 *chunkGroupsFiltered,
                                                Snapshot snapshot);

static ColumnBuffers *LoadColumnBuffers(Relation relation,
                                        ColumnChunkSkipNode *columnChunkSkipNodeArrUnderColumn,
                                        uint32 stripeChunkGroupCount, uint64 stripeFileOffset,
                                        Form_pg_attribute attributeForm);

static bool *SelectedChunkGroupMask(StripeSkipList *stripeSkipList,
                                    List *whereClauseList, List *whereClauseVars,
                                    int64 *chunkGroupsFiltered);

static Node *BuildBaseConstraint(Var *variable);

static List *GetClauseVars(List *clauses, int natts);

static OpExpr *MakeOpExpression(Var *variable, int16 strategyNumber);

static Oid GetOperatorByType(Oid typeId, Oid accessMethodId, int16 strategyNumber);

static void UpdateConstraint(Node *baseConstraint, Datum minValue, Datum maxValue);

static StripeSkipList *SelectedStripeSkipList(StripeSkipList *stripeSkipListOrigin,
                                              const bool *usedColumnMask,
                                              const bool *selectedChunkGroupMask);

static uint32 StripeSkipListRowCount(StripeSkipList *stripeSkipList);

static bool *ProjectedColumnMask(uint32 columnCount, List *usedColumnNaturalPosList);

static void DeserializeExistArray(StringInfo boolArrayBuffer, bool *boolArray,
                                  uint32 boolArrayLength);

static void DeserializeValueArray(StringInfo decompressedSourceValueBuffer, const bool *existsArray,
                                  uint32 chunkGroupRowCount, bool datumTypeByValue,
                                  int datumTypeLength, char datumTypeAlign,
                                  Datum *dest);

static ChunkData *DeserializeChunkData(StripeBuffers *stripeBuffers, uint64 chunkGroupIndex,
                                       uint32 chunkGroupRowCount, TupleDesc tupleDesc,
                                       List *usedColumnNaturalPosList);

static Datum ColumnDefaultValue(TupleConstr *tupleConstraints,
                                Form_pg_attribute attributeForm);

/*
 * ColumnarBeginRead initializes a columnar read operation. This function returns a
 * read handle that's used during reading rows and finishing the read operation.
 *
 * usedColNaturalPosList is an integer list of attribute numbers (1-indexed).
 */
ColumnarReadState *ColumnarBeginRead(Relation relation,
                                     TupleDesc tupleDesc,
                                     List *usedColNaturalPosList,
                                     List *whereClauseList,
                                     MemoryContext scanContext,
                                     Snapshot snapshot,
                                     bool randomAccess) {
    /*
     * We allocate all stripe specific data in the stripeReadContext, and reset
     * this memory context before loading a new stripe. This is to avoid memory leaks.
     */
    //  MemoryContext stripeReadContext = CreateStripeReadMemoryContext();

    ColumnarReadState *columnarReadState = palloc0(sizeof(ColumnarReadState));

    columnarReadState->relation = relation;
    columnarReadState->usedColumnNaturalPosList = usedColNaturalPosList;
    columnarReadState->whereClauseList = whereClauseList;
    columnarReadState->whereClauseVars = GetClauseVars(whereClauseList, tupleDesc->natts);
    columnarReadState->chunkGroupsFiltered = 0;
    columnarReadState->tupleDesc = tupleDesc;
    columnarReadState->stripeReadContext = CreateStripeReadMemoryContext();;
    columnarReadState->stripeReadState = NULL;
    columnarReadState->scanContext = scanContext;
    // Note that ColumnarReadFlushPendingWrites might update those two by registering a new snapshot.
    columnarReadState->snapshot = snapshot;
    columnarReadState->snapshotRegisteredByUs = false;

    if (!randomAccess) {
        /*
         * When doing random access (i.e.: index scan),don't need to flush pending writes until we need to read them.
         * columnar_index_fetch_tuple would do so when needed.
         */
        ColumnarReadFlushPendingWrites(columnarReadState);

        /*
         * AdvanceStripeRead sets currentStripeMetadata for the first stripe
         * to read if not doing random access. Otherwise, reader (i.e.:
         * ColumnarReadRowByRowNumber) would already decide the stripe to read
         * on-the-fly.
         *
         * Moreover, Since we don't flush pending writes for random access,
         * AdvanceStripeRead might encounter with stripe metadata entries due
         * to current transaction's pending writes even when using an MVCC
         * snapshot, but AdvanceStripeRead would throw an error for that.
         * Note that this is not the case with for plain table scan methods
         * (i.e.: SeqScan and Columnar CustomScan).
         *
         * For those reasons, we don't call AdvanceStripeRead if we will do random access.
         */
        AdvanceStripeRead(columnarReadState);
    }

    return columnarReadState;
}

/*
 * ColumnarReadFlushPendingWrites flushes pending writes for read operation
 * and sets a new (registered) snapshot if necessary.
 *
 * If it sets a new snapshot, then sets snapshotRegisteredByUs to true to
 * indicate that caller should unregister the snapshot after finishing read
 * operation.
 *
 * Note that this function assumes that columnarReadState's relation and snapshot
 * fields are already set.
 */
void ColumnarReadFlushPendingWrites(ColumnarReadState *columnarReadState) {
    Assert(!columnarReadState->snapshotRegisteredByUs);

    FlushWriteStateForRelfilenode(columnarReadState->relation->rd_node.relNode,
                                  GetCurrentSubTransactionId());

    if (columnarReadState->snapshot == InvalidSnapshot || !IsMVCCSnapshot(columnarReadState->snapshot)) {
        return;
    }

    /*
     * If we flushed any pending writes, then we should guarantee that
     * those writes are visible to us too. For this reason, if given
     * snapshot is an MVCC snapshot, then we set its curcid to current
     * command id.
     *
     * For simplicity, we do that even if we didn't flush any writes
     * since we don't see any problem with that.
     *
     * XXX: We should either not update cid if we are executing a FETCH
     * (from cursor) command, or we should have a better way to deal with
     * pending writes, see the discussion in
     * https://github.com/citusdata/citus/issues/5231.
     */
    PushCopiedSnapshot(columnarReadState->snapshot);

    /* now our snapshot is the active one */
    UpdateActiveSnapshotCommandId();
    Snapshot newSnapshot = GetActiveSnapshot();
    RegisterSnapshot(newSnapshot);

    /*
     * To be able to use UpdateActiveSnapshotCommandId, we pushed the
     * copied snapshot to the stack. However, we don't need to keep it
     * there since we will anyway rely on ColumnarReadState->snapshot
     * during read operation.
     *
     * Note that since we registered the snapshot already, we guarantee
     * that PopActiveSnapshot won't free it.
     */
    PopActiveSnapshot();

    columnarReadState->snapshot = newSnapshot;

    /* not forget to unregister it when finishing read operation */
    columnarReadState->snapshotRegisteredByUs = true;
}


// create a memory context to be used when reading a stripe.
static MemoryContext CreateStripeReadMemoryContext() {
    return AllocSetContextCreate(CurrentMemoryContext, "Stripe Read Memory Context", ALLOCSET_DEFAULT_SIZES);
}

/*
 * ColumnarReadNextRow tries to read a row from the columnar table. On success, it sets
 * column values, column nulls and rowNumber (if passed to be non-NULL), and returns true.
 * If there are no more rows to read, the function returns false.
 */
bool ColumnarReadNextRow(ColumnarReadState *columnarReadState,
                         Datum *columnValues,
                         bool *columnNulls,
                         uint64 *rowNumber) {
    while (true) {
        if (!StripeReadInProgress(columnarReadState)) { // columnarReadState->stripeReadState != NULL
            if (!HasUnreadStripe(columnarReadState)) {
                return false;
            }

            columnarReadState->stripeReadState = BeginStripeRead(columnarReadState->currentStripeMetadata,
                                                                 columnarReadState->relation,
                                                                 columnarReadState->tupleDesc,
                                                                 columnarReadState->usedColumnNaturalPosList,
                                                                 columnarReadState->whereClauseList,
                                                                 columnarReadState->whereClauseVars,
                                                                 columnarReadState->stripeReadContext,
                                                                 columnarReadState->snapshot);
        }

        // 说明stripe用光
        if (!ReadStripeNextRow(columnarReadState->stripeReadState, columnValues, columnNulls)) {
            AdvanceStripeRead(columnarReadState);
            continue;
        }

        if (rowNumber) {
            *rowNumber = columnarReadState->currentStripeMetadata->firstRowNumber +
                         columnarReadState->stripeReadState->currentRow - 1;
        }

        return true;
    }

    return false;
}


/*
 * ColumnarReadRowByRowNumberOrError is a wrapper around
 * ColumnarReadRowByRowNumber that throws an error if tuple
 * with rowNumber does not exist.
 */
void
ColumnarReadRowByRowNumberOrError(ColumnarReadState *readState,
                                  uint64 rowNumber, Datum *columnValues,
                                  bool *columnNulls) {
    if (!ColumnarReadRowByRowNumber(readState, rowNumber,
                                    columnValues, columnNulls)) {
        ereport(ERROR, (errmsg("cannot read from columnar table %s, tuple with "
                               "row number " UINT64_FORMAT " does not exist",
                RelationGetRelationName(readState->relation),
                rowNumber)));
    }
}


/*
 * ColumnarReadRowByRowNumber reads row with rowNumber from given relation
 * into columnValues and columnNulls, and returns true. If no such row
 * exists, then returns false.
 */
bool
ColumnarReadRowByRowNumber(ColumnarReadState *readState,
                           uint64 rowNumber, Datum *columnValues,
                           bool *columnNulls) {
    if (!ColumnarReadIsCurrentStripe(readState, rowNumber)) {
        Relation columnarRelation = readState->relation;
        Snapshot snapshot = readState->snapshot;
        StripeMetadata *stripeMetadata = FindStripeByRowNumber(columnarRelation,
                                                               rowNumber, snapshot);
        if (stripeMetadata == NULL) {
            /* no such row exists */
            return false;
        }

        if (StripeWriteState(stripeMetadata) != STRIPE_WRITE_FLUSHED) {
            /*
             * Callers are expected to skip stripes that are not flushed to
             * disk yet or should wait for the writer xact to commit or abort,
             * but let's be on the safe side.
             */
            ereport(ERROR, (errmsg(UNEXPECTED_STRIPE_READ_ERR_MSG,
                                   RelationGetRelationName(columnarRelation),
                                   stripeMetadata->id)));
        }

        /* do the cleanup before reading a new stripe */
        ColumnarResetRead(readState);

        TupleDesc relationTupleDesc = RelationGetDescr(columnarRelation);
        List *whereClauseList = NIL;
        List *whereClauseVars = NIL;
        MemoryContext stripeReadContext = readState->stripeReadContext;
        readState->stripeReadState = BeginStripeRead(stripeMetadata,
                                                     columnarRelation,
                                                     relationTupleDesc,
                                                     readState->usedColumnNaturalPosList,
                                                     whereClauseList,
                                                     whereClauseVars,
                                                     stripeReadContext,
                                                     snapshot);

        readState->currentStripeMetadata = stripeMetadata;
    }

    ReadStripeRowByRowNumber(readState, rowNumber, columnValues, columnNulls);

    return true;
}


/*
 * ColumnarReadIsCurrentStripe returns true if stripe being read contains
 * row with given rowNumber.
 */
static bool
ColumnarReadIsCurrentStripe(ColumnarReadState *readState, uint64 rowNumber) {
    if (!StripeReadInProgress(readState)) {
        return false;
    }

    StripeMetadata *currentStripeMetadata = readState->currentStripeMetadata;
    if (rowNumber >= currentStripeMetadata->firstRowNumber &&
        rowNumber <= StripeGetHighestRowNumber(currentStripeMetadata)) {
        return true;
    }

    return false;
}


/*
 * ColumnarReadGetCurrentStripe returns StripeMetadata for the stripe that is
 * being read.
 */
static StripeMetadata *
ColumnarReadGetCurrentStripe(ColumnarReadState *readState) {
    return readState->currentStripeMetadata;
}


/*
 * ReadStripeRowByRowNumber reads row with rowNumber from given
 * stripeReadState into columnValues and columnNulls.
 * Errors out if no such row exists in the stripe being read.
 */
static void
ReadStripeRowByRowNumber(ColumnarReadState *readState,
                         uint64 rowNumber, Datum *columnValues,
                         bool *columnNulls) {
    StripeMetadata *stripeMetadata = ColumnarReadGetCurrentStripe(readState);
    StripeReadState *stripeReadState = readState->stripeReadState;

    if (rowNumber < stripeMetadata->firstRowNumber) {
        /* not expected but be on the safe side */
        ereport(ERROR, (errmsg("row offset cannot be negative")));
    }

    /* find the exact chunk group to be read */
    uint64 stripeRowOffset = rowNumber - stripeMetadata->firstRowNumber;
    int chunkGroupIndex = stripeRowOffset / stripeMetadata->chunkGroupRowCount;
    if (!StripeReadIsCurrentChunkGroup(stripeReadState, chunkGroupIndex)) {
        if (stripeReadState->chunkGroupReadState) {
            EndChunkGroupRead(stripeReadState->chunkGroupReadState);
        }

        stripeReadState->chunkGroupIndex = chunkGroupIndex;
        stripeReadState->chunkGroupReadState = BeginChunkGroupRead(
                stripeReadState->stripeBuffers,
                stripeReadState->chunkGroupIndex,
                stripeReadState->tupleDesc,
                stripeReadState->usedColumnNaturalPosList,
                stripeReadState->stripeReadContext);
    }

    ReadChunkGroupRowByRowOffset(stripeReadState->chunkGroupReadState,
                                 stripeMetadata, stripeRowOffset,
                                 columnValues, columnNulls);
}


/*
 * StripeReadIsCurrentChunkGroup returns true if chunk group being read is
 * the has given chunkGroupIndex in its stripe.
 */
static bool
StripeReadIsCurrentChunkGroup(StripeReadState *stripeReadState, int chunkGroupIndex) {
    if (!stripeReadState->chunkGroupReadState) {
        return false;
    }

    return (stripeReadState->chunkGroupIndex == chunkGroupIndex);
}


/*
 * ReadChunkGroupRowByRowOffset reads row with stripeRowOffset from given
 * chunkGroupReadState into columnValues and columnNulls.
 * Errors out if no such row exists in the chunk group being read.
 */
static void
ReadChunkGroupRowByRowOffset(ChunkGroupReadState *chunkGroupReadState,
                             StripeMetadata *stripeMetadata,
                             uint64 stripeRowOffset, Datum *columnValues,
                             bool *columnNulls) {
    /* set the exact row number to be read from given chunk roup */
    chunkGroupReadState->currentRow = stripeRowOffset %
                                      stripeMetadata->chunkGroupRowCount;
    if (!ReadChunkGroupNextRow(chunkGroupReadState, columnValues, columnNulls)) {
        /* not expected but be on the safe side */
        ereport(ERROR, (errmsg("could not find the row in stripe")));
    }
}


// columnarReadState->stripeReadState != NULL;
// true if we already started reading a stripe.
static bool StripeReadInProgress(ColumnarReadState *columnarReadState) {
    return columnarReadState->stripeReadState != NULL;
}


/*
 * returns true if we still have stripes to read during current read operation.
 */
static bool HasUnreadStripe(ColumnarReadState *readState) {
    return readState->currentStripeMetadata != NULL;
}


/*
 * ColumnarRescan clears the position where we were scanning so that the next read starts at
 * the beginning again
 */
void
ColumnarRescan(ColumnarReadState *readState, List *scanQual) {
    MemoryContext oldContext = MemoryContextSwitchTo(readState->scanContext);

    ColumnarResetRead(readState);

    /* set currentStripeMetadata for the first stripe to read */
    AdvanceStripeRead(readState);

    readState->chunkGroupsFiltered = 0;

    readState->whereClauseList = copyObject(scanQual);
    MemoryContextSwitchTo(oldContext);
}


/*
 * Finishes a columnar read operation.
 */
void
ColumnarEndRead(ColumnarReadState *readState) {
    if (readState->snapshotRegisteredByUs) {
        /*
         * init_columnar_read_state created a new snapshot and registered it,
         * so now forget it.
         */
        UnregisterSnapshot(readState->snapshot);
    }

    MemoryContextDelete(readState->stripeReadContext);
    if (readState->currentStripeMetadata) {
        pfree(readState->currentStripeMetadata);
    }

    pfree(readState);
}


/*
 * ColumnarResetRead resets the stripe and the chunk group that is
 * being read currently (if any).
 */
void
ColumnarResetRead(ColumnarReadState *readState) {
    if (StripeReadInProgress(readState)) {
        pfree(readState->currentStripeMetadata);
        readState->currentStripeMetadata = NULL;

        readState->stripeReadState = NULL;
        MemoryContextReset(readState->stripeReadContext);
    }
}

// allocate state for reading a stripe.
static StripeReadState *BeginStripeRead(StripeMetadata *stripeMetadata,
                                        Relation relation,
                                        TupleDesc tupleDesc,
                                        List *usedColumnNaturalPosList,
                                        List *whereClauseList,
                                        List *whereClauseVars,
                                        MemoryContext stripeReadContext,
                                        Snapshot snapshot) {
    MemoryContext oldContext = MemoryContextSwitchTo(stripeReadContext);

    StripeReadState *stripeReadState = palloc0(sizeof(StripeReadState));

    stripeReadState->relation = relation;
    stripeReadState->tupleDesc = tupleDesc;
    stripeReadState->columnCount = tupleDesc->natts;
    stripeReadState->chunkGroupReadState = NULL;
    stripeReadState->usedColumnNaturalPosList = usedColumnNaturalPosList;
    stripeReadState->stripeReadContext = stripeReadContext;

    stripeReadState->stripeBuffers = LoadFilteredStripeBuffers(relation,
                                                               stripeMetadata,
                                                               tupleDesc,
                                                               usedColumnNaturalPosList,
                                                               whereClauseList,
                                                               whereClauseVars,
                                                               &stripeReadState->chunkGroupsFiltered,
                                                               snapshot);

    stripeReadState->rowCount = stripeReadState->stripeBuffers->rowCount;

    MemoryContextSwitchTo(oldContext);


    return stripeReadState;
}

// update chunkGroupsFiltered and set current StripeMetadata for next stripe read.
static void AdvanceStripeRead(ColumnarReadState *columnarReadState) {
    MemoryContext oldContext = MemoryContextSwitchTo(columnarReadState->scanContext);

    // if not read any stripes yet, start from the first one
    uint64 lastReadRowNumber = COLUMNAR_INVALID_ROW_NUMBER;

    if (StripeReadInProgress(columnarReadState)) {
        /* .. otherwise, continue with the next stripe */
        lastReadRowNumber = StripeGetHighestRowNumber(columnarReadState->currentStripeMetadata);

        columnarReadState->chunkGroupsFiltered += columnarReadState->stripeReadState->chunkGroupsFiltered;
    }

    columnarReadState->currentStripeMetadata = FindNextStripeByRowNumber(columnarReadState->relation,
                                                                         lastReadRowNumber,
                                                                         columnarReadState->snapshot);

    if (columnarReadState->currentStripeMetadata &&
        StripeWriteState(columnarReadState->currentStripeMetadata) != STRIPE_WRITE_FLUSHED &&
        !SnapshotMightSeeUnflushedStripes(columnarReadState->snapshot)) {
        /*
         * To be on the safe side, error out if we don't expect to encounter
         * with an un-flushed stripe. Otherwise, we will skip such stripes until finding a flushed one.
         */
        ereport(ERROR, (errmsg(UNEXPECTED_STRIPE_READ_ERR_MSG,
                               RelationGetRelationName(columnarReadState->relation),
                               columnarReadState->currentStripeMetadata->id)));
    }

    while (columnarReadState->currentStripeMetadata &&
           StripeWriteState(columnarReadState->currentStripeMetadata) != STRIPE_WRITE_FLUSHED) {
        columnarReadState->currentStripeMetadata = FindNextStripeByRowNumber(columnarReadState->relation,
                                                                             columnarReadState->currentStripeMetadata->firstRowNumber,
                                                                             columnarReadState->snapshot);
    }

    columnarReadState->stripeReadState = NULL; // 对应函数 StripeReadInProgress
    MemoryContextReset(columnarReadState->stripeReadContext);

    MemoryContextSwitchTo(oldContext);
}

/*
 * SnapshotMightSeeUnflushedStripes returns true if given snapshot is
 * expected to see un-flushed stripes either because of other backends'
 * pending writes or aborted transactions.
 */
static bool SnapshotMightSeeUnflushedStripes(Snapshot snapshot) {
    if (snapshot == InvalidSnapshot) {
        return false;
    }

    switch (snapshot->snapshot_type) {
        case SNAPSHOT_ANY:
        case SNAPSHOT_DIRTY:
        case SNAPSHOT_NON_VACUUMABLE: {
            return true;
        }

        default:
            return false;
    }
}

/*
 * ReadStripeNextRow: If more rows can be read from the current stripe, fill
 * in non-NULL columnValues and return true. Otherwise, return false.
 *
 * On entry, all entries in columnNulls should be true; this function only
 * sets non-NULL entries.
 */
static bool ReadStripeNextRow(StripeReadState *stripeReadState,
                              Datum *columnValues,
                              bool *columnNulls) {
    if (stripeReadState->currentRow >= stripeReadState->rowCount) {
        Assert(stripeReadState->currentRow == stripeReadState->rowCount);
        return false;
    }

    while (true) {
        if (stripeReadState->chunkGroupReadState == NULL) {
            stripeReadState->chunkGroupReadState = BeginChunkGroupRead(stripeReadState->stripeBuffers,
                                                                       stripeReadState->chunkGroupIndex,
                                                                       stripeReadState->tupleDesc,
                                                                       stripeReadState->usedColumnNaturalPosList,
                                                                       stripeReadState->stripeReadContext);
        }

        // 说明chunkGroup用光
        if (!ReadChunkGroupNextRow(stripeReadState->chunkGroupReadState,
                                   columnValues,
                                   columnNulls)) {
            /* if this chunk group is exhausted, fetch the next one and loop */
            EndChunkGroupRead(stripeReadState->chunkGroupReadState);
            stripeReadState->chunkGroupReadState = NULL;
            stripeReadState->chunkGroupIndex++;
            continue;
        }

        stripeReadState->currentRow++;
        return true;
    }

    Assert(stripeReadState->currentRow == stripeReadState->rowCount);
    return false;
}

// BeginChunkGroupRead allocate state for reading a chunk.
static ChunkGroupReadState *BeginChunkGroupRead(StripeBuffers *stripeBuffers,
                                                int chunkGroupIndex,
                                                TupleDesc tupleDesc,
                                                List *usedColumnNaturalPosList,
                                                MemoryContext cxt) {
    uint32 chunkGroupRowCount = stripeBuffers->selectedChunkGroupRowCounts[chunkGroupIndex];

    MemoryContext oldContext = MemoryContextSwitchTo(cxt);

    ChunkGroupReadState *chunkGroupReadState = palloc0(sizeof(ChunkGroupReadState));

    chunkGroupReadState->currentRow = 0;
    chunkGroupReadState->rowCount = chunkGroupRowCount;
    chunkGroupReadState->columnCount = tupleDesc->natts;
    chunkGroupReadState->usedColumnNaturalPosList = usedColumnNaturalPosList;

    chunkGroupReadState->chunkData = DeserializeChunkData(stripeBuffers,
                                                          chunkGroupIndex,
                                                          chunkGroupRowCount,
                                                          tupleDesc,
                                                          usedColumnNaturalPosList);
    MemoryContextSwitchTo(oldContext);

    return chunkGroupReadState;
}


/*
 * EndChunkRead finishes a chunk read.
 */
static void
EndChunkGroupRead(ChunkGroupReadState *chunkGroupReadState) {
    FreeChunkData(chunkGroupReadState->chunkData);
    pfree(chunkGroupReadState);
}


/*
 * if more rows can be read from the current chunk group, fill in non-NULL columnValues and return true.
 * Otherwise, return false.
 *
 * On entry, all entries in columnNulls should be true; this function only
 * sets non-NULL entries.
 */
static bool ReadChunkGroupNextRow(ChunkGroupReadState *chunkGroupReadState,
                                  Datum *columnValues,
                                  bool *columnNulls) {
    if (chunkGroupReadState->currentRow >= chunkGroupReadState->rowCount) {
        Assert(chunkGroupReadState->currentRow == chunkGroupReadState->rowCount);
        return false;
    }

    // initialize to all-NULL. Only non-NULL projected attributes will be set.
    memset(columnNulls, true, sizeof(bool) * chunkGroupReadState->columnCount);

    int usedColumnNaturalPos;
    foreach_int(usedColumnNaturalPos, chunkGroupReadState->usedColumnNaturalPosList) {
        const uint32 columnIndex = usedColumnNaturalPos - 1;

        const ChunkData *chunkData = chunkGroupReadState->chunkData;
        const int rowIndex = chunkGroupReadState->currentRow;

        if (chunkData->existsArray[columnIndex][rowIndex]) {
            columnValues[columnIndex] = chunkData->valueArray[columnIndex][rowIndex];
            columnNulls[columnIndex] = false;
        }
    }

    chunkGroupReadState->currentRow++;
    return true;
}

/*
 * ColumnarReadChunkGroupsFiltered
 *
 * Return the number of chunk groups filtered during this read operation.
 */
int64
ColumnarReadChunkGroupsFiltered(ColumnarReadState *state) {
    return state->chunkGroupsFiltered;
}


/*
 * CreateEmptyChunkDataArray creates data buffers to keep deserialized exist and
 * value arrays for requested columns in columnMask.
 */
ChunkData *CreateEmptyChunkData(uint32 columnCount,
                                const bool *columnMask,// 数量columnCount相同
                                uint32 chunkRowCount) {
    ChunkData *chunkData = palloc0(sizeof(ChunkData));

    chunkData->columnCount = columnCount;
    chunkData->rowCount = chunkRowCount;

    chunkData->existsArray = palloc0(columnCount * sizeof(bool *));
    chunkData->valueArray = palloc0(columnCount * sizeof(Datum *));
    chunkData->columnDataArr = palloc0(columnCount * sizeof(StringInfo));

    // allocate chunk memory for deserialized data
    // columnCount 乘以 chunkRowLimit
    for (uint32 columnIndex = 0; columnIndex < columnCount; columnIndex++) {
        if (columnMask[columnIndex]) {
            chunkData->existsArray[columnIndex] = palloc0(chunkRowCount * sizeof(bool));
            chunkData->valueArray[columnIndex] = palloc0(chunkRowCount * sizeof(Datum));
            chunkData->columnDataArr[columnIndex] = NULL;
        }
    }

    return chunkData;
}

/*
 * FreeChunkData deallocates data buffers to keep deserialized exist and
 * value arrays for requested columns in columnMask.
 * ColumnChunkData->serializedValueBuffer lives in memory read/write context
 * so it is deallocated automatically when the context is deleted.
 */
void FreeChunkData(ChunkData *chunkData) {
    if (chunkData == NULL) {
        return;
    }

    for (uint32 columnIndex = 0; columnIndex < chunkData->columnCount; columnIndex++) {
        if (chunkData->existsArray[columnIndex] != NULL) {
            pfree(chunkData->existsArray[columnIndex]);
        }

        if (chunkData->valueArray[columnIndex] != NULL) {
            pfree(chunkData->valueArray[columnIndex]);
        }
    }

    pfree(chunkData->existsArray);
    pfree(chunkData->valueArray);
    pfree(chunkData);
}

/* ColumnarTableRowCount returns the exact row count of a table using skiplists */
uint64
ColumnarTableRowCount(Relation relation) {
    ListCell *stripeMetadataCell = NULL;
    uint64 totalRowCount = 0;
    List *stripeList = StripesForRelfilenode(relation->rd_node);

    foreach(stripeMetadataCell, stripeList) {
        StripeMetadata *stripeMetadata = (StripeMetadata *) lfirst(stripeMetadataCell);
        totalRowCount += stripeMetadata->rowCount;
    }

    return totalRowCount;
}

/*
 * LoadFilteredStripeBuffers reads serialized stripe data from the given file.
 * The function skips over chunks whose rows are refuted by restriction qualifiers,
 * and only loads columns that are projected in the query.
 */
static StripeBuffers *LoadFilteredStripeBuffers(Relation relation,
                                                StripeMetadata *stripeMetadata, // columnarReadState->currentStripeMetadata,
                                                TupleDesc tupleDesc,
                                                List *usedColumnNaturalPosList,
                                                List *whereClauseList,
                                                List *whereClauseVars,
                                                int64 *chunkGroupsFiltered, // 该参数是用来输出的
                                                Snapshot snapshot) {
    uint32 columnCount = tupleDesc->natts;

    bool *usedColumnMask = ProjectedColumnMask(columnCount, usedColumnNaturalPosList);

    // 到db中读取stripe 这个是当前的全量
    // select * from columnar.stripe where storage_id = ? and stripe_num = ?
    StripeSkipList *stripeSkipList = ReadStripeSkipList(relation->rd_node,
                                                        stripeMetadata->id,
                                                        tupleDesc,
                                                        stripeMetadata->chunkGroupCount,
                                                        snapshot);
    // stripe麾下还有多个chunk还需要精确定位
    // 大小 chunkGroupCount 要没有where之类的那么数组全是true
    bool *selectedChunkGroupMask = SelectedChunkGroupMask(stripeSkipList,
                                                          whereClauseList,
                                                          whereClauseVars,
                                                          chunkGroupsFiltered);

    // 对之前的 stripeSkipList 通过 selectedChunkGroupMask 进行筛选
    StripeSkipList *selectedStripeSkipList = SelectedStripeSkipList(stripeSkipList,
                                                                    usedColumnMask,
                                                                    selectedChunkGroupMask);

    /* load column data for projected columns */
    ColumnBuffers **columnBuffersArray = palloc0(columnCount * sizeof(ColumnBuffers *)); // 多少column

    for (uint32 columnIndex = 0; columnIndex < stripeMetadata->columnCount; columnIndex++) {
        if (!usedColumnMask[columnIndex]) {
            continue;
        }

        ColumnChunkSkipNode *columnChunkSkipNodeArrUnderColumn = selectedStripeSkipList->columnChunkSkipNodeArray[columnIndex];
        Form_pg_attribute attributeForm = TupleDescAttr(tupleDesc, columnIndex);

        columnBuffersArray[columnIndex] = LoadColumnBuffers(relation,
                                                            columnChunkSkipNodeArrUnderColumn,
                                                            selectedStripeSkipList->chunkGroupCount,
                                                            stripeMetadata->fileOffset,
                                                            attributeForm);
    }

    StripeBuffers *stripeBuffers = palloc0(sizeof(StripeBuffers));
    stripeBuffers->columnCount = columnCount;
    stripeBuffers->rowCount = StripeSkipListRowCount(selectedStripeSkipList);
    stripeBuffers->columnBuffersArray = columnBuffersArray;
    stripeBuffers->selectedChunkGroupRowCounts = selectedStripeSkipList->chunkGroupRowCountArr;

    return stripeBuffers;
}

/*
 * LoadColumnBuffers reads serialized column data from the given file. These
 * column data are laid out as sequential chunks in the file; and chunk positions
 * and lengths are retrieved from the column chunk skip node array.
 */
static ColumnBuffers *LoadColumnBuffers(Relation relation,
                                        ColumnChunkSkipNode *columnChunkSkipNodeArrUnderColumn,
                                        uint32 stripeChunkGroupCount,
                                        uint64 stripeFileOffset,
                                        Form_pg_attribute attributeForm) {
    ColumnChunkBuffers **columnChunkBuffersArray = palloc0(stripeChunkGroupCount * sizeof(ColumnChunkBuffers *));

    for (uint32 stripeChunkGroupIndex = 0; stripeChunkGroupIndex < stripeChunkGroupCount; stripeChunkGroupIndex++) {
        columnChunkBuffersArray[stripeChunkGroupIndex] = palloc0(sizeof(ColumnChunkBuffers));
    }

    // exist
    for (uint32 stripeChunkGroupIndex = 0; stripeChunkGroupIndex < stripeChunkGroupCount; stripeChunkGroupIndex++) {
        ColumnChunkSkipNode *columnChunkSkipNode = &columnChunkSkipNodeArrUnderColumn[stripeChunkGroupIndex];

        uint64 existsOffset = stripeFileOffset + columnChunkSkipNode->existsChunkOffset;

        StringInfo rawExistsBuffer = makeStringInfo();
        enlargeStringInfo(rawExistsBuffer, columnChunkSkipNode->existsLength);
        rawExistsBuffer->len = columnChunkSkipNode->existsLength;

        ColumnarStorageRead(relation,
                            existsOffset,
                            rawExistsBuffer->data,
                            columnChunkSkipNode->existsLength);

        columnChunkBuffersArray[stripeChunkGroupIndex]->existsBuffer = rawExistsBuffer;
    }

    // values
    for (uint32 chunkIndex = 0; chunkIndex < stripeChunkGroupCount; chunkIndex++) {
        ColumnChunkSkipNode *chunkSkipNode = &columnChunkSkipNodeArrUnderColumn[chunkIndex];
        CompressionType compressionType = chunkSkipNode->valueCompressionType;
        uint64 valueOffset = stripeFileOffset + chunkSkipNode->valueChunkOffset;
        StringInfo rawValueBuffer = makeStringInfo();

        enlargeStringInfo(rawValueBuffer, chunkSkipNode->valueLength);
        rawValueBuffer->len = chunkSkipNode->valueLength;
        ColumnarStorageRead(relation,
                            valueOffset,
                            rawValueBuffer->data,
                            chunkSkipNode->valueLength);

        columnChunkBuffersArray[chunkIndex]->valueBuffer = rawValueBuffer;
        columnChunkBuffersArray[chunkIndex]->valueCompressionType = compressionType;
        columnChunkBuffersArray[chunkIndex]->decompressedValueSize = chunkSkipNode->decompressedValueSize;
    }

    ColumnBuffers *columnBuffers = palloc0(sizeof(ColumnBuffers));
    columnBuffers->columnChunkBuffersArray = columnChunkBuffersArray;

    return columnBuffers;
}


/*
 * SelectedChunkGroupMask walks over each column's chunks and checks if a chunk can
 * be filtered without reading its data. The filtering happens when all rows in
 * the chunk can be refuted by the given qualifier conditions.
 */
static bool *SelectedChunkGroupMask(StripeSkipList *stripeSkipList,
                                    List *whereClauseList,
                                    List *whereClauseVars,
                                    int64 *chunkGroupsFiltered) {
    ListCell *listCell = NULL;

    bool *selectedChunkMask = palloc0(stripeSkipList->chunkGroupCount * sizeof(bool));
    memset(selectedChunkMask, true, stripeSkipList->chunkGroupCount * sizeof(bool));

    foreach(listCell, whereClauseVars) {
        Var *whereColumn = lfirst(listCell);
        uint32 whereColumnIndex = whereColumn->varattno - 1;

        /* if this whereColumn's data type doesn't have a comparator, skip it */
        FmgrInfo *comparisonFunction = GetCompareFunc(whereColumn->vartype,
                                                      BTREE_AM_OID,
                                                      BTORDER_PROC);
        if (comparisonFunction == NULL) {
            continue;
        }

        Node *baseConstraint = BuildBaseConstraint(whereColumn);

        for (uint32 chunkGroupIndex = 0; chunkGroupIndex < stripeSkipList->chunkGroupCount; chunkGroupIndex++) {
            ColumnChunkSkipNode *chunkSkipNodeArray = stripeSkipList->columnChunkSkipNodeArray[whereColumnIndex];
            ColumnChunkSkipNode *chunkSkipNode = &chunkSkipNodeArray[chunkGroupIndex];

            // A whereColumn chunk with comparable data type can miss min/max values if all values in the chunk are NULL
            if (!chunkSkipNode->hasMinMax) {
                continue;
            }

            UpdateConstraint(baseConstraint,
                             chunkSkipNode->minimumValue,
                             chunkSkipNode->maximumValue);

            List *constraintList = list_make1(baseConstraint);
            bool predicateRefuted = predicate_refuted_by(constraintList,
                                                         whereClauseList,
                                                         false);

            if (predicateRefuted && selectedChunkMask[chunkGroupIndex]) {
                selectedChunkMask[chunkGroupIndex] = false;
                *chunkGroupsFiltered += 1;
            }
        }
    }

    return selectedChunkMask;
}


/*
 * GetCompareFunc first resolves the operator for the given data type,
 * access method, and support procedure. The function then uses the resolved
 * operator's identifier to fill in a function manager object, and returns
 * this object. This function is based on a similar function from CitusDB's code.
 */
FmgrInfo *GetCompareFunc(Oid typeId,
                         Oid accessMethodId,
                         int16 procedureId) {
    FmgrInfo *functionInfo = NULL;

    // get default operator class from pg_opclass for datum type
    // opclass 在创建index时候也会用到
    Oid operatorClassId = GetDefaultOpClass(typeId, accessMethodId);
    if (operatorClassId == InvalidOid) {
        return NULL;
    }

    Oid operatorFamilyId = get_opclass_family(operatorClassId);
    if (operatorFamilyId == InvalidOid) {
        return NULL;
    }

    Oid operatorId = get_opfamily_proc(operatorFamilyId, typeId, typeId, procedureId);
    if (operatorId != InvalidOid) {
        functionInfo = (FmgrInfo *) palloc0(sizeof(FmgrInfo));

        /* fill in the FmgrInfo struct using the operatorId */
        fmgr_info(operatorId, functionInfo);
    }

    return functionInfo;
}


/*
 * BuildBaseConstraint builds and returns a base constraint. This constraint
 * implements an expression in the form of (var <= max && var >= min), where
 * min and max values represent a chunk's min and max values. These chunk
 * values are filled in after the constraint is built. This function is based
 * on a similar function from CitusDB's shard pruning logic.
 */
static Node *
BuildBaseConstraint(Var *variable) {
    OpExpr *lessThanExpr = MakeOpExpression(variable, BTLessEqualStrategyNumber);
    OpExpr *greaterThanExpr = MakeOpExpression(variable, BTGreaterEqualStrategyNumber);

    Node *baseConstraint = make_and_qual((Node *) lessThanExpr, (Node *) greaterThanExpr);

    return baseConstraint;
}


/*
 * GetClauseVars extracts the Vars from the given clauses for the purpose of
 * building constraints that can be refuted by predicate_refuted_by(). It also
 * deduplicates and sorts them.
 */
static List *
GetClauseVars(List *whereClauseList, int natts) {
    /*
     * We don't recurse into or include aggregates, window functions, or
     * PHVs. We don't expect any PHVs during execution; and Vars found inside
     * an aggregate or window function aren't going to be useful in forming
     * constraints that can be refuted.
     */
    int flags = 0;
    List *vars = pull_var_clause((Node *) whereClauseList, flags);
    Var **deduplicate = palloc0(sizeof(Var *) * natts);

    ListCell *lc;
    foreach(lc, vars) {
        Node *node = lfirst(lc);
        Assert(IsA(node, Var));

        Var *var = (Var *) node;
        int idx = var->varattno - 1;

        if (deduplicate[idx] != NULL) {
            /* if they have the same varattno, the rest should be identical */
            Assert(equal(var, deduplicate[idx]));
        }

        deduplicate[idx] = var;
    }

    List *whereClauseVars = NIL;
    for (int i = 0; i < natts; i++) {
        Var *var = deduplicate[i];
        if (var != NULL) {
            whereClauseVars = lappend(whereClauseVars, var);
        }
    }

    pfree(deduplicate);

    return whereClauseVars;
}


/*
 * MakeOpExpression builds an operator expression node. This operator expression
 * implements the operator clause as defined by the variable and the strategy
 * number. The function is copied from CitusDB's shard pruning logic.
 */
static OpExpr *
MakeOpExpression(Var *variable, int16 strategyNumber) {
    Oid typeId = variable->vartype;
    Oid typeModId = variable->vartypmod;
    Oid collationId = variable->varcollid;

    Oid accessMethodId = BTREE_AM_OID;

    /* Load the operator from system catalogs */
    Oid operatorId = GetOperatorByType(typeId, accessMethodId, strategyNumber);

    Const *constantValue = makeNullConst(typeId, typeModId, collationId);

    /* Now make the expression with the given variable and a null constant */
    OpExpr *expression = (OpExpr *) make_opclause(operatorId,
                                                  InvalidOid, /* no result type yet */
                                                  false, /* no return set */
                                                  (Expr *) variable,
                                                  (Expr *) constantValue,
                                                  InvalidOid, collationId);

    /* Set implementing function id and result type */
    expression->opfuncid = get_opcode(operatorId);
    expression->opresulttype = get_func_rettype(expression->opfuncid);

    return expression;
}


/*
 * GetOperatorByType returns operator Oid for the given type, access method,
 * and strategy number. Note that this function incorrectly errors out when
 * the given type doesn't have its own operator but can use another compatible
 * type's default operator. The function is copied from CitusDB's shard pruning
 * logic.
 */
static Oid
GetOperatorByType(Oid typeId, Oid accessMethodId, int16 strategyNumber) {
    /* Get default operator class from pg_opclass */
    Oid operatorClassId = GetDefaultOpClass(typeId, accessMethodId);

    Oid operatorFamily = get_opclass_family(operatorClassId);

    Oid operatorId = get_opfamily_member(operatorFamily, typeId, typeId, strategyNumber);

    return operatorId;
}

/*
 * update the base constraint with the given min/max values.
 * The function is copied from CitusDB's shard pruning logic.
 */
static void UpdateConstraint(Node *baseConstraint,
                             Datum minValue,
                             Datum maxValue) {
    BoolExpr *andExpr = (BoolExpr *) baseConstraint;
    Node *lessThanExpr = (Node *) linitial(andExpr->args);
    Node *greaterThanExpr = (Node *) lsecond(andExpr->args);

    Node *minNode = get_rightop((Expr *) greaterThanExpr);
    Node *maxNode = get_rightop((Expr *) lessThanExpr);

    Assert(IsA(minNode, Const));
    Assert(IsA(maxNode, Const));

    Const *minConstant = (Const *) minNode;
    Const *maxConstant = (Const *) maxNode;

    minConstant->constvalue = minValue;
    maxConstant->constvalue = maxValue;

    minConstant->constisnull = false;
    maxConstant->constisnull = false;

    minConstant->constbyval = true;
    maxConstant->constbyval = true;
}


/*
 * constructs a new StripeSkipList where the non-selected chunks are removed from the given stripeSkipListOrigin
 */
static StripeSkipList *SelectedStripeSkipList(StripeSkipList *stripeSkipListOrigin, // 原始的全量
                                              const bool *usedColumnMask,
                                              const bool *selectedChunkGroupMask) {
    uint32 selectedChunkGroupCount = 0;
    uint32 columnCount = stripeSkipListOrigin->columnCount;

    // 遍历usedColumnMask得到true数量
    for (uint32 chunkGroupIndex = 0; chunkGroupIndex < stripeSkipListOrigin->chunkGroupCount; chunkGroupIndex++) {
        if (selectedChunkGroupMask[chunkGroupIndex]) {
            selectedChunkGroupCount++;
        }
    }

    // 组装重要的成员 columnChunkSkipNodeArray
    // columnCount * chunkGroupCount 二维数组
    ColumnChunkSkipNode **selectedChunkSkipNodeArray = palloc0(columnCount * sizeof(ColumnChunkSkipNode *));

    for (uint32 columnIndex = 0; columnIndex < columnCount; columnIndex++) {
        bool firstColumn = columnIndex == 0;

        /* first column's chunk skip node is always read */
        if (!usedColumnMask[columnIndex] && !firstColumn) {
            selectedChunkSkipNodeArray[columnIndex] = NULL;
            continue;
        }

        Assert(stripeSkipListOrigin->columnChunkSkipNodeArray[columnIndex] != NULL);

        selectedChunkSkipNodeArray[columnIndex] = palloc0(selectedChunkGroupCount * sizeof(ColumnChunkSkipNode));

        uint32 selectedChunkGroupIndex = 0;
        for (uint32 chunkGroupIndex = 0; chunkGroupIndex < stripeSkipListOrigin->chunkGroupCount; chunkGroupIndex++) {
            if (!selectedChunkGroupMask[chunkGroupIndex]) {
                continue;
            }

            selectedChunkSkipNodeArray[columnIndex][selectedChunkGroupIndex] =
                    stripeSkipListOrigin->columnChunkSkipNodeArray[columnIndex][chunkGroupIndex];

            selectedChunkGroupIndex++;
        }
    }

    uint32 selectedChunkGroupIndex = 0;
    // 记录了各个选中的chunkGroup各自有多少行
    uint32 *chunkGroupRowCountArr = palloc0(selectedChunkGroupCount * sizeof(uint32));
    for (uint32 chunkGroupIndex = 0; chunkGroupIndex < stripeSkipListOrigin->chunkGroupCount; chunkGroupIndex++) {
        if (!selectedChunkGroupMask[chunkGroupIndex]) {
            continue;
        }

        chunkGroupRowCountArr[selectedChunkGroupIndex++] = stripeSkipListOrigin->chunkGroupRowCountArr[chunkGroupIndex];
    }

    StripeSkipList *stripeSkipListSelected = palloc0(sizeof(StripeSkipList));
    stripeSkipListSelected->columnChunkSkipNodeArray = selectedChunkSkipNodeArray;
    stripeSkipListSelected->chunkGroupCount = selectedChunkGroupCount;
    stripeSkipListSelected->columnCount = stripeSkipListOrigin->columnCount;
    stripeSkipListSelected->chunkGroupRowCountArr = chunkGroupRowCountArr;

    return stripeSkipListSelected;
}


/*
 * StripeSkipListRowCount counts the number of rows in the given stripeSkipList.
 * To do this, the function finds the first column, and sums up row counts across
 * all chunks for that column.
 */
static uint32
StripeSkipListRowCount(StripeSkipList *stripeSkipList) {
    uint32 stripeSkipListRowCount = 0;
    uint32 chunkIndex = 0;
    uint32 *chunkGroupRowCounts = stripeSkipList->chunkGroupRowCountArr;

    for (chunkIndex = 0; chunkIndex < stripeSkipList->chunkGroupCount; chunkIndex++) {
        uint32 chunkGroupRowCount = chunkGroupRowCounts[chunkIndex];
        stripeSkipListRowCount += chunkGroupRowCount;
    }

    return stripeSkipListRowCount;
}


/*
 * ProjectedColumnMask returns a boolean array in which the projected columns
 * from the projected column list are marked as true.
 */
static bool *ProjectedColumnMask(uint32 columnCount, List *usedColumnNaturalPosList) {
    bool *projectedColumnMask = palloc0(columnCount * sizeof(bool));
    int naturalPos;

    foreach_int(naturalPos, usedColumnNaturalPosList) {
        /* naturalPos is 1-indexed; projectedColumnMask is 0-indexed */
        int columnIndex = naturalPos - 1;
        projectedColumnMask[columnIndex] = true;
    }

    return projectedColumnMask;
}

// read an array of bits from the given buffer and stores it in provided bool array.
static void DeserializeExistArray(StringInfo boolArrayBuffer,
                                  bool *boolArray,
                                  uint32 boolArrayLength) {

    uint32 maximumBoolCount = boolArrayBuffer->len * 8;
    if (boolArrayLength > maximumBoolCount) {
        ereport(ERROR, (errmsg("insufficient data for reading boolean array")));
    }

    for (uint32 boolArrayIndex = 0; boolArrayIndex < boolArrayLength; boolArrayIndex++) {
        uint32 byteIndex = boolArrayIndex / 8;
        uint32 bitIndex = boolArrayIndex % 8;
        uint8 bitmask = (1 << bitIndex);

        uint8 shiftedBit = (boolArrayBuffer->data[byteIndex] & bitmask);

        boolArray[boolArrayIndex] = shiftedBit == 0 ? false : true;
    }
}


/*
 * DeserializeValueArray reads an array of datums from the given buffer and stores
 * them in provided dest. If a value is marked as false in the exists array,
 * the function assumes that the datum isn't in the buffer, and simply skips it.
 */
static void DeserializeValueArray(StringInfo decompressedSourceValueBuffer,
                                  const bool *existsArray,
                                  uint32 chunkGroupRowCount,
                                  bool datumTypeByValue,
                                  int datumTypeLength,
                                  char datumTypeAlign,
                                  Datum *dest) {
    uint32 currentDatumDataOffset = 0;

    for (uint32 chunkGroupIndex = 0; chunkGroupIndex < chunkGroupRowCount; chunkGroupIndex++) {
        if (!existsArray[chunkGroupIndex]) {
            continue;
        }

        char *currentDatumDataPointer = decompressedSourceValueBuffer->data + currentDatumDataOffset;

        dest[chunkGroupIndex] = fetch_att(currentDatumDataPointer,
                                          datumTypeByValue,
                                          datumTypeLength);

        // 会以当前的currentDatumDataOffset为蓝本得到新的值
        currentDatumDataOffset = att_addlength_datum(currentDatumDataOffset,
                                                     datumTypeLength,
                                                     currentDatumDataPointer);
        currentDatumDataOffset = att_align_nominal(currentDatumDataOffset,
                                                   datumTypeAlign);

        if (currentDatumDataOffset > decompressedSourceValueBuffer->len) {
            ereport(ERROR, (errmsg("insufficient data left in datum buffer")));
        }
    }
}

/*
 * 数据由stripeBuffers导到chunkData
 * DeserializeChunkGroupData deserializes requested data chunk for all columns and
 * stores in chunkDataArray. It uncompresses serialized data if necessary. The
 * function also deallocates data buffers used for previous chunk, and compressed
 * data buffers for the current chunk which will not be needed again. If a column
 * data is not present serialized buffer, then default value (or null) is used
 * to fill value array.
 */
static ChunkData *DeserializeChunkData(StripeBuffers *stripeBuffers,
                                       uint64 chunkGroupIndex,
                                       uint32 chunkGroupRowCount,
                                       TupleDesc tupleDesc,
                                       List *usedColumnNaturalPosList) {
    bool *columnMask = ProjectedColumnMask(tupleDesc->natts, usedColumnNaturalPosList);

    ChunkData *chunkData = CreateEmptyChunkData(tupleDesc->natts,
                                                columnMask,
                                                chunkGroupRowCount);

    for (int columnIndex = 0; columnIndex < stripeBuffers->columnCount; columnIndex++) {
        Form_pg_attribute attributeForm = TupleDescAttr(tupleDesc, columnIndex);
        ColumnBuffers *columnBuffers = stripeBuffers->columnBuffersArray[columnIndex];
        bool columnAdded = false;

        if (columnBuffers == NULL && columnMask[columnIndex]) {
            columnAdded = true;
        }

        if (columnBuffers != NULL) {
            ColumnChunkBuffers *columnChunkBuffers = columnBuffers->columnChunkBuffersArray[chunkGroupIndex];

            // columnChunkBuffers->existsBuffer 导到 chunkData->existsArray[columnIndex]
            DeserializeExistArray(columnChunkBuffers->existsBuffer,
                                  chunkData->existsArray[columnIndex],
                                  chunkGroupRowCount);

            /* decompress and deserialize current chunk's data */
            StringInfo decompressedValueBuffer = DecompressBuffer(columnChunkBuffers->valueBuffer,
                                                                  columnChunkBuffers->valueCompressionType,
                                                                  columnChunkBuffers->decompressedValueSize);


            // decompressedValueBuffer 导到 chunkData->valueArray[columnIndex]
            DeserializeValueArray(decompressedValueBuffer,
                                  chunkData->existsArray[columnIndex],
                                  chunkGroupRowCount,
                                  attributeForm->attbyval,
                                  attributeForm->attlen,
                                  attributeForm->attalign,
                                  chunkData->valueArray[columnIndex]);

            /* store current chunk's data buffer to be freed at next chunk read */
            chunkData->columnDataArr[columnIndex] = decompressedValueBuffer;
        } else if (columnAdded) {
            // This is a column that was added after creation of this stripe,use either the default value or NULL.
            if (attributeForm->atthasdef) {
                Datum defaultValue = ColumnDefaultValue(tupleDesc->constr, attributeForm);

                for (int rowIndex = 0; rowIndex < chunkGroupRowCount; rowIndex++) {
                    chunkData->existsArray[columnIndex][rowIndex] = true;
                    chunkData->valueArray[columnIndex][rowIndex] = defaultValue;
                }
            } else {
                memset(chunkData->existsArray[columnIndex],
                       false,
                       chunkGroupRowCount * sizeof(bool));
            }
        }
    }

    return chunkData;
}

/*
 * ColumnDefaultValue returns default value for given column. Only const values
 * are supported. The function errors on any other default value expressions.
 */
static Datum ColumnDefaultValue(TupleConstr *tupleConstraints, Form_pg_attribute attributeForm) {
    Node *defaultValueNode = NULL;
    int defValIndex = 0;

    for (defValIndex = 0; defValIndex < tupleConstraints->num_defval; defValIndex++) {
        AttrDefault attrDefault = tupleConstraints->defval[defValIndex];
        if (attrDefault.adnum == attributeForm->attnum) {
            defaultValueNode = stringToNode(attrDefault.adbin);
            break;
        }
    }

    Assert(defaultValueNode != NULL);

    /* try reducing the default value node to a const node */
    defaultValueNode = eval_const_expressions(NULL, defaultValueNode);
    if (IsA(defaultValueNode, Const)) {
        Const *constNode = (Const *) defaultValueNode;
        return constNode->constvalue;
    }

    const char *columnName = NameStr(attributeForm->attname);
    ereport(ERROR, (errmsg("unsupported default value for column \"%s\"", columnName),
            errhint("Expression is either mutable or does not evaluate to constant value")));

}
