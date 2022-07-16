/*-------------------------------------------------------------------------
 *
 * columnar_metadata.c
 *
 * Copyright (c) Citus Data, Inc.
 *
 * Manages metadata for columnar relations in separate, shared metadata tables
 * in the "columnar" schema.
 *
 *   * holds basic stripe information including data size and row counts
 *   * holds basic chunk and chunk group information like data offsets and
 *     min/max values (used for Chunk Group Filtering)
 *   * useful for fast VACUUM operations (e.g. reporting with VACUUM VERBOSE)
 *   * useful for stats/costing
 *   * maps logical row numbers to stripe IDs
 *   * TODO: visibility information
 *
 *-------------------------------------------------------------------------
 */


#include "postgres.h"

#include "safe_lib.h"

#include "citus_version.h"
#include "columnar/columnar.h"
#include "columnar/columnar_storage.h"
#include "columnar/columnar_version_compat.h"
#include "distributed/listutils.h"

#include <sys/stat.h>
#include "access/heapam.h"
#include "access/htup_details.h"
#include "access/nbtree.h"
#include "access/xact.h"
#include "catalog/indexing.h"
#include "catalog/pg_namespace.h"
#include "catalog/pg_collation.h"
#include "catalog/pg_type.h"
#include "catalog/namespace.h"
#include "commands/defrem.h"
#include "commands/sequence.h"
#include "commands/trigger.h"
#include "distributed/metadata_cache.h"
#include "executor/executor.h"
#include "executor/spi.h"
#include "miscadmin.h"
#include "nodes/execnodes.h"
#include "lib/stringinfo.h"
#include "port.h"
#include "storage/fd.h"
#include "storage/lmgr.h"
#include "storage/procarray.h"
#include "storage/smgr.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/memutils.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/relfilenodemap.h"


typedef struct {
    Relation rel;
    EState *estate;
    ResultRelInfo *resultRelInfo;
} ModifyState;

/* RowNumberLookupMode to be used in StripeMetadataLookupByRowNumber */
typedef enum RowNumberLookupMode {
    // Find the stripe whose firstRowNumber is less than or equal to given input rowNumber.
    FIND_LESS_OR_EQUAL,

    // Find the stripe whose firstRowNumber is greater than input rowNumber.
    FIND_GREATER
} RowNumberLookupMode;

static void InsertEmptyStripeMetadataRow(uint64 storageId, uint64 stripeId,
                                         uint32 columnCount, uint32 chunkGroupRowLimit,
                                         uint64 firstRowNumber);

static void GetHighestUsedAddressAndId(uint64 storageId,
                                       uint64 *highestUsedAddress,
                                       uint64 *highestUsedId);

static StripeMetadata *UpdateStripeMetadataRow(uint64 storageId, uint64 stripeId,
                                               bool *update, Datum *newValues);

static List *ReadDataFileStripeList(uint64 storageId, Snapshot snapshot);

static StripeMetadata *BuildStripeMetadata(Relation columnarStripes,
                                           HeapTuple heapTuple);

static uint32 *ReadChunkGroupRowCounts(uint64 storageId, uint64 stripe, uint32
chunkGroupCount, Snapshot snapshot);

static Oid ColumnarStorageIdSequenceRelationId(void);

static Oid ColumnarStripeRelationId(void);

static Oid ColumnarStripePKeyIndexRelationId(void);

static Oid ColumnarStripeFirstRowNumberIndexRelationId(void);

static Oid ColumnarOptionsRelationId(void);

static Oid ColumnarOptionsIndexRegclass(void);

static Oid ColumnarChunkRelationId(void);

static Oid ColumnarChunkGroupRelationId(void);

static Oid ColumnarChunkIndexRelationId(void);

static Oid ColumnarChunkGroupIndexRelationId(void);

static Oid ColumnarNamespaceId(void);

static uint64 LookupStorageId(RelFileNode relFileNode);

static uint64 GetHighestUsedRowNumber(uint64 storageId);

static void DeleteStorageFromColumnarMetadataTable(Oid metadataTableId,
                                                   AttrNumber storageIdAtrrNumber,
                                                   Oid storageIdIndexId,
                                                   uint64 storageId);

static ModifyState *StartModifyRelation(Relation relation);

static void InsertTupleAndEnforceConstraints(ModifyState *modifyState, Datum *values,
                                             bool *nulls);

static void DeleteTupleAndEnforceConstraints(ModifyState *state, HeapTuple heapTuple);

static void FinishModifyRelation(ModifyState *state);

static EState *create_estate_for_relation(Relation rel);

static bytea *DatumToBytea(Datum value, Form_pg_attribute attrForm);

static Datum ByteaToDatum(bytea *bytes, Form_pg_attribute attrForm);

static bool WriteColumnarOptions(Oid regclass, ColumnarOptions *options, bool overwrite);

static StripeMetadata *StripeMetadataLookupByRowNumber(Relation relation, uint64 rowNumber,
                                                       Snapshot snapshot,
                                                       RowNumberLookupMode lookupMode);

static void CheckStripeMetadataConsistency(StripeMetadata *stripeMetadata);

PG_FUNCTION_INFO_V1(columnar_relation_storageid);

/* constants for columnar.options */
#define Natts_columnar_options 5
#define Anum_columnar_options_regclass 1
#define Anum_columnar_options_chunk_group_row_limit 2
#define Anum_columnar_options_stripe_row_limit 3
#define Anum_columnar_options_compression_level 4
#define Anum_columnar_options_compression 5

/* ----------------
 *		columnar.options definition.
 * ----------------
 */
typedef struct FormData_columnar_options {
    Oid regclass;
    int32 chunk_group_row_limit;
    int32 stripe_row_limit;
    int32 compressionLevel;
    NameData compression;

#ifdef CATALOG_VARLEN           /* variable-length fields start here */
#endif
} FormData_columnar_options;
typedef FormData_columnar_options *Form_columnar_options;


/**
 * CREATE TABLE stripe (
    storage_id bigint NOT NULL,
    stripe_num bigint NOT NULL,
    file_offset bigint NOT NULL,
    data_length bigint NOT NULL,
    column_count int NOT NULL,
    chunk_row_count int NOT NULL,
    row_count bigint NOT NULL,
    chunk_group_count int NOT NULL,
    PRIMARY KEY (storage_id, stripe_num)
) WITH (user_catalog_table = true);
 */
/* constants for columnar.stripe 总的column数量 各个column位置 */
#define Natts_columnar_stripe 9
#define Anum_columnar_stripe_storageid 1
#define Anum_columnar_stripe_stripe 2 // 更确切的说对应 strip id
#define Anum_columnar_stripe_file_offset 3
#define Anum_columnar_stripe_data_length 4
#define Anum_columnar_stripe_column_count 5
#define Anum_columnar_stripe_chunk_row_count 6 // 确切的应该是 chunkGroupRowLimit
#define Anum_columnar_stripe_row_count 7
#define Anum_columnar_stripe_chunk_count 8
#define Anum_columnar_stripe_first_row_number 9

/**
 * CREATE TABLE chunk_group (
    storage_id bigint NOT NULL,
    stripe_num bigint NOT NULL,
    chunk_group_num int NOT NULL,
    row_count bigint NOT NULL,
    PRIMARY KEY (storage_id, stripe_num, chunk_group_num),
    FOREIGN KEY (storage_id, stripe_num) REFERENCES stripe(storage_id, stripe_num) ON DELETE CASCADE
);
 */
/* constants for columnar.chunk_group */
#define Natts_columnar_chunkgroup 4
#define Anum_columnar_chunkgroup_storageid 1
#define Anum_columnar_chunkgroup_stripe 2
#define Anum_columnar_chunkgroup_chunk 3 // chunk id
#define Anum_columnar_chunkgroup_row_count 4

/**
 * CREATE TABLE chunk (
    storage_id bigint NOT NULL,
    stripe_num bigint NOT NULL,
    attr_num int NOT NULL,
    chunk_group_num int NOT NULL,
    minimum_value bytea,
    maximum_value bytea,
    value_stream_offset bigint NOT NULL,
    value_stream_length bigint NOT NULL,
    exists_stream_offset bigint NOT NULL,
    exists_stream_length bigint NOT NULL,
    value_compression_type int NOT NULL,
    value_compression_level int NOT NULL,
    value_decompressed_length bigint NOT NULL,
    value_count bigint NOT NULL,
    PRIMARY KEY (storage_id, stripe_num, attr_num, chunk_group_num),
    FOREIGN KEY (storage_id, stripe_num, chunk_group_num) REFERENCES chunk_group(storage_id, stripe_num, chunk_group_num) ON DELETE CASCADE
) WITH (user_catalog_table = true);
 */
/* constants for columnar.chunk */
#define Natts_columnar_chunk 14
#define Anum_columnar_chunk_storageid 1
#define Anum_columnar_chunk_stripe 2
#define Anum_columnar_chunk_attr 3
#define Anum_columnar_chunk_chunk 4 // 对应chunk_group_num
#define Anum_columnar_chunk_minimum_value 5
#define Anum_columnar_chunk_maximum_value 6
#define Anum_columnar_chunk_value_stream_offset 7
#define Anum_columnar_chunk_value_stream_length 8
#define Anum_columnar_chunk_exists_stream_offset 9
#define Anum_columnar_chunk_exists_stream_length 10
#define Anum_columnar_chunk_value_compression_type 11
#define Anum_columnar_chunk_value_compression_level 12
#define Anum_columnar_chunk_value_decompressed_size 13
#define Anum_columnar_chunk_value_count 14


/*
 * InitColumnarOptions initialized the columnar table options. Meaning it writes the
 * default options to the options table if not already existing.
 */
void InitColumnarOptions(Oid regclass) {
    /*
     * When upgrading we retain options for all columnar tables by upgrading
     * "columnar.options" catalog table, so we shouldn't do anything here.
     */
    if (IsBinaryUpgrade) {
        return;
    }

    ColumnarOptions defaultOptions = {
            .chunkRowLimit = columnar_chunk_group_row_limit,
            .stripeRowLimit = columnar_stripe_row_limit,
            .compressionType = columnar_compression,
            .compressionLevel = columnar_compression_level
    };

    WriteColumnarOptions(regclass, &defaultOptions, false);
}


/*
 * SetColumnarOptions writes the passed table options as the authoritive options to the
 * table irregardless of the optiones already existing or not. This can be used to put a
 * table in a certain state.
 */
void
SetColumnarOptions(Oid regclass, ColumnarOptions *options) {
    WriteColumnarOptions(regclass, options, true);
}


/*
 * WriteColumnarOptions writes the options to the catalog table for a given regclass.
 *  - If overwrite is false it will only write the values if there is not already a record
 *    found.
 *  - If overwrite is true it will always write the settings
 *
 * The return value indicates if the record has been written.
 */
static bool
WriteColumnarOptions(Oid regclass, ColumnarOptions *options, bool overwrite) {
    /*
     * When upgrading we should retain the options from the previous
     * cluster and don't write new options.
     */
    Assert(!IsBinaryUpgrade);

    bool written = false;

    bool nulls[Natts_columnar_options] = {0};
    Datum values[Natts_columnar_options] = {
            ObjectIdGetDatum(regclass),
            Int32GetDatum(options->chunkRowLimit),
            Int32GetDatum(options->stripeRowLimit),
            Int32GetDatum(options->compressionLevel),
            0, /* to be filled below */
    };

    NameData compressionName = {0};
    namestrcpy(&compressionName, CompressionTypeStr(options->compressionType));
    values[Anum_columnar_options_compression - 1] = NameGetDatum(&compressionName);

    /* create heap tuple and insert into catalog table */
    Relation columnarOptions = relation_open(ColumnarOptionsRelationId(),
                                             RowExclusiveLock);
    TupleDesc tupleDescriptor = RelationGetDescr(columnarOptions);

    /* find existing item to perform update if exist */
    ScanKeyData scanKey[1] = {0};
    ScanKeyInit(&scanKey[0], Anum_columnar_options_regclass, BTEqualStrategyNumber,
                F_OIDEQ,
                ObjectIdGetDatum(regclass));

    Relation index = index_open(ColumnarOptionsIndexRegclass(), AccessShareLock);
    SysScanDesc scanDescriptor = systable_beginscan_ordered(columnarOptions, index, NULL,
                                                            1, scanKey);

    HeapTuple heapTuple = systable_getnext_ordered(scanDescriptor, ForwardScanDirection);
    if (HeapTupleIsValid(heapTuple)) {
        if (overwrite) {
            /* TODO check if the options are actually different, skip if not changed */
            /* update existing record */
            bool update[Natts_columnar_options] = {0};
            update[Anum_columnar_options_chunk_group_row_limit - 1] = true;
            update[Anum_columnar_options_stripe_row_limit - 1] = true;
            update[Anum_columnar_options_compression_level - 1] = true;
            update[Anum_columnar_options_compression - 1] = true;

            HeapTuple tuple = heap_modify_tuple(heapTuple, tupleDescriptor,
                                                values, nulls, update);
            CatalogTupleUpdate(columnarOptions, &tuple->t_self, tuple);
            written = true;
        }
    } else {
        /* inserting new record */
        HeapTuple newTuple = heap_form_tuple(tupleDescriptor, values, nulls);
        CatalogTupleInsert(columnarOptions, newTuple);
        written = true;
    }

    if (written) {
        CommandCounterIncrement();
    }

    systable_endscan_ordered(scanDescriptor);
    index_close(index, AccessShareLock);
    relation_close(columnarOptions, RowExclusiveLock);

    return written;
}


/*
 * DeleteColumnarTableOptions removes the columnar table options for a regclass. When
 * missingOk is false it will throw an error when no table options can be found.
 *
 * Returns whether a record has been removed.
 */
bool
DeleteColumnarTableOptions(Oid regclass, bool missingOk) {
    bool result = false;

    /*
     * When upgrading we shouldn't delete or modify table options and
     * retain options from the previous cluster.
     */
    Assert(!IsBinaryUpgrade);

    Relation columnarOptions = try_relation_open(ColumnarOptionsRelationId(),
                                                 RowExclusiveLock);
    if (columnarOptions == NULL) {
        /* extension has been dropped */
        return false;
    }

    /* find existing item to remove */
    ScanKeyData scanKey[1] = {0};
    ScanKeyInit(&scanKey[0], Anum_columnar_options_regclass, BTEqualStrategyNumber,
                F_OIDEQ,
                ObjectIdGetDatum(regclass));

    Relation index = index_open(ColumnarOptionsIndexRegclass(), AccessShareLock);
    SysScanDesc scanDescriptor = systable_beginscan_ordered(columnarOptions, index, NULL,
                                                            1, scanKey);

    HeapTuple heapTuple = systable_getnext_ordered(scanDescriptor, ForwardScanDirection);
    if (HeapTupleIsValid(heapTuple)) {
        CatalogTupleDelete(columnarOptions, &heapTuple->t_self);
        CommandCounterIncrement();

        result = true;
    } else if (!missingOk) {
        ereport(ERROR, (errmsg("missing options for regclass: %d", regclass)));
    }

    systable_endscan_ordered(scanDescriptor);
    index_close(index, AccessShareLock);
    relation_close(columnarOptions, RowExclusiveLock);

    return result;
}

/**
 * CREATE TABLE options (
    regclass regclass NOT NULL PRIMARY KEY,
    chunk_group_row_limit int NOT NULL,
    stripe_row_limit int NOT NULL,
    compression_level int NOT NULL,
    compression name NOT NULL
) WITH (user_catalog_table = true);
 */
bool ReadColumnarOptions(Oid regclass, ColumnarOptions *columnarOptions) {
    ScanKeyData scanKey[1];

    // 到options表中得到 regclass是当前表对应的记录 该column是打头的 在上有索引名字是options_pkey
    ScanKeyInit(&scanKey[0],
                Anum_columnar_options_regclass, // 1
                BTEqualStrategyNumber,
                F_OIDEQ,
                ObjectIdGetDatum(regclass));

    // columnar.options表
    Oid optionsTableOid = ColumnarOptionsRelationId();
    Relation optionsTable = try_relation_open(optionsTableOid, AccessShareLock);
    if (optionsTable == NULL) {
        // Extension has been dropped. This can be called while dropping extension or database via ObjectAccess().
        return false;
    }

    // columnar.options表上的 名为 options_pkey 的index
    Relation index = try_relation_open(ColumnarOptionsIndexRegclass(), AccessShareLock);
    if (index == NULL) {
        table_close(optionsTable, AccessShareLock);

        /* extension has been dropped */
        return false;
    }

    SysScanDesc sysScanDesc = systable_beginscan_ordered(optionsTable,
                                                         index,
                                                         NULL,
                                                         1,
                                                         scanKey);

    // 类似result.next()套路,要是在表中没有对应的记录(专门设置option)那么使用通用的默认
    HeapTuple heapTuple = systable_getnext_ordered(sysScanDesc, ForwardScanDirection);
    if (HeapTupleIsValid(heapTuple)) {
        Form_columnar_options columnarOptionsModel = (Form_columnar_options) GETSTRUCT(heapTuple);

        columnarOptions->chunkRowLimit = columnarOptionsModel->chunk_group_row_limit;
        columnarOptions->stripeRowLimit = columnarOptionsModel->stripe_row_limit;
        columnarOptions->compressionLevel = columnarOptionsModel->compressionLevel;
        columnarOptions->compressionType = ParseCompressionType(NameStr(columnarOptionsModel->compression));
    } else {
        /* populate columnarOptions with system defaults */
        columnarOptions->chunkRowLimit = columnar_chunk_group_row_limit;
        columnarOptions->stripeRowLimit = columnar_stripe_row_limit;
        columnarOptions->compressionLevel = columnar_compression_level;
        columnarOptions->compressionType = columnar_compression;
    }

    systable_endscan_ordered(sysScanDesc);
    index_close(index, AccessShareLock);
    relation_close(optionsTable, AccessShareLock);

    return true;
}


// saves stripeSkipList for a given stripeId to columnar.chunk.
void SaveStripeSkipList(RelFileNode relFileNode,
                        uint64 stripeId,
                        StripeSkipList *stripeSkipList,
                        TupleDesc tupleDesc) {
    uint64 storageId = LookupStorageId(relFileNode);

    Oid columnarChunkTableOid = ColumnarChunkRelationId();
    Relation columnarChunkTable = table_open(columnarChunkTableOid, RowExclusiveLock);

    ModifyState *modifyState = StartModifyRelation(columnarChunkTable);

    for (uint32 columnIndex = 0; columnIndex < stripeSkipList->columnCount; columnIndex++) {
        for (uint32 chunkIndex = 0; chunkIndex < stripeSkipList->chunkGroupCount; chunkIndex++) {
            ColumnChunkSkipNode *columnChunkSkipNode = &stripeSkipList->columnChunkSkipNodeArray[columnIndex][chunkIndex];

            Datum values[Natts_columnar_chunk] = {
                    UInt64GetDatum(storageId),
                    Int64GetDatum(stripeId),
                    Int32GetDatum(columnIndex + 1),
                    Int32GetDatum(chunkIndex), // 对应chunk的4th列 chunk_group_num
                    0, /* to be filled below */
                    0, /* to be filled below */
                    Int64GetDatum(columnChunkSkipNode->valueChunkOffset),
                    Int64GetDatum(columnChunkSkipNode->valueLength),
                    Int64GetDatum(columnChunkSkipNode->existsChunkOffset),
                    Int64GetDatum(columnChunkSkipNode->existsLength),
                    Int32GetDatum(columnChunkSkipNode->valueCompressionType),
                    Int32GetDatum(columnChunkSkipNode->valueCompressionLevel),
                    Int64GetDatum(columnChunkSkipNode->decompressedValueSize),
                    Int64GetDatum(columnChunkSkipNode->rowCount)
            };

            bool nulls[Natts_columnar_chunk] = {false};

            if (columnChunkSkipNode->hasMinMax) {
                values[Anum_columnar_chunk_minimum_value - 1] =
                        PointerGetDatum(DatumToBytea(columnChunkSkipNode->minimumValue,
                                                     &tupleDesc->attrs[columnIndex]));

                values[Anum_columnar_chunk_maximum_value - 1] =
                        PointerGetDatum(DatumToBytea(columnChunkSkipNode->maximumValue,
                                                     &tupleDesc->attrs[columnIndex]));
            } else {
                nulls[Anum_columnar_chunk_minimum_value - 1] = true;
                nulls[Anum_columnar_chunk_maximum_value - 1] = true;
            }

            InsertTupleAndEnforceConstraints(modifyState, values, nulls);
        }
    }

    FinishModifyRelation(modifyState);

    table_close(columnarChunkTable, RowExclusiveLock);
}


// saves the metadata for given chunk groups to columnar.chunk_group.
void SaveChunkGroups(RelFileNode relfilenode,
                     uint64 stripeId,
                     List *chunkGroupRowCounts) {

    uint64 storageId = LookupStorageId(relfilenode);

    Oid columnarChunkGroupTableOid = ColumnarChunkGroupRelationId();
    Relation columnarChunkGroupTable = table_open(columnarChunkGroupTableOid, RowExclusiveLock);

    ModifyState *modifyState = StartModifyRelation(columnarChunkGroupTable);

    ListCell *lc = NULL;
    int chunkId = 0;

    foreach(lc, chunkGroupRowCounts) {
        int64 rowCount = lfirst_int(lc);
        Datum values[Natts_columnar_chunkgroup] = {
                UInt64GetDatum(storageId),
                Int64GetDatum(stripeId),
                Int32GetDatum(chunkId),
                Int64GetDatum(rowCount)
        };

        bool nulls[Natts_columnar_chunkgroup] = {false};

        InsertTupleAndEnforceConstraints(modifyState, values, nulls);
        chunkId++;
    }

    FinishModifyRelation(modifyState);

    table_close(columnarChunkGroupTable, NoLock);
}


// select * from columnar.stripe where storage_id = ? and stripe_num = ?
StripeSkipList *ReadStripeSkipList(RelFileNode relfilenode,
                                   uint64 stripeId,
                                   TupleDesc tupleDesc,
                                   uint32 stripeChunkGroupCount,
                                   Snapshot snapshot) {

    HeapTuple heapTuple = NULL;
    uint32 columnCount = tupleDesc->natts;


    uint64 storageId = LookupStorageId(relfilenode);

    Oid columnarChunkTableOid = ColumnarChunkRelationId();
    Relation columnarChunkTable = table_open(columnarChunkTableOid, AccessShareLock);
    Relation index = index_open(ColumnarChunkIndexRelationId(), AccessShareLock);

    ScanKeyData scanKey[2];
    ScanKeyInit(&scanKey[0],
                Anum_columnar_chunk_storageid,
                BTEqualStrategyNumber,
                F_OIDEQ,
                UInt64GetDatum(storageId));
    ScanKeyInit(&scanKey[1],
                Anum_columnar_chunk_stripe,
                BTEqualStrategyNumber,
                F_OIDEQ,
                Int32GetDatum(stripeId));

    SysScanDesc sysScanDesc = systable_beginscan_ordered(columnarChunkTable,
                                                         index,
                                                         snapshot,
                                                         2,
                                                         scanKey);

    StripeSkipList *stripeSkipList = palloc0(sizeof(StripeSkipList));
    stripeSkipList->chunkGroupCount = stripeChunkGroupCount;
    stripeSkipList->columnCount = columnCount;
    stripeSkipList->columnChunkSkipNodeArray = palloc0(columnCount * sizeof(ColumnChunkSkipNode *));
    for (int32 columnIndex = 0; columnIndex < columnCount; columnIndex++) {
        stripeSkipList->columnChunkSkipNodeArray[columnIndex] = palloc0(stripeChunkGroupCount * sizeof(ColumnChunkSkipNode));
    }

    while (HeapTupleIsValid(heapTuple = systable_getnext_ordered(sysScanDesc,
                                                                 ForwardScanDirection))) {
        Datum datumArray[Natts_columnar_chunk];
        bool isNullArray[Natts_columnar_chunk];

        heap_deform_tuple(heapTuple,
                          RelationGetDescr(columnarChunkTable),
                          datumArray,
                          isNullArray);

        int32 attr = DatumGetInt32(datumArray[Anum_columnar_chunk_attr - 1]);
        int32 chunkIndex = DatumGetInt32(datumArray[Anum_columnar_chunk_chunk - 1]);

        if (attr <= 0 || attr > columnCount) {
            ereport(ERROR, (errmsg("invalid columnar columnChunkSkipNode entry"),
                    errdetail("Attribute number out of range: %d", attr)));
        }

        if (chunkIndex < 0 || chunkIndex >= stripeChunkGroupCount) {
            ereport(ERROR, (errmsg("invalid columnar columnChunkSkipNode entry"),
                    errdetail("Chunk number out of range: %d", chunkIndex)));
        }

        int32 columnIndex = attr - 1;

        ColumnChunkSkipNode *columnChunkSkipNode = &stripeSkipList->columnChunkSkipNodeArray[columnIndex][chunkIndex];
        columnChunkSkipNode->rowCount = DatumGetInt64(datumArray[Anum_columnar_chunk_value_count - 1]);
        columnChunkSkipNode->valueChunkOffset = DatumGetInt64(datumArray[Anum_columnar_chunk_value_stream_offset - 1]);
        columnChunkSkipNode->valueLength = DatumGetInt64(datumArray[Anum_columnar_chunk_value_stream_length - 1]);
        columnChunkSkipNode->existsChunkOffset = DatumGetInt64(
                datumArray[Anum_columnar_chunk_exists_stream_offset - 1]);
        columnChunkSkipNode->existsLength = DatumGetInt64(datumArray[Anum_columnar_chunk_exists_stream_length - 1]);
        columnChunkSkipNode->valueCompressionType = DatumGetInt32(
                datumArray[Anum_columnar_chunk_value_compression_type - 1]);
        columnChunkSkipNode->valueCompressionLevel = DatumGetInt32(
                datumArray[Anum_columnar_chunk_value_compression_level - 1]);
        columnChunkSkipNode->decompressedValueSize = DatumGetInt64(
                datumArray[Anum_columnar_chunk_value_decompressed_size - 1]);

        if (isNullArray[Anum_columnar_chunk_minimum_value - 1] ||
            isNullArray[Anum_columnar_chunk_maximum_value - 1]) {
            columnChunkSkipNode->hasMinMax = false;
        } else {
            bytea *minValue = DatumGetByteaP(datumArray[Anum_columnar_chunk_minimum_value - 1]);
            bytea *maxValue = DatumGetByteaP(datumArray[Anum_columnar_chunk_maximum_value - 1]);

            columnChunkSkipNode->minimumValue = ByteaToDatum(minValue, &tupleDesc->attrs[columnIndex]);
            columnChunkSkipNode->maximumValue = ByteaToDatum(maxValue, &tupleDesc->attrs[columnIndex]);

            columnChunkSkipNode->hasMinMax = true;
        }
    }

    systable_endscan_ordered(sysScanDesc);
    index_close(index, AccessShareLock);
    table_close(columnarChunkTable, AccessShareLock);

    stripeSkipList->chunkGroupRowCountArr = ReadChunkGroupRowCounts(storageId,
                                                                    stripeId,
                                                                    stripeChunkGroupCount,
                                                                    snapshot);

    return stripeSkipList;
}

/*
 * returns StripeMetadata for the stripe whose firstRowNumber is greater than given rowNumber.
 * If no such stripe exists, then returns NULL.
 */
StripeMetadata *FindNextStripeByRowNumber(Relation relation,
                                          uint64 rowNumber,
                                          Snapshot snapshot) {
    return StripeMetadataLookupByRowNumber(relation,
                                           rowNumber,
                                           snapshot,
                                           FIND_GREATER);
}


/*
 * FindStripeByRowNumber returns StripeMetadata for the stripe that contains
 * the row with rowNumber. If no such stripe exists, then returns NULL.
 */
StripeMetadata *
FindStripeByRowNumber(Relation relation, uint64 rowNumber, Snapshot snapshot) {
    StripeMetadata *stripeMetadata =
            FindStripeWithMatchingFirstRowNumber(relation, rowNumber, snapshot);
    if (!stripeMetadata) {
        return NULL;
    }

    if (rowNumber > StripeGetHighestRowNumber(stripeMetadata)) {
        return NULL;
    }

    return stripeMetadata;
}


/*
 * FindStripeWithMatchingFirstRowNumber returns a StripeMetadata object for
 * the stripe that has the greatest firstRowNumber among the stripes whose
 * firstRowNumber is smaller than or equal to given rowNumber. If no such
 * stripe exists, then returns NULL.
 *
 * Note that this doesn't mean that found stripe certainly contains the tuple
 * with given rowNumber. This is because, it also needs to be verified if
 * highest row number that found stripe contains is greater than or equal to
 * given rowNumber. For this reason, unless that additional check is done,
 * this function is mostly useful for checking against "possible" constraint
 * violations due to concurrent writes that are not flushed by other backends
 * yet.
 */
StripeMetadata *
FindStripeWithMatchingFirstRowNumber(Relation relation, uint64 rowNumber,
                                     Snapshot snapshot) {
    return StripeMetadataLookupByRowNumber(relation, rowNumber, snapshot,
                                           FIND_LESS_OR_EQUAL);
}


/*
 * StripeWriteState returns write state of given stripe.
 */
StripeWriteStateEnum StripeWriteState(StripeMetadata *stripeMetadata) {
    if (stripeMetadata->aborted) {
        return STRIPE_WRITE_ABORTED;
    }

    if (stripeMetadata->rowCount > 0) {
        return STRIPE_WRITE_FLUSHED;
    }

    return STRIPE_WRITE_IN_PROGRESS;
}


/*
 * StripeGetHighestRowNumber returns rowNumber of the row with highest
 * rowNumber in given stripe.
 */
uint64
StripeGetHighestRowNumber(StripeMetadata *stripeMetadata) {
    return stripeMetadata->firstRowNumber + stripeMetadata->rowCount - 1;
}

/*
 * StripeMetadataLookupByRowNumber returns StripeMetadata for the stripe whose
 * firstRowNumber is less than or equal to (FIND_LESS_OR_EQUAL), or is
 * greater than (FIND_GREATER) given rowNumber by doing backward index
 * scan on stripe_first_row_number_idx.
 * If no such stripe exists, then returns NULL.
 */
static StripeMetadata *StripeMetadataLookupByRowNumber(Relation relation,
                                                       uint64 rowNumber,
                                                       Snapshot snapshot,
                                                       RowNumberLookupMode lookupMode) {
    Assert(lookupMode == FIND_LESS_OR_EQUAL || lookupMode == FIND_GREATER);

    StripeMetadata *result = NULL;

    uint64 storageId = ColumnarStorageGetStorageId(relation, false);

    ScanKeyData scanKey[2];
    ScanKeyInit(&scanKey[0],
                Anum_columnar_stripe_storageid,
                BTEqualStrategyNumber,
                F_OIDEQ,
                Int32GetDatum(storageId));

    StrategyNumber strategyNumber = InvalidStrategy;
    RegProcedure procedure = InvalidOid;
    if (lookupMode == FIND_LESS_OR_EQUAL) {
        strategyNumber = BTLessEqualStrategyNumber;
        procedure = F_INT8LE;
    } else {
        strategyNumber = BTGreaterStrategyNumber;
        procedure = F_INT8GT;
    }
    ScanKeyInit(&scanKey[1],
                Anum_columnar_stripe_first_row_number,
                strategyNumber,
                procedure,
                UInt64GetDatum(rowNumber));


    // columnar.stripe表
    Relation columnarStripeTable = table_open(ColumnarStripeRelationId(), AccessShareLock);
    Relation index = index_open(ColumnarStripeFirstRowNumberIndexRelationId(), AccessShareLock);
    SysScanDesc scanDescriptor = systable_beginscan_ordered(columnarStripeTable,
                                                            index,
                                                            snapshot,
                                                            2,
                                                            scanKey);

    ScanDirection scanDirection =
            lookupMode == FIND_LESS_OR_EQUAL ? BackwardScanDirection : ForwardScanDirection;

    HeapTuple heapTuple = systable_getnext_ordered(scanDescriptor, scanDirection);
    if (HeapTupleIsValid(heapTuple)) {
        result = BuildStripeMetadata(columnarStripeTable, heapTuple);
    }

    systable_endscan_ordered(scanDescriptor);
    index_close(index, AccessShareLock);
    table_close(columnarStripeTable, AccessShareLock);

    return result;
}


/*
 * CheckStripeMetadataConsistency first decides if stripe write operation for
 * given stripe is "flushed", "aborted" or "in-progress", then errors out if
 * its metadata entry contradicts with this fact.
 *
 * Checks performed here are just to catch bugs, so it is encouraged to call
 * this function whenever a StripeMetadata object is built from an heap tuple
 * of columnar.stripe. Currently, BuildStripeMetadata is the only function
 * that does this.
 */
static void
CheckStripeMetadataConsistency(StripeMetadata *stripeMetadata) {
    bool stripeLooksInProgress =
            stripeMetadata->rowCount == 0 && stripeMetadata->chunkGroupCount == 0 &&
            stripeMetadata->fileOffset == ColumnarInvalidLogicalOffset &&
            stripeMetadata->dataLength == 0;

    /*
     * Even if stripe is flushed, fileOffset and dataLength might be equal
     * to 0 for zero column tables, but those two should still be consistent
     * with respect to each other.
     */
    bool stripeLooksFlushed =
            stripeMetadata->rowCount > 0 && stripeMetadata->chunkGroupCount > 0 &&
            ((stripeMetadata->fileOffset != ColumnarInvalidLogicalOffset &&
              stripeMetadata->dataLength > 0) ||
             (stripeMetadata->fileOffset == ColumnarInvalidLogicalOffset &&
              stripeMetadata->dataLength == 0));

    StripeWriteStateEnum stripeWriteState = StripeWriteState(stripeMetadata);
    if (stripeWriteState == STRIPE_WRITE_FLUSHED && stripeLooksFlushed) {
        /*
         * If stripe was flushed to disk, then we expect stripe to store
         * at least one tuple.
         */
        return;
    } else if (stripeWriteState == STRIPE_WRITE_IN_PROGRESS && stripeLooksInProgress) {
        /*
         * If stripe was not flushed to disk, then values of given four
         * fields should match the columns inserted by
         * InsertEmptyStripeMetadataRow.
         */
        return;
    } else if (stripeWriteState == STRIPE_WRITE_ABORTED && (stripeLooksInProgress ||
                                                            stripeLooksFlushed)) {
        /*
         * Stripe metadata entry for an aborted write can be complete or
         * incomplete. We might have aborted the transaction before or after
         * inserting into stripe metadata.
         */
        return;
    }

    ereport(ERROR, (errmsg("unexpected stripe state, stripe metadata "
                           "entry for stripe with id=" UINT64_FORMAT
            " is not consistent", stripeMetadata->id)));
}


/*
 * FindStripeWithHighestRowNumber returns StripeMetadata for the stripe that
 * has the row with highest rowNumber by doing backward index scan on
 * stripe_first_row_number_idx. If given relation is empty, then returns NULL.
 */
StripeMetadata *
FindStripeWithHighestRowNumber(Relation relation, Snapshot snapshot) {
    StripeMetadata *stripeWithHighestRowNumber = NULL;

    uint64 storageId = ColumnarStorageGetStorageId(relation, false);
    ScanKeyData scanKey[1];
    ScanKeyInit(&scanKey[0], Anum_columnar_stripe_storageid,
                BTEqualStrategyNumber, F_OIDEQ, Int32GetDatum(storageId));

    Relation columnarStripes = table_open(ColumnarStripeRelationId(), AccessShareLock);
    Relation index = index_open(ColumnarStripeFirstRowNumberIndexRelationId(),
                                AccessShareLock);
    SysScanDesc scanDescriptor = systable_beginscan_ordered(columnarStripes, index,
                                                            snapshot, 1, scanKey);

    HeapTuple heapTuple = systable_getnext_ordered(scanDescriptor, BackwardScanDirection);
    if (HeapTupleIsValid(heapTuple)) {
        stripeWithHighestRowNumber = BuildStripeMetadata(columnarStripes, heapTuple);
    }

    systable_endscan_ordered(scanDescriptor);
    index_close(index, AccessShareLock);
    table_close(columnarStripes, AccessShareLock);

    return stripeWithHighestRowNumber;
}


/*
 * ReadChunkGroupRowCounts returns an array of row counts of chunk groups for the
 * given stripe.
 */
static uint32 *
ReadChunkGroupRowCounts(uint64 storageId, uint64 stripe, uint32 chunkGroupCount,
                        Snapshot snapshot) {
    Oid columnarChunkGroupOid = ColumnarChunkGroupRelationId();
    Relation columnarChunkGroup = table_open(columnarChunkGroupOid, AccessShareLock);
    Relation index = index_open(ColumnarChunkGroupIndexRelationId(), AccessShareLock);

    ScanKeyData scanKey[2];
    ScanKeyInit(&scanKey[0], Anum_columnar_chunkgroup_storageid,
                BTEqualStrategyNumber, F_OIDEQ, UInt64GetDatum(storageId));
    ScanKeyInit(&scanKey[1], Anum_columnar_chunkgroup_stripe,
                BTEqualStrategyNumber, F_OIDEQ, Int32GetDatum(stripe));

    SysScanDesc scanDescriptor =
            systable_beginscan_ordered(columnarChunkGroup, index, snapshot, 2, scanKey);

    uint32 chunkGroupIndex = 0;
    HeapTuple heapTuple = NULL;
    uint32 *chunkGroupRowCounts = palloc0(chunkGroupCount * sizeof(uint32));

    while (HeapTupleIsValid(heapTuple = systable_getnext_ordered(scanDescriptor,
                                                                 ForwardScanDirection))) {
        Datum datumArray[Natts_columnar_chunkgroup];
        bool isNullArray[Natts_columnar_chunkgroup];

        heap_deform_tuple(heapTuple,
                          RelationGetDescr(columnarChunkGroup),
                          datumArray, isNullArray);

        uint32 tupleChunkGroupIndex =
                DatumGetUInt32(datumArray[Anum_columnar_chunkgroup_chunk - 1]);
        if (chunkGroupIndex >= chunkGroupCount ||
            tupleChunkGroupIndex != chunkGroupIndex) {
            elog(ERROR, "unexpected chunk group");
        }

        chunkGroupRowCounts[chunkGroupIndex] =
                (uint32) DatumGetUInt64(datumArray[Anum_columnar_chunkgroup_row_count - 1]);
        chunkGroupIndex++;
    }

    if (chunkGroupIndex != chunkGroupCount) {
        elog(ERROR, "unexpected chunk group count");
    }

    systable_endscan_ordered(scanDescriptor);
    index_close(index, AccessShareLock);
    table_close(columnarChunkGroup, AccessShareLock);

    return chunkGroupRowCounts;
}


/*
 * InsertEmptyStripeMetadataRow adds a row to columnar.stripe for the empty
 * stripe reservation made for stripeId.
 */
static void InsertEmptyStripeMetadataRow(uint64 storageId,
                                         uint64 stripeId,
                                         uint32 columnCount,
                                         uint32 chunkGroupRowLimit,
                                         uint64 firstRowNumber) {
    bool nulls[Natts_columnar_stripe] = {false};

    Datum values[Natts_columnar_stripe] = {0};
    values[Anum_columnar_stripe_storageid - 1] =
            UInt64GetDatum(storageId);
    values[Anum_columnar_stripe_stripe - 1] =
            UInt64GetDatum(stripeId);
    values[Anum_columnar_stripe_column_count - 1] =
            UInt32GetDatum(columnCount);
    values[Anum_columnar_stripe_chunk_row_count - 1] =
            UInt32GetDatum(chunkGroupRowLimit);
    values[Anum_columnar_stripe_first_row_number - 1] =
            UInt64GetDatum(firstRowNumber);

    /* stripe has no rows yet, so initialize rest of the columns accordingly */
    values[Anum_columnar_stripe_row_count - 1] =
            UInt64GetDatum(0);
    values[Anum_columnar_stripe_file_offset - 1] =
            UInt64GetDatum(ColumnarInvalidLogicalOffset);
    values[Anum_columnar_stripe_data_length - 1] =
            UInt64GetDatum(0);
    values[Anum_columnar_stripe_chunk_count - 1] =
            UInt32GetDatum(0);

    Oid columnarStripesOid = ColumnarStripeRelationId();
    Relation columnarStripes = table_open(columnarStripesOid, RowExclusiveLock);

    ModifyState *modifyState = StartModifyRelation(columnarStripes);

    InsertTupleAndEnforceConstraints(modifyState, values, nulls);

    FinishModifyRelation(modifyState);

    table_close(columnarStripes, RowExclusiveLock);
}


/*
 * StripesForRelfilenode returns a list of StripeMetadata for stripes
 * of the given relfilenode.
 */
List *
StripesForRelfilenode(RelFileNode relfilenode) {
    uint64 storageId = LookupStorageId(relfilenode);

    return ReadDataFileStripeList(storageId, GetTransactionSnapshot());
}


/*
 * GetHighestUsedAddress returns the highest used address for the given
 * relfilenode across all active and inactive transactions.
 *
 * This is used by truncate stage of VACUUM, and VACUUM can be called
 * for empty tables. So this doesn't throw errors for empty tables and
 * returns 0.
 */
uint64
GetHighestUsedAddress(RelFileNode relfilenode) {
    uint64 storageId = LookupStorageId(relfilenode);

    uint64 highestUsedAddress = 0;
    uint64 highestUsedId = 0;
    GetHighestUsedAddressAndId(storageId, &highestUsedAddress, &highestUsedId);

    return highestUsedAddress;
}


/*
 * GetHighestUsedAddressAndId returns the highest used address and id for
 * the given relfilenode across all active and inactive transactions.
 */
static void GetHighestUsedAddressAndId(uint64 storageId,
                                       uint64 *highestUsedAddress,
                                       uint64 *highestUsedId) {
    ListCell *stripeMetadataCell = NULL;

    SnapshotData SnapshotDirty;
    InitDirtySnapshot(SnapshotDirty);

    List *stripeMetadataList = ReadDataFileStripeList(storageId, &SnapshotDirty);

    *highestUsedId = 0;

    /* file starts with metapage */
    *highestUsedAddress = COLUMNAR_BYTES_PER_PAGE;

    foreach(stripeMetadataCell, stripeMetadataList) {
        StripeMetadata *stripe = lfirst(stripeMetadataCell);
        uint64 lastByte = stripe->fileOffset + stripe->dataLength - 1;
        *highestUsedAddress = Max(*highestUsedAddress, lastByte);
        *highestUsedId = Max(*highestUsedId, stripe->id);
    }
}

/*
 * ReserveEmptyStripe reserves an empty stripe for given relation
 * and inserts it into columnar.stripe. It is guaranteed that concurrent
 * writes won't overwrite the returned stripe.
 */
EmptyStripeReservation *ReserveEmptyStripe(Relation targetTable,
                                           uint64 columnCount,
                                           uint64 chunkGroupRowLimit,
                                           uint64 stripeRowLimit) {
    EmptyStripeReservation *emptyStripeReservation = palloc0(sizeof(EmptyStripeReservation));

    uint64 storageId = ColumnarStorageGetStorageId(targetTable, false);

    emptyStripeReservation->stripeId = ColumnarStorageReserveStripeId(targetTable);
    emptyStripeReservation->stripeFirstRowNumber = ColumnarStorageReserveRowNumber(targetTable, stripeRowLimit);

    /*
     * 向 columnar.stripe表 insert
     * XXX: Instead of inserting a dummy entry to columnar.stripe and
     * updating it when flushing the stripe, we could have a hash table
     * in shared memory for the bookkeeping of ongoing writes.
     */
    InsertEmptyStripeMetadataRow(storageId,
                                 emptyStripeReservation->stripeId,
                                 columnCount,
                                 chunkGroupRowLimit,
                                 emptyStripeReservation->stripeFirstRowNumber);

    return emptyStripeReservation;
}


/*
 * update columnar.stripe表的 对应 storageId stripeId 的那条记录
 * completes reservation of the stripe with stripeId under given size
 * in-place updates related stripe metadata tuple to complete reservation.
 */
StripeMetadata *CompleteStripeReservation(Relation targetTable,
                                          uint64 stripeId,
                                          uint64 stripeSize,
                                          uint64 stripeRowCount,
                                          uint64 chunkCount) {
    uint64 resLogicalStart = ColumnarStorageReserveData(targetTable, stripeSize);
    uint64 storageId = ColumnarStorageGetStorageId(targetTable, false);

    // 以下是update内部管理用的表columnar.stripe,使用代码的套路进行update
    // 要 减去 1的原因是 对应的宏是1起始的
    bool update[Natts_columnar_stripe] = {false};
    update[Anum_columnar_stripe_file_offset - 1] = true;
    update[Anum_columnar_stripe_data_length - 1] = true;
    update[Anum_columnar_stripe_row_count - 1] = true;
    update[Anum_columnar_stripe_chunk_count - 1] = true;

    Datum newValues[Natts_columnar_stripe] = {0};
    newValues[Anum_columnar_stripe_file_offset - 1] = Int64GetDatum(resLogicalStart);
    newValues[Anum_columnar_stripe_data_length - 1] = Int64GetDatum(stripeSize);
    newValues[Anum_columnar_stripe_row_count - 1] = UInt64GetDatum(stripeRowCount);
    newValues[Anum_columnar_stripe_chunk_count - 1] = Int32GetDatum(chunkCount);

    return UpdateStripeMetadataRow(storageId, stripeId, update, newValues);
}


/*
 * 实现对columnar.stipe表update 读取到 指定的 storageId 和 stripeId对应的行 对其修改
 * UpdateStripeMetadataRow updates stripe metadata tuple for the stripe with
 * stripeId according to given newValues and update arrays.
 * Note that this function shouldn't be used for the cases where any indexes
 * of stripe metadata should be updated according to modifications done.
 */
static StripeMetadata *UpdateStripeMetadataRow(uint64 storageId,
                                               uint64 stripeId,
                                               bool *update,// 各个的column有没有变化是不是要update
                                               Datum *newValues) {
    SnapshotData dirtySnapshot;
    InitDirtySnapshot(dirtySnapshot);

    // index
    ScanKeyData scanKey[2];
    ScanKeyInit(&scanKey[0],
                Anum_columnar_stripe_storageid,
                BTEqualStrategyNumber,
                F_OIDEQ,
                Int32GetDatum(storageId));
    ScanKeyInit(&scanKey[1],
                Anum_columnar_stripe_stripe,
                BTEqualStrategyNumber,
                F_OIDEQ,
                Int32GetDatum(stripeId));


    // 得到了表columnar.stripe的oid
    Oid columnarStripesOid = ColumnarStripeRelationId();

    Relation columnarStripeTable = table_open(columnarStripesOid, AccessShareLock);
    Relation columnarStripePkeyIndex = index_open(ColumnarStripePKeyIndexRelationId(),
                                                  AccessShareLock);

    SysScanDesc scanDescriptor = systable_beginscan_ordered(columnarStripeTable, // 表
                                                            columnarStripePkeyIndex, // index
                                                            &dirtySnapshot,
                                                            2,
                                                            scanKey);

    // 更改之前的该行的数据
    HeapTuple oldTuple = systable_getnext_ordered(scanDescriptor, ForwardScanDirection);
    if (!HeapTupleIsValid(oldTuple)) {
        ereport(ERROR, (errmsg("attempted to modify an unexpected stripe, columnar storage with id=" UINT64_FORMAT
                " does not have stripe with id=" UINT64_FORMAT, storageId, stripeId)));
    }

    /*
     * heap_inplace_update already doesn't allow changing size of the original
     * tuple, so we don't allow setting any Datum's to NULL values.
     */
    bool newNulls[Natts_columnar_stripe] = {false};
    TupleDesc tupleDesc = RelationGetDescr(columnarStripeTable);

    // 在old的基础上得到更改过的
    HeapTuple modifiedTuple = heap_modify_tuple(oldTuple,
                                                tupleDesc,
                                                newValues,
                                                newNulls,
                                                update);

    // 真正执行update
    heap_inplace_update(columnarStripeTable, modifiedTuple);

    // Existing tuple now contains modifications, because we used heap_inplace_update().
    HeapTuple newTuple = oldTuple;

    /*
     * Must not pass modifiedTuple, because BuildStripeMetadata expects a real heap tuple with MVCC fields.
     */
    StripeMetadata *modifiedStripeMetadata = BuildStripeMetadata(columnarStripeTable,
                                                                 newTuple);

    CommandCounterIncrement();

    // 关闭
    systable_endscan_ordered(scanDescriptor);
    index_close(columnarStripePkeyIndex, AccessShareLock);
    table_close(columnarStripeTable, AccessShareLock);

    // return StripeMetadata object built from modified tuple
    return modifiedStripeMetadata;
}


/*
 * ReadDataFileStripeList reads the stripe list for a given storageId
 * in the given snapshot.
 */
static List *
ReadDataFileStripeList(uint64 storageId, Snapshot snapshot) {
    List *stripeMetadataList = NIL;
    ScanKeyData scanKey[1];
    HeapTuple heapTuple;

    ScanKeyInit(&scanKey[0], Anum_columnar_stripe_storageid,
                BTEqualStrategyNumber, F_OIDEQ, Int32GetDatum(storageId));

    Oid columnarStripesOid = ColumnarStripeRelationId();

    Relation columnarStripes = table_open(columnarStripesOid, AccessShareLock);
    Relation index = index_open(ColumnarStripeFirstRowNumberIndexRelationId(),
                                AccessShareLock);

    SysScanDesc scanDescriptor = systable_beginscan_ordered(columnarStripes, index,
                                                            snapshot, 1,
                                                            scanKey);

    while (HeapTupleIsValid(heapTuple = systable_getnext_ordered(scanDescriptor,
                                                                 ForwardScanDirection))) {
        StripeMetadata *stripeMetadata = BuildStripeMetadata(columnarStripes, heapTuple);
        stripeMetadataList = lappend(stripeMetadataList, stripeMetadata);
    }

    systable_endscan_ordered(scanDescriptor);
    index_close(index, AccessShareLock);
    table_close(columnarStripes, AccessShareLock);

    return stripeMetadataList;
}


/**
 * BuildStripeMetadata builds a StripeMetadata object from given heap tuple.
 * NB: heapTuple must be a proper heap tuple with MVCC fields.
 *
 * @param columnarStripes columnar.stripe表
 * @param heapTuple columnar.stripe表上的1行的记录
 * @return
 */
static StripeMetadata *BuildStripeMetadata(Relation columnarStripes,
                                           HeapTuple heapTuple) {
    Assert(RelationGetRelid(columnarStripes) == ColumnarStripeRelationId());

    Datum datumArray[Natts_columnar_stripe];
    bool isNullArray[Natts_columnar_stripe];

    // 把heapTuple转化为 Datum数组
    heap_deform_tuple(heapTuple,
                      RelationGetDescr(columnarStripes),
                      datumArray,
                      isNullArray);

    // 几乎是 columnar.strip表对应的model
    StripeMetadata *stripeMetadata = palloc0(sizeof(StripeMetadata));

    // strip表的行数据 注入
    stripeMetadata->id = DatumGetInt64(datumArray[Anum_columnar_stripe_stripe - 1]);
    stripeMetadata->fileOffset = DatumGetInt64(datumArray[Anum_columnar_stripe_file_offset - 1]);
    stripeMetadata->dataLength = DatumGetInt64(datumArray[Anum_columnar_stripe_data_length - 1]);
    stripeMetadata->columnCount = DatumGetInt32(datumArray[Anum_columnar_stripe_column_count - 1]);
    stripeMetadata->chunkGroupCount = DatumGetInt32(datumArray[Anum_columnar_stripe_chunk_count - 1]);
    stripeMetadata->chunkGroupRowCount = DatumGetInt32(datumArray[Anum_columnar_stripe_chunk_row_count - 1]);
    stripeMetadata->rowCount = DatumGetInt64(datumArray[Anum_columnar_stripe_row_count - 1]);
    stripeMetadata->firstRowNumber = DatumGetUInt64(datumArray[Anum_columnar_stripe_first_row_number - 1]);

    /*
     * If there is unflushed data in a parent transaction, then we would
     * have already thrown an error before starting to scan the table.. If
     * the data is from an earlier subxact that committed, then it would
     * have been flushed already. For this reason, we don't care about
     * subtransaction id here.
     */
    TransactionId entryXmin = HeapTupleHeaderGetXmin(heapTuple->t_data);

    stripeMetadata->aborted = !TransactionIdIsInProgress(entryXmin) && TransactionIdDidAbort(entryXmin);
    stripeMetadata->insertedByCurrentXact = TransactionIdIsCurrentTransactionId(entryXmin);

    CheckStripeMetadataConsistency(stripeMetadata);

    return stripeMetadata;
}


/*
 * DeleteMetadataRows removes the rows with given relfilenode from columnar
 * metadata tables.
 */
void
DeleteMetadataRows(RelFileNode relfilenode) {
    /*
     * During a restore for binary upgrade, metadata tables and indexes may or
     * may not exist.
     */
    if (IsBinaryUpgrade) {
        return;
    }

    uint64 storageId = LookupStorageId(relfilenode);

    DeleteStorageFromColumnarMetadataTable(ColumnarStripeRelationId(),
                                           Anum_columnar_stripe_storageid,
                                           ColumnarStripePKeyIndexRelationId(),
                                           storageId);
    DeleteStorageFromColumnarMetadataTable(ColumnarChunkGroupRelationId(),
                                           Anum_columnar_chunkgroup_storageid,
                                           ColumnarChunkGroupIndexRelationId(),
                                           storageId);
    DeleteStorageFromColumnarMetadataTable(ColumnarChunkRelationId(),
                                           Anum_columnar_chunk_storageid,
                                           ColumnarChunkIndexRelationId(),
                                           storageId);
}


/*
 * DeleteStorageFromColumnarMetadataTable removes the rows with given
 * storageId from given columnar metadata table.
 */
static void
DeleteStorageFromColumnarMetadataTable(Oid metadataTableId,
                                       AttrNumber storageIdAtrrNumber,
                                       Oid storageIdIndexId, uint64 storageId) {
    ScanKeyData scanKey[1];
    ScanKeyInit(&scanKey[0], storageIdAtrrNumber, BTEqualStrategyNumber,
                F_INT8EQ, UInt64GetDatum(storageId));

    Relation metadataTable = try_relation_open(metadataTableId, AccessShareLock);
    if (metadataTable == NULL) {
        /* extension has been dropped */
        return;
    }

    Relation index = index_open(storageIdIndexId, AccessShareLock);

    SysScanDesc scanDescriptor = systable_beginscan_ordered(metadataTable, index, NULL,
                                                            1, scanKey);

    ModifyState *modifyState = StartModifyRelation(metadataTable);

    HeapTuple heapTuple;
    while (HeapTupleIsValid(heapTuple = systable_getnext_ordered(scanDescriptor,
                                                                 ForwardScanDirection))) {
        DeleteTupleAndEnforceConstraints(modifyState, heapTuple);
    }

    systable_endscan_ordered(scanDescriptor);

    FinishModifyRelation(modifyState);

    index_close(index, AccessShareLock);
    table_close(metadataTable, AccessShareLock);
}


/*
 * StartModifyRelation allocates resources for modifications.
 */
static ModifyState *StartModifyRelation(Relation relation) {
    EState *estate = create_estate_for_relation(relation);

#if PG_VERSION_NUM >= PG_VERSION_14
    ResultRelInfo *resultRelInfo = makeNode(ResultRelInfo);
    InitResultRelInfo(resultRelInfo, relation, 1, NULL, 0);
#else
    ResultRelInfo *resultRelInfo = estate->es_result_relation_info;
#endif

    /* ExecSimpleRelationInsert, ... require caller to open indexes */
    ExecOpenIndices(resultRelInfo, false);

    ModifyState *modifyState = palloc(sizeof(ModifyState));
    modifyState->rel = relation;
    modifyState->estate = estate;
    modifyState->resultRelInfo = resultRelInfo;

    return modifyState;
}


/*
 * InsertTupleAndEnforceConstraints inserts a tuple into a relation and makes
 * sure constraints are enforced and indexes are updated.
 */
static void InsertTupleAndEnforceConstraints(ModifyState *modifyState,
                                             Datum *values,
                                             bool *nulls) {
    TupleDesc tupleDesc = RelationGetDescr(modifyState->rel);

    HeapTuple heapTuple = heap_form_tuple(tupleDesc,
                                          values,
                                          nulls);

    TupleTableSlot *tupleTableSlot = ExecInitExtraTupleSlot(modifyState->estate,
                                                            tupleDesc,
                                                            &TTSOpsHeapTuple);

    ExecStoreHeapTuple(heapTuple, tupleTableSlot, false);

    /* use ExecSimpleRelationInsert to enforce constraints */
    ExecSimpleRelationInsert_compat(modifyState->resultRelInfo, modifyState->estate, tupleTableSlot);
}


/*
 * DeleteTupleAndEnforceConstraints deletes a tuple from a relation and
 * makes sure constraints (e.g. FK constraints) are enforced.
 */
static void
DeleteTupleAndEnforceConstraints(ModifyState *state, HeapTuple heapTuple) {
    EState *estate = state->estate;
    ResultRelInfo *resultRelInfo = state->resultRelInfo;

    ItemPointer tid = &(heapTuple->t_self);
    simple_heap_delete(state->rel, tid);

    /* execute AFTER ROW DELETE Triggers to enforce constraints */
    ExecARDeleteTriggers(estate, resultRelInfo, tid, NULL, NULL);
}


/*
 * FinishModifyRelation cleans up resources after modifications are done.
 */
static void
FinishModifyRelation(ModifyState *state) {
    ExecCloseIndices(state->resultRelInfo);

    AfterTriggerEndQuery(state->estate);

#if PG_VERSION_NUM >= PG_VERSION_14
    ExecCloseResultRelations(state->estate);
    ExecCloseRangeTableRelations(state->estate);
#else
    ExecCleanUpTriggerState(state->estate);
#endif

    ExecResetTupleTable(state->estate->es_tupleTable, false);
    FreeExecutorState(state->estate);

    CommandCounterIncrement();
}


/*
 * Based on a similar function from
 * postgres/src/backend/replication/logical/worker.c.
 *
 * Executor state preparation for evaluation of constraint expressions,
 * indexes and triggers.
 *
 * This is based on similar code in copy.c
 */
static EState *
create_estate_for_relation(Relation rel) {
    EState *estate = CreateExecutorState();

    RangeTblEntry *rte = makeNode(RangeTblEntry);
    rte->rtekind = RTE_RELATION;
    rte->relid = RelationGetRelid(rel);
    rte->relkind = rel->rd_rel->relkind;
    rte->rellockmode = AccessShareLock;
    ExecInitRangeTable(estate, list_make1(rte));

#if PG_VERSION_NUM < PG_VERSION_14
    ResultRelInfo *resultRelInfo = makeNode(ResultRelInfo);
    InitResultRelInfo(resultRelInfo, rel, 1, NULL, 0);

    estate->es_result_relations = resultRelInfo;
    estate->es_num_result_relations = 1;
    estate->es_result_relation_info = resultRelInfo;
#endif

    estate->es_output_cid = GetCurrentCommandId(true);

    /* Prepare to catch AFTER triggers. */
    AfterTriggerBeginQuery();

    return estate;
}


// DatumToBytea serializes a datum into a bytea value.
static bytea *DatumToBytea(Datum value, Form_pg_attribute attrForm) {
    int datumLength = att_addlength_datum(0, attrForm->attlen, value);
    bytea *result = palloc0(datumLength + VARHDRSZ);

    SET_VARSIZE(result, datumLength + VARHDRSZ);

    if (attrForm->attlen > 0) {
        if (attrForm->attbyval) { // 是值类型
            Datum tmp;
            store_att_byval(&tmp, value, attrForm->attlen);

            memcpy_s(VARDATA(result),
                     datumLength + VARHDRSZ,
                     &tmp,
                     attrForm->attlen);
        } else { // 是指针的
            memcpy_s(VARDATA(result),
                     datumLength + VARHDRSZ,
                     DatumGetPointer(value),
                     attrForm->attlen);
        }
    } else {
        memcpy_s(VARDATA(result),
                 datumLength + VARHDRSZ,
                 DatumGetPointer(value),
                 datumLength);
    }

    return result;
}


/*
 * ByteaToDatum deserializes a value which was previously serialized using
 * DatumToBytea.
 */
static Datum
ByteaToDatum(bytea *bytes, Form_pg_attribute attrForm) {
    /*
     * We copy the data so the result of this function lives even
     * after the byteaDatum is freed.
     */
    char *binaryDataCopy = palloc0(VARSIZE_ANY_EXHDR(bytes));
    memcpy_s(binaryDataCopy, VARSIZE_ANY_EXHDR(bytes),
             VARDATA_ANY(bytes), VARSIZE_ANY_EXHDR(bytes));

    return fetch_att(binaryDataCopy, attrForm->attbyval, attrForm->attlen);
}


/*
 * ColumnarStorageIdSequenceRelationId returns relation id of columnar.stripe.
 * TODO: should we cache this similar to citus?
 */
static Oid
ColumnarStorageIdSequenceRelationId(void) {
    return get_relname_relid("storageid_seq", ColumnarNamespaceId());
}


/*
 * ColumnarStripeRelationId returns relation id of columnar.stripe.
 * TODO: should we cache this similar to citus?
 */
static Oid
ColumnarStripeRelationId(void) {
    return get_relname_relid("stripe", ColumnarNamespaceId());
}


/*
 * ColumnarStripePKeyIndexRelationId returns relation id of columnar.stripe_pkey.
 * TODO: should we cache this similar to citus?
 */
static Oid
ColumnarStripePKeyIndexRelationId(void) {
    return get_relname_relid("stripe_pkey", ColumnarNamespaceId());
}


/*
 * columnar.stripe表的唯1索引
 * constraint stripe_first_row_number_idx unique (storage_id, first_row_number)
 * TODO: should we cache this similar to citus?
 */
static Oid ColumnarStripeFirstRowNumberIndexRelationId(void) {
    return get_relname_relid("stripe_first_row_number_idx", ColumnarNamespaceId());
}


/*
 * ColumnarOptionsRelationId returns relation id of columnar.options.
 */
static Oid ColumnarOptionsRelationId(void) {
    return get_relname_relid("options", ColumnarNamespaceId());
}


/*
 * ColumnarOptionsIndexRegclass returns relation id of columnar.options_pkey.
 */
static Oid
ColumnarOptionsIndexRegclass(void) {
    return get_relname_relid("options_pkey", ColumnarNamespaceId());
}


/*
 * ColumnarChunkRelationId returns relation id of columnar.chunk.
 * TODO: should we cache this similar to citus?
 */
static Oid
ColumnarChunkRelationId(void) {
    return get_relname_relid("chunk", ColumnarNamespaceId());
}


/*
 * 得到 columnar.chunk_group 的oid
 * TODO: should we cache this similar to citus?
 */
static Oid ColumnarChunkGroupRelationId(void) {
    return get_relname_relid("chunk_group", ColumnarNamespaceId());
}


/*
 * ColumnarChunkIndexRelationId returns relation id of columnar.chunk_pkey.
 * TODO: should we cache this similar to citus?
 */
static Oid
ColumnarChunkIndexRelationId(void) {
    return get_relname_relid("chunk_pkey", ColumnarNamespaceId());
}


/*
 * ColumnarChunkGroupIndexRelationId returns relation id of columnar.chunk_group_pkey.
 * TODO: should we cache this similar to citus?
 */
static Oid
ColumnarChunkGroupIndexRelationId(void) {
    return get_relname_relid("chunk_group_pkey", ColumnarNamespaceId());
}


/*
 * ColumnarNamespaceId returns namespace id of the schema we store columnar
 * related tables.
 */
static Oid
ColumnarNamespaceId(void) {
    return get_namespace_oid("columnar", false);
}


/*
 * LookupStorageId reads storage metapage to find the storage ID for the given relFileNode. It returns
 * false if the relation doesn't have a meta page yet.
 */
static uint64 LookupStorageId(RelFileNode relFileNode) {
    Oid relationId = RelidByRelfilenode(relFileNode.spcNode,
                                        relFileNode.relNode);

    Relation relation = relation_open(relationId, AccessShareLock);
    uint64 storageId = ColumnarStorageGetStorageId(relation, false);
    table_close(relation, AccessShareLock);

    return storageId;
}


// ColumnarMetadataNewStorageId - create a new, unique storage id and return it
uint64 ColumnarMetadataNewStorageId() {
    return nextval_internal(ColumnarStorageIdSequenceRelationId(), false);
}


/*
 * columnar_relation_storageid returns storage id associated with the
 * given relation id, or -1 if there is no associated storage id yet.
 */
Datum
columnar_relation_storageid(PG_FUNCTION_ARGS) {
    Oid relationId = PG_GETARG_OID(0);
    Relation relation = relation_open(relationId, AccessShareLock);
    if (!IsColumnarTableAmTable(relationId)) {
        elog(ERROR, "relation \"%s\" is not a columnar table",
             RelationGetRelationName(relation));
    }

    uint64 storageId = ColumnarStorageGetStorageId(relation, false);

    relation_close(relation, AccessShareLock);

    PG_RETURN_INT64(storageId);
}


/*
 * ColumnarStorageUpdateIfNeeded - upgrade columnar storage to the current version by
 * using information from the metadata tables.
 */
void ColumnarStorageUpdateIfNeeded(Relation rel, bool isUpgrade) {
    if (ColumnarStorageIsCurrent(rel)) {
        return;
    }

    RelationOpenSmgr(rel);
    BlockNumber nblocks = smgrnblocks(rel->rd_smgr, MAIN_FORKNUM);
    if (nblocks < 2) {
        ColumnarStorageInit(rel->rd_smgr, ColumnarMetadataNewStorageId());
        return;
    }

    uint64 storageId = ColumnarStorageGetStorageId(rel, true);

    uint64 highestId;
    uint64 highestOffset;
    GetHighestUsedAddressAndId(storageId, &highestOffset, &highestId);

    uint64 reservedStripeId = highestId + 1;
    uint64 reservedOffset = highestOffset + 1;
    uint64 reservedRowNumber = GetHighestUsedRowNumber(storageId) + 1;
    ColumnarStorageUpdateCurrent(rel, isUpgrade, reservedStripeId,
                                 reservedRowNumber, reservedOffset);
}


/*
 * GetHighestUsedRowNumber returns the highest used rowNumber for given
 * storageId. Returns COLUMNAR_INVALID_ROW_NUMBER if storage with
 * storageId has no stripes.
 * Note that normally we would use ColumnarStorageGetReservedRowNumber
 * to decide that. However, this function is designed to be used when
 * building the metapage itself during upgrades.
 */
static uint64
GetHighestUsedRowNumber(uint64 storageId) {
    uint64 highestRowNumber = COLUMNAR_INVALID_ROW_NUMBER;

    List *stripeMetadataList = ReadDataFileStripeList(storageId,
                                                      GetTransactionSnapshot());
    StripeMetadata *stripeMetadata = NULL;
    foreach_ptr(stripeMetadata, stripeMetadataList) {
        highestRowNumber = Max(highestRowNumber,
                               StripeGetHighestRowNumber(stripeMetadata));
    }

    return highestRowNumber;
}
