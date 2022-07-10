/*-------------------------------------------------------------------------
 *
 * columnar.h
 *
 * Type and function declarations for Columnar
 *
 * Copyright (c) Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#ifndef COLUMNAR_H
#define COLUMNAR_H

#include "postgres.h"

#include "fmgr.h"
#include "lib/stringinfo.h"
#include "nodes/parsenodes.h"
#include "storage/bufpage.h"
#include "storage/lockdefs.h"
#include "storage/relfilenode.h"
#include "utils/relcache.h"
#include "utils/snapmgr.h"

#include "columnar/columnar_compression.h"
#include "columnar/columnar_metadata.h"

/* Defines for valid option names */
#define OPTION_NAME_COMPRESSION_TYPE "compression"
#define OPTION_NAME_STRIPE_ROW_COUNT "stripe_row_limit"
#define OPTION_NAME_CHUNK_ROW_COUNT "chunk_group_row_limit"

/* Limits for option parameters */
#define STRIPE_ROW_COUNT_MINIMUM 1000
#define STRIPE_ROW_COUNT_MAXIMUM 10000000
#define CHUNK_ROW_COUNT_MINIMUM 1000
#define CHUNK_ROW_COUNT_MAXIMUM 100000
#define COMPRESSION_LEVEL_MIN 1
#define COMPRESSION_LEVEL_MAX 19

/* Columnar file signature */
#define COLUMNAR_VERSION_MAJOR 2
#define COLUMNAR_VERSION_MINOR 0

/* miscellaneous defines */
#define COLUMNAR_TUPLE_COST_MULTIPLIER 10
#define COLUMNAR_POSTSCRIPT_SIZE_LENGTH 1
#define COLUMNAR_POSTSCRIPT_SIZE_MAX 256
#define COLUMNAR_BYTES_PER_PAGE (BLCKSZ - SizeOfPageHeaderData)

// 对应 Form_columnar_options
typedef struct ColumnarOptions {
    uint64 stripeRowLimit;
    uint32 chunkRowLimit;
    CompressionType compressionType;
    int compressionLevel;
} ColumnarOptions;


/*
 * ColumnarTableDDLContext holds the instance variable for the TableDDLCommandFunction
 * instance described below.
 */
typedef struct ColumnarTableDDLContext {
    char *schemaName;
    char *relationName;
    ColumnarOptions options;
} ColumnarTableDDLContext;

// contains statistics for a ChunkData ,几乎对应了 columnar.chunk表
typedef struct ColumnChunkSkipNode {
    /* statistics about values of a column chunk */
    bool hasMinMax;
    Datum minimumValue;
    Datum maximumValue;
    uint64 rowCount;

    /*
     * Offsets and sizes of value and exists streams in the column data.
     * These enable us to skip reading suppressed row chunks, and start reading
     * a chunk without reading previous chunks.
     */
    uint64 existsChunkOffset; // 在隶属的stripe中的byte偏移
    uint64 existsLength;
    uint64 valueChunkOffset;
    uint64 valueLength;

    /*
     * This is used for
     * (1) determining destination size when decompressing,
     * (2) calculating compression rates when logging stats.
     */
    uint64 decompressedValueSize;

    CompressionType valueCompressionType;
    int valueCompressionLevel;
} ColumnChunkSkipNode;


/*
 * 对应 columnCount * maxChunkCount columnar.chunk表的信息
 *
 * StripeSkipList can be used for skipping row chunks. It contains a column chunk
 * skip node for each chunk of each column. columnChunkSkipNodeArray[column][chunk]
 * is the entry for the specified column chunk.
 */
typedef struct StripeSkipList {
    ColumnChunkSkipNode **columnChunkSkipNodeArray; // columnCount * maxChunkCount 二维数组
    uint32 *chunkGroupRowCounts; // 记录各个的chunk用了多少行
    uint32 columnCount;
    uint32 chunkCount; // 当前处理过的chunk数量 也意味着是实际需要的chunk数量
} StripeSkipList;


/*
 * ChunkData represents a chunk of data for multiple columns. valueArray stores
 * the values of data, and existsArray stores whether a value is present.
 * valueBuffer is used to store (uncompressed) serialized values
 * referenced by Datum's in valueArray. It is only used for by-reference Datum's.
 * There is a one-to-one correspondence between valueArray and existsArray.
 */
typedef struct ChunkData {
    uint32 rowCount;
    uint32 columnCount;

    /*
     * Following are indexed by [column][row].
     * If a column is not projected then existsArray[column] and valueArray[column] are NULL.
     */
    bool **existsArray; // columnCount 乘以 chunkRowLimit
    Datum **valueArray; // columnCount 乘以 chunkRowLimit 读取的时候用到

    // 数量和columnCount相同,用来临时保存的
    // valueBuffer keeps actual data for type-by-reference datums from valueArray
    StringInfo *columnDataArr;
} ChunkData;


/*
 * ColumnChunkBuffers represents a chunk of serialized data in a column.
 * valueBuffer stores the serialized values of data, and existsBuffer stores
 * serialized value of presence information. valueCompressionType contains
 * compression type if valueBuffer is compressed. Finally rowCount has
 * the number of rows in this chunk.
 */
typedef struct ColumnChunkBuffers {
    StringInfo existsBuffer; // 使用了bit 0/1 代表 true/false
    StringInfo valueBuffer;
    CompressionType valueCompressionType;
    uint64 decompressedValueSize;
} ColumnChunkBuffers;


/*
 * ColumnBuffers represents data buffers for a column in a row stripe. Each
 * column is made of multiple column chunks.
 */
typedef struct ColumnBuffers {
    ColumnChunkBuffers **columnChunkBuffersArray;
} ColumnBuffers;


/* StripeBuffers 对应 data for 1行 column.stripe表 */
typedef struct StripeBuffers {
    uint32 columnCount;
    uint32 rowCount; // 起始的时候是0 记录处理过的行数
    ColumnBuffers **columnBuffersArray; // 本质也是 column数量 * chunk数量

    uint32 *selectedChunkGroupRowCounts;
} StripeBuffers;


/* return value of StripeWriteState to decide stripe write state */
typedef enum StripeWriteStateEnum {
    /* stripe write is flushed to disk, so it's readable */
    STRIPE_WRITE_FLUSHED,

    // Writer transaction did abort either before inserting into columnar.stripe or after.
    STRIPE_WRITE_ABORTED,

    /*
     * Writer transaction is still in-progress. Note that it is not certain
     * if it is being written by current backend's current transaction or another backend.
     */
    STRIPE_WRITE_IN_PROGRESS
} StripeWriteStateEnum;


/* ColumnarReadState represents state of a columnar scan. */
struct ColumnarReadState;
typedef struct ColumnarReadState ColumnarReadState;


/* ColumnarWriteState represents state of a columnar write operation. */
struct ColumnarWriteState;
typedef struct ColumnarWriteState ColumnarWriteState;

extern int columnar_compression; // zstd
extern int columnar_stripe_row_limit; // 150000
extern int columnar_chunk_group_row_limit; // 10000
extern int columnar_compression_level; // 3

extern void columnar_init_gucs(void);

extern CompressionType ParseCompressionType(const char *compressionTypeString);


extern ColumnarWriteState *ColumnarBeginWrite(RelFileNode relFileNode,
                                              ColumnarOptions columnarOptions,
                                              TupleDesc tupleDesc);

extern uint64 ColumnarWriteRow(ColumnarWriteState *columnarWriteState, Datum *rowData,
                               const bool *columnNulls);

extern void ColumnarFlushPendingWrites(ColumnarWriteState *columnarWriteState);

extern void ColumnarEndWrite(ColumnarWriteState *columnarWriteState);

extern bool ContainsPendingWrites(ColumnarWriteState *state);

extern MemoryContext ColumnarWritePerTupleContext(ColumnarWriteState *columnarWriteState);

/* Function declarations for reading from columnar table */

/* functions applicable for both sequential and random access */
extern ColumnarReadState *ColumnarBeginRead(Relation relation,
                                            TupleDesc tupleDesc,
                                            List *usedColNaturalPosList,
                                            List *qualConditions,
                                            MemoryContext scanContext,
                                            Snapshot snaphot,
                                            bool randomAccess);

extern void ColumnarReadFlushPendingWrites(ColumnarReadState *columnarReadState);

extern void ColumnarEndRead(ColumnarReadState *state);

extern void ColumnarResetRead(ColumnarReadState *readState);

/* functions only applicable for sequential access */
extern bool ColumnarReadNextRow(ColumnarReadState *columnarReadState, Datum *columnValues,
                                bool *columnNulls, uint64 *rowNumber);

extern int64 ColumnarReadChunkGroupsFiltered(ColumnarReadState *state);

extern void ColumnarRescan(ColumnarReadState *readState, List *scanQual);

/* functions only applicable for random access */
extern void ColumnarReadRowByRowNumberOrError(ColumnarReadState *readState,
                                              uint64 rowNumber, Datum *columnValues,
                                              bool *columnNulls);

extern bool ColumnarReadRowByRowNumber(ColumnarReadState *readState,
                                       uint64 rowNumber, Datum *columnValues,
                                       bool *columnNulls);

/* Function declarations for common functions */
extern FmgrInfo *GetCompareFunc(Oid typeId, Oid accessMethodId,
                                int16 procedureId);

extern ChunkData *CreateEmptyChunkData(uint32 columnCount, const bool *columnMask,
                                       uint32 chunkRowCount);

extern void FreeChunkData(ChunkData *chunkData);

extern uint64 ColumnarTableRowCount(Relation relation);

extern const char *CompressionTypeStr(CompressionType type);

/* columnar_metadata_tables.c */
extern void InitColumnarOptions(Oid regclass);

extern void SetColumnarOptions(Oid regclass, ColumnarOptions *options);

extern bool DeleteColumnarTableOptions(Oid regclass, bool missingOk);

extern bool ReadColumnarOptions(Oid regclass, ColumnarOptions *columnarOptions);

extern bool IsColumnarTableAmTable(Oid relationId);

/* columnar_metadata_tables.c */
extern void DeleteMetadataRows(RelFileNode relfilenode);

extern uint64 ColumnarMetadataNewStorageId(void);

extern uint64 GetHighestUsedAddress(RelFileNode relfilenode);

extern EmptyStripeReservation *ReserveEmptyStripe(Relation targetTable, uint64 columnCount,
                                                  uint64 chunkGroupRowLimit,
                                                  uint64 stripeRowLimit);

extern StripeMetadata *CompleteStripeReservation(Relation targetTable, uint64 stripeId,
                                                 uint64 stripeSize, uint64 stripeRowCount,
                                                 uint64 chunkCount);

extern void SaveStripeSkipList(RelFileNode relFileNode, uint64 stripeId,
                               StripeSkipList *stripeSkipList,
                               TupleDesc tupleDesc);

extern void SaveChunkGroups(RelFileNode relfilenode, uint64 stripeId,
                            List *chunkGroupRowCounts);

extern StripeSkipList *ReadStripeSkipList(RelFileNode relfilenode, uint64 stripe,
                                          TupleDesc tupleDescriptor,
                                          uint32 chunkCount,
                                          Snapshot snapshot);

extern StripeMetadata *FindNextStripeByRowNumber(Relation relation, uint64 rowNumber,
                                                 Snapshot snapshot);

extern StripeMetadata *FindStripeByRowNumber(Relation relation, uint64 rowNumber,
                                             Snapshot snapshot);

extern StripeMetadata *FindStripeWithMatchingFirstRowNumber(Relation relation,
                                                            uint64 rowNumber,
                                                            Snapshot snapshot);

extern StripeWriteStateEnum StripeWriteState(StripeMetadata *stripeMetadata);

extern uint64 StripeGetHighestRowNumber(StripeMetadata *stripeMetadata);

extern StripeMetadata *FindStripeWithHighestRowNumber(Relation relation,
                                                      Snapshot snapshot);

extern Datum columnar_relation_storageid(PG_FUNCTION_ARGS);


/* write_state_management.c */
extern ColumnarWriteState *columnar_init_write_state(Relation relation, TupleDesc
tupleDesc,
                                                     SubTransactionId currentSubXid);

extern void FlushWriteStateForRelfilenode(Oid relfilenode, SubTransactionId
currentSubXid);

extern void FlushWriteStateForAllRels(SubTransactionId currentSubXid, SubTransactionId
parentSubXid);

extern void DiscardWriteStateForAllRels(SubTransactionId currentSubXid, SubTransactionId
parentSubXid);

extern void MarkRelfilenodeDropped(Oid relfilenode, SubTransactionId currentSubXid);

extern void NonTransactionDropWriteState(Oid relfilenode);

extern bool PendingWritesInUpperTransactions(Oid relfilenode,
                                             SubTransactionId currentSubXid);

extern MemoryContext GetWriteContextForDebug(void);


#endif /* COLUMNAR_H */
