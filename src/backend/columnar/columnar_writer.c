/*-------------------------------------------------------------------------
 *
 * columnar_writer.c
 *
 * This file contains function definitions for writing columnar tables. This
 * includes the logic for writing file level metadata, writing row stripes,
 * and calculating chunk skip nodes.
 *
 * Copyright (c) 2016, Citus Data, Inc.
 *
 * $Id$
 *
 *-------------------------------------------------------------------------
 */


#include "postgres.h"

#include "safe_lib.h"

#include "access/heapam.h"
#include "access/nbtree.h"
#include "catalog/pg_am.h"
#include "miscadmin.h"
#include "storage/fd.h"
#include "storage/smgr.h"
#include "utils/guc.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "utils/relfilenodemap.h"

#include "columnar/columnar.h"
#include "columnar/columnar_storage.h"
#include "columnar/columnar_version_compat.h"

struct ColumnarWriteState {
    TupleDesc tupleDesc;
    FmgrInfo **compareFuncArr; // 数量和columnCount相同
    RelFileNode relfilenode;

    MemoryContext perTupleContext;
    MemoryContext stripeWriteContext;
    StripeBuffers *stripeBuffers; // stripe维度上的数据临时容器
    StripeSkipList *stripeSkipList;
    EmptyStripeReservation *emptyStripeReservation;
    ColumnarOptions options;
    ChunkData *chunkData; // chunk维度上的公交 数据的临时容器

    List *chunkGroupRowCounts; // 记录各个chunk的row用量

    /*
     * StringInfo 类似 bytebuffer,用来在rotate chunk数据(chunk装满的时候)临时存放压缩data的
     * compressionBuffer buffer is used as temporary storage during
     * data value compression operation. It is kept here to minimize
     * memory allocations. It lives in stripeWriteContext and gets
     * deallocated when memory context is reset.
     */
    StringInfo compressionBuffer;
};

static StripeBuffers *CreateEmptyStripeBuffers(uint32 stripeRowLimit,
                                               uint32 chunkRowLimit,
                                               uint32 columnCount);

static StripeSkipList *CreateEmptyStripeSkipList(uint32 stripeMaxRowCount,
                                                 uint32 chunkRowCount,
                                                 uint32 columnCount);

static void FlushStripe(ColumnarWriteState *columnarWriteState);

static StringInfo SerializeBoolArray(const bool *boolArray, uint32 boolArrayLength);

static void SerializeSingleDatum(StringInfo columnMultiRowDataContainer, Datum columnOneRowData,
                                 bool columnTypeIsByValue, int columnTypeLength,
                                 char columnTypeAlign);

static void SerializeChunkData(ColumnarWriteState *columnarWriteState, uint32 chunkIndex,
                               uint32 chunkUsedRowCount);

static void UpdateChunkSkipNodeMinMax(ColumnChunkSkipNode *columnChunkSkipNode,
                                      Datum columnValue, bool columnTypeIsByValue,
                                      int columnTypeLength, Oid columnCollation,
                                      FmgrInfo *compareFunc);

static Datum DatumCopy(Datum datum, bool datumTypeByValue, int datumTypeLength);

static StringInfo CopyStringInfo(StringInfo sourceString);

/*
 * ColumnarBeginWrite initializes a columnar data load operation and returns a table
 * handle. This handle should be used for adding the row values and finishing thecdata load operation.
 */
ColumnarWriteState *ColumnarBeginWrite(RelFileNode relFileNode,
                                       ColumnarOptions columnarOptions,
                                       TupleDesc tupleDesc) {

    // get comparison function pointers for each of the columns
    uint32 columnCount = tupleDesc->natts;
    FmgrInfo **compareFuncArr = palloc0(columnCount * sizeof(FmgrInfo *));

    for (uint32 columnIndex = 0; columnIndex < columnCount; columnIndex++) {
        FmgrInfo *compareFunc = NULL;


        // 得到该表的column的属性,在表 pg_attribute
        FormData_pg_attribute *columnAttribute = TupleDescAttr(tupleDesc, columnIndex);

        if (!columnAttribute->attisdropped) {
            Oid typeId = columnAttribute->atttypid;

            compareFunc = GetCompareFunc(typeId,
                                         BTREE_AM_OID,
                                         BTORDER_PROC);
        }

        compareFuncArr[columnIndex] = compareFunc;
    }

    /*
     * We allocate all stripe specific data in the stripeWriteContext, and
     * reset this memory context once we have flushed the stripe to the file.This is to avoid memory leaks.
     */
    MemoryContext stripeWriteContext = AllocSetContextCreate(CurrentMemoryContext,
                                                             "Stripe Write Memory Context",
                                                             ALLOCSET_DEFAULT_SIZES);

    bool *columnMaskArray = palloc(columnCount * sizeof(bool));
    memset(columnMaskArray, true, columnCount * sizeof(bool));

    ChunkData *chunkData = CreateEmptyChunkData(columnCount,
                                                columnMaskArray,
                                                columnarOptions.chunkRowLimit);

    ColumnarWriteState *columnarWriteState = palloc0(sizeof(ColumnarWriteState));

    columnarWriteState->relfilenode = relFileNode;
    columnarWriteState->options = columnarOptions;
    columnarWriteState->tupleDesc = CreateTupleDescCopy(tupleDesc);
    columnarWriteState->compareFuncArr = compareFuncArr;
    columnarWriteState->stripeWriteContext = stripeWriteContext;
    columnarWriteState->chunkData = chunkData;
    columnarWriteState->perTupleContext = AllocSetContextCreate(CurrentMemoryContext,
                                                                "Columnar per tuple context",
                                                                ALLOCSET_DEFAULT_SIZES);
    columnarWriteState->stripeBuffers = NULL;
    columnarWriteState->stripeSkipList = NULL;
    columnarWriteState->emptyStripeReservation = NULL;
    columnarWriteState->compressionBuffer = NULL;


    return columnarWriteState;
}


/**
 * ColumnarWriteRow adds a row to the columnar table. If the stripe is not initialized,
 * we create structures to hold stripe data and skip list. Then, we serialize and
 * append data to serialized value buffer for each of the columns and update
 * corresponding skip nodes. Then, whole chunk data is compressed at every
 * rowChunkCount insertion. Then, if row count exceeds stripeMaxRowCount, we flush
 * the stripe, and add its metadata to the table footer.
 *
 * @param columnarWriteState
 * @param rowData
 * @param columnNulls tupleTableSlot->tts_isnull
 * @return the "row number" assigned to written row
 */
uint64 ColumnarWriteRow(ColumnarWriteState *columnarWriteState,
                        Datum *rowData,
                        const bool *columnNulls) {

    StripeBuffers *stripeBuffers = columnarWriteState->stripeBuffers;
    StripeSkipList *stripeSkipList = columnarWriteState->stripeSkipList;

    uint32 columnCount = columnarWriteState->tupleDesc->natts;

    ColumnarOptions *columnarOptions = &columnarWriteState->options;
    const uint32 chunkRowLimit = columnarOptions->chunkRowLimit;


    //ChunkData *chunkData = columnarWriteState->chunkData;

    MemoryContext oldContext = MemoryContextSwitchTo(columnarWriteState->stripeWriteContext);

    if (stripeBuffers == NULL) {
        // 注入 stripeBuffers
        stripeBuffers = CreateEmptyStripeBuffers(columnarOptions->stripeRowLimit,
                                                 chunkRowLimit,
                                                 columnCount);
        columnarWriteState->stripeBuffers = stripeBuffers;

        // 注入 stripeSkipList
        stripeSkipList = CreateEmptyStripeSkipList(columnarOptions->stripeRowLimit,
                                                   chunkRowLimit,
                                                   columnCount);
        columnarWriteState->stripeSkipList = stripeSkipList;

        // 注入 compressionBuffer
        columnarWriteState->compressionBuffer = makeStringInfo();

        // 注入 emptyStripeReservation
        Oid relationId = RelidByRelfilenode(columnarWriteState->relfilenode.spcNode,
                                            columnarWriteState->relfilenode.relNode);
        Relation targetTable = relation_open(relationId, NoLock);

        columnarWriteState->emptyStripeReservation = ReserveEmptyStripe(targetTable,
                                                                        columnCount,
                                                                        chunkRowLimit,
                                                                        columnarOptions->stripeRowLimit);
        relation_close(targetTable, NoLock);

        // serializedValueBuffer lives in stripe write memory context, need be initialized when the stripe is created
        for (uint32 columnIndex = 0; columnIndex < columnCount; columnIndex++) {
            columnarWriteState->chunkData->columnDataArr[columnIndex] = makeStringInfo();
        }
    }

    uint32 chunkIndex = stripeBuffers->rowCount / chunkRowLimit;
    uint32 chunkRowIndex = stripeBuffers->rowCount % chunkRowLimit;

    // 1行data要分摊到各个的column
    for (uint32 columnIndex = 0; columnIndex < columnCount; columnIndex++) {
        ColumnChunkSkipNode **chunkSkipNodeArray = stripeSkipList->columnChunkSkipNodeArray;
        ColumnChunkSkipNode *columnChunkSkipNode = &chunkSkipNodeArray[columnIndex][chunkIndex];

        if (columnNulls[columnIndex]) {
            columnarWriteState->chunkData->existsArray[columnIndex][chunkRowIndex] = false;
        } else {
            columnarWriteState->chunkData->existsArray[columnIndex][chunkRowIndex] = true;

            FmgrInfo *compareFunc = columnarWriteState->compareFuncArr[columnIndex];

            Form_pg_attribute attributeForm = TupleDescAttr(columnarWriteState->tupleDesc, columnIndex);
            bool columnTypeIsByValue = attributeForm->attbyval;
            int columnTypeLength = attributeForm->attlen;
            char columnTypeAlign = attributeForm->attalign;
            Oid columnCollation = attributeForm->attcollation;

            // 该行当前列的data落到当前column对应的valueBufferArray
            SerializeSingleDatum(columnarWriteState->chunkData->columnDataArr[columnIndex],
                                 rowData[columnIndex],
                                 columnTypeIsByValue,
                                 columnTypeLength,
                                 columnTypeAlign);

            UpdateChunkSkipNodeMinMax(columnChunkSkipNode,
                                      rowData[columnIndex],
                                      columnTypeIsByValue,
                                      columnTypeLength,
                                      columnCollation,
                                      compareFunc);
        }

        // 这个的原因是 当前是在scan单行的各列 因为是列式保存的 columnChunkSkipNode是对应各个列上的
        columnChunkSkipNode->rowCount++;
    }

    stripeSkipList->chunkGroupCount = chunkIndex + 1;

    // 到了chunk容量临界点 rotate 到stripeBuffer对应点位
    if (chunkRowIndex == chunkRowLimit - 1) {
        SerializeChunkData(columnarWriteState, chunkIndex, chunkRowLimit);
    }

    uint64 writtenRowNumber =
            columnarWriteState->emptyStripeReservation->stripeFirstRowNumber + stripeBuffers->rowCount;

    stripeBuffers->rowCount++;

    // 到了stripe容量临界点
    if (stripeBuffers->rowCount >= columnarOptions->stripeRowLimit) {
        ColumnarFlushPendingWrites(columnarWriteState);
    }

    MemoryContextSwitchTo(oldContext);

    return writtenRowNumber;
}

// ColumnarEndWrite finishes a columnar data load operation. If we have an unflushed stripe, we flush it.
void ColumnarEndWrite(ColumnarWriteState *columnarWriteState) {
    ColumnarFlushPendingWrites(columnarWriteState);

    MemoryContextDelete(columnarWriteState->stripeWriteContext);
    pfree(columnarWriteState->compareFuncArr);
    FreeChunkData(columnarWriteState->chunkData);
    pfree(columnarWriteState);
}

void ColumnarFlushPendingWrites(ColumnarWriteState *columnarWriteState) {
    if (columnarWriteState->stripeBuffers == NULL) {
        return;
    }

    MemoryContext oldContext = MemoryContextSwitchTo(columnarWriteState->stripeWriteContext);

    FlushStripe(columnarWriteState);

    MemoryContextReset(columnarWriteState->stripeWriteContext);

    // set stripe data and skip list to NULL then they are recreated next time
    columnarWriteState->stripeBuffers = NULL;
    columnarWriteState->stripeSkipList = NULL;

    MemoryContextSwitchTo(oldContext);
}


/*
 * ColumnarWritePerTupleContext
 *
 * Return per-tuple context for columnar write operation.
 */
MemoryContext ColumnarWritePerTupleContext(ColumnarWriteState *columnarWriteState) {
    return columnarWriteState->perTupleContext;
}


/*
 * CreateEmptyStripeBuffers allocates an empty StripeBuffers structure with the given column count.
 */
static StripeBuffers *CreateEmptyStripeBuffers(uint32 stripeRowLimit,
                                               uint32 chunkRowLimit,
                                               uint32 columnCount) {

    uint32 maxChunkCount = (stripeRowLimit / chunkRowLimit) + 1;

    ColumnBuffers **columnBuffersArray = palloc0(columnCount * sizeof(ColumnBuffers *));

    // 遍历column
    for (uint32 columnIndex = 0; columnIndex < columnCount; columnIndex++) {
        ColumnChunkBuffers **columnChunkBuffersArray = palloc0(maxChunkCount * sizeof(ColumnChunkBuffers *));

        // 遍历chunk
        for (uint32 chunkIndex = 0; chunkIndex < maxChunkCount; chunkIndex++) {
            columnChunkBuffersArray[chunkIndex] = palloc0(sizeof(ColumnChunkBuffers));
            columnChunkBuffersArray[chunkIndex]->existsBuffer = NULL;
            columnChunkBuffersArray[chunkIndex]->valueBuffer = NULL;
            columnChunkBuffersArray[chunkIndex]->valueCompressionType = COMPRESSION_NONE;
        }

        columnBuffersArray[columnIndex] = palloc0(sizeof(ColumnBuffers));
        columnBuffersArray[columnIndex]->columnChunkBuffersArray = columnChunkBuffersArray;
    }

    StripeBuffers *stripeBuffers = palloc0(sizeof(StripeBuffers));

    stripeBuffers->columnBuffersArray = columnBuffersArray;
    stripeBuffers->columnCount = columnCount;
    stripeBuffers->rowCount = 0;

    return stripeBuffers;
}

/*
 * CreateEmptyStripeSkipList allocates an empty StripeSkipList structure with
 * the given column count. This structure has enough chunks to hold statistics
 * for stripeMaxRowCount rows.
 */
static StripeSkipList *CreateEmptyStripeSkipList(uint32 stripeMaxRowCount,
                                                 uint32 chunkRowCount,
                                                 uint32 columnCount) {

    uint32 maxChunkCount = (stripeMaxRowCount / chunkRowCount) + 1;

    ColumnChunkSkipNode **columnChunkSkipNodeArray = palloc0(columnCount * sizeof(ColumnChunkSkipNode *));
    for (uint32 columnIndex = 0; columnIndex < columnCount; columnIndex++) {
        columnChunkSkipNodeArray[columnIndex] = palloc0(maxChunkCount * sizeof(ColumnChunkSkipNode));
    }

    StripeSkipList *stripeSkipList = palloc0(sizeof(StripeSkipList));

    stripeSkipList->columnCount = columnCount;
    stripeSkipList->chunkGroupCount = 0;
    stripeSkipList->columnChunkSkipNodeArray = columnChunkSkipNodeArray;

    return stripeSkipList;
}

/*
 * flushes current stripe data into the file.
 * The function first ensures the last data chunk for each column is properly serialized and compressed.
 * Then the function creates the skip list and footer buffers.
 * Finally, the function flushes the skip list, data, and footer buffers to the file.
 */
static void FlushStripe(ColumnarWriteState *columnarWriteState) {
    //StripeBuffers *stripeBuffers = columnarWriteState->stripeBuffers;
    //StripeSkipList *stripeSkipList = columnarWriteState->stripeSkipList;

    uint32 columnCount = columnarWriteState->tupleDesc->natts;

    uint32 chunkGroupCount = columnarWriteState->stripeSkipList->chunkGroupCount;

    uint32 lastChunkIndex =
            columnarWriteState->stripeBuffers->rowCount / columnarWriteState->options.chunkRowLimit;
    uint32 lastChunkUsedRowCount =
            columnarWriteState->stripeBuffers->rowCount % columnarWriteState->options.chunkRowLimit;

    elog(DEBUG1, "Flushing Stripe of size %d", columnarWriteState->stripeBuffers->rowCount);

    Oid targetTableOid = RelidByRelfilenode(columnarWriteState->relfilenode.spcNode,
                                            columnarWriteState->relfilenode.relNode);
    Relation targetTable = relation_open(targetTableOid, NoLock);

    // 处理残留的尾巴
    // check if the last chunk needs serialization
    if (lastChunkUsedRowCount > 0) {
        SerializeChunkData(columnarWriteState,
                           lastChunkIndex,
                           lastChunkUsedRowCount);
    }

    // update buffer sizes in stripeSkipList
    uint64 stripeSize = 0;
    for (uint32 columnIndex = 0; columnIndex < columnCount; columnIndex++) {
        // column上的 chunkCount个 ColumnChunkSkipNode
        // ColumnChunkSkipNode *chunkSkipNodeArray = columnarWriteState->stripeSkipList->columnChunkSkipNodeArray[columnIndex];

        //ColumnBuffers *columnBuffers = stripeBuffers->columnBuffersArray[columnIndex];

        // exist体系
        for (uint32 chunkGroupIndex = 0; chunkGroupIndex < chunkGroupCount; chunkGroupIndex++) {
            // 精确到了 [column][chunk]
            ColumnChunkBuffers *columnChunkBuffers =
                    columnarWriteState->
                            stripeBuffers->
                            columnBuffersArray[columnIndex]->
                            columnChunkBuffersArray[chunkGroupIndex];

            ColumnChunkSkipNode *columnChunkSkipNode =
                    &columnarWriteState->
                            stripeSkipList->
                            columnChunkSkipNodeArray[columnIndex][chunkGroupIndex];

            columnChunkSkipNode->existsChunkOffset = stripeSize;
            columnChunkSkipNode->existsLength = columnChunkBuffers->existsBuffer->len;

            stripeSize += columnChunkBuffers->existsBuffer->len;
        }

        // value体系
        for (uint32 chunkIndex = 0; chunkIndex < chunkGroupCount; chunkIndex++) {
            // 精确到了 [column][chunk]
            ColumnChunkBuffers *columnChunkBuffers =
                    columnarWriteState->
                            stripeBuffers->
                            columnBuffersArray[columnIndex]->
                            columnChunkBuffersArray[chunkIndex];

            ColumnChunkSkipNode *columnChunkSkipNode =
                    &columnarWriteState->
                            stripeSkipList->
                            columnChunkSkipNodeArray[columnIndex][chunkIndex];

            columnChunkSkipNode->valueChunkOffset = stripeSize;
            columnChunkSkipNode->valueLength = columnChunkBuffers->valueBuffer->len;
            columnChunkSkipNode->valueCompressionType = columnChunkBuffers->valueCompressionType;
            columnChunkSkipNode->valueCompressionLevel = columnarWriteState->options.compressionLevel;
            columnChunkSkipNode->decompressedValueSize = columnChunkBuffers->decompressedValueSize;

            stripeSize += columnChunkBuffers->valueBuffer->len;
        }
    }

    // 对应相应的columnar.stripe记录 进行 update
    StripeMetadata *stripeMetadata = CompleteStripeReservation(targetTable,
                                                               columnarWriteState->emptyStripeReservation->stripeId,
                                                               stripeSize,
                                                               columnarWriteState->stripeBuffers->rowCount,
                                                               chunkGroupCount);

    // 该stripe在整个表的存储中的偏移
    uint64 currentFileOffset = stripeMetadata->fileOffset;

    /*
     * Each stripe has only one section:
     * Data section, in which we store data for each column continuously.
     * We store data for each for each column in chunks. For each chunk, we
     * store two buffers: "exists" buffer, and "value" buffer. "exists" buffer
     * tells which values are not NULL. "value" buffer contains values for
     * present values. For each column, we first store all "exists" buffers,
     * and then all "value" buffers.
     */
    //  遍历column遍历chunk flush the data buffers
    for (uint32 columnIndex = 0; columnIndex < columnCount; columnIndex++) {
        // ColumnBuffers *columnBuffers = columnarWriteState->stripeBuffers->columnBuffersArray[columnIndex];

        // exist体系
        for (uint32 chunkIndex = 0; chunkIndex < chunkGroupCount; chunkIndex++) {
            ColumnChunkBuffers *columnChunkBuffers =
                    columnarWriteState->
                            stripeBuffers->
                            columnBuffersArray[columnIndex]->
                            columnChunkBuffersArray[chunkIndex];

            ColumnarStorageWrite(targetTable,
                                 currentFileOffset,
                                 columnChunkBuffers->existsBuffer->data,
                                 columnChunkBuffers->existsBuffer->len);

            currentFileOffset += columnChunkBuffers->existsBuffer->len;
        }

        // value体系
        for (uint32 chunkIndex = 0; chunkIndex < chunkGroupCount; chunkIndex++) {

            StringInfo valueBuffer =
                    columnarWriteState->
                            stripeBuffers->
                            columnBuffersArray[columnIndex]->
                            columnChunkBuffersArray[chunkIndex]->
                            valueBuffer;

            ColumnarStorageWrite(targetTable,
                                 currentFileOffset,
                                 valueBuffer->data,
                                 valueBuffer->len);

            currentFileOffset += valueBuffer->len;
        }
    }

    // 写入columnar.chunk_group
    SaveChunkGroups(columnarWriteState->relfilenode,
                    stripeMetadata->id,
                    columnarWriteState->chunkGroupRowCounts);

    // 写入columnar.chunk
    SaveStripeSkipList(columnarWriteState->relfilenode,
                       stripeMetadata->id,
                       columnarWriteState->stripeSkipList,
                       columnarWriteState->tupleDesc);

    columnarWriteState->chunkGroupRowCounts = NIL;

    relation_close(targetTable, NoLock);
}


/*
 * SerializeBoolArray serializes the given boolean array and returns the result
 * as a StringInfo. This function packs every 8 boolean values into one byte.
 */
static StringInfo SerializeBoolArray(const bool *boolArray, uint32 boolArrayLength) {
    uint32 boolArrayIndex = 0;
    uint32 byteCount = ((boolArrayLength * sizeof(bool)) + (8 - sizeof(bool))) / 8;

    StringInfo boolArrayBuffer = makeStringInfo();
    enlargeStringInfo(boolArrayBuffer, byteCount);
    boolArrayBuffer->len = byteCount;
    memset(boolArrayBuffer->data, 0, byteCount);

    for (boolArrayIndex = 0; boolArrayIndex < boolArrayLength; boolArrayIndex++) {
        if (boolArray[boolArrayIndex]) {
            uint32 byteIndex = boolArrayIndex / 8;
            uint32 bitIndex = boolArrayIndex % 8;
            boolArrayBuffer->data[byteIndex] |= (1 << bitIndex);
        }
    }

    return boolArrayBuffer;
}


// SerializeSingleDatum serializes the given columnOneRowData value and appends it to the provided string info buffer.
static void SerializeSingleDatum(StringInfo columnMultiRowDataContainer,
                                 Datum columnOneRowData,
                                 bool columnTypeIsByValue,
                                 int columnTypeLength,
                                 char columnTypeAlign) {

    uint32 datumLength = att_addlength_datum(0, columnTypeLength, columnOneRowData);
    uint32 datumLengthAligned = att_align_nominal(datumLength, columnTypeAlign);

    enlargeStringInfo(columnMultiRowDataContainer, datumLengthAligned);

    char *currentDatumDataPointer = columnMultiRowDataContainer->data + columnMultiRowDataContainer->len;
    memset(currentDatumDataPointer, 0, datumLengthAligned);

    if (columnTypeLength > 0) {
        if (columnTypeIsByValue) {
            store_att_byval(currentDatumDataPointer, columnOneRowData, columnTypeLength);
        } else {
            memcpy_s(currentDatumDataPointer,
                     columnMultiRowDataContainer->maxlen - columnMultiRowDataContainer->len,
                     DatumGetPointer(columnOneRowData),
                     columnTypeLength);
        }
    } else {
        Assert(!columnTypeIsByValue);

        memcpy_s(currentDatumDataPointer,
                 columnMultiRowDataContainer->maxlen - columnMultiRowDataContainer->len,
                 DatumGetPointer(columnOneRowData),
                 datumLength);
    }

    columnMultiRowDataContainer->len += datumLengthAligned;
}


// 当前的chunkData装满了 要落地到 各个column 某个chunk 对应的 columnChunkBuffers 的 valueBuffer
// serializes and compresses chunk data at given chunk index with given compression type for every column.
static void SerializeChunkData(ColumnarWriteState *columnarWriteState,
                               uint32 chunkIndex,
                               uint32 chunkUsedRowCount) {

    // chunkData是总的 columnarWriteState维度
    //ChunkData *chunkData = columnarWriteState->chunkData;

    CompressionType compressionType = columnarWriteState->options.compressionType;
    int compressionLevel = columnarWriteState->options.compressionLevel;

    const uint32 columnCount = columnarWriteState->stripeBuffers->columnCount;
    StringInfo compressionBuffer = columnarWriteState->compressionBuffer;

    columnarWriteState->chunkGroupRowCounts =
            lappend_int(columnarWriteState->chunkGroupRowCounts, chunkUsedRowCount);

    /* serialize exist values, data values are already serialized */
    for (uint32 columnIndex = 0; columnIndex < columnCount; columnIndex++) {
        ColumnChunkBuffers *columnChunkBuffers =
                columnarWriteState->
                        stripeBuffers->
                        columnBuffersArray[columnIndex]->
                        columnChunkBuffersArray[chunkIndex];

        columnChunkBuffers->existsBuffer = SerializeBoolArray(columnarWriteState->chunkData->existsArray[columnIndex],
                                                              chunkUsedRowCount);
    }

    /*
     * check and compress value buffers, if a value buffer is not compressable
     * then keep it as uncompressed, store compression information.
     */
    for (uint32 columnIndex = 0; columnIndex < columnCount; columnIndex++) { // 定位到某个column下某chunk
        ColumnChunkBuffers *columnChunkBuffers =
                columnarWriteState->
                        stripeBuffers->
                        columnBuffersArray[columnIndex]->
                        columnChunkBuffersArray[chunkIndex];

        CompressionType actualCompressionType = COMPRESSION_NONE;

        StringInfo columnData = columnarWriteState->chunkData->columnDataArr[columnIndex];

        Assert(compressionType >= 0 && compressionType < COMPRESSION_COUNT);

        columnChunkBuffers->decompressedValueSize = columnarWriteState->chunkData->columnDataArr[columnIndex]->len;

        // if columnData need compressed, update columnData with compressed data and store compression type.
        bool compressed = CompressBuffer(columnData,
                                         compressionBuffer,
                                         compressionType,
                                         compressionLevel);
        if (compressed) {
            columnData = compressionBuffer;
            actualCompressionType = compressionType;
        }

        // store (compressed) value buffer
        columnChunkBuffers->valueCompressionType = actualCompressionType;
        columnChunkBuffers->valueBuffer = CopyStringInfo(columnData);

        // valueBuffer needs to be reset for next chunk's data
        resetStringInfo(columnarWriteState->chunkData->columnDataArr[columnIndex]);
    }
}


/*
 * UpdateChunkSkipNodeMinMax takes the given column value, and checks if this
 * value falls outside the range of minimum/maximum values of the given column
 * chunk skip node. If it does, the function updates the column chunk skip node
 * accordingly.
 */
static void UpdateChunkSkipNodeMinMax(ColumnChunkSkipNode *columnChunkSkipNode,
                                      Datum columnValue,
                                      bool columnTypeIsByValue,
                                      int columnTypeLength,
                                      Oid columnCollation,
                                      FmgrInfo *compareFunc) {
    bool hasMinMax = columnChunkSkipNode->hasMinMax;

    Datum previousMinimum = columnChunkSkipNode->minimumValue;
    Datum previousMaximum = columnChunkSkipNode->maximumValue;

    Datum currentMinimum;
    Datum currentMaximum;

    // if type doesn't have a comparison function, skip min/max values
    if (compareFunc == NULL) {
        return;
    }

    if (!hasMinMax) {
        currentMinimum = DatumCopy(columnValue, columnTypeIsByValue, columnTypeLength);
        currentMaximum = DatumCopy(columnValue, columnTypeIsByValue, columnTypeLength);
    } else {
        // 和之前最小值比
        Datum minimumComparisonDatum = FunctionCall2Coll(compareFunc,
                                                         columnCollation,
                                                         columnValue,
                                                         previousMinimum);
        int minCompareResult = DatumGetInt32(minimumComparisonDatum);

        // 和之前最大值比
        Datum maximumComparisonDatum = FunctionCall2Coll(compareFunc,
                                                         columnCollation,
                                                         columnValue,
                                                         previousMaximum);
        int maxCompareResult = DatumGetInt32(maximumComparisonDatum);

        // 说明要比之前的最小值还要小 是新的最小的值
        if (minCompareResult < 0) {
            currentMinimum = DatumCopy(columnValue, columnTypeIsByValue, columnTypeLength);
        } else { // 保持原样
            currentMinimum = previousMinimum;
        }

        // 说明要比之前最大值还要大 是新的最大的值
        if (maxCompareResult > 0) {
            currentMaximum = DatumCopy(columnValue, columnTypeIsByValue, columnTypeLength);
        } else { // 保持原样
            currentMaximum = previousMaximum;
        }
    }

    columnChunkSkipNode->hasMinMax = true;
    columnChunkSkipNode->minimumValue = currentMinimum;
    columnChunkSkipNode->maximumValue = currentMaximum;
}


/* Creates a copy of the given datum. */
static Datum
DatumCopy(Datum datum, bool datumTypeByValue, int datumTypeLength) {
    Datum datumCopy = 0;

    if (datumTypeByValue) {
        datumCopy = datum;
    } else {
        uint32 datumLength = att_addlength_datum(0, datumTypeLength, datum);
        char *datumData = palloc0(datumLength);
        memcpy_s(datumData, datumLength, DatumGetPointer(datum), datumLength);

        datumCopy = PointerGetDatum(datumData);
    }

    return datumCopy;
}


/*
 * CopyStringInfo creates a deep copy of given source string allocating only needed
 * amount of memory.
 */
static StringInfo
CopyStringInfo(StringInfo sourceString) {
    StringInfo targetString = palloc0(sizeof(StringInfoData));

    if (sourceString->len > 0) {
        targetString->data = palloc0(sourceString->len);
        targetString->len = sourceString->len;
        targetString->maxlen = sourceString->len;
        memcpy_s(targetString->data, sourceString->len,
                 sourceString->data, sourceString->len);
    }

    return targetString;
}


bool
ContainsPendingWrites(ColumnarWriteState *state) {
    return state->stripeBuffers != NULL && state->stripeBuffers->rowCount != 0;
}
