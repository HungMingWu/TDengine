/*
 * Copyright (c) 2019 TAOS Data, Inc. <jhtao@taosdata.com>
 *
 * This program is free software: you can use, redistribute, and/or modify
 * it under the terms of the GNU Affero General Public License, version 3
 * or later ("AGPL"), as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
#include <memory>
#include "tdataformat.h"
#include "tulog.h"
#include "talgo.h"
#include "tcoding.h"
#include "wchar.h"

static void tdMergeTwoDataCols(SDataCols *target, SDataCols *src1, int *iter1, int limit1, SDataCols *src2, int *iter2,
                               int limit2, int tRows);

/**
 * Duplicate the schema and return a new object
 */
STSchema *tdDupSchema(STSchema *pSchema) {

  int tlen = sizeof(STSchema) + sizeof(STColumn) * schemaNCols(pSchema);
  STSchema *tSchema = (STSchema *)malloc(tlen);
  if (tSchema == NULL) return NULL;

  memcpy((void *)tSchema, (void *)pSchema, tlen);

  return tSchema;
}

/**
 * Encode a schema to dst, and return the next pointer
 */
int tdEncodeSchema(void **buf, STSchema *pSchema) {
  int tlen = 0;
  tlen += taosEncodeFixedI32(buf, schemaVersion(pSchema));
  tlen += taosEncodeFixedI32(buf, schemaNCols(pSchema));

  for (int i = 0; i < schemaNCols(pSchema); i++) {
    STColumn *pCol = schemaColAt(pSchema, i);
    tlen += taosEncodeFixedI8(buf, colType(pCol));
    tlen += taosEncodeFixedI16(buf, colColId(pCol));
    tlen += taosEncodeFixedI16(buf, colBytes(pCol));
  }

  return tlen;
}

/**
 * Decode a schema from a binary.
 */
void *tdDecodeSchema(void *buf, STSchema **pRSchema) {
  int32_t version = 0;
  int numOfCols = 0;
  STSchemaBuilder schemaBuilder{version};

  buf = taosDecodeFixedI32(buf, &version);
  buf = taosDecodeFixedI32(buf, &numOfCols);

  for (int i = 0; i < numOfCols; i++) {
    int8_t  type = 0;
    int16_t colId = 0;
    int16_t bytes = 0;
    buf = taosDecodeFixedI8(buf, &type);
    buf = taosDecodeFixedI16(buf, &colId);
    buf = taosDecodeFixedI16(buf, &bytes);
    if (tdAddColToSchema(&schemaBuilder, type, colId, bytes) < 0) {
      return NULL;
    }
  }

  *pRSchema = tdGetSchemaFromBuilder(&schemaBuilder);
  return buf;
}

int tdAddColToSchema(STSchemaBuilder *pBuilder, int8_t type, int16_t colId, int16_t bytes) {
  if (!isValidDataType(type)) return -1;

  int16_t offset = [pBuilder]() {
    if (pBuilder->columns.empty()) return 0;
    auto last = pBuilder->columns.back();
    return last.offset + TYPE_BYTES[last.type];
  }();
  int16_t newbytes = IS_VAR_DATA_TYPE(type) ? bytes : TYPE_BYTES[type];
  pBuilder->columns.push_back({type, colId, newbytes, offset});

  if (IS_VAR_DATA_TYPE(type)) {
    pBuilder->tlen += (TYPE_BYTES[type] + bytes);
    pBuilder->vlen += bytes - sizeof(VarDataLenT);
  } else {
    pBuilder->tlen += TYPE_BYTES[type];
    pBuilder->vlen += TYPE_BYTES[type];
  }

  pBuilder->flen += TYPE_BYTES[type];

  ASSERT(pBuilder->columns.back().offset < pBuilder->flen);

  return 0;
}

STSchema *tdGetSchemaFromBuilder(STSchemaBuilder *pBuilder) {
  if (pBuilder->columns.empty()) return NULL;

  int tlen = sizeof(STSchema) + sizeof(STColumn) * pBuilder->columns.size();

  STSchema *pSchema = (STSchema *)malloc(tlen);
  if (pSchema == NULL) return NULL;

  schemaVersion(pSchema) = pBuilder->version;
  schemaNCols(pSchema) = pBuilder->columns.size();
  schemaTLen(pSchema) = pBuilder->tlen;
  schemaFLen(pSchema) = pBuilder->flen;
  schemaVLen(pSchema) = pBuilder->vlen;

  memcpy(schemaColAt(pSchema, 0), &pBuilder->columns[0], sizeof(STColumn) * pBuilder->columns.size());

  return pSchema;
}

/**
 * Initialize a data row
 */
void tdInitDataRow(SDataRow row, STSchema *pSchema) {
  dataRowSetLen(row, TD_DATA_ROW_HEAD_SIZE + schemaFLen(pSchema));
  dataRowSetVersion(row, schemaVersion(pSchema));
}

SDataRow tdNewDataRowFromSchema(STSchema *pSchema) {
  int32_t size = dataRowMaxBytesFromSchema(pSchema);

  SDataRow row = malloc(size);
  if (row == NULL) return NULL;

  tdInitDataRow(row, pSchema);
  return row;
}

/**
 * Free the SDataRow object
 */
void tdFreeDataRow(SDataRow row) {
  if (row) free(row);
}

SDataRow tdDataRowDup(SDataRow row) {
  SDataRow trow = malloc(dataRowLen(row));
  if (trow == NULL) return NULL;

  dataRowCpy(trow, row);
  return trow;
}

void dataColInit(SDataCol *pDataCol, STColumn *pCol, void **pBuf, int maxPoints) {
  pDataCol->type = colType(pCol);
  pDataCol->colId = colColId(pCol);
  pDataCol->bytes = colBytes(pCol);
  pDataCol->offset = colOffset(pCol) + TD_DATA_ROW_HEAD_SIZE;

  pDataCol->len = 0;
  if (IS_VAR_DATA_TYPE(pDataCol->type)) {
    pDataCol->dataOff = (VarDataOffsetT *)(*pBuf);
    pDataCol->pData = POINTER_SHIFT(*pBuf, sizeof(VarDataOffsetT) * maxPoints);
    pDataCol->spaceSize = pDataCol->bytes * maxPoints;
    *pBuf = POINTER_SHIFT(*pBuf, pDataCol->spaceSize + sizeof(VarDataOffsetT) * maxPoints);
  } else {
    pDataCol->spaceSize = pDataCol->bytes * maxPoints;
    pDataCol->dataOff = NULL;
    pDataCol->pData = *pBuf;
    *pBuf = POINTER_SHIFT(*pBuf, pDataCol->spaceSize);
  }
}

// value from timestamp should be TKEY here instead of TSKEY
void SDataCol::appendVal(void *value, int numOfRows, int maxPoints) {
  ASSERT(value != NULL);

  if (IS_VAR_DATA_TYPE(type)) {
    // set offset
    dataOff[numOfRows] = len;
    // Copy data
    memcpy(POINTER_SHIFT(pData, len), value, varDataTLen(value));
    // Update the length
    len += varDataTLen(value);
  } else {
    ASSERT(len == TYPE_BYTES[type] * numOfRows);
    memcpy(POINTER_SHIFT(pData, len), value, bytes);
    len += bytes;
  }
}

bool isNEleNull(SDataCol *pCol, int nEle) {
  for (int i = 0; i < nEle; i++) {
    if (!isNull((char *)pCol->getDataOfRow(i), pCol->type)) return false;
  }
  return true;
}

void dataColSetNullAt(SDataCol *pCol, int index) {
  if (IS_VAR_DATA_TYPE(pCol->type)) {
    pCol->dataOff[index] = pCol->len;
    char *ptr = (char*)POINTER_SHIFT(pCol->pData, pCol->len);
    setVardataNull(ptr, pCol->type);
    pCol->len += varDataTLen(ptr);
  } else {
    setNull((char *)POINTER_SHIFT(pCol->pData, TYPE_BYTES[pCol->type] * index), pCol->type, pCol->bytes);
    pCol->len += TYPE_BYTES[pCol->type];
  }
}

void dataColSetNEleNull(SDataCol *pCol, int nEle, int maxPoints) {

  if (IS_VAR_DATA_TYPE(pCol->type)) {
    pCol->len = 0;
    for (int i = 0; i < nEle; i++) {
      dataColSetNullAt(pCol, i);
    }
  } else {
    setNullN((char*)pCol->pData, pCol->type, pCol->bytes, nEle);
    pCol->len = TYPE_BYTES[pCol->type] * nEle;
  }
}

void SDataCol::setOffset(int nEle) {
  ASSERT(((type == TSDB_DATA_TYPE_BINARY) || (type == TSDB_DATA_TYPE_NCHAR)));

  void *tptr = pData;
  // char *tptr = (char *)(pCol->pData);

  VarDataOffsetT offset = 0;
  for (int i = 0; i < nEle; i++) {
    dataOff[i] = offset;
    offset += varDataTLen(tptr);
    tptr = POINTER_SHIFT(tptr, varDataTLen(tptr));
  }
}

std::unique_ptr<SDataCols> tdNewDataCols(int maxRowSize, int maxCols, int maxRows) {
  std::unique_ptr<SDataCols> pCols(new SDataCols);
  if (pCols == NULL) {
    uDebug("malloc failure, size:%" PRId64 " failed, reason:%s", (int64_t)sizeof(SDataCols), strerror(errno));
    return NULL;
  }

  pCols->cols.resize(maxCols);
  pCols->maxRowSize = maxRowSize;
  pCols->maxPoints = maxRows;
  pCols->bufSize = maxRowSize * maxRows;

  pCols->buf = malloc(pCols->bufSize);
  if (pCols->buf == NULL) {
    uDebug("malloc failure, size:%" PRId64 " failed, reason:%s", (int64_t)sizeof(SDataCol) * maxCols, strerror(errno));
    return nullptr;
  }

  return pCols;
}

int tdInitDataCols(SDataCols *pCols, STSchema *pSchema) {
  pCols->cols.resize(schemaNCols(pSchema));

  if (schemaTLen(pSchema) > pCols->maxRowSize) {
    pCols->maxRowSize = schemaTLen(pSchema);
    pCols->bufSize = schemaTLen(pSchema) * pCols->maxPoints;
    pCols->buf = realloc(pCols->buf, pCols->bufSize);
    if (pCols->buf == NULL) return -1;
  }

  tdResetDataCols(pCols);

  void *ptr = pCols->buf;
  for (int i = 0; i < schemaNCols(pSchema); i++) {
    dataColInit(&pCols->cols[i], schemaColAt(pSchema, i), &ptr, pCols->maxPoints);
    ASSERT((char *)ptr - (char *)(pCols->buf) <= pCols->bufSize);
  }
  
  return 0;
}

SDataCols::~SDataCols() {
  tfree(buf);
}

std::unique_ptr<SDataCols> tdDupDataCols(SDataCols *pDataCols, bool keepData) {
  auto pRet = tdNewDataCols(pDataCols->maxRowSize, pDataCols->cols.size(), pDataCols->maxPoints);
  if (!pRet) return nullptr;

  pRet->sversion = pDataCols->sversion;
  if (keepData) pRet->numOfRows = pDataCols->numOfRows;

  for (int i = 0; i < pDataCols->cols.size(); i++) {
    pRet->cols[i].type = pDataCols->cols[i].type;
    pRet->cols[i].colId = pDataCols->cols[i].colId;
    pRet->cols[i].bytes = pDataCols->cols[i].bytes;
    pRet->cols[i].offset = pDataCols->cols[i].offset;

    pRet->cols[i].spaceSize = pDataCols->cols[i].spaceSize;
    pRet->cols[i].pData = (void *)((char *)pRet->buf + ((char *)(pDataCols->cols[i].pData) - (char *)(pDataCols->buf)));

    if (IS_VAR_DATA_TYPE(pRet->cols[i].type)) {
      ASSERT(pDataCols->cols[i].dataOff != NULL);
      pRet->cols[i].dataOff =
          (int32_t *)((char *)pRet->buf + ((char *)(pDataCols->cols[i].dataOff) - (char *)(pDataCols->buf)));
    }

    if (keepData) {
      pRet->cols[i].len = pDataCols->cols[i].len;
      if (pDataCols->cols[i].len > 0) {
        memcpy(pRet->cols[i].pData, pDataCols->cols[i].pData, pDataCols->cols[i].len);
        if (IS_VAR_DATA_TYPE(pRet->cols[i].type)) {
          memcpy(pRet->cols[i].dataOff, pDataCols->cols[i].dataOff, sizeof(VarDataOffsetT) * pDataCols->maxPoints);
        }
      }
    }
  }

  return pRet;
}

void tdResetDataCols(SDataCols *pCols) {
  if (pCols != NULL) {
    pCols->numOfRows = 0;
    for (auto &col : pCols->cols)
      col.reset();
  }
}

void tdAppendDataRowToDataCol(SDataRow row, STSchema *pSchema, SDataCols *pCols) {
  ASSERT(pCols->numOfRows == 0 || dataColsKeyLast(pCols) < dataRowKey(row));

  int rcol = 0;
  int dcol = 0;

  if (dataRowDeleted(row)) {
    for (; dcol < pCols->cols.size(); dcol++) {
      SDataCol *pDataCol = &(pCols->cols[dcol]);
      if (dcol == 0) {
        pDataCol->appendVal(dataRowTuple(row), pCols->numOfRows, pCols->maxPoints);
      } else {
        dataColSetNullAt(pDataCol, pCols->numOfRows);
      }
    }
  } else {
    while (dcol < pCols->cols.size()) {
      SDataCol *pDataCol = &(pCols->cols[dcol]);
      if (rcol >= schemaNCols(pSchema)) {
        dataColSetNullAt(pDataCol, pCols->numOfRows);
        dcol++;
        continue;
      }

      STColumn *pRowCol = schemaColAt(pSchema, rcol);
      if (pRowCol->colId == pDataCol->colId) {
        void *value = tdGetRowDataOfCol(row, pRowCol->type, pRowCol->offset + TD_DATA_ROW_HEAD_SIZE);
        pDataCol->appendVal(value, pCols->numOfRows, pCols->maxPoints);
        dcol++;
        rcol++;
      } else if (pRowCol->colId < pDataCol->colId) {
        rcol++;
      } else {
        dataColSetNullAt(pDataCol, pCols->numOfRows);
        dcol++;
      }
    }
  }
  pCols->numOfRows++;
}

int tdMergeDataCols(SDataCols *target, SDataCols *source, int rowsToMerge) {
  ASSERT(rowsToMerge > 0 && rowsToMerge <= source->numOfRows);
  ASSERT(target->cols.size() == source->cols.size());

  if (dataColsKeyLast(target) < dataColsKeyFirst(source)) {  // No overlap
    ASSERT(target->numOfRows + rowsToMerge <= target->maxPoints);
    for (int i = 0; i < rowsToMerge; i++) {
      for (int j = 0; j < source->cols.size(); j++) {
        if (source->cols[j].len > 0) {
          target->cols[j].appendVal(source->cols[j].getDataOfRow(i), target->numOfRows,
                           target->maxPoints);
        }
      }
      target->numOfRows++;
    }
  } else {
    auto pTarget = tdDupDataCols(target, true);
    if (!pTarget) return -1;

    int iter1 = 0;
    int iter2 = 0;
    tdMergeTwoDataCols(target, pTarget.get(), &iter1, pTarget->numOfRows, source, &iter2, source->numOfRows,
                       pTarget->numOfRows + rowsToMerge);
  }

  return 0;
}

// src2 data has more priority than src1
static void tdMergeTwoDataCols(SDataCols *target, SDataCols *src1, int *iter1, int limit1, SDataCols *src2, int *iter2,
                               int limit2, int tRows) {
  tdResetDataCols(target);
  ASSERT(limit1 <= src1->numOfRows && limit2 <= src2->numOfRows);

  while (target->numOfRows < tRows) {
    if (*iter1 >= limit1 && *iter2 >= limit2) break;

    TSKEY key1 = (*iter1 >= limit1) ? INT64_MAX : dataColsKeyAt(src1, *iter1);
    TKEY  tkey1 = (*iter1 >= limit1) ? TKEY_NULL : dataColsTKeyAt(src1, *iter1);
    TSKEY key2 = (*iter2 >= limit2) ? INT64_MAX : dataColsKeyAt(src2, *iter2);
    TKEY  tkey2 = (*iter2 >= limit2) ? TKEY_NULL : dataColsTKeyAt(src2, *iter2);

    ASSERT(tkey1 == TKEY_NULL || (!TKEY_IS_DELETED(tkey1)));

    if (key1 < key2) {
      for (int i = 0; i < src1->cols.size(); i++) {
        ASSERT(target->cols[i].type == src1->cols[i].type);
        if (src1->cols[i].len > 0) {
          target->cols[i].appendVal(src1->cols[i].getDataOfRow(*iter1),
              target->numOfRows, target->maxPoints);
        }
      }

      target->numOfRows++;
      (*iter1)++;
    } else if (key1 >= key2) {
      if ((key1 > key2) || (key1 == key2 && !TKEY_IS_DELETED(tkey2))) {
        for (int i = 0; i < src2->cols.size(); i++) {
          ASSERT(target->cols[i].type == src2->cols[i].type);
          if (src2->cols[i].len > 0) {
            target->cols[i].appendVal(src2->cols[i].getDataOfRow(*iter2),
                target->numOfRows, target->maxPoints);
          }
        }
        target->numOfRows++;
      }

      (*iter2)++;
      if (key1 == key2) (*iter1)++;
    }

    ASSERT(target->numOfRows <= target->maxPoints);
  }
}

SKVRow tdKVRowDup(SKVRow row) {
  SKVRow trow = malloc(kvRowLen(row));
  if (trow == NULL) return NULL;

  kvRowCpy(trow, row);
  return trow;
}

static int compareColIdx(const void* a, const void* b) {
  const SColIdx* x = (const SColIdx*)a;
  const SColIdx* y = (const SColIdx*)b;
  if (x->colId > y->colId) {
    return 1;
  }
  if (x->colId < y->colId) {
    return -1;
  }
  return 0;
}

void tdSortKVRowByColIdx(SKVRow row) {
  qsort(kvRowColIdx(row), kvRowNCols(row), sizeof(SColIdx), compareColIdx);
}

int tdSetKVRowDataOfCol(SKVRow *orow, int16_t colId, int8_t type, void *value) {
  SColIdx *pColIdx = NULL;
  SKVRow   row = *orow;
  SKVRow   nrow = NULL;
  void *   ptr = taosbsearch(&colId, kvRowColIdx(row), kvRowNCols(row), sizeof(SColIdx), comparTagId, TD_GE);

  if (ptr == NULL || ((SColIdx *)ptr)->colId > colId) { // need to add a column value to the row
    int diff = IS_VAR_DATA_TYPE(type) ? varDataTLen(value) : TYPE_BYTES[type];
    nrow = malloc(kvRowLen(row) + sizeof(SColIdx) + diff);
    if (nrow == NULL) return -1;

    kvRowSetLen(nrow, kvRowLen(row) + (int16_t)sizeof(SColIdx) + diff);
    kvRowSetNCols(nrow, kvRowNCols(row) + 1);

    if (ptr == NULL) {
      memcpy(kvRowColIdx(nrow), kvRowColIdx(row), sizeof(SColIdx) * kvRowNCols(row));
      memcpy(kvRowValues(nrow), kvRowValues(row), POINTER_DISTANCE(kvRowEnd(row), kvRowValues(row)));
      int colIdx = kvRowNCols(nrow) - 1;
      kvRowColIdxAt(nrow, colIdx)->colId = colId;
      kvRowColIdxAt(nrow, colIdx)->offset = (int16_t)(POINTER_DISTANCE(kvRowEnd(row), kvRowValues(row)));
      memcpy(kvRowColVal(nrow, kvRowColIdxAt(nrow, colIdx)), value, diff);
    } else {
      int16_t tlen = (int16_t)(POINTER_DISTANCE(ptr, kvRowColIdx(row)));
      if (tlen > 0) {
        memcpy(kvRowColIdx(nrow), kvRowColIdx(row), tlen);
        memcpy(kvRowValues(nrow), kvRowValues(row), ((SColIdx *)ptr)->offset);
      }

      int colIdx = tlen / sizeof(SColIdx);
      kvRowColIdxAt(nrow, colIdx)->colId = colId;
      kvRowColIdxAt(nrow, colIdx)->offset = ((SColIdx *)ptr)->offset;
      memcpy(kvRowColVal(nrow, kvRowColIdxAt(nrow, colIdx)), value, diff);

      for (int i = colIdx; i < kvRowNCols(row); i++) {
        kvRowColIdxAt(nrow, i + 1)->colId = kvRowColIdxAt(row, i)->colId;
        kvRowColIdxAt(nrow, i + 1)->offset = kvRowColIdxAt(row, i)->offset + diff;
      }
      memcpy(kvRowColVal(nrow, kvRowColIdxAt(nrow, colIdx + 1)), kvRowColVal(row, kvRowColIdxAt(row, colIdx)),
             POINTER_DISTANCE(kvRowEnd(row), kvRowColVal(row, kvRowColIdxAt(row, colIdx)))

      );
    }

    *orow = nrow;
    free(row);
  } else {
    ASSERT(((SColIdx *)ptr)->colId == colId);
    if (IS_VAR_DATA_TYPE(type)) {
      void *pOldVal = kvRowColVal(row, (SColIdx *)ptr);

      if (varDataTLen(value) == varDataTLen(pOldVal)) { // just update the column value in place
        memcpy(pOldVal, value, varDataTLen(value));
      } else { // need to reallocate the memory
        int16_t diff = varDataTLen(value) - varDataTLen(pOldVal);
        int16_t nlen = kvRowLen(row) + diff;
        ASSERT(nlen > 0);
        nrow = malloc(nlen);
        if (nrow == NULL) return -1;

        kvRowSetLen(nrow, nlen);
        kvRowSetNCols(nrow, kvRowNCols(row));

        // Copy part ahead
        nlen = (int16_t)(POINTER_DISTANCE(ptr, kvRowColIdx(row)));
        ASSERT(nlen % sizeof(SColIdx) == 0);
        if (nlen > 0) {
          ASSERT(((SColIdx *)ptr)->offset > 0);
          memcpy(kvRowColIdx(nrow), kvRowColIdx(row), nlen);
          memcpy(kvRowValues(nrow), kvRowValues(row), ((SColIdx *)ptr)->offset);
        }

        // Construct current column value
        int colIdx = nlen / sizeof(SColIdx);
        pColIdx = kvRowColIdxAt(nrow, colIdx);
        pColIdx->colId = ((SColIdx *)ptr)->colId;
        pColIdx->offset = ((SColIdx *)ptr)->offset;
        memcpy(kvRowColVal(nrow, pColIdx), value, varDataTLen(value));
 
        // Construct columns after
        if (kvRowNCols(nrow) - colIdx - 1 > 0) {
          for (int i = colIdx + 1; i < kvRowNCols(nrow); i++) {
            kvRowColIdxAt(nrow, i)->colId = kvRowColIdxAt(row, i)->colId;
            kvRowColIdxAt(nrow, i)->offset = kvRowColIdxAt(row, i)->offset + diff;
          }
          memcpy(kvRowColVal(nrow, kvRowColIdxAt(nrow, colIdx + 1)), kvRowColVal(row, kvRowColIdxAt(row, colIdx + 1)),
                 POINTER_DISTANCE(kvRowEnd(row), kvRowColVal(row, kvRowColIdxAt(row, colIdx + 1))));
        }

        *orow = nrow;
        free(row);
      }
    } else {
      memcpy(kvRowColVal(row, (SColIdx *)ptr), value, TYPE_BYTES[type]);
    }
  }

  return 0;
}

int tdEncodeKVRow(void **buf, SKVRow row) {
  // May change the encode purpose
  if (buf != NULL) {
    kvRowCpy(*buf, row);
    *buf = POINTER_SHIFT(*buf, kvRowLen(row));
  }

  return kvRowLen(row);
}

void *tdDecodeKVRow(void *buf, SKVRow *row) {
  *row = tdKVRowDup(buf);
  if (*row == NULL) return NULL;
  return POINTER_SHIFT(buf, kvRowLen(*row));
}

int tdInitKVRowBuilder(SKVRowBuilder *pBuilder) {
  pBuilder->alloc = 1024;
  pBuilder->size = 0;
  pBuilder->buf = malloc(pBuilder->alloc);
  if (pBuilder->buf == NULL) {
    return -1;
  }
  return 0;
}

void tdDestroyKVRowBuilder(SKVRowBuilder *pBuilder) {
  tfree(pBuilder->buf);
}

void tdResetKVRowBuilder(SKVRowBuilder *pBuilder) {
  pBuilder->pColIdx.clear();
  pBuilder->size = 0;
}

SKVRow tdGetKVRowFromBuilder(SKVRowBuilder *pBuilder) {
  int tlen = sizeof(SColIdx) * pBuilder->pColIdx.size() + pBuilder->size;
  if (tlen == 0) return NULL;

  tlen += TD_KV_ROW_HEAD_SIZE;

  SKVRow row = malloc(tlen);
  if (row == NULL) return NULL;

  kvRowSetNCols(row, pBuilder->pColIdx.size());
  kvRowSetLen(row, tlen);

  memcpy(kvRowColIdx(row), &pBuilder->pColIdx[0], sizeof(SColIdx) * pBuilder->pColIdx.size());
  memcpy(kvRowValues(row), pBuilder->buf, pBuilder->size);

  return row;
}
