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

#include "os.h"

#include "taosdef.h"
#include "taosmsg.h"
#include "tsqlfunction.h"
#include "ttype.h"

#include "qFill.h"
#include "qExtbuffer.h"
#include "queryLog.h"

#define DO_INTERPOLATION(_v1, _v2, _k1, _k2, _k) ((_v1) + ((_v2) - (_v1)) * (((double)(_k)) - ((double)(_k1))) / (((double)(_k2)) - ((double)(_k1))))

void SFillInfo::setTagsValue(tFilePage** data, int32_t genRows) {
  for(int32_t j = 0; j < numOfCols; ++j) {
    SFillColInfo* pCol = &pFillCol[j];
    if (TSDB_COL_IS_NORMAL_COL(pCol->flag)) {
      continue;
    }

    char* val1 = (char*)elePtrAt(data[j]->data, pCol->col.bytes, genRows);

    assert(pCol->tagIndex >= 0 && pCol->tagIndex < numOfTags);
    SFillTagColInfo* pTag = &pTags[pCol->tagIndex];

    assert (pTag->col.colId == pCol->col.colId);
    assignVal(val1, pTag->tagVal, pCol->col.bytes, pCol->col.type);
  }
}

void SFillInfo::setNullValueForRow(tFilePage** data, int32_t numOfCol, int32_t rowIndex) {
  // the first are always the timestamp column, so start from the second column.
  for (int32_t i = 1; i < numOfCol; ++i) {
    SFillColInfo* pCol = &pFillCol[i];

    char* output = (char*)elePtrAt(data[i]->data, pCol->col.bytes, rowIndex);
    setNull(output, pCol->col.type, pCol->col.bytes);
  }
}

void SFillInfo::doFillOneRowResult(tFilePage** data, char** srcData, int64_t ts, bool outOfBound) {
  char* prev = prevValues.data();
  char* next = nextValues.data();

  SPoint point1, point2, point;
  int32_t step = GET_FORWARD_DIRECTION_FACTOR(order);

  // set the primary timestamp column value
  int32_t index = numOfCurrent;
  char* val = (char*)elePtrAt(data[0]->data, TSDB_KEYSIZE, index);
  *(TSKEY*) val = currentKey;

  // set the other values
  if (type == TSDB_FILL_PREV) {
    char* p = isASCOrder() ? prevValues.data() : next;

    if (p != NULL) {
      for (int32_t i = 1; i < numOfCols; ++i) {
        SFillColInfo* pCol = &pFillCol[i];
        if (TSDB_COL_IS_TAG(pCol->flag)) {
          continue;
        }

        char* output = (char*)elePtrAt(data[i]->data, pCol->col.bytes, index);
        assignVal(output, p + pCol->col.offset, pCol->col.bytes, pCol->col.type);
      }
    } else {  // no prev value yet, set the value for NULL
      setNullValueForRow(data, numOfCols, index);
    }
  } else if (type == TSDB_FILL_NEXT) {
    char* p = isASCOrder() ? next : prev;

    if (p != NULL) {
      for (int32_t i = 1; i < numOfCols; ++i) {
        SFillColInfo* pCol = &pFillCol[i];
        if (TSDB_COL_IS_TAG(pCol->flag)) {
          continue;
        }

        char* output = (char*)elePtrAt(data[i]->data, pCol->col.bytes, index);
        assignVal(output, p + pCol->col.offset, pCol->col.bytes, pCol->col.type);
      }
    } else { // no prev value yet, set the value for NULL
      setNullValueForRow(data, numOfCols, index);
    }
  } else if (type == TSDB_FILL_LINEAR) {
    // TODO : linear interpolation supports NULL value
    if (prev != NULL && !outOfBound) {
      for (int32_t i = 1; i < numOfCols; ++i) {
        SFillColInfo* pCol = &pFillCol[i];
        if (TSDB_COL_IS_TAG(pCol->flag)) {
          continue;
        }

        int16_t type  = pCol->col.type;
        int16_t bytes = pCol->col.bytes;

        char *val1 = (char*)elePtrAt(data[i]->data, pCol->col.bytes, index);
        if (type == TSDB_DATA_TYPE_BINARY|| type == TSDB_DATA_TYPE_NCHAR || type == TSDB_DATA_TYPE_BOOL) {
          setNull(val1, pCol->col.type, bytes);
          continue;
        }

        point1 = (SPoint){.key = *(TSKEY*)(prev), .val = prev + pCol->col.offset};
        point2 = (SPoint){.key = ts, .val = srcData[i] + index * bytes};
        point  = (SPoint){.key = currentKey, .val = val1};
        taosGetLinearInterpolationVal(type, &point1, &point2, &point);
      }
    } else {
      setNullValueForRow(data, numOfCols, index);
    }
  } else { /* fill the default value */
    for (int32_t i = 1; i < numOfCols; ++i) {
      SFillColInfo* pCol = &pFillCol[i];
      if (TSDB_COL_IS_TAG(pCol->flag)) {
        continue;
      }

      char* val1 = (char*)elePtrAt(data[i]->data, pCol->col.bytes, index);
      assignVal(val1, (char*)&pCol->fillVal.i, pCol->col.bytes, pCol->col.type);
    }
  }

  setTagsValue(data, index);
  currentKey = taosTimeAdd(currentKey, interval.sliding * step, interval.slidingUnit, precision);
  numOfCurrent++;
}

void SFillInfo::initBeforeAfterDataBuf(std::vector<char> &next) {
  next.resize(rowSize);
  for (int i = 1; i < numOfCols; i++) {
    SFillColInfo* pCol = &pFillCol[i];
    setNull(&next[pCol->col.offset], pCol->col.type, pCol->col.bytes);
  }
}

void SFillInfo::copyCurrentRowIntoBuf(char** srcData, std::vector<char> &buf) {
  int32_t rowIndex = index;
  for (int32_t i = 0; i < numOfCols; ++i) {
    SFillColInfo* pCol = &pFillCol[i];
    memcpy(&buf[pCol->col.offset], srcData[i] + rowIndex * pCol->col.bytes, pCol->col.bytes);
  }
}

int32_t SFillInfo::fillResultImpl(tFilePage** data, int32_t outputRows) {
  numOfCurrent = 0;

  char** srcData = pData;
  auto& prev = prevValues;
  auto& next = nextValues;

  int32_t step = GET_FORWARD_DIRECTION_FACTOR(order);

  if (isASCOrder()) {
    assert(currentKey >= start);
  } else {
    assert(currentKey <= start);
  }

  while (numOfCurrent < outputRows) {
    int64_t ts = ((int64_t*)pData[0])[index];

    // set the next value for interpolation
    if ((currentKey < ts && isASCOrder()) ||
        (currentKey > ts && !isASCOrder())) {
      initBeforeAfterDataBuf(next);
      copyCurrentRowIntoBuf(srcData, next);
    }

    if (((currentKey < ts && isASCOrder()) ||
         (currentKey > ts && !isASCOrder())) &&
        numOfCurrent < outputRows) {

      // fill the gap between two actual input rows
      while (((currentKey < ts && isASCOrder()) ||
              (currentKey > ts && !isASCOrder())) &&
             numOfCurrent < outputRows) {
        doFillOneRowResult(data, srcData, ts, false);
      }

      // output buffer is full, abort
      if (numOfCurrent == outputRows) {
        numOfTotal += numOfCurrent;
        return outputRows;
      }
    } else {
      assert(currentKey == ts);
      initBeforeAfterDataBuf(prev);

      // assign rows to dst buffer
      for (int32_t i = 0; i < numOfCols; ++i) {
        SFillColInfo* pCol = &pFillCol[i];
        if (TSDB_COL_IS_TAG(pCol->flag)) {
          continue;
        }

        char* output = (char*)elePtrAt(data[i]->data, pCol->col.bytes, numOfCurrent);
        char* src = (char*)elePtrAt(srcData[i], pCol->col.bytes, index);

        if (i == 0 || (pCol->functionId != TSDB_FUNC_COUNT && !isNull(src, pCol->col.type)) ||
            (pCol->functionId == TSDB_FUNC_COUNT && GET_INT64_VAL(src) != 0)) {
          assignVal(output, src, pCol->col.bytes, pCol->col.type);
          memcpy(&prev[pCol->col.offset], src, pCol->col.bytes);
        } else {  // i > 0 and data is null , do interpolation
          if (type == TSDB_FILL_PREV) {
            assignVal(output, &prev[pCol->col.offset], pCol->col.bytes, pCol->col.type);
          } else if (type == TSDB_FILL_LINEAR) {
            assignVal(output, src, pCol->col.bytes, pCol->col.type);
            memcpy(&prev[pCol->col.offset], src, pCol->col.bytes);
          } else {
            assignVal(output, (char*)&pCol->fillVal.i, pCol->col.bytes, pCol->col.type);
          }
        }
      }

      // set the tag value for final result
      setTagsValue(data, numOfCurrent);

      currentKey = taosTimeAdd(currentKey, interval.sliding * step,
                                          interval.slidingUnit, precision);
      index += 1;
      numOfCurrent += 1;
    }

    if (index >= numOfRows || numOfCurrent >= outputRows) {
      /* the raw data block is exhausted, next value does not exists */
      if (index >= numOfRows) {
        next.clear();
      }

      numOfTotal += numOfCurrent;
      return numOfCurrent;
    }
  }

  return numOfCurrent;
}

int64_t SFillInfo::appendFilledResult(tFilePage** output, int64_t resultCapacity) {
  /*
   * These data are generated according to fill strategy, since the current timestamp is out of the time window of
   * real result set. Note that we need to keep the direct previous result rows, to generated the filled data.
   */
  numOfCurrent = 0;
  while (numOfCurrent < resultCapacity) {
    doFillOneRowResult(output, pData, start, true);
  }

  numOfTotal += numOfCurrent;

  assert(numOfCurrent == resultCapacity);
  return resultCapacity;
}

// there are no duplicated tags in the SFillTagColInfo list
static int32_t setTagColumnInfo(SFillInfo* pFillInfo, int32_t numOfCols, int32_t capacity) {
  int32_t rowsize = 0;

  int32_t k = 0;
  for (int32_t i = 0; i < numOfCols; ++i) {
    SFillColInfo* pColInfo = &pFillInfo->pFillCol[i];
    pFillInfo->pData[i] = (char*)calloc(1, pColInfo->col.bytes * capacity);

    if (TSDB_COL_IS_TAG(pColInfo->flag)) {
      bool exists = false;
      int32_t index = -1;
      for (int32_t j = 0; j < k; ++j) {
        if (pFillInfo->pTags[j].col.colId == pColInfo->col.colId) {
          exists = true;
          index = j;
          break;
        }
      }

      if (!exists) {
        SSchema* pSchema = &pFillInfo->pTags[k].col;
        pSchema->colId = pColInfo->col.colId;
        pSchema->type  = pColInfo->col.type;
        pSchema->bytes = pColInfo->col.bytes;

        pFillInfo->pTags[k].tagVal = (char*)calloc(1, pColInfo->col.bytes);
        pColInfo->tagIndex = k;

        k += 1;
      } else {
        pColInfo->tagIndex = index;
      }
    }

    rowsize += pColInfo->col.bytes;
  }

  assert(k <= pFillInfo->numOfTags);
  return rowsize;
}

static int32_t taosNumOfRemainRows(SFillInfo* pFillInfo) {
  if (pFillInfo->numOfRows == 0 || (pFillInfo->numOfRows > 0 && pFillInfo->index >= pFillInfo->numOfRows)) {
    return 0;
  }

  return pFillInfo->numOfRows - pFillInfo->index;
}

SFillInfo::SFillInfo(int32_t order, TSKEY skey, int32_t numOfTags, int32_t capacity, int32_t numOfCols,
                            int64_t slidingTime, int8_t slidingUnit, int8_t precision, int32_t fillType,
                            std::vector<SFillColInfo>&& pCol, void* handle) {
  reset(skey);

  this->order     = order;
  this->type = fillType;
  this->pFillCol = std::move(pCol);
  this->numOfTags = numOfTags;
  this->numOfCols = numOfCols;
  this->precision = precision;
  this->alloc = capacity;
  this->handle = handle;

  this->interval.interval = slidingTime;
  this->interval.intervalUnit = slidingUnit;
  this->interval.sliding = slidingTime;
  this->interval.slidingUnit = slidingUnit;

  pData = (char**)malloc(POINTER_BYTES * numOfCols);
  if (numOfTags > 0) {
    pTags = (SFillTagColInfo*)calloc(numOfTags, sizeof(SFillTagColInfo));
    for (int32_t i = 0; i < numOfTags; ++i) {
      pTags[i].col.colId = -2;  // TODO
    }
  }

  rowSize = setTagColumnInfo(this, numOfCols, alloc);
  assert(rowSize > 0);
}

void SFillInfo::reset(TSKEY startTimestamp) {
  start        = startTimestamp;
  currentKey   = startTimestamp;
  index        = -1;
  numOfRows    = 0;
  numOfCurrent = 0;
  numOfTotal   = 0;
}

SFillInfo::~SFillInfo() {
  tfree(pTags);
  
  for(int32_t i = 0; i < numOfCols; ++i) {
    tfree(pData[i]);
  }
  
  tfree(pData);
}

void SFillInfo::fillSetStartInfo(int32_t numOfRows, TSKEY endKey) {
  if (type == TSDB_FILL_NONE) {
    return;
  }

  end = endKey;
  if (!isASCOrder()) {
    end = taosTimeTruncate(endKey, &interval, precision);
  }

  index     = 0;
  numOfRows = numOfRows;
  
  // ensure the space
  if (alloc < numOfRows) {
    for(int32_t i = 0; i < numOfCols; ++i) {
      char* tmp = (char*)realloc(pData[i], numOfRows*pFillCol[i].col.bytes);
      assert(tmp != NULL); // todo handle error
      
      memset(tmp, 0, numOfRows*pFillCol[i].col.bytes);
      pData[i] = tmp;
    }
  }
}

// copy the data into source data buffer
void SFillInfo::fillCopyInputDataFromFilePage(const tFilePage** pInput) {
  for (int32_t i = 0; i < numOfCols; ++i) {
    memcpy(pData[i], pInput[i]->data, numOfRows * pFillCol[i].col.bytes);
  }
}

void SFillInfo::fillCopyInputDataFromOneFilePage(const tFilePage* pInput) {
  assert(numOfRows == pInput->num);

  for(int32_t i = 0; i < numOfCols; ++i) {
    SFillColInfo* pCol = &pFillCol[i];

    const char* data = pInput->data + pCol->col.offset * pInput->num;
    memcpy(pData[i], data, (size_t)(pInput->num * pCol->col.bytes));

    if (TSDB_COL_IS_TAG(pCol->flag)) {  // copy the tag value to tag value buffer
      SFillTagColInfo* pTag = &pTags[pCol->tagIndex];
      assert (pTag->col.colId == pCol->col.colId);
      memcpy(pTag->tagVal, data, pCol->col.bytes);
    }
  }
}

bool SFillInfo::hasMoreResults() {
  return taosNumOfRemainRows(this) > 0;
}

int64_t SFillInfo::getNumOfResultsAfterFillGap(TSKEY ekey, int32_t maxNumOfRows) {
  int64_t* tsList = (int64_t*) pData[0];

  int32_t numOfRows = taosNumOfRemainRows(this);

  TSKEY ekey1 = ekey;
  if (!isASCOrder()) {
    end = taosTimeTruncate(ekey, &interval, precision);
  }

  int64_t numOfRes = -1;
  if (numOfRows > 0) {  // still fill gap within current data block, not generating data after the result set.
    TSKEY lastKey = tsList[numOfRows - 1];
    numOfRes = taosTimeCountInterval(
      lastKey,
      currentKey,
      interval.sliding,
      interval.slidingUnit,
      precision);
    numOfRes += 1;
    assert(numOfRes >= numOfRows);
  } else { // reach the end of data
    if ((ekey1 < currentKey && isASCOrder()) ||
        (ekey1 > currentKey && !isASCOrder())) {
      return 0;
    }
    numOfRes = taosTimeCountInterval(
      ekey1,
      currentKey,
      interval.sliding,
      interval.slidingUnit,
      precision);
    numOfRes += 1;
  }

  return (numOfRes > maxNumOfRows) ? maxNumOfRows : numOfRes;
}

int32_t taosGetLinearInterpolationVal(int32_t type, SPoint* point1, SPoint* point2, SPoint* point) {
  double v1 = -1;
  double v2 = -1;

  GET_TYPED_DATA(v1, double, type, point1->val);
  GET_TYPED_DATA(v2, double, type, point2->val);

  double r = DO_INTERPOLATION(v1, v2, point1->key, point2->key, point->key);

  switch(type) {
    case TSDB_DATA_TYPE_TINYINT:  *(int8_t*) point->val  = (int8_t) r;break;
    case TSDB_DATA_TYPE_SMALLINT: *(int16_t*) point->val = (int16_t) r;break;
    case TSDB_DATA_TYPE_INT:      *(int32_t*) point->val = (int32_t) r;break;
    case TSDB_DATA_TYPE_BIGINT:   *(int64_t*) point->val = (int64_t) r;break;
    case TSDB_DATA_TYPE_DOUBLE:   *(double*) point->val  = (double) r;break;
    case TSDB_DATA_TYPE_FLOAT:    *(float*) point->val   = (float) r;break;
    default:
      assert(0);
  }

  return TSDB_CODE_SUCCESS;
}

int64_t SFillInfo::fillResultDataBlock(tFilePage** output, int32_t capacity) {
  int32_t remain = taosNumOfRemainRows(this);

  int64_t numOfRes = getNumOfResultsAfterFillGap(end, capacity);
  assert(numOfRes <= capacity);

  // no data existed for fill operation now, append result according to the fill strategy
  if (remain == 0) {
    appendFilledResult(output, numOfRes);
  } else {
    fillResultImpl(output, (int32_t) numOfRes);
    assert(numOfRes == numOfCurrent);
  }

  qDebug("fill:%p, generated fill result, src block:%d, index:%d, brange:%"PRId64"-%"PRId64", currentKey:%"PRId64", current:%d, total:%d, %p",
      this, numOfRows, index, start, end, currentKey, numOfCurrent,
         numOfTotal, handle);

  return numOfRes;
}
