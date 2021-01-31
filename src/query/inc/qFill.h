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

#ifndef TDENGINE_QFILL_H
#define TDENGINE_QFILL_H

#include <vector>
#include "os.h"
#include "qExtbuffer.h"
#include "taosdef.h"

typedef struct {
  STColumn col;             // column info
  int16_t  functionId;      // sql function id
  int16_t  flag;            // column flag: TAG COLUMN|NORMAL COLUMN
  int16_t  tagIndex;        // index of current tag in SFillTagColInfo array list
  union {int64_t i; double d;} fillVal;
} SFillColInfo;

typedef struct {
  SSchema col;
  char*   tagVal;
} SFillTagColInfo;
  
struct SFillInfo {
  TSKEY     start;                // start timestamp
  TSKEY     end;                  // endKey for fill
  TSKEY     currentKey;           // current active timestamp, the value may be changed during the fill procedure.
  int32_t   order;                // order [TSDB_ORDER_ASC|TSDB_ORDER_DESC]
  int32_t   type;                 // fill type
  int32_t   numOfRows;            // number of rows in the input data block
  int32_t   index;                // active row index
  int32_t   numOfTotal;           // number of filled rows in one round
  int32_t   numOfCurrent;         // number of filled rows in current results

  int32_t   numOfTags;            // number of tags
  int32_t   numOfCols;            // number of columns, including the tags columns
  int32_t   rowSize;              // size of each row
  SInterval interval;
  std::vector<char>    prevValues;           // previous row of data, to generate the interpolation results
  std::vector<char>    nextValues;           // next row of data
  char**    pData;                // original result data block involved in filling data
  int32_t   alloc;                // data buffer size in rows
  int8_t    precision;            // time resoluation

  std::vector<SFillColInfo> pFillCol;         // column info for fill operations
  SFillTagColInfo* pTags;         // tags value for filling gap
  void*     handle;               // for dubug purpose
 private:
  void    initBeforeAfterDataBuf(std::vector<char> &next);
  void    copyCurrentRowIntoBuf(char** srcData, std::vector<char> &buf);
  void    setNullValueForRow(tFilePage** data, int32_t numOfCol, int32_t rowIndex);
  void    setTagsValue(tFilePage** data, int32_t genRows);
  int64_t appendFilledResult(tFilePage** output, int64_t resultCapacity);
  int32_t fillResultImpl(tFilePage** data, int32_t outputRows);
  void doFillOneRowResult(tFilePage** data, char** srcData, int64_t ts, bool outOfBound);
  bool isASCOrder() const { return order == TSDB_ORDER_ASC; }
 public:
  SFillInfo(int32_t order, TSKEY skey, int32_t numOfTags, int32_t capacity, int32_t numOfCols,
                              int64_t slidingTime, int8_t slidingUnit, int8_t precision, int32_t fillType,
                              std::vector<SFillColInfo>&& pFillCol, void* handle);
  ~SFillInfo();
  void reset(TSKEY startTimestamp);
  bool hasMoreResults();
  int64_t fillResultDataBlock(tFilePage** output, int32_t capacity);
  int64_t getNumOfResultsAfterFillGap(int64_t ekey, int32_t maxNumOfRows);
  void    fillCopyInputDataFromOneFilePage(const tFilePage* pInput);
  void    fillCopyInputDataFromFilePage(const tFilePage** pInput);
  void    fillSetStartInfo(int32_t numOfRows, TSKEY endKey);
};

typedef struct SPoint {
  int64_t key;
  void *  val;
} SPoint;

int32_t taosGetLinearInterpolationVal(int32_t type, SPoint *point1, SPoint *point2, SPoint *point);

#endif  // TDENGINE_QFILL_H
