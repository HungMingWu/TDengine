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

#include "tscUtil.h"
#include "hash.h"
#include "os.h"
#include "qAst.h"
#include "taosmsg.h"
#include "tkey.h"
#include "tmd5.h"
#include "tscLocalMerge.h"
#include "tscLog.h"
#include "tscProfile.h"
#include "tscSubquery.h"
#include "tschemautil.h"
#include "tsclient.h"
#include "ttimer.h"
#include "ttokendef.h"
#include "defer.h"

static void freeQueryInfoImpl(SQueryInfo* pQueryInfo);
static void clearAllTableMetaInfo(SQueryInfo* pQueryInfo);
static std::atomic<int32_t> tscNumOfObj{0};  // number of sqlObj in current process.
SCond* tsGetSTableQueryCond(STagCond* pTagCond, uint64_t uid) {
  for (auto& pCond : pTagCond->pCond)
    if (uid == pCond.uid) 
      return &pCond;
  return nullptr;
}

void tsSetSTableQueryCond(STagCond* pTagCond, uint64_t uid, SBufferWriter* bw) {
  if (tbufTell(bw) == 0) {
    return;
  }
  
  SCond cond = {
    .uid = uid,
    .len = (int32_t)(tbufTell(bw)),
    .cond = NULL,
  };
  
  cond.cond = tbufGetData(bw, true);
    
  pTagCond->pCond.push_back(cond);
}

bool tscQueryTags(SQueryInfo* pQueryInfo) {
  int32_t numOfCols = (int32_t) tscSqlExprNumOfExprs(pQueryInfo);

  for (int32_t i = 0; i < numOfCols; ++i) {
    const SSqlExpr* pExpr = pQueryInfo->getExpr(i);
    int32_t functId = pExpr->functionId;

    // "select count(tbname)" query
    if (functId == TSDB_FUNC_COUNT && pExpr->colInfo.colId == TSDB_TBNAME_COLUMN_INDEX) {
      continue;
    }

    if (functId != TSDB_FUNC_TAGPRJ && functId != TSDB_FUNC_TID_TAG) {
      return false;
    }
  }

  return true;
}

// todo refactor, extract methods and move the common module
void tscGetDBInfoFromTableFullName(char* tableId, char* db) {
  char* st = strstr(tableId, TS_PATH_DELIMITER);
  if (st != NULL) {
    char* end = strstr(st + 1, TS_PATH_DELIMITER);
    if (end != NULL) {
      memcpy(db, tableId, (end - tableId));
      db[end - tableId] = 0;
      return;
    }
  }

  db[0] = 0;
}

bool tscIsTwoStageSTableQuery(SQueryInfo* pQueryInfo, int32_t tableIndex) {
  if (pQueryInfo == NULL) {
    return false;
  }

  STableMetaInfo* pTableMetaInfo = tscGetMetaInfo(pQueryInfo, tableIndex);
  if (pTableMetaInfo == NULL) {
    return false;
  }
  
  // for select query super table, the super table vgroup list can not be null in any cases.
  // if (pQueryInfo->command == TSDB_SQL_SELECT && UTIL_TABLE_IS_SUPER_TABLE(pTableMetaInfo)) {
  //   assert(pTableMetaInfo->vgroupList != NULL);
  // }
  
  if ((pQueryInfo->type & TSDB_QUERY_TYPE_FREE_RESOURCE) == TSDB_QUERY_TYPE_FREE_RESOURCE) {
    return false;
  }

  // for ordered projection query, iterate all qualified vnodes sequentially
  if (tscNonOrderedProjectionQueryOnSTable(pQueryInfo, tableIndex)) {
    return false;
  }

  if (!TSDB_QUERY_HAS_TYPE(pQueryInfo->type, TSDB_QUERY_TYPE_STABLE_SUBQUERY) && pQueryInfo->command == TSDB_SQL_SELECT) {
    return UTIL_TABLE_IS_SUPER_TABLE(pTableMetaInfo);
  }

  return false;
}

bool tscIsProjectionQueryOnSTable(SQueryInfo* pQueryInfo, int32_t tableIndex) {
  STableMetaInfo* pTableMetaInfo = tscGetMetaInfo(pQueryInfo, tableIndex);
  
  /*
   * In following cases, return false for non ordered project query on super table
   * 1. failed to get tableMeta from server; 2. not a super table; 3. limitation is 0;
   * 4. show queries, instead of a select query
   */
  size_t numOfExprs = tscSqlExprNumOfExprs(pQueryInfo);
  if (pTableMetaInfo == NULL || !UTIL_TABLE_IS_SUPER_TABLE(pTableMetaInfo) ||
      pQueryInfo->command == TSDB_SQL_RETRIEVE_EMPTY_RESULT || numOfExprs == 0) {
    return false;
  }
  
  for (int32_t i = 0; i < numOfExprs; ++i) {
    int32_t functionId = pQueryInfo->getExpr(i)->functionId;

    if (functionId != TSDB_FUNC_PRJ &&
        functionId != TSDB_FUNC_TAGPRJ &&
        functionId != TSDB_FUNC_TAG &&
        functionId != TSDB_FUNC_TS &&
        functionId != TSDB_FUNC_ARITHM &&
        functionId != TSDB_FUNC_TS_COMP &&
        functionId != TSDB_FUNC_TID_TAG) {
      return false;
    }
  }
  
  return true;
}

// not order by timestamp projection query on super table
bool tscNonOrderedProjectionQueryOnSTable(SQueryInfo* pQueryInfo, int32_t tableIndex) {
  if (!tscIsProjectionQueryOnSTable(pQueryInfo, tableIndex)) {
    return false;
  }
  
  // order by columnIndex exists, not a non-ordered projection query
  return pQueryInfo->order.orderColId < 0;
}

bool tscOrderedProjectionQueryOnSTable(SQueryInfo* pQueryInfo, int32_t tableIndex) {
  if (!tscIsProjectionQueryOnSTable(pQueryInfo, tableIndex)) {
    return false;
  }
  
  // order by columnIndex exists, a non-ordered projection query
  return pQueryInfo->order.orderColId >= 0;
}

bool tscIsProjectionQuery(SQueryInfo* pQueryInfo) {
  size_t size = tscSqlExprNumOfExprs(pQueryInfo);

  for (int32_t i = 0; i < size; ++i) {
    int32_t functionId = pQueryInfo->getExpr(i)->functionId;

    if (functionId != TSDB_FUNC_PRJ && functionId != TSDB_FUNC_TAGPRJ && functionId != TSDB_FUNC_TAG &&
        functionId != TSDB_FUNC_TS && functionId != TSDB_FUNC_ARITHM) {
      return false;
    }
  }

  return true;
}

bool tscIsPointInterpQuery(SQueryInfo* pQueryInfo) {
  size_t size = tscSqlExprNumOfExprs(pQueryInfo);
  for (int32_t i = 0; i < size; ++i) {
    const SSqlExpr* pExpr = pQueryInfo->getExpr(i);
    assert(pExpr != NULL);

    int32_t functionId = pExpr->functionId;
    if (functionId == TSDB_FUNC_TAG) {
      continue;
    }

    if (functionId != TSDB_FUNC_INTERP) {
      return false;
    }
  }

  return true;
}

bool tscIsSecondStageQuery(SQueryInfo* pQueryInfo) {
  if (tscIsProjectionQuery(pQueryInfo)) {
    return false;
  }

  size_t numOfOutput = tscNumOfFields(pQueryInfo);
  for(int32_t i = 0; i < numOfOutput; ++i) {
    SExprInfo* pExprInfo = pQueryInfo->fieldsInfo.internalField[i].pArithExprInfo;
    if (pExprInfo != NULL) {
      return true;
    }
  }

  return false;
}

bool tscIsTWAQuery(SQueryInfo* pQueryInfo) {
  size_t numOfExprs = tscSqlExprNumOfExprs(pQueryInfo);
  for (int32_t i = 0; i < numOfExprs; ++i) {
    const SSqlExpr* pExpr = pQueryInfo->getExpr(i);
    if (pExpr == NULL) {
      continue;
    }

    int32_t functionId = pExpr->functionId;
    if (functionId == TSDB_FUNC_TWA) {
      return true;
    }
  }

  return false;
}

void tscClearInterpInfo(SQueryInfo* pQueryInfo) {
  if (!tscIsPointInterpQuery(pQueryInfo)) {
    return;
  }

  pQueryInfo->fillType = TSDB_FILL_NONE;
  tfree(pQueryInfo->fillVal);
}

int32_t tscCreateResPointerInfo(SSqlRes* pRes, SQueryInfo* pQueryInfo) {
  if (pRes->tsrow == NULL) {
    pRes->numOfCols = pQueryInfo->fieldsInfo.internalField.size();

    pRes->tsrow  = (TAOS_ROW)calloc(pRes->numOfCols, POINTER_BYTES);
    pRes->urow   = (TAOS_ROW)calloc(pRes->numOfCols, POINTER_BYTES);
    pRes->length = (int32_t*)calloc(pRes->numOfCols, sizeof(int32_t));
    pRes->buffer = (char**)calloc(pRes->numOfCols, POINTER_BYTES);

    // not enough memory
    if (pRes->tsrow == NULL  || pRes->urow == NULL || pRes->length == NULL || (pRes->buffer == NULL && pRes->numOfCols > 0)) {
      tfree(pRes->tsrow);
      tfree(pRes->urow);
      tfree(pRes->length);
      tfree(pRes->buffer);

      pRes->code = TSDB_CODE_TSC_OUT_OF_MEMORY;
      return pRes->code;
    }
  }

  return TSDB_CODE_SUCCESS;
}

void tscSetResRawPtr(SSqlRes* pRes, SQueryInfo* pQueryInfo) {
  assert(pRes->numOfCols > 0);

  int32_t offset = 0;

  for (int32_t i = 0; i < pRes->numOfCols; ++i) {
    SInternalField* pInfo = &pQueryInfo->fieldsInfo.internalField[i];

    pRes->urow[i] = pRes->data + offset * pRes->numOfRows;
    pRes->length[i] = pInfo->field.bytes;

    offset += pInfo->field.bytes;

    // generated the user-defined column result
    if (pInfo->pSqlExpr != NULL && TSDB_COL_IS_UD_COL(pInfo->pSqlExpr->colInfo.flag)) {
      if (pInfo->pSqlExpr->param[1].nType == TSDB_DATA_TYPE_NULL) {
        setNullN((char*)pRes->urow[i], pInfo->field.type, pInfo->field.bytes, (int32_t) pRes->numOfRows);
      } else {
        if (pInfo->field.type == TSDB_DATA_TYPE_NCHAR || pInfo->field.type == TSDB_DATA_TYPE_BINARY) {
          assert(pInfo->pSqlExpr->param[1].nLen <= pInfo->field.bytes);

          for (int32_t k = 0; k < pRes->numOfRows; ++k) {
            char* p = ((char**)pRes->urow)[i] + k * pInfo->field.bytes;

            memcpy(varDataVal(p), pInfo->pSqlExpr->param[1].pz, pInfo->pSqlExpr->param[1].nLen);
            varDataSetLen(p, pInfo->pSqlExpr->param[1].nLen);
          }
        } else {
          for (int32_t k = 0; k < pRes->numOfRows; ++k) {
            char* p = ((char**)pRes->urow)[i] + k * pInfo->field.bytes;
            memcpy(p, &pInfo->pSqlExpr->param[1].i64, pInfo->field.bytes);
          }
        }
      }

    } else if (pInfo->field.type == TSDB_DATA_TYPE_NCHAR) {
      // convert unicode to native code in a temporary buffer extra one byte for terminated symbol
      pRes->buffer[i] = (char*)realloc(pRes->buffer[i], pInfo->field.bytes * pRes->numOfRows);

      // string terminated char for binary data
      memset(pRes->buffer[i], 0, pInfo->field.bytes * pRes->numOfRows);

      char* p = (char*)pRes->urow[i];
      for (int32_t k = 0; k < pRes->numOfRows; ++k) {
        char* dst = pRes->buffer[i] + k * pInfo->field.bytes;

        if (isNull(p, TSDB_DATA_TYPE_NCHAR)) {
          memcpy(dst, p, varDataTLen(p));
        } else if (varDataLen(p) > 0) {
          int32_t length = taosUcs4ToMbs(varDataVal(p), varDataLen(p), (char*)varDataVal(dst));
          varDataSetLen(dst, length);

          if (length == 0) {
            tscError("charset:%s to %s. val:%s convert failed.", DEFAULT_UNICODE_ENCODEC, tsCharset, (char*)p);
          }
        } else {
          varDataSetLen(dst, 0);
        }

        p += pInfo->field.bytes;
      }

      memcpy(pRes->urow[i], pRes->buffer[i], pInfo->field.bytes * pRes->numOfRows);
    }
  }
}

static void tscDestroyResPointerInfo(SSqlRes* pRes) {
  if (pRes->buffer != NULL) { // free all buffers containing the multibyte string
    for (int i = 0; i < pRes->numOfCols; i++) {
      tfree(pRes->buffer[i]);
    }
    
    pRes->numOfCols = 0;
  }
  
  tfree(pRes->pRsp);

  tfree(pRes->tsrow);
  tfree(pRes->length);
  tfree(pRes->buffer);
  tfree(pRes->urow);

  tfree(pRes->pGroupRec);
  tfree(pRes->pColumnIndex);

  if (pRes->pArithSup != NULL) {
    tfree(pRes->pArithSup->data);
    tfree(pRes->pArithSup);
  }
  
  pRes->data = NULL;  // pRes->data points to the buffer of pRsp, no need to free
}

void tscFreeQueryInfo(SSqlCmd* pCmd) {
  if (pCmd == NULL || pCmd->numOfClause == 0) {
    return;
  }
  
  for (int32_t i = 0; i < pCmd->numOfClause; ++i) {
    SQueryInfo* pQueryInfo = tscGetQueryInfoDetail(pCmd, i);
    
    freeQueryInfoImpl(pQueryInfo);
    clearAllTableMetaInfo(pQueryInfo);
    tfree(pQueryInfo);
  }
  
  pCmd->numOfClause = 0;
  tfree(pCmd->pQueryInfo);
}

void tscResetSqlCmdObj(SSqlCmd* pCmd) {
  pCmd->command   = 0;
  pCmd->numOfCols = 0;
  pCmd->count     = 0;
  pCmd->curSql    = NULL;
  pCmd->msgType   = 0;
  pCmd->parseFinished = 0;
  pCmd->autoCreated = 0;
  pCmd->pTableNameList.clear();
  pCmd->pTableBlockHashList = (SHashObj*)tscDestroyBlockHashTable(pCmd->pTableBlockHashList);
  pCmd->pDataBlocks = (SArray*)tscDestroyBlockArrayList(pCmd->pDataBlocks);
  tscFreeQueryInfo(pCmd);
}

void tscFreeSqlResult(SSqlObj* pSql) {
  tscDestroyLocalReducer(pSql);
  
  SSqlRes* pRes = &pSql->res;
  tscDestroyResPointerInfo(pRes);
  
  memset(&pSql->res, 0, sizeof(SSqlRes));
}

static void tscFreeSubobj(SSqlObj* pSql) {
  if (pSql->subState.numOfSub == 0) {
    return;
  }

  tscDebug("%p start to free sub SqlObj, numOfSub:%d", pSql, pSql->subState.numOfSub);

  for(int32_t i = 0; i < pSql->subState.numOfSub; ++i) {
    tscDebug("%p free sub SqlObj:%p, index:%d", pSql, pSql->pSubs[i], i);
    taos_free_result(pSql->pSubs[i]);
    pSql->pSubs[i] = NULL;
  }

  pSql->subState.numOfSub = 0;
}

/**
 * The free operation will cause the pSql to be removed from hash table and free it in
 * the function of processmsgfromserver is impossible in this case, since it will fail
 * to retrieve pSqlObj in hashtable.
 *
 * @param pSql
 */
void tscFreeRegisteredSqlObj(void *pSql) {
  assert(pSql != NULL);

  SSqlObj* p = (SSqlObj*)pSql;
  STscObj* pTscObj = p->pTscObj;

  assert(RID_VALID(p->self));

  int32_t num = --pTscObj->numOfObj;
  int32_t total = --tscNumOfObj;

  tscDebug("%p free SqlObj, total in tscObj:%d, total:%d", pSql, num, total);
  tscFreeSqlObj(p);
  taosReleaseRef(tscRefId, pTscObj->rid);

}

void tscFreeMetaSqlObj(int64_t *rid){
  if (RID_VALID(*rid)) {
    SSqlObj* pSql = (SSqlObj*)taosAcquireRef(tscObjRef, *rid);
    if (pSql) {
      taosRemoveRef(tscObjRef, *rid);
      taosReleaseRef(tscObjRef, *rid);
    }

    *rid = 0;
  }
}

void tscFreeSqlObj(SSqlObj* pSql) {
  if (pSql == NULL) {
    return;
  }

  tscDebug("%p start to free sqlObj", pSql);

  pSql->res.code = TSDB_CODE_TSC_QUERY_CANCELLED;

  tscFreeMetaSqlObj(&pSql->metaRid);
  tscFreeMetaSqlObj(&pSql->svgroupRid);

  tscFreeSubobj(pSql);

  SSqlCmd* pCmd = &pSql->cmd;
  int32_t cmd = pCmd->command;
  if (cmd < TSDB_SQL_INSERT || cmd == TSDB_SQL_RETRIEVE_LOCALMERGE || cmd == TSDB_SQL_RETRIEVE_EMPTY_RESULT ||
      cmd == TSDB_SQL_TABLE_JOIN_RETRIEVE) {
    tscRemoveFromSqlList(pSql);
  }

  tfree(pSql->sqlstr);

  pSql->self = 0;

  tscFreeSqlResult(pSql);
  tscResetSqlCmdObj(pCmd);

  tfree(pCmd->tagData.data);
  pCmd->tagData.dataLen = 0;
    
  tsem_destroy(&pSql->rspSem);
  delete pSql;
}

STableDataBlocks::~STableDataBlocks() {
  tfree(pData);

  // free the refcount for metermeta
  if (pTableMeta != NULL) {
    tfree(pTableMeta);
  }
}

void*  tscDestroyBlockArrayList(SArray* pDataBlockList) {
  if (pDataBlockList == NULL) {
    return NULL;
  }

  size_t size = taosArrayGetSize(pDataBlockList);
  for (int32_t i = 0; i < size; i++) {
    void* d = taosArrayGetP(pDataBlockList, i);
    delete (STableDataBlocks*)d;
  }

  taosArrayDestroy(pDataBlockList);
  return NULL;
}

void* tscDestroyBlockHashTable(SHashObj* pBlockHashTable) {
  if (pBlockHashTable == NULL) {
    return NULL;
  }

  STableDataBlocks** p = (STableDataBlocks**)taosHashIterate(pBlockHashTable, NULL);
  while(p) {
    delete *p;
    p = (STableDataBlocks**)taosHashIterate(pBlockHashTable, p);
  }

  taosHashCleanup(pBlockHashTable);
  return NULL;
}

int32_t tscCopyDataBlockToPayload(SSqlObj* pSql, const STableDataBlocks* pDataBlock) {
  SSqlCmd* pCmd = &pSql->cmd;
  assert(pDataBlock->pTableMeta != NULL);

  pCmd->numOfTablesInSubmit = pDataBlock->numOfTables;

  assert(pCmd->numOfClause == 1);
  STableMetaInfo* pTableMetaInfo = pCmd->getMetaInfo(pCmd->clauseIndex, 0);

  // todo refactor
  // set the correct table meta object, the table meta has been locked in pDataBlocks, so it must be in the cache
  if (pTableMetaInfo->pTableMeta != pDataBlock->pTableMeta) {
    tstrncpy(&pTableMetaInfo->name[0], pDataBlock->tableName, sizeof(pTableMetaInfo->name));

    if (pTableMetaInfo->pTableMeta != NULL) {
      tfree(pTableMetaInfo->pTableMeta);
    }

    pTableMetaInfo->pTableMeta = tscTableMetaClone(pDataBlock->pTableMeta);
  } else {
    assert(strncmp(&pTableMetaInfo->name[0], pDataBlock->tableName, tListLen(pDataBlock->tableName)) == 0);
  }

  assert(pDataBlock->size <= pDataBlock->nAllocSize);
  pCmd->payload = std::string(pDataBlock->pData, pDataBlock->size);

  return TSDB_CODE_SUCCESS;
}

/**
 * create the in-memory buffer for each table to keep the submitted data block
 * @param initialSize
 * @param rowSize
 * @param startOffset
 * @param name
 * @param dataBlocks
 * @return
 */
STableDataBlocks* tscCreateDataBlock(size_t initialSize, int32_t rowSize, int32_t startOffset, const char* name,
                           STableMeta* pTableMeta) {
  auto dataBuf = new STableDataBlocks;
  dataBuf->nAllocSize = (uint32_t)initialSize;
  dataBuf->headerSize = startOffset; // the header size will always be the startOffset value, reserved for the subumit block header
  if (dataBuf->nAllocSize <= dataBuf->headerSize) {
    dataBuf->nAllocSize = dataBuf->headerSize*2;
  } 
  dataBuf->pData = (char*)calloc(1, dataBuf->nAllocSize);
  dataBuf->ordered = true;
  dataBuf->prevTS = INT64_MIN;
  dataBuf->rowSize = rowSize;
  dataBuf->size = startOffset;
  dataBuf->tsSource = -1;
  tstrncpy(dataBuf->tableName, name, sizeof(dataBuf->tableName));

  //Here we keep the tableMeta to avoid it to be remove by other threads.
  dataBuf->pTableMeta = tscTableMetaClone(pTableMeta);
  assert(initialSize > 0 && pTableMeta != NULL && dataBuf->pTableMeta != NULL);
  return dataBuf;
}

void tscGetDataBlockFromList(SHashObj* pHashList, int64_t id, int32_t size, int32_t startOffset, int32_t rowSize, const char* tableId, STableMeta* pTableMeta,
                                STableDataBlocks** dataBlocks, SArray* pBlockList) {
  *dataBlocks = NULL;
  STableDataBlocks** t1 = (STableDataBlocks**)taosHashGet(pHashList, (const char*)&id, sizeof(id));
  if (t1 != NULL) {
    *dataBlocks = *t1;
  }

  if (*dataBlocks == NULL) {
    *dataBlocks = tscCreateDataBlock((size_t)size, rowSize, startOffset, tableId, pTableMeta);
    taosHashPut(pHashList, (const char*)&id, sizeof(int64_t), (char*)dataBlocks, POINTER_BYTES);
    if (pBlockList) {
      taosArrayPush(pBlockList, dataBlocks);
    }
  }
}

static int trimDataBlock(void* pDataBlock, STableDataBlocks* pTableDataBlock, bool includeSchema) {
  // TODO: optimize this function, handle the case while binary is not presented
  STableMeta*   pTableMeta = pTableDataBlock->pTableMeta;
  const auto& tinfo = pTableMeta->getInfo();
  const SSchema*      pSchema = pTableMeta->getSchema();

  SSubmitBlk* pBlock = (SSubmitBlk*)pDataBlock;
  memcpy(pDataBlock, pTableDataBlock->pData, sizeof(SSubmitBlk));
  pDataBlock = (char*)pDataBlock + sizeof(SSubmitBlk);

  int32_t flen = 0;  // original total length of row

  // schema needs to be included into the submit data block
  if (includeSchema) {
    int32_t numOfCols = pTableDataBlock->pTableMeta->numOfColumns();
    for(int32_t j = 0; j < numOfCols; ++j) {
      STColumn* pCol = (STColumn*) pDataBlock;
      pCol->colId = htons(pSchema[j].colId);
      pCol->type  = pSchema[j].type;
      pCol->bytes = htons(pSchema[j].bytes);
      pCol->offset = 0;

      pDataBlock = (char*)pDataBlock + sizeof(STColumn);
      flen += TYPE_BYTES[pSchema[j].type];
    }

    int32_t schemaSize = sizeof(STColumn) * numOfCols;
    pBlock->schemaLen = schemaSize;
  } else {
    for (int32_t j = 0; j < tinfo.numOfColumns; ++j) {
      flen += TYPE_BYTES[pSchema[j].type];
    }

    pBlock->schemaLen = 0;
  }

  char* p = pTableDataBlock->pData + sizeof(SSubmitBlk);
  pBlock->dataLen = 0;
  int32_t numOfRows = htons(pBlock->numOfRows);
  
  for (int32_t i = 0; i < numOfRows; ++i) {
    SDataRow trow = (SDataRow) pDataBlock;
    dataRowSetLen(trow, (uint16_t)(TD_DATA_ROW_HEAD_SIZE + flen));
    dataRowSetVersion(trow, pTableMeta->sversion);

    int toffset = 0;
    for (int32_t j = 0; j < tinfo.numOfColumns; j++) {
      tdAppendColVal(trow, p, pSchema[j].type, pSchema[j].bytes, toffset);
      toffset += TYPE_BYTES[pSchema[j].type];
      p += pSchema[j].bytes;
    }

    pDataBlock = (char*)pDataBlock + dataRowLen(trow);
    pBlock->dataLen += dataRowLen(trow);
  }

  int32_t len = pBlock->dataLen + pBlock->schemaLen;
  pBlock->dataLen = htonl(pBlock->dataLen);
  pBlock->schemaLen = htonl(pBlock->schemaLen);

  return len;
}

static int32_t getRowExpandSize(STableMeta* pTableMeta) {
  int32_t result = TD_DATA_ROW_HEAD_SIZE;
  int32_t columns = pTableMeta->numOfColumns();
  const SSchema* pSchema = pTableMeta->getSchema();
  for(int32_t i = 0; i < columns; i++) {
    if (IS_VAR_DATA_TYPE((pSchema + i)->type)) {
      result += TYPE_BYTES[TSDB_DATA_TYPE_BINARY];
    }
  }
  return result;
}

static void extractTableNameList(SSqlCmd* pCmd, bool freeBlockMap) {
  pCmd->pTableNameList.clear();

  STableDataBlocks **p1 = (STableDataBlocks **)taosHashIterate(pCmd->pTableBlockHashList, NULL);
  while (p1) {
    STableDataBlocks* pBlocks = *p1;
    pCmd->pTableNameList.emplace_back(pBlocks->tableName, TSDB_TABLE_FNAME_LEN);
    p1 = (STableDataBlocks **)taosHashIterate(pCmd->pTableBlockHashList, p1);
  }

  if (freeBlockMap) {
    pCmd->pTableBlockHashList = (SHashObj*)tscDestroyBlockHashTable(pCmd->pTableBlockHashList);
  }
}

int32_t tscMergeTableDataBlocks(SSqlObj* pSql, bool freeBlockMap) {
  const int INSERT_HEAD_SIZE = sizeof(SMsgDesc) + sizeof(SSubmitMsg); 
  SSqlCmd* pCmd = &pSql->cmd;

  auto pVnodeDataBlockHashList =
      (SHashObj*)taosHashInit(128, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT), true, HASH_NO_LOCK);
  SArray* pVnodeDataBlockList = (SArray*)taosArrayInit(8, POINTER_BYTES);

  STableDataBlocks** p = (STableDataBlocks**)taosHashIterate(pCmd->pTableBlockHashList, NULL);

  STableDataBlocks* pOneTableBlock = *p;
  while(pOneTableBlock) {
    // the maximum expanded size in byte when a row-wise data is converted to SDataRow format
    int32_t expandSize = getRowExpandSize(pOneTableBlock->pTableMeta);
    STableDataBlocks* dataBuf = NULL;
    
    tscGetDataBlockFromList(pVnodeDataBlockHashList, pOneTableBlock->vgId, TSDB_PAYLOAD_SIZE,
                                INSERT_HEAD_SIZE, 0, pOneTableBlock->tableName, pOneTableBlock->pTableMeta, &dataBuf, pVnodeDataBlockList);

    SSubmitBlk* pBlocks = (SSubmitBlk*) pOneTableBlock->pData;
    int64_t destSize = dataBuf->size + pOneTableBlock->size + pBlocks->numOfRows * expandSize + sizeof(STColumn) * pOneTableBlock->pTableMeta->numOfColumns();

    if (dataBuf->nAllocSize < destSize) {
      while (dataBuf->nAllocSize < destSize) {
        dataBuf->nAllocSize = (uint32_t)(dataBuf->nAllocSize * 1.5);
      }

      char* tmp = (char*)realloc(dataBuf->pData, dataBuf->nAllocSize);
      if (tmp != NULL) {
        dataBuf->pData = tmp;
        memset(dataBuf->pData + dataBuf->size, 0, dataBuf->nAllocSize - dataBuf->size);
      } else {  // failed to allocate memory, free already allocated memory and return error code
        tscError("%p failed to allocate memory for merging submit block, size:%d", pSql, dataBuf->nAllocSize);

        taosHashCleanup(pVnodeDataBlockHashList);
        tscDestroyBlockArrayList(pVnodeDataBlockList);
        tfree(dataBuf->pData);

        return TSDB_CODE_TSC_OUT_OF_MEMORY;
      }
    }

    tscSortRemoveDataBlockDupRows(pOneTableBlock);
    char* ekey = (char*)pBlocks->data + pOneTableBlock->rowSize*(pBlocks->numOfRows-1);
    
    tscDebug("%p name:%s, sid:%d rows:%d sversion:%d skey:%" PRId64 ", ekey:%" PRId64, pSql, pOneTableBlock->tableName,
        pBlocks->tid, pBlocks->numOfRows, pBlocks->sversion, GET_INT64_VAL(pBlocks->data), GET_INT64_VAL(ekey));

    int32_t len = pBlocks->numOfRows * (pOneTableBlock->rowSize + expandSize) + sizeof(STColumn) * pOneTableBlock->pTableMeta->numOfColumns();

    pBlocks->tid = htonl(pBlocks->tid);
    pBlocks->uid = htobe64(pBlocks->uid);
    pBlocks->sversion = htonl(pBlocks->sversion);
    pBlocks->numOfRows = htons(pBlocks->numOfRows);
    pBlocks->schemaLen = 0;

    // erase the empty space reserved for binary data
    int32_t finalLen = trimDataBlock(dataBuf->pData + dataBuf->size, pOneTableBlock, pCmd->submitSchema);
    assert(finalLen <= len);

    dataBuf->size += (finalLen + sizeof(SSubmitBlk));
    assert(dataBuf->size <= dataBuf->nAllocSize);

    // the length does not include the SSubmitBlk structure
    pBlocks->dataLen = htonl(finalLen);
    dataBuf->numOfTables += 1;

    p = (STableDataBlocks **)taosHashIterate(pCmd->pTableBlockHashList, p);
    if (p == NULL) {
      break;
    }

    pOneTableBlock = *p;
  }

  extractTableNameList(pCmd, freeBlockMap);

  // free the table data blocks;
  pCmd->pDataBlocks = pVnodeDataBlockList;
  taosHashCleanup(pVnodeDataBlockHashList);

  return TSDB_CODE_SUCCESS;
}

// TODO: all subqueries should be freed correctly before close this connection.
void tscCloseTscObj(void *param) {
  STscObj *pObj = (STscObj*)param;

  taosTmrStopA(&(pObj->pTimer));

  void* p = pObj->pDnodeConn;
  if (pObj->pDnodeConn != NULL) {
    rpcClose(pObj->pDnodeConn);
    pObj->pDnodeConn = NULL;
  }

  tfree(pObj->tscCorMgmtEpSet);

  tscDebug("%p DB connection is closed, dnodeConn:%p", pObj, p);
  delete pObj;
}

bool tscIsInsertData(char* sqlstr) {
  int32_t index = 0;

  do {
    SStrToken t0 = tStrGetToken(sqlstr, &index, false, 0, NULL);
    if (t0.type != TK_LP) {
      return t0.type == TK_INSERT || t0.type == TK_IMPORT;
    }
  } while (1);
}

TAOS_FIELD tscCreateField(int8_t type, const char* name, int16_t bytes) {
  TAOS_FIELD f = { .type = type, .bytes = bytes, };
  tstrncpy(f.name, name, sizeof(f.name));
  return f;
}

void tscFieldInfoUpdateOffset(SQueryInfo* pQueryInfo) {
  size_t numOfExprs = tscSqlExprNumOfExprs(pQueryInfo);
  
  SSqlExpr* pExpr = pQueryInfo->getExpr(0);
  pExpr->offset = 0;

  for (int32_t i = 1; i < numOfExprs; ++i) {
    SSqlExpr* prev = pQueryInfo->getExpr(i - 1);
    SSqlExpr* p = pQueryInfo->getExpr(i);

    p->offset = prev->offset + prev->resBytes;
  }
}

int16_t tscFieldInfoGetOffset(SQueryInfo* pQueryInfo, int32_t index) {
  SInternalField* pInfo = &pQueryInfo->fieldsInfo.internalField[index];
  assert(pInfo != NULL && pInfo->pSqlExpr != NULL);

  return pInfo->pSqlExpr->offset;
}

int32_t tscFieldInfoCompare(const SFieldInfo* pFieldInfo1, const SFieldInfo* pFieldInfo2) {
  assert(pFieldInfo1 != NULL && pFieldInfo2 != NULL);

  if (pFieldInfo1->internalField.size() != pFieldInfo2->internalField.size()) {
    return pFieldInfo1->internalField.size() - pFieldInfo2->internalField.size();
  }

  for (int32_t i = 0; i < pFieldInfo1->internalField.size(); ++i) {
    const TAOS_FIELD* pField1 = &pFieldInfo1->internalField[i].field;
    const TAOS_FIELD* pField2 = &pFieldInfo2->internalField[i].field;

    if (pField1->type != pField2->type ||
        pField1->bytes != pField2->bytes ||
        strcasecmp(pField1->name, pField2->name) != 0) {
      return 1;
    }
  }

  return 0;
}

int32_t tscGetResRowLength(SArray* pExprList) {
  size_t num = taosArrayGetSize(pExprList);
  if (num == 0) {
    return 0;
  }
  
  int32_t size = 0;
  for(int32_t i = 0; i < num; ++i) {
    SSqlExpr* pExpr = (SSqlExpr*)taosArrayGetP(pExprList, i);
    size += pExpr->resBytes;
  }
  
  return size;
}
SInternalField::~SInternalField() 
{
  if (pArithExprInfo != NULL) {
    tExprTreeDestroy(&pArithExprInfo->pExpr, NULL);

    SSqlFuncMsg* pFuncMsg = &pArithExprInfo->base;
    for (int32_t j = 0; j < pFuncMsg->numOfParams; ++j) {
      if (pFuncMsg->arg[j].argType == TSDB_DATA_TYPE_BINARY) {
        tfree(pFuncMsg->arg[j].argValue.pz);
      }
    }

    tfree(pArithExprInfo);
  }
}

void SFieldInfo::clear() { 
  internalField.clear();
  tfree(final);
}

static SSqlExpr* doBuildSqlExpr(SQueryInfo* pQueryInfo, int16_t functionId, SColumnIndex* pColIndex, int16_t type,
    int16_t size, int16_t resColId, int16_t interSize, int32_t colType) {
  STableMetaInfo* pTableMetaInfo = tscGetMetaInfo(pQueryInfo, pColIndex->tableIndex);
  
  auto pExpr = new SSqlExpr;
  if (pExpr == NULL) {
    return NULL;
  }

  pExpr->functionId = functionId;

  // set the correct columnIndex index
  if (pColIndex->columnIndex == TSDB_TBNAME_COLUMN_INDEX) {
    pExpr->colInfo.colId = TSDB_TBNAME_COLUMN_INDEX;
  } else if (pColIndex->columnIndex <= TSDB_UD_COLUMN_INDEX) {
    pExpr->colInfo.colId = pColIndex->columnIndex;
  } else {
    if (TSDB_COL_IS_TAG(colType)) {
      const SSchema* pSchema = pTableMetaInfo->pTableMeta->getTagSchema();
      pExpr->colInfo.colId = pSchema[pColIndex->columnIndex].colId;
      tstrncpy(pExpr->colInfo.name, pSchema[pColIndex->columnIndex].name, sizeof(pExpr->colInfo.name));
    } else if (pTableMetaInfo->pTableMeta != NULL) {
      // in handling select database/version/server_status(), the pTableMeta is NULL
      const SSchema* pSchema = pTableMetaInfo->pTableMeta->getColumnSchema(pColIndex->columnIndex);
      pExpr->colInfo.colId = pSchema->colId;
      tstrncpy(pExpr->colInfo.name, pSchema->name, sizeof(pExpr->colInfo.name));
    }
  }
  
  pExpr->colInfo.flag     = colType;
  pExpr->colInfo.colIndex = pColIndex->columnIndex;

  pExpr->resType       = type;
  pExpr->resBytes      = size;
  pExpr->resColId      = resColId;
  pExpr->interBytes    = interSize;

  if (pTableMetaInfo->pTableMeta) {
    pExpr->uid = pTableMetaInfo->pTableMeta->id.uid;
  }
  
  return pExpr;
}

SSqlExpr* tscSqlExprInsert(SQueryInfo* pQueryInfo, int32_t index, int16_t functionId, SColumnIndex* pColIndex, int16_t type,
                           int16_t size, int16_t resColId, int16_t interSize, bool isTagCol) {
  int32_t num = (int32_t)tscSqlExprNumOfExprs(pQueryInfo);
  if (index == num) {
    return tscSqlExprAppend(pQueryInfo, functionId, pColIndex, type, size, resColId, interSize, isTagCol);
  }
  
  SSqlExpr* pExpr = doBuildSqlExpr(pQueryInfo, functionId, pColIndex, type, size, resColId, interSize, isTagCol);
  taosArrayInsert(pQueryInfo->exprList, index, &pExpr);
  return pExpr;
}

SSqlExpr* tscSqlExprAppend(SQueryInfo* pQueryInfo, int16_t functionId, SColumnIndex* pColIndex, int16_t type,
    int16_t size, int16_t resColId, int16_t interSize, bool isTagCol) {
  SSqlExpr* pExpr = doBuildSqlExpr(pQueryInfo, functionId, pColIndex, type, size, resColId, interSize, isTagCol);
  taosArrayPush(pQueryInfo->exprList, &pExpr);
  return pExpr;
}

SSqlExpr* tscSqlExprUpdate(SQueryInfo* pQueryInfo, int32_t index, int16_t functionId, int16_t srcColumnIndex,
                           int16_t type, int16_t size) {
  STableMetaInfo* pTableMetaInfo = tscGetMetaInfo(pQueryInfo, 0);
  SSqlExpr* pExpr = pQueryInfo->getExpr(index);
  if (pExpr == NULL) {
    return NULL;
  }

  pExpr->functionId = functionId;

  pExpr->colInfo.colIndex = srcColumnIndex;
  pExpr->colInfo.colId = pTableMetaInfo->pTableMeta->getColumnSchema(srcColumnIndex)->colId;

  pExpr->resType = type;
  pExpr->resBytes = size;

  return pExpr;
}

size_t tscSqlExprNumOfExprs(SQueryInfo* pQueryInfo) {
  return taosArrayGetSize(pQueryInfo->exprList);
}

void addExprParams(SSqlExpr* pExpr, char* argument, int32_t type, int32_t bytes) {
  assert (pExpr != NULL || argument != NULL || bytes != 0);

  // set parameter value
  // transfer to tVariant from byte data/no ascii data
  tVariantCreateFromBinary(&pExpr->param[pExpr->numOfParams], argument, bytes, type);
  pExpr->numOfParams += 1;

  assert(pExpr->numOfParams <= 3);
}

SSqlExpr* SQueryInfo::getExpr(int32_t index) {
  return (SSqlExpr*)taosArrayGetP(exprList, index);
}

SSqlExpr::~SSqlExpr() {
  for (int32_t i = 0; i < tListLen(param); ++i) {
    tVariantDestroy(&param[i]);
  }
}

/*
 * NOTE: Does not release SSqlExprInfo here.
 */
void tscSqlExprInfoDestroy(SArray* pExprInfo) {
  size_t size = taosArrayGetSize(pExprInfo);
  
  for(int32_t i = 0; i < size; ++i) {
    SSqlExpr* pExpr = (SSqlExpr*)taosArrayGetP(pExprInfo, i);
    delete pExpr;
  }
  
  taosArrayDestroy(pExprInfo);
}

int32_t tscSqlExprCopy(SArray* dst, const SArray* src, uint64_t uid, bool deepcopy) {
  assert(src != NULL && dst != NULL);
  
  size_t size = taosArrayGetSize(src);
  for (int32_t i = 0; i < size; ++i) {
    SSqlExpr* pExpr = (SSqlExpr*)taosArrayGetP(src, i);
    
    if (pExpr->uid == uid) {
      
      if (deepcopy) {
        auto p1 = new SSqlExpr;
        if (p1 == NULL) {
          return -1;
        }

        *p1 = *pExpr;
        memset(p1->param, 0, sizeof(tVariant) * tListLen(p1->param));

        for (int32_t j = 0; j < pExpr->numOfParams; ++j) {
          tVariantAssign(&p1->param[j], &pExpr->param[j]);
        }
        
        taosArrayPush(dst, &p1);
      } else {
        taosArrayPush(dst, &pExpr);
      }
    }
  }

  return 0;
}

SColumn* tscColumnListInsert(std::vector<SColumn>& pColumnList, SColumnIndex* pColIndex) {
  // ignore the tbname columnIndex to be inserted into source list
  if (pColIndex->columnIndex < 0) {
    return NULL;
  }
  
  size_t numOfCols = pColumnList.size();
  int16_t col = pColIndex->columnIndex;

  int32_t i = 0;
  while (i < numOfCols) {
    const auto &pCol = pColumnList[i];
    if (pCol.colIndex.columnIndex < col) {
      i++;
    } else if (pCol.colIndex.tableIndex < pColIndex->tableIndex) {
      i++;
    } else {
      break;
    }
  }

  if (i >= numOfCols || numOfCols == 0) {
    pColumnList.push_back({*pColIndex});
  } else {
    const auto &pCol = pColumnList[i];
  
    if (i < numOfCols && (pCol.colIndex.columnIndex > col || pCol.colIndex.tableIndex != pColIndex->tableIndex)) {
      pColumnList.insert(pColumnList.begin() + i, {*pColIndex});
    }
  }

  return &pColumnList[i];
}

SColumnFilterInfo::~SColumnFilterInfo() {
  if (filterstr) {
    tfree(pz);
  }
}

void tscColumnListCopy(std::vector<SColumn>& dst, const std::vector<SColumn> &src, int16_t tableIndex) { 
  for (const auto &pCol : src) {
    if (pCol.colIndex.tableIndex == tableIndex || tableIndex < 0) {
      dst.push_back(pCol);
    }
  }
}

/*
 * 1. normal name, not a keyword or number
 * 2. name with quote
 * 3. string with only one delimiter '.'.
 *
 * only_one_part
 * 'only_one_part'
 * first_part.second_part
 * first_part.'second_part'
 * 'first_part'.second_part
 * 'first_part'.'second_part'
 * 'first_part.second_part'
 *
 */
static int32_t validateQuoteToken(SStrToken* pToken) {
  tscDequoteAndTrimToken(pToken);

  int32_t k = tSQLGetToken(pToken->z, &pToken->type);

  if (pToken->type == TK_STRING) {
    return tscValidateName(pToken);
  }

  if (k != pToken->n || pToken->type != TK_ID) {
    return TSDB_CODE_TSC_INVALID_SQL;
  }
  return TSDB_CODE_SUCCESS;
}

void tscDequoteAndTrimToken(SStrToken* pToken) {
  uint32_t first = 0, last = pToken->n;

  // trim leading spaces
  while (first < last) {
    char c = pToken->z[first];
    if (c != ' ' && c != '\t') {
      break;
    }
    first++;
  }

  // trim ending spaces
  while (first < last) {
    char c = pToken->z[last - 1];
    if (c != ' ' && c != '\t') {
      break;
    }
    last--;
  }

  // there are still at least two characters
  if (first < last - 1) {
    char c = pToken->z[first];
    // dequote
    if ((c == '\'' || c == '"') && c == pToken->z[last - 1]) {
      first++;
      last--;
    }
  }

  // left shift the string and pad spaces
  for (uint32_t i = 0; i + first < last; i++) {
    pToken->z[i] = pToken->z[first + i];
  }
  for (uint32_t i = last - first; i < pToken->n; i++) {
    pToken->z[i] = ' ';
  }

  // adjust token length
  pToken->n = last - first;
}

int32_t tscValidateName(SStrToken* pToken) {
  if (pToken->type != TK_STRING && pToken->type != TK_ID) {
    return TSDB_CODE_TSC_INVALID_SQL;
  }

  char* sep = strnchr(pToken->z, TS_PATH_DELIMITER[0], pToken->n, true);
  if (sep == NULL) {  // single part
    if (pToken->type == TK_STRING) {
      strdequote(pToken->z);
      pToken->n = (uint32_t)strtrim(pToken->z);

      int len = tSQLGetToken(pToken->z, &pToken->type);

      // single token, validate it
      if (len == pToken->n) {
        return validateQuoteToken(pToken);
      } else {
        sep = strnchr(pToken->z, TS_PATH_DELIMITER[0], pToken->n, true);
        if (sep == NULL) {
          return TSDB_CODE_TSC_INVALID_SQL;
        }

        return tscValidateName(pToken);
      }
    } else {
      if (isNumber(pToken)) {
        return TSDB_CODE_TSC_INVALID_SQL;
      }
    }
  } else {  // two part
    int32_t oldLen = pToken->n;
    char*   pStr = pToken->z;

    if (pToken->type == TK_SPACE) {
      pToken->n = (uint32_t)strtrim(pToken->z);
    }

    pToken->n = tSQLGetToken(pToken->z, &pToken->type);
    if (pToken->z[pToken->n] != TS_PATH_DELIMITER[0]) {
      return TSDB_CODE_TSC_INVALID_SQL;
    }

    if (pToken->type != TK_STRING && pToken->type != TK_ID) {
      return TSDB_CODE_TSC_INVALID_SQL;
    }

    if (pToken->type == TK_STRING && validateQuoteToken(pToken) != TSDB_CODE_SUCCESS) {
      return TSDB_CODE_TSC_INVALID_SQL;
    }

    int32_t firstPartLen = pToken->n;

    pToken->z = sep + 1;
    pToken->n = (uint32_t)(oldLen - (sep - pStr) - 1);
    int32_t len = tSQLGetToken(pToken->z, &pToken->type);
    if (len != pToken->n || (pToken->type != TK_STRING && pToken->type != TK_ID)) {
      return TSDB_CODE_TSC_INVALID_SQL;
    }

    if (pToken->type == TK_STRING && validateQuoteToken(pToken) != TSDB_CODE_SUCCESS) {
      return TSDB_CODE_TSC_INVALID_SQL;
    }

    // re-build the whole name string
    if (pStr[firstPartLen] == TS_PATH_DELIMITER[0]) {
      // first part do not have quote do nothing
    } else {
      pStr[firstPartLen] = TS_PATH_DELIMITER[0];
      memmove(&pStr[firstPartLen + 1], pToken->z, pToken->n);
      uint32_t offset = (uint32_t)(pToken->z - (pStr + firstPartLen + 1));
      memset(pToken->z + pToken->n - offset, ' ', offset);
    }
    pToken->n += (firstPartLen + sizeof(TS_PATH_DELIMITER[0]));
    pToken->z = pStr;
  }

  return TSDB_CODE_SUCCESS;
}

void tscIncStreamExecutionCount(void* pStream) {
  if (pStream == NULL) {
    return;
  }

  SSqlStream* ps = (SSqlStream*)pStream;
  ps->num += 1;
}

bool tscValidateColumnId(STableMetaInfo* pTableMetaInfo, int32_t colId, int32_t numOfParams) {
  if (pTableMetaInfo->pTableMeta == NULL) {
    return false;
  }

  if (colId == TSDB_TBNAME_COLUMN_INDEX || (colId <= TSDB_UD_COLUMN_INDEX && numOfParams == 2)) {
    return true;
  }

  const SSchema*    pSchema = pTableMetaInfo->pTableMeta->getSchema();
  const auto& tinfo = pTableMetaInfo->pTableMeta->getInfo();
  
  int32_t  numOfTotal = tinfo.numOfTags + tinfo.numOfColumns;

  for (int32_t i = 0; i < numOfTotal; ++i) {
    if (pSchema[i].colId == colId) {
      return true;
    }
  }

  return false;
}

void tscGetSrcColumnInfo(SSrcColumnInfo* pColInfo, SQueryInfo* pQueryInfo) {
  STableMetaInfo* pTableMetaInfo = tscGetMetaInfo(pQueryInfo, 0);
  const SSchema*        pSchema = pTableMetaInfo->pTableMeta->getSchema();
  
  size_t numOfExprs = tscSqlExprNumOfExprs(pQueryInfo);
  for (int32_t i = 0; i < numOfExprs; ++i) {
    const SSqlExpr* pExpr = pQueryInfo->getExpr(i);
    pColInfo[i].functionId = pExpr->functionId;

    if (TSDB_COL_IS_TAG(pExpr->colInfo.flag)) {
      const SSchema* pTagSchema = pTableMetaInfo->pTableMeta->getTagSchema();
      
      int16_t index = pExpr->colInfo.colIndex;
      pColInfo[i].type = (index != -1) ? pTagSchema[index].type : TSDB_DATA_TYPE_BINARY;
    } else {
      pColInfo[i].type = pSchema[pExpr->colInfo.colIndex].type;
    }
  }
}

/*
 * the following four kinds of SqlObj should not be freed
 * 1. SqlObj for stream computing
 * 2. main SqlObj
 * 3. heartbeat SqlObj
 * 4. SqlObj for subscription
 *
 * If res code is error and SqlObj does not belong to above types, it should be
 * automatically freed for async query, ignoring that connection should be kept.
 *
 * If connection need to be recycled, the SqlObj also should be freed.
 */
bool tscShouldBeFreed(SSqlObj* pSql) {
  if (pSql == NULL) {
    return false;
  }
  
  STscObj* pTscObj = pSql->pTscObj;
  if (pSql->pStream != NULL || pTscObj->hbrid == pSql->self || pSql->pSubscription != NULL) {
    return false;
  }

  // only the table meta and super table vgroup query will free resource automatically
  int32_t command = pSql->cmd.command;
  if (command == TSDB_SQL_META || command == TSDB_SQL_STABLEVGROUP) {
    return true;
  }

  return false;
}


STableMetaInfo* SSqlCmd::getMetaInfo(int32_t clauseIndex, int32_t tableIndex) {
  if (numOfClause == 0) {
    return NULL;
  }

  assert(clauseIndex >= 0 && clauseIndex < numOfClause);

  SQueryInfo* pQueryInfo = tscGetQueryInfoDetail(this, clauseIndex);
  return tscGetMetaInfo(pQueryInfo, tableIndex);
}

STableMetaInfo* tscGetMetaInfo(const SQueryInfo* pQueryInfo, int32_t tableIndex) {
  assert(pQueryInfo != NULL);

  if (pQueryInfo->pTableMetaInfo == NULL) {
    assert(pQueryInfo->numOfTables == 0);
    return NULL;
  }

  assert(tableIndex >= 0 && tableIndex <= pQueryInfo->numOfTables && pQueryInfo->pTableMetaInfo != NULL);

  return pQueryInfo->pTableMetaInfo[tableIndex];
}

SQueryInfo* tscGetQueryInfoDetailSafely(SSqlCmd* pCmd, int32_t subClauseIndex) {
  SQueryInfo* pQueryInfo = tscGetQueryInfoDetail(pCmd, subClauseIndex);
  int32_t ret = TSDB_CODE_SUCCESS;

  while ((pQueryInfo) == NULL) {
    if ((ret = tscAddSubqueryInfo(pCmd)) != TSDB_CODE_SUCCESS) {
      terrno = TSDB_CODE_TSC_OUT_OF_MEMORY;
      return NULL;
    }

    pQueryInfo = tscGetQueryInfoDetail(pCmd, subClauseIndex);
  }

  return pQueryInfo;
}

STableMetaInfo* tscGetTableMetaInfoByUid(SQueryInfo* pQueryInfo, uint64_t uid, int32_t* index) {
  int32_t k = -1;

  for (int32_t i = 0; i < pQueryInfo->numOfTables; ++i) {
    if (pQueryInfo->pTableMetaInfo[i]->pTableMeta->id.uid == uid) {
      k = i;
      break;
    }
  }

  if (index != NULL) {
    *index = k;
  }

  assert(k != -1);
  return tscGetMetaInfo(pQueryInfo, k);
}

void tscInitQueryInfo(SQueryInfo* pQueryInfo) { 
  assert(pQueryInfo->exprList == NULL);
  pQueryInfo->exprList   = (SArray*)taosArrayInit(4, POINTER_BYTES);
  pQueryInfo->udColumnId = TSDB_UD_COLUMN_INDEX;
  pQueryInfo->resColumnId= -1000;
}

int32_t tscAddSubqueryInfo(SSqlCmd* pCmd) {
  assert(pCmd != NULL);

  // todo refactor: remove this structure
  size_t s = pCmd->numOfClause + 1;
  char*  tmp = (char*)realloc(pCmd->pQueryInfo, s * POINTER_BYTES);
  if (tmp == NULL) {
    return TSDB_CODE_TSC_OUT_OF_MEMORY;
  }

  pCmd->pQueryInfo = (SQueryInfo**)tmp;

  auto pQueryInfo = new SQueryInfo;
  if (pQueryInfo == NULL) {
    return TSDB_CODE_TSC_OUT_OF_MEMORY;
  }

  tscInitQueryInfo(pQueryInfo);

  pQueryInfo->window = TSWINDOW_INITIALIZER;
  pQueryInfo->msg = pCmd->payload;  // pointer to the parent error message buffer

  pCmd->pQueryInfo[pCmd->numOfClause++] = pQueryInfo;
  return TSDB_CODE_SUCCESS;
}

static void freeQueryInfoImpl(SQueryInfo* pQueryInfo) {
  pQueryInfo->fieldsInfo.clear();

  tscSqlExprInfoDestroy(pQueryInfo->exprList);
  pQueryInfo->exprList = NULL;

  pQueryInfo->colList.clear();  
  pQueryInfo->tsBuf = (STSBuf*)tsBufDestroy(pQueryInfo->tsBuf);

  tfree(pQueryInfo->fillVal);
}

void tscClearSubqueryInfo(SSqlCmd* pCmd) {
  for (int32_t i = 0; i < pCmd->numOfClause; ++i) {
    SQueryInfo* pQueryInfo = tscGetQueryInfoDetail(pCmd, i);
    freeQueryInfoImpl(pQueryInfo);
  }
}

void clearAllTableMetaInfo(SQueryInfo* pQueryInfo) {
  for(int32_t i = 0; i < pQueryInfo->numOfTables; ++i) {
    STableMetaInfo* pTableMetaInfo = tscGetMetaInfo(pQueryInfo, i);

    pTableMetaInfo->pVgroupTables.clear();
    tscClearTableMetaInfo(pTableMetaInfo);
    free(pTableMetaInfo);
  }
  
  tfree(pQueryInfo->pTableMetaInfo);
}

STableMetaInfo* tscAddTableMetaInfo(SQueryInfo* pQueryInfo, const char* name, STableMeta* pTableMeta,
                                    SVgroupsInfo* vgroupList, const std::vector<SColumn> *pTagCols, 
    const std::vector<SVgroupTableInfo>  &pVgroupTables) {
  void* pAlloc = realloc(pQueryInfo->pTableMetaInfo, (pQueryInfo->numOfTables + 1) * POINTER_BYTES);
  if (pAlloc == NULL) {
    terrno = TSDB_CODE_TSC_OUT_OF_MEMORY;
    return NULL;
  }

  pQueryInfo->pTableMetaInfo = (STableMetaInfo**)pAlloc;
  auto pTableMetaInfo = new STableMetaInfo;
  if (pTableMetaInfo == NULL) {
    terrno = TSDB_CODE_TSC_OUT_OF_MEMORY;
    return NULL;
  }

  pQueryInfo->pTableMetaInfo[pQueryInfo->numOfTables] = pTableMetaInfo;

  if (name != NULL) {
    tstrncpy(&pTableMetaInfo->name[0], name, sizeof(pTableMetaInfo->name));
  }

  pTableMetaInfo->pTableMeta = pTableMeta;
  
  if (vgroupList != NULL) {
    pTableMetaInfo->vgroupList = tscVgroupInfoClone(vgroupList);
  }

  if (pTagCols != NULL) {
    tscColumnListCopy(pTableMetaInfo->tagColList, *pTagCols, -1);
  }

  pTableMetaInfo->pVgroupTables = pVgroupTables;
  
  pQueryInfo->numOfTables += 1;
  return pTableMetaInfo;
}

STableMetaInfo* tscAddEmptyMetaInfo(SQueryInfo* pQueryInfo) {
  return tscAddTableMetaInfo(pQueryInfo, NULL, NULL, NULL, NULL, {});
}

void tscClearTableMetaInfo(STableMetaInfo* pTableMetaInfo) {
  if (pTableMetaInfo == NULL) {
    return;
  }

  tfree(pTableMetaInfo->pTableMeta);

  pTableMetaInfo->vgroupList = (SVgroupsInfo*)tscVgroupInfoClear(pTableMetaInfo->vgroupList);
  pTableMetaInfo->tagColList.clear();
}

void tscResetForNextRetrieve(SSqlRes* pRes) {
  if (pRes == NULL) {
    return;
  }

  pRes->row = 0;
  pRes->numOfRows = 0;
}

void registerSqlObj(SSqlObj* pSql) {
  taosAcquireRef(tscRefId, pSql->pTscObj->rid);
  pSql->self = taosAddRef(tscObjRef, pSql);

  int32_t num   = ++pSql->pTscObj->numOfObj;
  int32_t total = ++tscNumOfObj;
  tscDebug("%p new SqlObj from %p, total in tscObj:%d, total:%d", pSql, pSql->pTscObj, num, total);
}

SSqlObj* createSimpleSubObj(SSqlObj* pSql, __async_cb_func_t fp, void* param, int32_t cmd) {
  SSqlObj* pNew = (SSqlObj*)calloc(1, sizeof(SSqlObj));
  if (pNew == NULL) {
    tscError("%p new subquery failed, tableIndex:%d", pSql, 0);
    return NULL;
  }

  pNew->pTscObj = pSql->pTscObj;

  SSqlCmd* pCmd = &pNew->cmd;
  pCmd->command = cmd;
  pCmd->parseFinished = 1;
  pCmd->autoCreated = pSql->cmd.autoCreated;

  int32_t code = copyTagData(&pNew->cmd.tagData, &pSql->cmd.tagData);
  if (code != TSDB_CODE_SUCCESS) {
    tscError("%p new subquery failed, unable to malloc tag data, tableIndex:%d", pSql, 0);
    free(pNew);
    return NULL;
  }

  if (tscAddSubqueryInfo(pCmd) != TSDB_CODE_SUCCESS) {
    tscFreeSqlObj(pNew);
    return NULL;
  }

  pNew->fp      = fp;
  pNew->fetchFp = fp;
  pNew->param   = param;
  pNew->sqlstr  = NULL;
  pNew->maxRetry = TSDB_MAX_REPLICA;

  SQueryInfo* pQueryInfo = tscGetQueryInfoDetailSafely(pCmd, 0);

  assert(pSql->cmd.clauseIndex == 0);
  const STableMetaInfo* pMasterTableMetaInfo = pSql->cmd.getMetaInfo(pSql->cmd.clauseIndex, 0);

  tscAddTableMetaInfo(pQueryInfo, &pMasterTableMetaInfo->name[0], NULL, NULL, NULL, {});
  registerSqlObj(pNew);

  return pNew;
}

static void doSetSqlExprAndResultFieldInfo(SQueryInfo* pNewQueryInfo, int64_t uid) {
  int32_t numOfOutput = (int32_t)tscSqlExprNumOfExprs(pNewQueryInfo);
  if (numOfOutput == 0) {
    return;
  }

  // set the field info in pNewQueryInfo object according to sqlExpr information
  size_t numOfExprs = tscSqlExprNumOfExprs(pNewQueryInfo);
  for (int32_t i = 0; i < numOfExprs; ++i) {
    SSqlExpr* pExpr = pNewQueryInfo->getExpr(i);

    TAOS_FIELD f = tscCreateField((int8_t) pExpr->resType, pExpr->aliasName, pExpr->resBytes);
    pNewQueryInfo->fieldsInfo.internalField.push_back({f});
    SInternalField* pInfo1 = &pNewQueryInfo->fieldsInfo.internalField.back();
    pInfo1->pSqlExpr = pExpr;
  }

  // update the pSqlExpr pointer in SInternalField according the field name
  // make sure the pSqlExpr point to the correct SqlExpr in pNewQueryInfo, not SqlExpr in pQueryInfo
  for (int32_t f = 0; f < pNewQueryInfo->fieldsInfo.internalField.size(); ++f) {
    TAOS_FIELD* field = &pNewQueryInfo->fieldsInfo.internalField[f].field;

    bool matched = false;
    for (int32_t k1 = 0; k1 < numOfExprs; ++k1) {
      SSqlExpr* pExpr1 = pNewQueryInfo->getExpr(k1);

      if (strcmp(field->name, pExpr1->aliasName) == 0) {  // establish link according to the result field name
        SInternalField* pInfo = &pNewQueryInfo->fieldsInfo.internalField[f];
        pInfo->pSqlExpr = pExpr1;

        matched = true;
        break;
      }
    }

    assert(matched);
    (void)matched;
  }

  tscFieldInfoUpdateOffset(pNewQueryInfo);
}

SSqlObj* createSubqueryObj(SSqlObj* pSql, int16_t tableIndex, __async_cb_func_t fp, void* param, int32_t cmd, SSqlObj* pPrevSql) {
  SSqlCmd* pCmd = &pSql->cmd;

  SSqlObj* pNew = (SSqlObj*)calloc(1, sizeof(SSqlObj));
  if (pNew == NULL) {
    tscError("%p new subquery failed, tableIndex:%d", pSql, tableIndex);
    terrno = TSDB_CODE_TSC_OUT_OF_MEMORY;
    return NULL;
  }
  auto _1 = defer([&] { tscFreeSqlObj(pNew); });
  const STableMetaInfo* pTableMetaInfo = pCmd->getMetaInfo(pCmd->clauseIndex, tableIndex);

  pNew->pTscObj   = pSql->pTscObj;
  pNew->sqlstr    = strdup(pSql->sqlstr);

  SSqlCmd* pnCmd = &pNew->cmd;
  memcpy(pnCmd, pCmd, sizeof(SSqlCmd));
  
  pnCmd->command = cmd;

  pnCmd->pQueryInfo = NULL;
  pnCmd->numOfClause = 0;
  pnCmd->clauseIndex = 0;
  pnCmd->pDataBlocks = NULL;

  pnCmd->parseFinished = 1;
  pnCmd->pTableNameList.clear();
  pnCmd->pTableBlockHashList = NULL;

  if (tscAddSubqueryInfo(pnCmd) != TSDB_CODE_SUCCESS) {
    terrno = TSDB_CODE_TSC_OUT_OF_MEMORY;
    return NULL;
  }

  SQueryInfo* pNewQueryInfo = (SQueryInfo* )tscGetQueryInfoDetail(pnCmd, 0);
  SQueryInfo* pQueryInfo = (SQueryInfo* )tscGetQueryInfoDetail(pCmd, pCmd->clauseIndex);

  pNewQueryInfo->command = pQueryInfo->command;
  memcpy(&pNewQueryInfo->interval, &pQueryInfo->interval, sizeof(pNewQueryInfo->interval));
  pNewQueryInfo->type   = pQueryInfo->type;
  pNewQueryInfo->window = pQueryInfo->window;
  pNewQueryInfo->limit  = pQueryInfo->limit;
  pNewQueryInfo->slimit = pQueryInfo->slimit;
  pNewQueryInfo->order  = pQueryInfo->order;
  pNewQueryInfo->vgroupLimit = pQueryInfo->vgroupLimit;
  pNewQueryInfo->tsBuf  = NULL;
  pNewQueryInfo->fillType = pQueryInfo->fillType;
  pNewQueryInfo->fillVal  = NULL;
  pNewQueryInfo->clauseLimit = pQueryInfo->clauseLimit;
  pNewQueryInfo->numOfTables = 0;
  pNewQueryInfo->pTableMetaInfo = NULL;
  
  pNewQueryInfo->groupbyExpr = pQueryInfo->groupbyExpr;
  pNewQueryInfo->groupbyExpr.columnInfo = pQueryInfo->groupbyExpr.columnInfo;
  
  pNewQueryInfo->tagCond = pQueryInfo->tagCond;

  if (pQueryInfo->fillType != TSDB_FILL_NONE) {
    pNewQueryInfo->fillVal = (int64_t*)malloc(pQueryInfo->fieldsInfo.internalField.size() * sizeof(int64_t));
    if (pNewQueryInfo->fillVal == NULL) {
      terrno = TSDB_CODE_TSC_OUT_OF_MEMORY;
      return NULL;
    }

    memcpy(pNewQueryInfo->fillVal, pQueryInfo->fillVal, pQueryInfo->fieldsInfo.internalField.size() * sizeof(int64_t));
  }

  pnCmd->payload.resize(TSDB_DEFAULT_PAYLOAD_SIZE);
 
  tscColumnListCopy(pNewQueryInfo->colList, pQueryInfo->colList, (int16_t)tableIndex);

  // set the correct query type
  if (pPrevSql != NULL) {
    SQueryInfo* pPrevQueryInfo = tscGetQueryInfoDetail(&pPrevSql->cmd, pPrevSql->cmd.clauseIndex);
    pNewQueryInfo->type = pPrevQueryInfo->type;
  } else {
    TSDB_QUERY_SET_TYPE(pNewQueryInfo->type, TSDB_QUERY_TYPE_SUBQUERY);// it must be the subquery
  }

  uint64_t uid = pTableMetaInfo->pTableMeta->id.uid;
  if (tscSqlExprCopy(pNewQueryInfo->exprList, pQueryInfo->exprList, uid, true) != 0) {
    terrno = TSDB_CODE_TSC_OUT_OF_MEMORY;
    return NULL;
  }

  doSetSqlExprAndResultFieldInfo(pNewQueryInfo, uid);

  pNew->fp      = fp;
  pNew->fetchFp = fp;
  pNew->param   = param;
  pNew->maxRetry = TSDB_MAX_REPLICA;

  const char* name = &pTableMetaInfo->name[0];
  STableMetaInfo* pFinalInfo = NULL;

  if (pPrevSql == NULL) {
    STableMeta* pTableMeta = tscTableMetaClone(pTableMetaInfo->pTableMeta);
    assert(pTableMeta != NULL);

    pFinalInfo = tscAddTableMetaInfo(pNewQueryInfo, name, pTableMeta, pTableMetaInfo->vgroupList,
                                     &pTableMetaInfo->tagColList, pTableMetaInfo->pVgroupTables);
  } else {  // transfer the ownership of pTableMeta to the newly create sql object.
    STableMetaInfo* pPrevInfo = pPrevSql->cmd.getMetaInfo(pPrevSql->cmd.clauseIndex, 0);

    STableMeta*  pPrevTableMeta = tscTableMetaClone(pPrevInfo->pTableMeta);
    SVgroupsInfo* pVgroupsInfo = pPrevInfo->vgroupList;
    pFinalInfo = tscAddTableMetaInfo(pNewQueryInfo, name, pPrevTableMeta, pVgroupsInfo, &pTableMetaInfo->tagColList,
        pTableMetaInfo->pVgroupTables);
  }

  // this case cannot be happened
  if (pFinalInfo->pTableMeta == NULL) {
    tscError("%p new subquery failed since no tableMeta, name:%s", pSql, name);

    if (pPrevSql != NULL) { // pass the previous error to client
      assert(pPrevSql->res.code != TSDB_CODE_SUCCESS);
      terrno = pPrevSql->res.code;
    } else {
      terrno = TSDB_CODE_TSC_APP_ERROR;
    }

    return NULL;
  }
  
  assert(pNewQueryInfo->numOfTables == 1);
  
  if (UTIL_TABLE_IS_SUPER_TABLE(pTableMetaInfo)) {
    assert(pFinalInfo->vgroupList != NULL);
  }

  if (cmd == TSDB_SQL_SELECT) {
    size_t size = pNewQueryInfo->colList.size();
    
    tscDebug(
        "%p new subquery:%p, tableIndex:%d, vgroupIndex:%d, type:%d, exprInfo:%" PRIzu ", colList:%" PRIzu ","
        "fieldInfo:%d, name:%s, qrang:%" PRId64 " - %" PRId64 " order:%d, limit:%" PRId64,
        pSql, pNew, tableIndex, pTableMetaInfo->vgroupIndex, pNewQueryInfo->type, tscSqlExprNumOfExprs(pNewQueryInfo),
        size, pNewQueryInfo->fieldsInfo.internalField.size(), pFinalInfo->name, pNewQueryInfo->window.skey,
        pNewQueryInfo->window.ekey, pNewQueryInfo->order.order, pNewQueryInfo->limit.limit);
    
    tscPrintSelectClause(pNew, 0);
  } else {
    tscDebug("%p new sub insertion: %p, vnodeIdx:%d", pSql, pNew, pTableMetaInfo->vgroupIndex);
  }
  _1.cancel();
  registerSqlObj(pNew);
  return pNew;
}

/**
 * To decide if current is a two-stage super table query, join query, or insert. And invoke different
 * procedure accordingly
 * @param pSql
 */
void tscDoQuery(SSqlObj* pSql) {
  SSqlCmd* pCmd = &pSql->cmd;
  SSqlRes* pRes = &pSql->res;
  
  pRes->code = TSDB_CODE_SUCCESS;
  
  if (pCmd->command > TSDB_SQL_LOCAL) {
    tscProcessLocalCmd(pSql);
    return;
  }
  
  if (pCmd->command == TSDB_SQL_SELECT) {
    tscAddIntoSqlList(pSql);
  }

  if (pCmd->dataSourceType == DATA_FROM_DATA_FILE) {
    tscProcessMultiVnodesImportFromFile(pSql);
  } else {
    SQueryInfo *pQueryInfo = tscGetQueryInfoDetail(pCmd, pCmd->clauseIndex);
    uint16_t type = pQueryInfo->type;
  
    if (TSDB_QUERY_HAS_TYPE(type, TSDB_QUERY_TYPE_INSERT)) {  // multi-vnodes insertion
      tscHandleMultivnodeInsert(pSql);
      return;
    }
  
    if (QUERY_IS_JOIN_QUERY(type)) {
      if (!TSDB_QUERY_HAS_TYPE(type, TSDB_QUERY_TYPE_SUBQUERY)) {
        tscHandleMasterJoinQuery(pSql);
      } else { // for first stage sub query, iterate all vnodes to get all timestamp
        if (!TSDB_QUERY_HAS_TYPE(type, TSDB_QUERY_TYPE_JOIN_SEC_STAGE)) {
          tscProcessSql(pSql);
        } else { // secondary stage join query.
          if (tscIsTwoStageSTableQuery(pQueryInfo, 0)) {  // super table query
            tscLockByThread(&pSql->squeryLock);
            tscHandleMasterSTableQuery(pSql);
            tscUnlockByThread(&pSql->squeryLock);
          } else {
            tscProcessSql(pSql);
          }
        }
      }

      return;
    } else if (tscIsTwoStageSTableQuery(pQueryInfo, 0)) {  // super table query
      tscLockByThread(&pSql->squeryLock);
      tscHandleMasterSTableQuery(pSql);
      tscUnlockByThread(&pSql->squeryLock);
      return;
    }
    
    tscProcessSql(pSql);
  }
}

int16_t tscGetJoinTagColIdByUid(STagCond* pTagCond, uint64_t uid) {
  if (pTagCond->joinInfo.left.uid == uid) {
    return pTagCond->joinInfo.left.tagColId;
  } else if (pTagCond->joinInfo.right.uid == uid) {
    return pTagCond->joinInfo.right.tagColId;
  } else {
    assert(0);
    return -1;
  }
}

int16_t tscGetTagColIndexById(STableMeta* pTableMeta, int16_t colId) {
  int32_t numOfTags = pTableMeta->numOfTags();

  const SSchema* pSchema = pTableMeta->getTagSchema();
  for(int32_t i = 0; i < numOfTags; ++i) {
    if (pSchema[i].colId == colId) {
      return i;
    }
  }

  // can not reach here
  assert(0);
  return INT16_MIN;
}

bool tscIsUpdateQuery(SSqlObj* pSql) {
  if (pSql == NULL) {
    terrno = TSDB_CODE_TSC_DISCONNECTED;
    return TSDB_CODE_TSC_DISCONNECTED;
  }

  SSqlCmd* pCmd = &pSql->cmd;
  return ((pCmd->command >= TSDB_SQL_INSERT && pCmd->command <= TSDB_SQL_DROP_DNODE) || TSDB_SQL_USE_DB == pCmd->command);
}

int32_t tscSQLSyntaxErrMsg(std::string &msg, const char* additionalInfo,  const char* sql) {
  const char* msgFormat1 = "syntax error near \'%s\'";
  const char* msgFormat2 = "syntax error near \'%s\' (%s)";
  const char* msgFormat3 = "%s";

  const char* prefix = "syntax error"; 
  const int32_t BACKWARD_CHAR_STEP = 0;
  char          buffer[1024];
  if (sql == NULL) {
    assert(additionalInfo != NULL);
    sprintf(buffer, msgFormat1, additionalInfo);
    msg = std::string(buffer);
    return TSDB_CODE_TSC_SQL_SYNTAX_ERROR;
  }

  char buf[64] = {0};  // only extract part of sql string
  strncpy(buf, (sql - BACKWARD_CHAR_STEP), tListLen(buf) - 1);

  if (additionalInfo != NULL) {
    sprintf(buffer, msgFormat2, buf, additionalInfo);
    msg = std::string(buffer);
  } else {
    const char* msgFormat = (0 == strncmp(sql, prefix, strlen(prefix))) ? msgFormat3 : msgFormat1; 
    sprintf(buffer, msgFormat, buf);
    msg = std::string(buffer);
  }

  return TSDB_CODE_TSC_SQL_SYNTAX_ERROR;
  
}

int32_t tscInvalidSQLErrMsg(std::string &msg, const char* additionalInfo, const char* sql) {
  const char* msgFormat1 = "invalid SQL: %s";
  const char* msgFormat2 = "invalid SQL: \'%s\' (%s)";
  const char* msgFormat3 = "invalid SQL: \'%s\'";

  const int32_t BACKWARD_CHAR_STEP = 0;
  char          buffer[1024];
  if (sql == NULL) {
    assert(additionalInfo != NULL);
    sprintf(buffer, msgFormat1, additionalInfo);
    msg = std::string(buffer);
    return TSDB_CODE_TSC_INVALID_SQL;
  }

  char buf[64] = {0};  // only extract part of sql string
  strncpy(buf, (sql - BACKWARD_CHAR_STEP), tListLen(buf) - 1);

  if (additionalInfo != NULL) {
    sprintf(buffer, msgFormat2, buf, additionalInfo);
    msg = std::string(buffer);
  } else {
    sprintf(buffer, msgFormat3, buf);  // no additional information for invalid sql error
    msg = std::string(buffer);
  }

  return TSDB_CODE_TSC_INVALID_SQL;
}

bool tscHasReachLimitation(SQueryInfo* pQueryInfo, SSqlRes* pRes) {
  assert(pQueryInfo != NULL && pQueryInfo->clauseLimit != 0);
  return (pQueryInfo->clauseLimit > 0 && pRes->numOfClauseTotal >= pQueryInfo->clauseLimit);
}

/**
 *  If current vnode query does not return results anymore (pRes->numOfRows == 0), try the next vnode if exists,
 *  while multi-vnode super table projection query and the result does not reach the limitation.
 */
bool hasMoreVnodesToTry(SSqlObj* pSql) {
  SSqlCmd* pCmd = &pSql->cmd;
  SSqlRes* pRes = &pSql->res;
  if (pCmd->command != TSDB_SQL_FETCH) {
    return false;
  }

  assert(pRes->completed);
  SQueryInfo* pQueryInfo = tscGetQueryInfoDetail(pCmd, pCmd->clauseIndex);
  STableMetaInfo* pTableMetaInfo = tscGetMetaInfo(pQueryInfo, 0);

  // for normal table, no need to try any more if results are all retrieved from one vnode
  if (!UTIL_TABLE_IS_SUPER_TABLE(pTableMetaInfo) || (pTableMetaInfo->vgroupList == NULL)) {
    return false;
  }
  
  int32_t numOfVgroups = pTableMetaInfo->vgroupList->numOfVgroups;
  if (!pTableMetaInfo->pVgroupTables.empty()) {
    numOfVgroups = pTableMetaInfo->pVgroupTables.size();
  }

  return tscNonOrderedProjectionQueryOnSTable(pQueryInfo, 0) &&
         (!tscHasReachLimitation(pQueryInfo, pRes)) && (pTableMetaInfo->vgroupIndex < numOfVgroups - 1);
}

bool hasMoreClauseToTry(SSqlObj* pSql) {
  return pSql->cmd.clauseIndex < pSql->cmd.numOfClause - 1;
}

void tscTryQueryNextVnode(SSqlObj* pSql, __async_cb_func_t fp) {
  SSqlCmd* pCmd = &pSql->cmd;
  SSqlRes* pRes = &pSql->res;

  SQueryInfo* pQueryInfo = tscGetQueryInfoDetail(pCmd, pCmd->clauseIndex);

  /*
   * no result returned from the current virtual node anymore, try the next vnode if exists
   * if case of: multi-vnode super table projection query
   */
  assert(pRes->numOfRows == 0 && tscNonOrderedProjectionQueryOnSTable(pQueryInfo, 0) && !tscHasReachLimitation(pQueryInfo, pRes));
  STableMetaInfo* pTableMetaInfo = tscGetMetaInfo(pQueryInfo, 0);
  
  int32_t totalVgroups = pTableMetaInfo->vgroupList->numOfVgroups;
  if (++pTableMetaInfo->vgroupIndex < totalVgroups) {
    tscDebug("%p results from vgroup index:%d completed, try next:%d. total vgroups:%d. current numOfRes:%" PRId64, pSql,
             pTableMetaInfo->vgroupIndex - 1, pTableMetaInfo->vgroupIndex, totalVgroups, pRes->numOfClauseTotal);

    /*
     * update the limit and offset value for the query on the next vnode,
     * according to current retrieval results
     *
     * NOTE:
     * if the pRes->offset is larger than 0, the start returned position has not reached yet.
     * Therefore, the pRes->numOfRows, as well as pRes->numOfClauseTotal, must be 0.
     * The pRes->offset value will be updated by virtual node, during query execution.
     */
    if (pQueryInfo->clauseLimit >= 0) {
      pQueryInfo->limit.limit = pQueryInfo->clauseLimit - pRes->numOfClauseTotal;
    }

    pQueryInfo->limit.offset = pRes->offset;
    assert((pRes->offset >= 0 && pRes->numOfRows == 0) || (pRes->offset == 0 && pRes->numOfRows >= 0));
    
    tscDebug("%p new query to next vgroup, index:%d, limit:%" PRId64 ", offset:%" PRId64 ", glimit:%" PRId64,
        pSql, pTableMetaInfo->vgroupIndex, pQueryInfo->limit.limit, pQueryInfo->limit.offset, pQueryInfo->clauseLimit);

    /*
     * For project query with super table join, the numOfSub is equalled to the number of all subqueries.
     * Therefore, we need to reset the value of numOfSubs to be 0.
     *
     * For super table join with projection query, if anyone of the subquery is exhausted, the query completed.
     */
    pSql->subState.numOfSub = 0;
    pCmd->command = TSDB_SQL_SELECT;

    tscResetForNextRetrieve(pRes);

    // set the callback function
    pSql->fp = fp;
    tscProcessSql(pSql);
  } else {
    tscDebug("%p try all %d vnodes, query complete. current numOfRes:%" PRId64, pSql, totalVgroups, pRes->numOfClauseTotal);
  }
}

void tscTryQueryNextClause(SSqlObj* pSql, __async_cb_func_t fp) {
  SSqlCmd* pCmd = &pSql->cmd;
  SSqlRes* pRes = &pSql->res;

  // current subclause is completed, try the next subclause
  assert(pCmd->clauseIndex < pCmd->numOfClause - 1);

  pCmd->clauseIndex++;
  SQueryInfo* pQueryInfo = tscGetQueryInfoDetail(pCmd, pCmd->clauseIndex);

  pSql->cmd.command = pQueryInfo->command;

  //backup the total number of result first
  int64_t num = pRes->numOfTotal + pRes->numOfClauseTotal;
  tscFreeSqlResult(pSql);
  
  pRes->numOfTotal = num;
  
  pSql->pSubs.clear();
  pSql->subState.numOfSub = 0;
  pSql->fp = fp;

  tscDebug("%p try data in the next subclause:%d, total subclause:%d", pSql, pCmd->clauseIndex, pCmd->numOfClause);
  if (pCmd->command > TSDB_SQL_LOCAL) {
    tscProcessLocalCmd(pSql);
  } else {
    tscDoQuery(pSql);
  }
}

void* malloc_throw(size_t size) {
  void* p = malloc(size);
  if (p == NULL) {
    THROW(TSDB_CODE_TSC_OUT_OF_MEMORY);
  }
  return p;
}

void* calloc_throw(size_t nmemb, size_t size) {
  void* p = calloc(nmemb, size);
  if (p == NULL) {
    THROW(TSDB_CODE_TSC_OUT_OF_MEMORY);
  }
  return p;
}

char* strdup_throw(const char* str) {
  char* p = strdup(str);
  if (p == NULL) {
    THROW(TSDB_CODE_TSC_OUT_OF_MEMORY);
  }
  return p;
}

int tscSetMgmtEpSetFromCfg(const char *first, const char *second, SRpcCorEpSet *corMgmtEpSet) {
  corMgmtEpSet->version = 0;
  // init mgmt ip set 
  SRpcEpSet *mgmtEpSet = &(corMgmtEpSet->epSet);
  mgmtEpSet->numOfEps = 0;
  mgmtEpSet->inUse = 0;

  if (first && first[0] != 0) {
    if (strlen(first) >= TSDB_EP_LEN) {
      terrno = TSDB_CODE_TSC_INVALID_FQDN;
      return -1;
    }
    taosGetFqdnPortFromEp(first, mgmtEpSet->fqdn[mgmtEpSet->numOfEps], &(mgmtEpSet->port[mgmtEpSet->numOfEps]));
    mgmtEpSet->numOfEps++;
  }

  if (second && second[0] != 0) {
    if (strlen(second) >= TSDB_EP_LEN) {
      terrno = TSDB_CODE_TSC_INVALID_FQDN;
      return -1;
    }
    taosGetFqdnPortFromEp(second, mgmtEpSet->fqdn[mgmtEpSet->numOfEps], &(mgmtEpSet->port[mgmtEpSet->numOfEps]));
    mgmtEpSet->numOfEps++;
  }

  if (mgmtEpSet->numOfEps == 0) {
    terrno = TSDB_CODE_TSC_INVALID_FQDN;
    return -1;
  }

  return 0;
}

bool tscSetSqlOwner(SSqlObj* pSql) {
  SSqlRes* pRes = &pSql->res;

  // set the sql object owner
  uint64_t threadId = taosGetSelfPthreadId();
  if (atomic_val_compare_exchange_64(&pSql->owner, 0, threadId) != 0) {
    pRes->code = TSDB_CODE_QRY_IN_EXEC;
    return false;
  }

  return true;
}

void tscClearSqlOwner(SSqlObj* pSql) {
  assert(taosCheckPthreadValid(pSql->owner));
  atomic_store_64(&pSql->owner, 0);
}

SVgroupsInfo* tscVgroupInfoClone(SVgroupsInfo *vgroupList) {
  if (vgroupList == NULL) {
    return NULL;
  }

  size_t size = sizeof(SVgroupsInfo) + sizeof(SVgroupInfo) * vgroupList->numOfVgroups;
  SVgroupsInfo* pNew = (SVgroupsInfo*)calloc(1, size);
  if (pNew == NULL) {
    return NULL;
  }

  pNew->numOfVgroups = vgroupList->numOfVgroups;

  for(int32_t i = 0; i < vgroupList->numOfVgroups; ++i) {
    SVgroupInfo* pNewVInfo = &pNew->vgroups[i];

    SVgroupInfo* pvInfo = &vgroupList->vgroups[i];
    pNewVInfo->vgId = pvInfo->vgId;
    pNewVInfo->numOfEps = pvInfo->numOfEps;

    for(int32_t j = 0; j < pvInfo->numOfEps; ++j) {
      pNewVInfo->epAddr[j].fqdn = pvInfo->epAddr[j].fqdn;
      pNewVInfo->epAddr[j].port = pvInfo->epAddr[j].port;
    }
  }

  return pNew;
}

void* tscVgroupInfoClear(SVgroupsInfo *vgroupList) {
  if (vgroupList == NULL) {
    return NULL;
  }

  delete vgroupList;
  return NULL;
}

char* serializeTagData(STagData* pTagData, char* pMsg) {
  int32_t n = (int32_t) strlen(&pTagData->name[0]);
  *(int32_t*) pMsg = htonl(n);
  pMsg += sizeof(n);

  memcpy(pMsg, &pTagData->name[0], n);
  pMsg += n;

  *(int32_t*)pMsg = htonl(pTagData->dataLen);
  pMsg += sizeof(int32_t);

  memcpy(pMsg, pTagData->data, pTagData->dataLen);
  pMsg += pTagData->dataLen;

  return pMsg;
}

int32_t copyTagData(STagData* dst, const STagData* src) {
  dst->dataLen = src->dataLen;
  dst->name = src->name;

  if (dst->dataLen > 0) {
    dst->data = (char*)malloc(dst->dataLen);
    if (dst->data == NULL) {
      return -1;
    }

    memcpy(dst->data, src->data, dst->dataLen);
  }

  return 0;
}

STableMeta* createSuperTableMeta(STableMetaMsg* pChild) {
  assert(pChild != NULL);
  int32_t total = pChild->numOfColumns + pChild->numOfTags;

  auto pTableMeta = new STableMeta;
  pTableMeta->tableType = TSDB_SUPER_TABLE;
  pTableMeta->tableInfo.numOfTags = pChild->numOfTags;
  pTableMeta->tableInfo.numOfColumns = pChild->numOfColumns;
  pTableMeta->tableInfo.precision = pChild->precision;

  pTableMeta->id.tid = 0;
  pTableMeta->id.uid = pChild->suid;
  pTableMeta->tversion = pChild->tversion;
  pTableMeta->sversion = pChild->sversion;
  pTableMeta->schema.resize(total);
  memcpy(&pTableMeta->schema[0], pChild->schema, sizeof(SSchema) * total);

  int32_t num = pTableMeta->tableInfo.numOfColumns;
  for(int32_t i = 0; i < num; ++i) {
    pTableMeta->tableInfo.rowSize += pTableMeta->schema[i].bytes;
  }

  return pTableMeta;
}

uint32_t tscGetTableMetaSize(STableMeta* pTableMeta) {
  assert(pTableMeta != NULL);

  int32_t totalCols = pTableMeta->tableInfo.numOfColumns + pTableMeta->tableInfo.numOfTags;
  return sizeof(STableMeta) + totalCols * sizeof(SSchema);
}

int32_t tscCreateTableMetaFromCChildMeta(STableMeta* pChild, const char* name) {
  assert(pChild != NULL);

  uint32_t size = tscGetTableMetaMaxSize();
  STableMeta* p = (STableMeta*)calloc(1, size);

  taosHashGetClone(tscTableMetaInfo, &pChild->sTableName[0], strnlen(&pChild->sTableName[0], TSDB_TABLE_FNAME_LEN), NULL, p, -1);
  if (p->id.uid > 0) { // tableMeta exists, build child table meta and return
    pChild->sversion = p->sversion;
    pChild->tversion = p->tversion;

    memcpy(&pChild->tableInfo, &p->tableInfo, sizeof(STableInfo));
    int32_t total = pChild->tableInfo.numOfColumns + pChild->tableInfo.numOfTags;

    memcpy(&pChild->schema[0], &p->schema[0], sizeof(SSchema) *total);

    tfree(p);
    return TSDB_CODE_SUCCESS;
  } else { // super table has been removed, current tableMeta is also expired. remove it here
    taosHashRemove(tscTableMetaInfo, name, strnlen(name, TSDB_TABLE_FNAME_LEN));

    tfree(p);
    return -1;
  }
}

uint32_t tscGetTableMetaMaxSize() {
  return sizeof(STableMeta) + TSDB_MAX_COLUMNS * sizeof(SSchema);
}

STableMeta* tscTableMetaClone(STableMeta* pTableMeta) {
  assert(pTableMeta != NULL);
  uint32_t size = tscGetTableMetaSize(pTableMeta);
  STableMeta* p = (STableMeta*)calloc(1, size);
  memcpy(p, pTableMeta, size);
  return p;
}


