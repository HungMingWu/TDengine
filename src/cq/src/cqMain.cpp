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

#include <errno.h>
#include <pthread.h>
#include <stdlib.h>
#include <string.h>

#include "taos.h"
#include "tsclient.h"
#include "taosdef.h"
#include "taosmsg.h"
#include "ttimer.h"
#include "tcq.h"
#include "tdataformat.h"
#include "tglobal.h"
#include "tlog.h"
#include "walInt.h"
#include <algorithm>
#include <list>
#include <mutex>
#include <string>
#include <vector>

#define cFatal(...) { if (cqDebugFlag & DEBUG_FATAL) { taosPrintLog("CQ  FATAL ", 255, __VA_ARGS__); }}
#define cError(...) { if (cqDebugFlag & DEBUG_ERROR) { taosPrintLog("CQ  ERROR ", 255, __VA_ARGS__); }}
#define cWarn(...)  { if (cqDebugFlag & DEBUG_WARN)  { taosPrintLog("CQ  WARN ", 255, __VA_ARGS__); }}
#define cInfo(...)  { if (cqDebugFlag & DEBUG_INFO)  { taosPrintLog("CQ  ", 255, __VA_ARGS__); }}
#define cDebug(...) { if (cqDebugFlag & DEBUG_DEBUG) { taosPrintLog("CQ  ", cqDebugFlag, __VA_ARGS__); }}
#define cTrace(...) { if (cqDebugFlag & DEBUG_TRACE) { taosPrintLog("CQ  ", cqDebugFlag, __VA_ARGS__); }}

typedef struct {
  int32_t  vgId;
  int32_t  master;
  int32_t  num;      // number of continuous streams
  char     user[TSDB_USER_LEN];
  char     pass[TSDB_KEY_LEN];
  char     db[TSDB_DB_NAME_LEN];
  FCqWrite cqWrite;
  std::list<struct SCqObj *> pObjectList;
  void    *dbConn;
  void    *tmrCtrl;
  std::mutex mutex;
} SCqContext;

typedef struct SCqObj {
  tmr_h          tmrId;
  uint64_t       uid;
  int32_t        tid;      // table ID
  int32_t        rowSize;  // bytes of a row
  std::string    dstTable;
  std::string    sqlStr;   // SQL string
  STSchema *     pSchema;  // pointer to schema array
  void *         pStream;
  SCqContext *   pContext;
} SCqObj;

static void cqProcessStreamRes(void *param, TAOS_RES *tres, TAOS_ROW row); 
static void cqCreateStream(SCqContext *pContext, SCqObj *pObj);

void *cqOpen(void *ahandle, const SCqCfg *pCfg) {
  if (tsEnableStream == 0) {
    return NULL;
  }
  auto pContext = new(std::nothrow) SCqContext{};
  if (pContext == NULL) {
    terrno = TAOS_SYSTEM_ERROR(errno);
    return NULL;
  }

  pContext->tmrCtrl = taosTmrInit(0, 0, 0, "CQ");

  tstrncpy(pContext->user, pCfg->user, sizeof(pContext->user));
  tstrncpy(pContext->pass, pCfg->pass, sizeof(pContext->pass));
  const char* db = pCfg->db;
  for (const char* p = db; *p != 0; p++) {
    if (*p == '.') {
      db = p + 1;
      break;
    }
  }
  tstrncpy(pContext->db, db, sizeof(pContext->db));
  pContext->vgId = pCfg->vgId;
  pContext->cqWrite = pCfg->cqWrite;
  tscEmbedded = 1;

  cDebug("vgId:%d, CQ is opened", pContext->vgId);

  return pContext;
}

void cqClose(void *handle) {
  if (tsEnableStream == 0) {
    return;
  }
  SCqContext *pContext = new (std::nothrow) SCqContext{};
  if (handle == NULL) return;

  // stop all CQs
  cqStop(pContext);

  // free all resources
  pContext->mutex.lock();

  for (auto &pObj : pContext->pObjectList) {
    tdFreeSchema(pObj->pSchema);
    delete pObj;
  } 
  
  pContext->mutex.unlock();

  taosTmrCleanUp(pContext->tmrCtrl);
  pContext->tmrCtrl = NULL;

  cDebug("vgId:%d, CQ is closed", pContext->vgId);
  delete pContext;
}

void cqStart(void *handle) {
  if (tsEnableStream == 0) {
    return;
  }
  SCqContext *pContext = static_cast<SCqContext *>(handle);
  if (pContext->dbConn || pContext->master) return;

  cDebug("vgId:%d, start all CQs", pContext->vgId);
  std::lock_guard<std::mutex> lock(pContext->mutex);

  pContext->master = 1;

  for (auto &pObj : pContext->pObjectList) {
    cqCreateStream(pContext, pObj);
  }

}

void cqStop(void *handle) {
  if (tsEnableStream == 0) {
    return;
  }
  SCqContext *pContext = static_cast<SCqContext *>(handle);
  cDebug("vgId:%d, stop all CQs", pContext->vgId);
  if (pContext->dbConn == NULL || pContext->master == 0) return;

  std::lock_guard<std::mutex> lock(pContext->mutex);

  pContext->master = 0;
  for (auto &pObj : pContext->pObjectList) {
    if (pObj->pStream) {
      taos_close_stream(pObj->pStream);
      pObj->pStream = NULL;
      cInfo("vgId:%d, id:%d CQ:%s is closed", pContext->vgId, pObj->tid, pObj->sqlStr);
    } else {
      taosTmrStop(pObj->tmrId);
      pObj->tmrId = 0;
    }
  }

  if (pContext->dbConn) taos_close(pContext->dbConn);
  pContext->dbConn = NULL;
}

void *cqCreate(void *handle, uint64_t uid, int32_t sid, const char* dstTable, char *sqlStr, STSchema *pSchema) {
  if (tsEnableStream == 0) {
    return NULL;
  }
  SCqContext *pContext = static_cast<SCqContext *>(handle);

  auto pObj = new (std::nothrow) SCqObj{};
  if (pObj == NULL) return NULL;

  pObj->uid = uid;
  pObj->tid = sid;
  if (dstTable != NULL) {
    pObj->dstTable = std::string(dstTable);
  }
  pObj->sqlStr = std::string(sqlStr);

  pObj->pSchema = tdDupSchema(pSchema);
  pObj->rowSize = schemaTLen(pSchema);

  cInfo("vgId:%d, id:%d CQ:%s is created", pContext->vgId, pObj->tid, pObj->sqlStr);

  std::lock_guard<std::mutex> lock(pContext->mutex);

  pContext->pObjectList.emplace_front(pObj);

  cqCreateStream(pContext, pObj);

  return pObj;
}

void cqDrop(void *handle) {
  if (tsEnableStream == 0) {
    return;
  }
  SCqObj *pObj = static_cast<SCqObj *>(handle);
  SCqContext *pContext = pObj->pContext;

  std::lock_guard<std::mutex> lock(pContext->mutex);

  auto it = std::find(begin(pContext->pObjectList), end(pContext->pObjectList), pObj);
  pContext->pObjectList.erase(it);

  // free the resources associated
  if (pObj->pStream) {
    taos_close_stream(pObj->pStream);
    pObj->pStream = NULL;
  } else {
    taosTmrStop(pObj->tmrId);
    pObj->tmrId = 0;
  }

  cInfo("vgId:%d, id:%d CQ:%s is dropped", pContext->vgId, pObj->tid, pObj->sqlStr); 
  tdFreeSchema(pObj->pSchema);
  delete pObj;

}

static void doCreateStream(void *param, TAOS_RES *result, int32_t code) {
  SCqObj* pObj = (SCqObj*)param;
  SCqContext* pContext = pObj->pContext;
  SSqlObj* pSql = (SSqlObj*)result;
  if (atomic_val_compare_exchange_ptr(&(pContext->dbConn), NULL, pSql->pTscObj) != NULL) {
    taos_close(pSql->pTscObj);
  }
  std::lock_guard<std::mutex> lock(pContext->mutex);
  cqCreateStream(pContext, pObj);
}

static void cqProcessCreateTimer(SCqObj *pObj, void *tmrId) {
  SCqContext* pContext = pObj->pContext;

  if (pContext->dbConn == NULL) {
    cDebug("vgId:%d, try connect to TDengine", pContext->vgId);
    taos_connect_a(NULL, pContext->user, pContext->pass, pContext->db, 0, doCreateStream, pObj, NULL);
  } else {
    std::lock_guard<std::mutex> lock(pContext->mutex);
    cqCreateStream(pContext, pObj);
  }
}

static void cqCreateStream(SCqContext *pContext, SCqObj *pObj) {
  pObj->pContext = pContext;

  if (pContext->dbConn == NULL) {
    cDebug("vgId:%d, create dbConn after 1000 ms", pContext->vgId);
    pObj->tmrId = taosTmrStart([pObj](void *tmrId) { cqProcessCreateTimer(pObj, tmrId); }, 1000, pContext->tmrCtrl);
    return;
  }
  pObj->tmrId = 0;

  if (pObj->pStream == NULL) {
    pObj->pStream = taos_open_stream(pContext->dbConn, pObj->sqlStr.c_str(), cqProcessStreamRes, 0, pObj, NULL);

    // TODO the pObj->pStream may be released if error happens
    if (pObj->pStream) {
      tscSetStreamDestTable(static_cast<SSqlStream*>(pObj->pStream), pObj->dstTable.c_str());
      pContext->num++;
      cDebug("vgId:%d, id:%d CQ:%s is opened", pContext->vgId, pObj->tid, pObj->sqlStr);
    } else {
      cError("vgId:%d, id:%d CQ:%s, failed to open", pContext->vgId, pObj->tid, pObj->sqlStr);
    }
  }
}

static void cqProcessStreamRes(void *param, TAOS_RES *tres, TAOS_ROW row) {
  SCqObj *pObj = (SCqObj *)param;
  if (tres == NULL && row == NULL) {
    taos_close_stream(pObj->pStream);

    pObj->pStream = NULL;
    return;
  }

  SCqContext *pContext = pObj->pContext;
  STSchema   *pSchema = pObj->pSchema;
  if (pObj->pStream == NULL) return;

  cDebug("vgId:%d, id:%d CQ:%s stream result is ready", pContext->vgId, pObj->tid, pObj->sqlStr);

  int32_t size = sizeof(SWalHead) + sizeof(SSubmitMsg) + sizeof(SSubmitBlk) + TD_DATA_ROW_HEAD_SIZE + pObj->rowSize;
  std::vector<char> buffer(size);

  SWalHead   *pHead = (SWalHead *)(buffer.data());
  SSubmitMsg *pMsg = (SSubmitMsg *) (buffer.data() + sizeof(SWalHead));
  SSubmitBlk *pBlk = (SSubmitBlk *) (buffer.data() + sizeof(SWalHead) + sizeof(SSubmitMsg));

  SDataRow trow = (SDataRow)pBlk->data;
  tdInitDataRow(trow, pSchema);

  for (int32_t i = 0; i < pSchema->numOfCols; i++) {
    STColumn *c = pSchema->columns + i;
    void* val = row[i];
    if (val == NULL) {
      val = getNullValue(c->type);
    } else if (c->type == TSDB_DATA_TYPE_BINARY) {
      val = ((char*)val) - sizeof(VarDataLenT);
    } else if (c->type == TSDB_DATA_TYPE_NCHAR) {
      char buf[TSDB_MAX_NCHAR_LEN];
      int32_t len = taos_fetch_lengths(tres)[i];
      taosMbsToUcs4(static_cast<char*>(val), len, buf, sizeof(buf), &len);
      memcpy(val + sizeof(VarDataLenT), buf, len);
      varDataLen(val) = len;
    }
    tdAppendColVal(trow, val, c->type, c->bytes, c->offset);
  }
  pBlk->dataLen = htonl(dataRowLen(trow));
  pBlk->schemaLen = 0;

  pBlk->uid = htobe64(pObj->uid);
  pBlk->tid = htonl(pObj->tid);
  pBlk->numOfRows = htons(1);
  pBlk->sversion = htonl(pSchema->version);
  pBlk->padding = 0;

  pHead->len = sizeof(SSubmitMsg) + sizeof(SSubmitBlk) + dataRowLen(trow);

  pMsg->header.vgId = htonl(pContext->vgId);
  pMsg->header.contLen = htonl(pHead->len);
  pMsg->length = pMsg->header.contLen;
  pMsg->numOfBlocks = htonl(1);

  pHead->msgType = TSDB_MSG_TYPE_SUBMIT;
  pHead->version = 0;

  // write into vnode write queue
  pContext->cqWrite(pContext->vgId, pHead, TAOS_QTYPE_CQ, NULL);
}

