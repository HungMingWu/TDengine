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

#include <atomic>
#include "os.h"
#include "http.h"
#include "mnode.h"
#include "dnodeVRead.h"
#include "dnodeVWrite.h"
#include "dnodeMRead.h"
#include "dnodeMWrite.h"
#include "dnodeShell.h"
#include "dnodeStep.h"

static void    dnodeProcessMsgFromShell(SRpcMsg *pMsg, SRpcEpSet *);
static int     dnodeRetrieveUserAuthInfo(char *user, char *spi, char *encrypt, char *secret, char *ckey);
static void  * tsShellRpc = NULL;
static std::atomic<int32_t> tsQueryReqNum{0};
static std::atomic<int32_t> tsSubmitReqNum{0};

int32_t dnodeInitShell() {
  int32_t numOfThreads = (tsNumOfCores * tsNumOfThreadsPerCore) / 2.0;
  if (numOfThreads < 1) {
    numOfThreads = 1;
  }

  SRpcInit rpcInit;
  memset(&rpcInit, 0, sizeof(rpcInit));
  rpcInit.localPort    = tsDnodeShellPort;
  rpcInit.label        = "SHELL";
  rpcInit.numOfThreads = numOfThreads;
  rpcInit.cfp          = dnodeProcessMsgFromShell;
  rpcInit.sessions     = tsMaxShellConns;
  rpcInit.connType     = TAOS_CONN_SERVER;
  rpcInit.idleTime     = tsShellActivityTimer * 1000;
  rpcInit.afp          = dnodeRetrieveUserAuthInfo;

  tsShellRpc = rpcOpen(rpcInit);
  if (tsShellRpc == NULL) {
    dError("failed to init shell rpc server");
    return -1;
  }

  dInfo("dnode shell rpc server is initialized");
  return 0;
}

void dnodeCleanupShell() {
  if (tsShellRpc) {
    rpcClose(tsShellRpc);
    tsShellRpc = NULL;
  }
}

static void dnodeProcessMsgFromShell(SRpcMsg *pMsg, SRpcEpSet *pEpSet) {
  SRpcMsg rpcMsg;
  rpcMsg.handle = pMsg->handle;
  rpcMsg.pCont = NULL;
  rpcMsg.contLen = 0;

  if (pMsg->pCont == NULL) return;

  if (dnodeGetRunStatus() != TSDB_RUN_STATUS_RUNING) {
    dError("RPC %p, shell msg:%s is ignored since dnode not running", pMsg->handle, taosMsg[pMsg->msgType]);
    rpcMsg.code = TSDB_CODE_APP_NOT_READY;
    rpcSendResponse(&rpcMsg);
    rpcFreeCont(pMsg->pCont);
    return;
  }

  if (pMsg->msgType == TSDB_MSG_TYPE_QUERY) {
    tsQueryReqNum++;
  } else if (pMsg->msgType == TSDB_MSG_TYPE_SUBMIT) {
    tsSubmitReqNum++;
  } else {}

  if (pMsg->msgType == TSDB_MSG_TYPE_SUBMIT || pMsg->msgType  == TSDB_MSG_TYPE_UPDATE_TAG_VAL) {
    dnodeDispatchToVWriteQueue(pMsg);
  } else if (pMsg->msgType == TSDB_MSG_TYPE_QUERY || pMsg->msgType == TSDB_MSG_TYPE_FETCH) {
    dnodeDispatchToVReadQueue(pMsg);
  } else if (pMsg->msgType == TSDB_MSG_TYPE_NETWORK_TEST) {
    dnodeSendStartupStep(pMsg);
  } else if (pMsg->msgType == TSDB_MSG_TYPE_CM_CREATE_ACCT ||
      pMsg->msgType == TSDB_MSG_TYPE_CM_ALTER_ACCT ||
      pMsg->msgType == TSDB_MSG_TYPE_CM_DROP_ACCT ||
      pMsg->msgType == TSDB_MSG_TYPE_CM_CREATE_USER ||
      pMsg->msgType == TSDB_MSG_TYPE_CM_ALTER_USER ||
      pMsg->msgType == TSDB_MSG_TYPE_CM_DROP_USER ||
      pMsg->msgType == TSDB_MSG_TYPE_CM_CREATE_DNODE ||
      pMsg->msgType == TSDB_MSG_TYPE_CM_DROP_DNODE ||
      pMsg->msgType == TSDB_MSG_TYPE_CM_CREATE_DB ||
      pMsg->msgType == TSDB_MSG_TYPE_CM_DROP_DB ||
      pMsg->msgType == TSDB_MSG_TYPE_CM_ALTER_DB ||
      pMsg->msgType == TSDB_MSG_TYPE_CM_CREATE_TABLE ||
      pMsg->msgType == TSDB_MSG_TYPE_CM_DROP_TABLE ||
      pMsg->msgType == TSDB_MSG_TYPE_CM_ALTER_TABLE ||
      pMsg->msgType == TSDB_MSG_TYPE_CM_ALTER_STREAM ||
      pMsg->msgType == TSDB_MSG_TYPE_CM_KILL_QUERY ||
      pMsg->msgType == TSDB_MSG_TYPE_CM_KILL_STREAM ||
      pMsg->msgType == TSDB_MSG_TYPE_CM_KILL_CONN ||
      pMsg->msgType == TSDB_MSG_TYPE_CM_CONFIG_DNODE) {
    dnodeDispatchToMWriteQueue(pMsg);
  } else if (pMsg->msgType == TSDB_MSG_TYPE_CM_HEARTBEAT ||
             pMsg->msgType == TSDB_MSG_TYPE_CM_CONNECT ||
             pMsg->msgType == TSDB_MSG_TYPE_CM_USE_DB ||
             pMsg->msgType == TSDB_MSG_TYPE_CM_TABLE_META ||
             pMsg->msgType == TSDB_MSG_TYPE_CM_STABLE_VGROUP ||
             pMsg->msgType == TSDB_MSG_TYPE_CM_TABLES_META ||
             pMsg->msgType == TSDB_MSG_TYPE_CM_SHOW ||
             pMsg->msgType == TSDB_MSG_TYPE_CM_RETRIEVE
      ) {
    dnodeDispatchToMReadQueue(pMsg);
  } else {
    dError("RPC %p, shell msg:%s is not processed", pMsg->handle, taosMsg[pMsg->msgType]);
    rpcMsg.code = TSDB_CODE_DND_MSG_NOT_PROCESSED;
    rpcSendResponse(&rpcMsg);
    rpcFreeCont(pMsg->pCont);
    return;
  }
}

static int32_t dnodeAuthNettestUser(char *user, char *spi, char *encrypt, char *secret, char *ckey) {
  if (strcmp(user, "nettestinternal") == 0) {
    char pass[32] = {0};
    taosEncryptPass((uint8_t *)user, strlen(user), pass);
    *spi = 0;
    *encrypt = 0;
    *ckey = 0;
    memcpy(secret, pass, TSDB_KEY_LEN);
    dTrace("nettest user is authorized");
    return 0;
  }

  return -1;
}

static int dnodeRetrieveUserAuthInfo(char *user, char *spi, char *encrypt, char *secret, char *ckey) {
  if (dnodeAuthNettestUser(user, spi, encrypt, secret, ckey) == 0) return 0;  
  int code = mnodeRetriveAuth(user, spi, encrypt, secret, ckey);
  if (code != TSDB_CODE_APP_NOT_READY) return code;

  SAuthMsg *pMsg = static_cast<SAuthMsg *>(rpcMallocCont(sizeof(SAuthMsg)));
  tstrncpy(pMsg->user, user, sizeof(pMsg->user));

  SRpcMsg rpcMsg = {0};
  rpcMsg.pCont = pMsg;
  rpcMsg.contLen = sizeof(SAuthMsg);
  rpcMsg.msgType = TSDB_MSG_TYPE_DM_AUTH;
  
  dDebug("user:%s, send auth msg to mnodes", user);
  SRpcMsg rpcRsp = {0};
  dnodeSendMsgToMnodeRecv(&rpcMsg, &rpcRsp);

  if (rpcRsp.code != 0) {
    dError("user:%s, auth msg received from mnodes, error:%s", user, tstrerror(rpcRsp.code));
  } else {
    SAuthRsp *pRsp = static_cast<SAuthRsp *>(rpcRsp.pCont);
    dDebug("user:%s, auth msg received from mnodes", user);
    memcpy(secret, pRsp->secret, TSDB_KEY_LEN);
    memcpy(ckey, pRsp->ckey, TSDB_KEY_LEN);
    *spi = pRsp->spi;
    *encrypt = pRsp->encrypt;
  }

  rpcFreeCont(rpcRsp.pCont);
  return rpcRsp.code;
}

void *dnodeSendCfgTableToRecv(int32_t vgId, int32_t tid) {
  dDebug("vgId:%d, tid:%d send config table msg to mnode", vgId, tid);

  int32_t contLen = sizeof(SConfigTableMsg);
  SConfigTableMsg *pMsg = static_cast<SConfigTableMsg *>(rpcMallocCont(contLen));

  pMsg->dnodeId = htonl(dnodeGetDnodeId());
  pMsg->vgId = htonl(vgId);
  pMsg->tid = htonl(tid);

  SRpcMsg rpcMsg = {0};
  rpcMsg.pCont = pMsg;
  rpcMsg.contLen = contLen;
  rpcMsg.msgType = TSDB_MSG_TYPE_DM_CONFIG_TABLE;

  SRpcMsg rpcRsp = {0};
  dnodeSendMsgToMnodeRecv(&rpcMsg, &rpcRsp);
  terrno = rpcRsp.code;
  
  if (rpcRsp.code != 0) {
    rpcFreeCont(rpcRsp.pCont);
    dError("vgId:%d, tid:%d failed to config table from mnode", vgId, tid);
    return NULL;
  } else {
    dInfo("vgId:%d, tid:%d config table msg is received", vgId, tid);
    
    // delete this after debug finished
    SMDCreateTableMsg *pTable = static_cast<SMDCreateTableMsg *>(rpcRsp.pCont);
    int16_t   numOfColumns = htons(pTable->numOfColumns);
    int16_t   numOfTags = htons(pTable->numOfTags);
    int32_t   tableId = htonl(pTable->tid);
    uint64_t  uid = htobe64(pTable->uid);
    dInfo("table:%s, numOfColumns:%d numOfTags:%d tid:%d uid:%" PRIu64, pTable->tableFname, numOfColumns, numOfTags, tableId, uid);

    return rpcRsp.pCont;
  }
}

SStatisInfo dnodeGetStatisInfo() {
  SStatisInfo info = {0};
  if (dnodeGetRunStatus() == TSDB_RUN_STATUS_RUNING) {
    info.httpReqNum   = httpGetReqCount();
    info.queryReqNum  = tsQueryReqNum.exchange(0);
    info.submitReqNum = tsSubmitReqNum.exchange(0);
  }

  return info;
}