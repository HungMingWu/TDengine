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

#define _DEFAULT_SOURCE
#include "os.h"
#include "tqueue.h"
#include "tworker.h"
#include "dnodeVRead.h"
#include "vnodeMgmt.h"
#include "vnodeRead.h"

static void *dnodeProcessReadQueue(void *pWorker);

// module global variable
static SWorkerPool tsVQueryWP;
static SWorkerPool tsVFetchWP;

int32_t dnodeInitVRead() {
  const int32_t maxFetchThreads = 4;

  // calculate the available query thread
  float threadsForQuery = MAX(tsNumOfCores * tsRatioOfQueryCores, 1);

  tsVQueryWP.name = "vquery";
  tsVQueryWP.workerFp = dnodeProcessReadQueue;
  tsVQueryWP.min = (int32_t) threadsForQuery;
  tsVQueryWP.max = tsVQueryWP.min;
  if (tWorkerInit(&tsVQueryWP) != 0) return -1;

  tsVFetchWP.name = "vfetch";
  tsVFetchWP.workerFp = dnodeProcessReadQueue;
  tsVFetchWP.min = MIN(maxFetchThreads, tsNumOfCores);
  tsVFetchWP.max = tsVFetchWP.min;
  if (tWorkerInit(&tsVFetchWP) != 0) return -1;

  return 0;
}

void dnodeCleanupVRead() {
  tWorkerCleanup(&tsVFetchWP);
  tWorkerCleanup(&tsVQueryWP);
}

void dnodeDispatchToVReadQueue(SRpcMsg *pMsg) {
  int32_t queuedMsgNum = 0;
  int32_t leftLen = pMsg->contLen;
  int32_t code = TSDB_CODE_VND_INVALID_VGROUP_ID;
  char *  pCont = static_cast<char*>(pMsg->pCont);

  while (leftLen > 0) {
    SMsgHead *pHead = (SMsgHead *)pCont;
    pHead->vgId = htonl(pHead->vgId);
    pHead->contLen = htonl(pHead->contLen);

    assert(pHead->contLen > 0);
    SVnodeObj *pVnode = vnodeAcquire(pHead->vgId);
    if (pVnode != NULL) {
      code = vnodeWriteToRQueue(pVnode, pCont, pHead->contLen, TAOS_QTYPE_RPC, pMsg);
      if (code == TSDB_CODE_SUCCESS) queuedMsgNum++;
      vnodeRelease(pVnode);
    }

    leftLen -= pHead->contLen;
    pCont -= pHead->contLen;
  }

  if (queuedMsgNum == 0) {
    SRpcMsg rpcRsp;
    rpcRsp.handle = pMsg->handle;
    rpcRsp.code = code;
    rpcSendResponse(&rpcRsp);
  }

  rpcFreeCont(pMsg->pCont);
}

void *dnodeAllocVQueryQueue(void *pVnode) {
  return tWorkerAllocQueue(&tsVQueryWP, pVnode);
}

void *dnodeAllocVFetchQueue(void *pVnode) {
  return tWorkerAllocQueue(&tsVFetchWP, pVnode);
}

void dnodeFreeVQueryQueue(void *pQqueue) {
  tWorkerFreeQueue(&tsVQueryWP, pQqueue);
}

void dnodeFreeVFetchQueue(void *pFqueue) {
  tWorkerFreeQueue(&tsVFetchWP, pFqueue);
}

void dnodeSendRpcVReadRsp(void *pVnode, SVReadMsg *pRead, int32_t code) {
  SRpcMsg rpcRsp;
  rpcRsp.handle = pRead->rpcHandle;
  rpcRsp.pCont = pRead->rspRet.rsp;
  rpcRsp.contLen = pRead->rspRet.len;
  rpcRsp.code = code;

  rpcSendResponse(&rpcRsp);
}

void dnodeDispatchNonRspMsg(void *pVnode, SVReadMsg *pRead, int32_t code) {
}

static void *dnodeProcessReadQueue(void *wparam) {
  SWorker *    pWorker = static_cast<SWorker *>(wparam);
  SWorkerPool *pPool = pWorker->pPool;
  SVReadMsg *  pRead;
  int32_t      qtype;
  SVnodeObj *  pVnode;

  while (1) {
    if (taosReadQitemFromQset(pPool->qset, &qtype, (void **)&pRead, (void**)&pVnode) == 0) {
      dDebug("dnode vquery got no message from qset:%p, exiting", pPool->qset);
      break;
    }

    dTrace("msg:%p, app:%p type:%s will be processed in vquery queue, qtype:%d", pRead, pRead->rpcAhandle,
           taosMsg[pRead->msgType], qtype);

    int32_t code = vnodeProcessRead(pVnode, pRead);

    if (qtype == TAOS_QTYPE_RPC && code != TSDB_CODE_QRY_NOT_READY) {
      dnodeSendRpcVReadRsp(pVnode, pRead, code);
    } else {
      if (code == TSDB_CODE_QRY_HAS_RSP) {
        dnodeSendRpcVReadRsp(pVnode, pRead, pRead->code);
      } else {  // code == TSDB_CODE_QRY_NOT_READY, do not return msg to client
        assert(pRead->rpcHandle == NULL || (pRead->rpcHandle != NULL && pRead->msgType == 5));
        dnodeDispatchNonRspMsg(pVnode, pRead, code);
      }
    }

    vnodeFreeFromRQueue(pVnode, pRead);
  }

  return NULL;
}
