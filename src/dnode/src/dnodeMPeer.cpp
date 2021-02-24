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
#include "os.h"
#include "tqueue.h"
#include "mnode.h"
#include "dnodeVMgmt.h"
#include "dnodeMInfos.h"
#include "dnodeMWrite.h"

typedef struct {
  pthread_t thread;
  int32_t   workerId;
} SMPeerWorker;

typedef struct {
  int32_t curNum;
  int32_t maxNum;
  SMPeerWorker *worker;
} SMPeerWorkerPool;

static SMPeerWorkerPool tsMPeerWP;
static std::unique_ptr<STaosQset> tsMPeerQset;
static std::unique_ptr<STaosQueue> tsMPeerQueue;

static void *dnodeProcessMPeerQueue(void *param);

int32_t dnodeInitMPeer() {
  tsMPeerQset.reset(new STaosQset());
  
  tsMPeerWP.maxNum = 1;
  tsMPeerWP.curNum = 0;
  tsMPeerWP.worker = (SMPeerWorker *)calloc(sizeof(SMPeerWorker), tsMPeerWP.maxNum);

  if (tsMPeerWP.worker == NULL) return -1;
  for (int32_t i = 0; i < tsMPeerWP.maxNum; ++i) {
    SMPeerWorker *pWorker = tsMPeerWP.worker + i;
    pWorker->workerId = i;
    dDebug("dnode mpeer worker:%d is created", i);
  }

  dDebug("dnode mpeer is initialized, workers:%d qset:%p", tsMPeerWP.maxNum, tsMPeerQset.get());
  return 0;
}

void dnodeCleanupMPeer() {
  for (int32_t i = 0; i < tsMPeerWP.maxNum; ++i) {
    SMPeerWorker *pWorker = tsMPeerWP.worker + i;
    if (pWorker->thread) {
      tsMPeerQset->threadResume();
    }
    dDebug("dnode mpeer worker:%d is closed", i);
  }

  for (int32_t i = 0; i < tsMPeerWP.maxNum; ++i) {
    SMPeerWorker *pWorker = tsMPeerWP.worker + i;
    dDebug("dnode mpeer worker:%d start to join", i);
    if (pWorker->thread) {
      pthread_join(pWorker->thread, NULL);
    }
    dDebug("dnode mpeer worker:%d join success", i);
  }

  dDebug("dnode mpeer is closed, qset:%p", tsMPeerQset.get());

  tsMPeerQset.reset();
  tfree(tsMPeerWP.worker);
}

int32_t dnodeAllocateMPeerQueue() {
  tsMPeerQueue.reset(new STaosQueue);
  tsMPeerQset->addIntoQset(tsMPeerQueue.get(), NULL);

  for (int32_t i = tsMPeerWP.curNum; i < tsMPeerWP.maxNum; ++i) {
    SMPeerWorker *pWorker = tsMPeerWP.worker + i;
    pWorker->workerId = i;

    pthread_attr_t thAttr;
    pthread_attr_init(&thAttr);
    pthread_attr_setdetachstate(&thAttr, PTHREAD_CREATE_JOINABLE);

    if (pthread_create(&pWorker->thread, &thAttr, dnodeProcessMPeerQueue, pWorker) != 0) {
      dError("failed to create thread to process mpeer queue, reason:%s", strerror(errno));
    }

    pthread_attr_destroy(&thAttr);

    tsMPeerWP.curNum = i + 1;
    dDebug("dnode mpeer worker:%d is launched, total:%d", pWorker->workerId, tsMPeerWP.maxNum);
  }

  dDebug("dnode mpeer queue:%p is allocated", tsMPeerQueue.get());
  return TSDB_CODE_SUCCESS;
}

void dnodeFreeMPeerQueue() {
  dDebug("dnode mpeer queue:%p is freed", tsMPeerQueue.get());
  tsMPeerQueue.reset();
}

void dnodeDispatchToMPeerQueue(SRpcMsg *pMsg) {
  if (!mnodeIsRunning() || !tsMPeerQueue) {
    dnodeSendRedirectMsg(pMsg, false);
  } else {
    SMnodeMsg *pPeer = static_cast<SMnodeMsg *>(mnodeCreateMsg(pMsg));
    tsMPeerQueue->writeQitem(TAOS_QTYPE_RPC, pPeer);
  }

  rpcFreeCont(pMsg->pCont);
}

static void dnodeFreeMPeerMsg(SMnodeMsg *pPeer) {
  delete pPeer;
  taosFreeQitem(pPeer);
}

static void dnodeSendRpcMPeerRsp(SMnodeMsg *pPeer, int32_t code) {
  if (code == TSDB_CODE_MND_ACTION_IN_PROGRESS) return;

  SRpcMsg rpcRsp;
  rpcRsp.handle = pPeer->rpcMsg.handle;
  rpcRsp.pCont = pPeer->rpcRsp.rsp;
  rpcRsp.contLen = pPeer->rpcRsp.len;
  rpcRsp.code = code;

  rpcSendResponse(&rpcRsp);
  dnodeFreeMPeerMsg(pPeer);
}

static void *dnodeProcessMPeerQueue(void *param) {
  SMnodeMsg *pPeerMsg;
  int32_t    type;
  void *     unUsed;
  
  while (1) {
    if (tsMPeerQset->readQitem(&type, (void **)&pPeerMsg, &unUsed) == 0) {
      dDebug("qset:%p, mnode peer got no message from qset, exiting", tsMPeerQset.get());
      break;
    }

    dTrace("msg:%s will be processed in mpeer queue", taosMsg[pPeerMsg->rpcMsg.msgType]);    
    int32_t code = mnodeProcessPeerReq(pPeerMsg);    
    dnodeSendRpcMPeerRsp(pPeerMsg, code);    
  }

  return NULL;
}
