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
#include "dnodeVWrite.h"
#include "vnodeMgmt.h"
#include "vnodeWrite.h"
#include "vnodeSync.h"

struct SVWriteWorker {
  std::unique_ptr<STaosQall> qall;
  std::unique_ptr<STaosQset> qset;      // queue set
  int32_t   workerId;  // worker ID
  pthread_t thread;    // thread
};

typedef struct {
  int32_t max;     // max number of workers
  int32_t nextId;  // from 0 to max-1, cyclic
  SVWriteWorker * worker;
  std::mutex mutex;
} SVWriteWorkerPool;

static SVWriteWorkerPool tsVWriteWP;
static void *dnodeProcessVWriteQueue(void *pWorker);

int32_t dnodeInitVWrite() {
  tsVWriteWP.max = tsNumOfCores;
  tsVWriteWP.worker = static_cast<SVWriteWorker *>(tcalloc(sizeof(SVWriteWorker), tsVWriteWP.max));
  if (tsVWriteWP.worker == NULL) return -1;

  for (int32_t i = 0; i < tsVWriteWP.max; ++i) {
    tsVWriteWP.worker[i].workerId = i;
  }

  dInfo("dnode vwrite is initialized, max worker %d", tsVWriteWP.max);
  return 0;
}

void dnodeCleanupVWrite() {
  for (int32_t i = 0; i < tsVWriteWP.max; ++i) {
    SVWriteWorker *pWorker = tsVWriteWP.worker + i;
    if (pWorker->thread) {
      pWorker->qset->threadResume();
    }
  }

  for (int32_t i = 0; i < tsVWriteWP.max; ++i) {
    SVWriteWorker *pWorker = tsVWriteWP.worker + i;
    if (pWorker->thread) {
      pthread_join(pWorker->thread, NULL);
      pWorker->qall.reset();
      pWorker->qset.reset();
    }
  }

  tfree(tsVWriteWP.worker);
  dInfo("dnode vwrite is closed");
}

void dnodeDispatchToVWriteQueue(SRpcMsg *pRpcMsg) {
  int32_t code;
  char *pCont = static_cast<char*>(pRpcMsg->pCont);

  if (pRpcMsg->msgType == TSDB_MSG_TYPE_SUBMIT) {
    SMsgDesc *pDesc = (SMsgDesc *)pCont;
    pDesc->numOfVnodes = htonl(pDesc->numOfVnodes);
    pCont += sizeof(SMsgDesc);
  }

  SMsgHead *pMsg = (SMsgHead *)pCont;
  pMsg->vgId = htonl(pMsg->vgId);
  pMsg->contLen = htonl(pMsg->contLen);

  SVnodeObj *pVnode = vnodeAcquire(pMsg->vgId);
  if (pVnode == NULL) {
    code = TSDB_CODE_VND_INVALID_VGROUP_ID;
  } else {
    SWalHead *pHead = (SWalHead *)(pCont - sizeof(SWalHead));
    pHead->msgType = pRpcMsg->msgType;
    pHead->version = 0;
    pHead->len = pMsg->contLen;
    code = vnodeWriteToWQueue(pVnode, pHead, TAOS_QTYPE_RPC, pRpcMsg);
  }

  if (code != TSDB_CODE_SUCCESS) {
    SRpcMsg rpcRsp;
    rpcRsp.handle = pRpcMsg->handle;
    rpcRsp.code = code;
    rpcSendResponse(&rpcRsp);
  }

  pVnode->Release();
  rpcFreeCont(pRpcMsg->pCont);
}

std::unique_ptr<STaosQueue> dnodeAllocVWriteQueue(void *pVnode) {
  std::lock_guard<std::mutex> lock(tsVWriteWP.mutex);
  SVWriteWorker *pWorker = tsVWriteWP.worker + tsVWriteWP.nextId;
  std::unique_ptr<STaosQueue> queue(new STaosQueue);

  if (pWorker->qset == NULL) {
    pWorker->qset.reset(new STaosQset());
    pWorker->qset->addIntoQset(queue.get(), pVnode);
    pWorker->qall.reset(new STaosQall());
    pthread_attr_t thAttr;
    pthread_attr_init(&thAttr);
    pthread_attr_setdetachstate(&thAttr, PTHREAD_CREATE_JOINABLE);

    if (pthread_create(&pWorker->thread, &thAttr, dnodeProcessVWriteQueue, pWorker) != 0) {
      dError("failed to create thread to process vwrite queue since %s", strerror(errno));
      pWorker->qall.reset();
      pWorker->qset.reset();
      queue.reset();
    } else {
      dDebug("dnode vwrite worker:%d is launched", pWorker->workerId);
      tsVWriteWP.nextId = (tsVWriteWP.nextId + 1) % tsVWriteWP.max;
    }

    pthread_attr_destroy(&thAttr);
  } else {
    pWorker->qset->addIntoQset(queue.get(), pVnode);
    tsVWriteWP.nextId = (tsVWriteWP.nextId + 1) % tsVWriteWP.max;
  }

  dDebug("pVnode:%p, dnode vwrite queue:%p is allocated", pVnode, queue.get());

  return queue;
}

void dnodeSendRpcVWriteRsp(void *pVnode, void *wparam, int32_t code) {
  if (wparam == NULL) return;
  SVWriteMsg *pWrite = static_cast<SVWriteMsg *>(wparam);

  if (code < 0) pWrite->code = code;
  int32_t count = pWrite->processedCount++;

  if (count <= 1) return;

  SRpcMsg rpcRsp;
  rpcRsp.handle  = pWrite->rpcMsg.handle;
  rpcRsp.pCont = pWrite->rspRet.rsp;
  rpcRsp.contLen = pWrite->rspRet.len;
  rpcRsp.code = pWrite->code;

  rpcSendResponse(&rpcRsp);
  vnodeFreeFromWQueue(pVnode, pWrite);
}

static void *dnodeProcessVWriteQueue(void *wparam) {
  SVWriteWorker *pWorker = static_cast<SVWriteWorker *>(wparam);
  SVWriteMsg *   pWrite;
  void *         pVnode;
  int32_t        numOfMsgs;
  int32_t        qtype;

  taosBlockSIGPIPE();
  dDebug("dnode vwrite worker:%d is running", pWorker->workerId);

  while (1) {
    numOfMsgs = pWorker->qset->readAllQitems(pWorker->qall.get(), &pVnode);
    if (numOfMsgs == 0) {
      dDebug("qset:%p, dnode vwrite got no message from qset, exiting", pWorker->qset.get());
      break;
    }

    bool forceFsync = false;
    for (int32_t i = 0; i < numOfMsgs; ++i) {
      pWorker->qall->getQitem(&qtype, (void **)&pWrite);
      dTrace("msg:%p, app:%p type:%s will be processed in vwrite queue, qtype:%s hver:%" PRIu64, pWrite,
             pWrite->rpcMsg.ahandle, taosMsg[pWrite->pHead.msgType], qtypeStr[qtype], pWrite->pHead.version);

      pWrite->code = vnodeProcessWrite(pVnode, &pWrite->pHead, qtype, pWrite);
      if (pWrite->code <= 0) pWrite->processedCount = 1;
      if (pWrite->code > 0) pWrite->code = 0;
      if (pWrite->code == 0 && pWrite->pHead.msgType != TSDB_MSG_TYPE_SUBMIT) forceFsync = true;

      dTrace("msg:%p is processed in vwrite queue, code:0x%x", pWrite, pWrite->code);
    }

    vnodeGetWal(pVnode)->fsync(forceFsync);

    // browse all items, and process them one by one
    pWorker->qall->resetQitems();
    for (int32_t i = 0; i < numOfMsgs; ++i) {
      pWorker->qall->getQitem(&qtype, (void **)&pWrite);
      if (qtype == TAOS_QTYPE_RPC) {
        dnodeSendRpcVWriteRsp(pVnode, pWrite, pWrite->code);
      } else {
        if (qtype == TAOS_QTYPE_FWD) {
          vnodeConfirmForward(pVnode, pWrite->pHead.version, 0);
        }
        if (pWrite->rspRet.rsp) {
          rpcFreeCont(pWrite->rspRet.rsp);
        }
        vnodeFreeFromWQueue(pVnode, pWrite);
      }
    }
  }

  return NULL;
}
