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
#include "taoserror.h"
#include "taosmsg.h"
#include "tutil.h"
#include "tqueue.h"
#include "tglobal.h"
#include "vnodeWorker.h"
#include "vnodeMain.h"

typedef struct {
  int32_t vgId;
  int32_t code;
  void *  rpcHandle;
  SVnodeObj *pVnode;
  EVMWorkerAction action;
} SVMWorkerMsg;

typedef struct {
  pthread_t thread;
  int32_t   workerId;
} SVMWorker;

typedef struct {
  int32_t    curNum;
  int32_t    maxNum;
  SVMWorker *worker;
} SVMWorkerPool;

static SVMWorkerPool tsVMWorkerPool;
static std::unique_ptr<STaosQset>    tsVMWorkerQset;
static std::unique_ptr<STaosQueue>   tsVMWorkerQueue;

static void *vnodeMWorkerFunc(void *param);

static int32_t vnodeStartMWorker() {
  tsVMWorkerQueue.reset(new STaosQueue);

  tsVMWorkerQset->addIntoQset(tsVMWorkerQueue.get(), NULL);

  for (int32_t i = tsVMWorkerPool.curNum; i < tsVMWorkerPool.maxNum; ++i) {
    SVMWorker *pWorker = tsVMWorkerPool.worker + i;
    pWorker->workerId = i;

    pthread_attr_t thAttr;
    pthread_attr_init(&thAttr);
    pthread_attr_setdetachstate(&thAttr, PTHREAD_CREATE_JOINABLE);

    if (pthread_create(&pWorker->thread, &thAttr, vnodeMWorkerFunc, pWorker) != 0) {
      vError("failed to create thread to process vmworker queue, reason:%s", strerror(errno));
    }

    pthread_attr_destroy(&thAttr);

    tsVMWorkerPool.curNum = i + 1;
    vDebug("vmworker:%d is launched, total:%d", pWorker->workerId, tsVMWorkerPool.maxNum);
  }

  vDebug("vmworker queue:%p is allocated", tsVMWorkerQueue.get());
  return TSDB_CODE_SUCCESS;
}

int32_t vnodeInitMWorker() {
  tsVMWorkerQset.reset(new STaosQset());

  tsVMWorkerPool.maxNum = 1;
  tsVMWorkerPool.curNum = 0;
  tsVMWorkerPool.worker = (SVMWorker*)calloc(sizeof(SVMWorker), tsVMWorkerPool.maxNum);

  if (tsVMWorkerPool.worker == NULL) return -1;
  for (int32_t i = 0; i < tsVMWorkerPool.maxNum; ++i) {
    SVMWorker *pWorker = tsVMWorkerPool.worker + i;
    pWorker->workerId = i;
    vDebug("vmworker:%d is created", i);
  }

  vDebug("vmworker is initialized, num:%d qset:%p", tsVMWorkerPool.maxNum, tsVMWorkerQset.get());

  return vnodeStartMWorker();
}

static void vnodeStopMWorker() {
  vDebug("vmworker queue:%p is freed", tsVMWorkerQueue.get());
  tsVMWorkerQueue.reset();
}

void vnodeCleanupMWorker() {
  for (int32_t i = 0; i < tsVMWorkerPool.maxNum; ++i) {
    SVMWorker *pWorker = tsVMWorkerPool.worker + i;
    if (pWorker->thread) {
      tsVMWorkerQset->threadResume();
    }
    vDebug("vmworker:%d is closed", i);
  }

  for (int32_t i = 0; i < tsVMWorkerPool.maxNum; ++i) {
    SVMWorker *pWorker = tsVMWorkerPool.worker + i;
    vDebug("vmworker:%d start to join", i);
    if (pWorker->thread) {
      pthread_join(pWorker->thread, NULL);
    }
    vDebug("vmworker:%d join success", i);
  }

  vDebug("vmworker is closed, qset:%p", tsVMWorkerQset.get());

  tsVMWorkerQset.reset();
  tfree(tsVMWorkerPool.worker);

  vnodeStopMWorker();
}

static int32_t vnodeWriteIntoMWorker(SVnodeObj *pVnode, EVMWorkerAction action, void *rpcHandle) {
  SVMWorkerMsg *pMsg = (SVMWorkerMsg*)taosAllocateQitem(sizeof(SVMWorkerMsg));
  if (pMsg == NULL) return TSDB_CODE_VND_OUT_OF_MEMORY;

  pMsg->vgId = pVnode->vgId;
  pMsg->pVnode = pVnode;
  pMsg->rpcHandle = rpcHandle;
  pMsg->action = action;

  int32_t code = tsVMWorkerQueue->writeQitem(TAOS_QTYPE_RPC, pMsg);
  if (code == 0) code = TSDB_CODE_DND_ACTION_IN_PROGRESS;

  return code;
}

int32_t SVnodeObj::WriteIntoMWorker(EVMWorkerAction action) {
  vTrace("vgId:%d, will write action %d into vmworker", vgId, (int)action);
  return vnodeWriteIntoMWorker(this, action, NULL);
}

static void vnodeFreeMWorkerMsg(SVMWorkerMsg *pMsg) {
  vTrace("vgId:%d, disposed in vmworker", pMsg->vgId);
  taosFreeQitem(pMsg);
}

static void vnodeSendVMWorkerRpcRsp(SVMWorkerMsg *pMsg) {
  if (pMsg->rpcHandle != NULL) {
    SRpcMsg rpcRsp = {
        .code = pMsg->code,
        .handle = pMsg->rpcHandle
    };
    rpcSendResponse(&rpcRsp);
  }

  vnodeFreeMWorkerMsg(pMsg);
}

static void vnodeProcessMWorkerMsg(SVMWorkerMsg *pMsg) {
  pMsg->code = 0;

  switch (pMsg->action) {
    case VNODE_WORKER_ACTION_CLEANUP:
      pMsg->pVnode->CleanUp();
      break;
    case VNODE_WORKER_ACTION_DESTROUY:
      pMsg->pVnode->Destroy();
      break;
    default:
      break;
  }
}

static void *vnodeMWorkerFunc(void *param) {
  while (1) {
    SVMWorkerMsg *pMsg = NULL;
    if (tsVMWorkerQset->readQitem(NULL, (void **)&pMsg, NULL) == 0) {
      vDebug("qset:%p, vmworker got no message from qset, exiting", tsVMWorkerQset.get());
      break;
    }

    vTrace("vgId:%d, action:%d will be processed in vmworker queue", pMsg->vgId, pMsg->action);
    vnodeProcessMWorkerMsg(pMsg);
    vnodeSendVMWorkerRpcRsp(pMsg);
  }

  return NULL;
}
