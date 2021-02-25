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
#include "workpool.h"

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

static void vnodeProcessMWorkerMsg(SVMWorkerMsg *pMsg);
static void vnodeSendVMWorkerRpcRsp(SVMWorkerMsg *pMsg);

using SVMWorkerItem = std::pair<int, SVMWorkerMsg*>;
class VNodePool : public workpool<SVMWorkerItem, VNodePool> {
 public:
  void process(std::vector<SVMWorkerItem> &&items) {
    for (auto &item : items) {
      auto pMsg = item.second;
      vTrace("vgId:%d, action:%d will be processed in vmworker queue", pMsg->vgId, pMsg->action);
      vnodeProcessMWorkerMsg(pMsg);
      vnodeSendVMWorkerRpcRsp(pMsg);
    }
  }
  using workpool<SVMWorkerItem, VNodePool>::workpool;
};

static std::shared_ptr<VNodePool> tsVMWorkerPool;

int32_t vnodeInitMWorker() {
  tsVMWorkerPool = std::make_shared<VNodePool>(1);
  return 0;
}

void vnodeCleanupMWorker() {
  tsVMWorkerPool->stop();
  tsVMWorkerPool.reset();
}

static int32_t vnodeWriteIntoMWorker(SVnodeObj *pVnode, EVMWorkerAction action, void *rpcHandle) {
  SVMWorkerMsg *pMsg = (SVMWorkerMsg*)taosAllocateQitem(sizeof(SVMWorkerMsg));
  if (pMsg == NULL) return TSDB_CODE_VND_OUT_OF_MEMORY;

  pMsg->vgId = pVnode->vgId;
  pMsg->pVnode = pVnode;
  pMsg->rpcHandle = rpcHandle;
  pMsg->action = action;

  tsVMWorkerPool->put(std::make_pair(TAOS_QTYPE_RPC, pMsg));

  return 0;
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