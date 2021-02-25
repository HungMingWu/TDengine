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
#include "ttimer.h"
#include "tqueue.h"
#include "mnode.h"
#include "dnodeMInfos.h"
#include "dnodeMWrite.h"
#include "workpool.h"

using SMnodeMsgItem = std::pair<int, SMnodeMsg *>;
class MWritePool : public workpool<SMnodeMsgItem, MWritePool> {
 public:
  void process(std::vector<SMnodeMsgItem> &&items) {
    for (auto &item : items) {
      auto pWrite = item.second;
      dTrace("msg:%p, app:%p type:%s will be processed in mwrite queue", pWrite, pWrite->rpcMsg.ahandle,
             taosMsg[pWrite->rpcMsg.msgType]);

      int32_t code = mnodeProcessWrite(pWrite);
      dnodeSendRpcMWriteRsp(pWrite, code);
    }
  }
  using workpool<SMnodeMsgItem, MWritePool>::workpool;
};

static std::shared_ptr<MWritePool> tsMWriteWP;
extern void *            tsDnodeTmr;

int32_t dnodeInitMWrite() {
  tsMWriteWP = std::make_shared<MWritePool>(1);
  return 0;
}

void dnodeCleanupMWrite() {
  tsMWriteWP->stop();
  tsMWriteWP.reset();
}

void dnodeDispatchToMWriteQueue(SRpcMsg *pMsg) {
  if (!mnodeIsRunning() || tsMWriteWP == NULL) {
    dnodeSendRedirectMsg(pMsg, true);
  } else {
    SMnodeMsg *pWrite = static_cast<SMnodeMsg *>(mnodeCreateMsg(pMsg));
    dTrace("msg:%p, app:%p type:%s is put into mwrite queue:%p", pWrite, pWrite->rpcMsg.ahandle,
           taosMsg[pWrite->rpcMsg.msgType], tsMWriteWP.get());
    tsMWriteWP->put(std::make_pair(TAOS_QTYPE_RPC, pWrite));
  }

  rpcFreeCont(pMsg->pCont);
}

static void dnodeFreeMWriteMsg(SMnodeMsg *pWrite) {
  dTrace("msg:%p, app:%p type:%s is freed from mwrite queue:%p", pWrite, pWrite->rpcMsg.ahandle,
         taosMsg[pWrite->rpcMsg.msgType], tsMWriteWP.get());

  delete pWrite;
  taosFreeQitem(pWrite);
}

void dnodeSendRpcMWriteRsp(void *pMsg, int32_t code) {
  SMnodeMsg *pWrite = static_cast<SMnodeMsg *>(pMsg);
  if (pWrite == NULL) return;
  if (code == TSDB_CODE_MND_ACTION_IN_PROGRESS) return;
  if (code == TSDB_CODE_MND_ACTION_NEED_REPROCESSED) {
    dnodeReprocessMWriteMsg(pWrite);
    return;
  }

  SRpcMsg rpcRsp;
  rpcRsp.handle = pWrite->rpcMsg.handle;
  rpcRsp.pCont = pWrite->rpcRsp.rsp;
  rpcRsp.contLen = pWrite->rpcRsp.len;
  rpcRsp.code = code;

  rpcSendResponse(&rpcRsp);
  dnodeFreeMWriteMsg(pWrite);
}

void dnodeReprocessMWriteMsg(SMnodeMsg *pWrite) {
  if (!mnodeIsRunning() || tsMWriteWP == NULL) {
    dDebug("msg:%p, app:%p type:%s is redirected for mnode not running, retry times:%d", pWrite, pWrite->rpcMsg.ahandle,
           taosMsg[pWrite->rpcMsg.msgType], pWrite->retry);

    if (pWrite->pBatchMasterMsg) {
      ++pWrite->pBatchMasterMsg->received;
      if (pWrite->pBatchMasterMsg->successed + pWrite->pBatchMasterMsg->received
	  >= pWrite->pBatchMasterMsg->expected) {
        dnodeSendRedirectMsg(&pWrite->pBatchMasterMsg->rpcMsg, true);
        dnodeFreeMWriteMsg(pWrite->pBatchMasterMsg);
      }

      mnodeDestroySubMsg(pWrite);

      return;
    }
    dnodeSendRedirectMsg(&pWrite->rpcMsg, true);
    dnodeFreeMWriteMsg(pWrite);
  } else {
    dDebug("msg:%p, app:%p type:%s is reput into mwrite queue:%p, retry times:%d", pWrite, pWrite->rpcMsg.ahandle,
           taosMsg[pWrite->rpcMsg.msgType], tsMWriteWP.get(), pWrite->retry);

    tsMWriteWP->put(std::make_pair(TAOS_QTYPE_RPC, pWrite));
  }
}

void dnodeDelayReprocessMWriteMsg(SMnodeMsg *pMsg) {
  void *unUsed = NULL;
  taosTmrReset([pMsg](void *) { dnodeReprocessMWriteMsg(pMsg); }, 300, tsDnodeTmr,
               &unUsed);
}
