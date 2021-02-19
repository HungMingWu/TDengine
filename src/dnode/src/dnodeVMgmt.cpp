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
#include <memory>
#include "os.h"
#include "tqueue.h"
#include "tworker.h"
#include "dnodeVMgmt.h"
#include "vnodeMgmt.h"
#include "vnodeMain.h"

typedef struct {
  SRpcMsg rpcMsg;
  char    pCont[];
} SMgmtMsg;

static SWorkerPool tsVMgmtWP;
static std::unique_ptr<STaosQueue> tsVMgmtQueue;

static void *  dnodeProcessMgmtQueue(void *param);
static int32_t dnodeProcessCreateVnodeMsg(SRpcMsg *pMsg);
static int32_t dnodeProcessAlterVnodeMsg(SRpcMsg *pMsg);
static int32_t dnodeProcessDropVnodeMsg(SRpcMsg *pMsg);
static int32_t dnodeProcessAlterStreamMsg(SRpcMsg *pMsg);
static int32_t dnodeProcessConfigDnodeMsg(SRpcMsg *pMsg);
static int32_t dnodeProcessCreateMnodeMsg(SRpcMsg *pMsg);

int32_t dnodeInitVMgmt() {
  int32_t code = vnodeInitMgmt();
  if (code != TSDB_CODE_SUCCESS) return -1;

  tsVMgmtWP.name = "vmgmt";
  tsVMgmtWP.workerFp = dnodeProcessMgmtQueue;
  tsVMgmtWP.min = 1;
  tsVMgmtWP.max = 1;
  if (tWorkerInit(&tsVMgmtWP) != 0) return -1;

  tsVMgmtQueue.reset(tWorkerAllocQueue(&tsVMgmtWP, NULL));

  dInfo("dnode vmgmt is initialized");
  return TSDB_CODE_SUCCESS;
}

void dnodeCleanupVMgmt() {
  tsVMgmtQueue.reset();
  tWorkerCleanup(&tsVMgmtWP);

  vnodeCleanupMgmt();
}

static int32_t dnodeWriteToMgmtQueue(SRpcMsg *pMsg) {
  int32_t   size = sizeof(SMgmtMsg) + pMsg->contLen;
  SMgmtMsg *pMgmt = static_cast<SMgmtMsg *>(taosAllocateQitem(size));
  if (pMgmt == NULL) return TSDB_CODE_DND_OUT_OF_MEMORY;

  pMgmt->rpcMsg = *pMsg;
  pMgmt->rpcMsg.pCont = pMgmt->pCont;
  memcpy(pMgmt->pCont, pMsg->pCont, pMsg->contLen);
  tsVMgmtQueue->writeQitem(TAOS_QTYPE_RPC, pMgmt);

  return TSDB_CODE_SUCCESS;
}

void dnodeDispatchToVMgmtQueue(SRpcMsg *pMsg) {
  int32_t code = dnodeWriteToMgmtQueue(pMsg);
  if (code != TSDB_CODE_SUCCESS) {
    SRpcMsg rsp;
    rsp.handle = pMsg->handle;
    rsp.code = code;
    rpcSendResponse(&rsp);
  }

  rpcFreeCont(pMsg->pCont);
}

static void *dnodeProcessMgmtQueue(void *wparam) {
  SWorker *    pWorker = static_cast<SWorker *>(wparam);
  SWorkerPool *pPool = pWorker->pPool;
  SMgmtMsg *   pMgmt;
  SRpcMsg *    pMsg;
  SRpcMsg      rsp = {0};
  int32_t      qtype;
  void *       handle;

  while (1) {
    if (pPool->qset->readQitem(&qtype, (void **)&pMgmt, &handle) == 0) {
      dDebug("qdnode mgmt got no message from qset:%p, , exit", pPool->qset.get());
      break;
    }

    pMsg = &pMgmt->rpcMsg;
    dTrace("msg:%p, ahandle:%p type:%s will be processed", pMgmt, pMsg->ahandle, taosMsg[pMsg->msgType]);
    if (pMsg->msgType == TSDB_MSG_TYPE_MD_CREATE_VNODE) {
      rsp.code = dnodeProcessCreateVnodeMsg(pMsg);
    } else if (pMsg->msgType == TSDB_MSG_TYPE_MD_ALTER_VNODE) {
      rsp.code = dnodeProcessAlterVnodeMsg(pMsg);
    } else if (pMsg->msgType == TSDB_MSG_TYPE_MD_DROP_VNODE) {
      rsp.code = dnodeProcessDropVnodeMsg(pMsg);
    } else if (pMsg->msgType == TSDB_MSG_TYPE_MD_ALTER_STREAM) {
      rsp.code = dnodeProcessAlterStreamMsg(pMsg);
    } else if (pMsg->msgType == TSDB_MSG_TYPE_MD_CONFIG_DNODE) {
      rsp.code = dnodeProcessConfigDnodeMsg(pMsg);
    } else if (pMsg->msgType == TSDB_MSG_TYPE_MD_CREATE_MNODE) {
      rsp.code = dnodeProcessCreateMnodeMsg(pMsg);
    } else {
      rsp.code = TSDB_CODE_DND_MSG_NOT_PROCESSED;
    }

    dTrace("msg:%p, is processed, code:0x%x", pMgmt, rsp.code);
    if (rsp.code != TSDB_CODE_DND_ACTION_IN_PROGRESS) {
      rsp.handle = pMsg->handle;
      rsp.pCont = NULL;
      rpcSendResponse(&rsp);
    }

    taosFreeQitem(pMsg);
  }

  return NULL;
}

static SCreateVnodeMsg* dnodeParseVnodeMsg(SRpcMsg *rpcMsg) {
  SCreateVnodeMsg *pCreate = static_cast<SCreateVnodeMsg *>(rpcMsg->pCont);
  pCreate->cfg.vgId                = htonl(pCreate->cfg.vgId);
  pCreate->cfg.dbCfgVersion        = htonl(pCreate->cfg.dbCfgVersion);
  pCreate->cfg.vgCfgVersion        = htonl(pCreate->cfg.vgCfgVersion);
  pCreate->cfg.maxTables           = htonl(pCreate->cfg.maxTables);
  pCreate->cfg.cacheBlockSize      = htonl(pCreate->cfg.cacheBlockSize);
  pCreate->cfg.totalBlocks         = htonl(pCreate->cfg.totalBlocks);
  pCreate->cfg.daysPerFile         = htonl(pCreate->cfg.daysPerFile);
  pCreate->cfg.daysToKeep1         = htonl(pCreate->cfg.daysToKeep1);
  pCreate->cfg.daysToKeep2         = htonl(pCreate->cfg.daysToKeep2);
  pCreate->cfg.daysToKeep          = htonl(pCreate->cfg.daysToKeep);
  pCreate->cfg.minRowsPerFileBlock = htonl(pCreate->cfg.minRowsPerFileBlock);
  pCreate->cfg.maxRowsPerFileBlock = htonl(pCreate->cfg.maxRowsPerFileBlock);
  pCreate->cfg.fsyncPeriod         = htonl(pCreate->cfg.fsyncPeriod);
  pCreate->cfg.commitTime          = htonl(pCreate->cfg.commitTime);

  for (int32_t j = 0; j < pCreate->cfg.vgReplica; ++j) {
    pCreate->nodes[j].nodeId = htonl(pCreate->nodes[j].nodeId);
  }

  return pCreate;
}

static int32_t dnodeProcessCreateVnodeMsg(SRpcMsg *rpcMsg) {
  SCreateVnodeMsg *pCreate = dnodeParseVnodeMsg(rpcMsg);

  SVnodeObj *pVnode = vnodeAcquire(pCreate->cfg.vgId);
  if (pVnode != NULL) {
    dDebug("vgId:%d, already exist, return success", pCreate->cfg.vgId);
    pVnode->Release();
    return TSDB_CODE_SUCCESS;
  } else {
    dDebug("vgId:%d, create vnode msg is received", pCreate->cfg.vgId);
    return vnodeCreate(pCreate);
  }
}

static int32_t dnodeProcessAlterVnodeMsg(SRpcMsg *rpcMsg) {
  SAlterVnodeMsg *pAlter = dnodeParseVnodeMsg(rpcMsg);

  SVnodeObj *pVnode = vnodeAcquire(pAlter->cfg.vgId);
  if (pVnode != NULL) {
    dDebug("vgId:%d, alter vnode msg is received", pAlter->cfg.vgId);
    int32_t code = vnodeAlter(pVnode, pAlter);
    pVnode->Release();
    return code;
  } else {
    dError("vgId:%d, vnode not exist, can't alter it", pAlter->cfg.vgId);
    return TSDB_CODE_VND_INVALID_VGROUP_ID;
  }
}

static int32_t dnodeProcessDropVnodeMsg(SRpcMsg *rpcMsg) {
  SDropVnodeMsg *pDrop = static_cast<SDropVnodeMsg *>(rpcMsg->pCont);
  pDrop->vgId = htonl(pDrop->vgId);

  return vnodeDrop(pDrop->vgId);
}

static int32_t dnodeProcessAlterStreamMsg(SRpcMsg *pMsg) {
  return 0;
}

static int32_t dnodeProcessConfigDnodeMsg(SRpcMsg *pMsg) {
  SCfgDnodeMsg *pCfg = static_cast<SCfgDnodeMsg *>(pMsg->pCont);
  return taosCfgDynamicOptions(pCfg->config);
}

static int32_t dnodeProcessCreateMnodeMsg(SRpcMsg *pMsg) {
  SCreateMnodeMsg *pCfg = static_cast<SCreateMnodeMsg *>(pMsg->pCont);
  pCfg->dnodeId = htonl(pCfg->dnodeId);
  if (pCfg->dnodeId != dnodeGetDnodeId()) {
    dDebug("dnode:%d, in create mnode msg is not equal with saved dnodeId:%d", pCfg->dnodeId, dnodeGetDnodeId());
    return TSDB_CODE_MND_DNODE_ID_NOT_CONFIGURED;
  }

  if (strcmp(pCfg->dnodeEp, tsLocalEp) != 0) {
    dDebug("dnodeEp:%s, in create mnode msg is not equal with saved dnodeEp:%s", pCfg->dnodeEp, tsLocalEp);
    return TSDB_CODE_MND_DNODE_EP_NOT_CONFIGURED;
  }

  dDebug("dnode:%d, create mnode msg is received from mnodes, numOfMnodes:%d", pCfg->dnodeId, pCfg->mnodes.mnodeNum);
  for (int i = 0; i < pCfg->mnodes.mnodeNum; ++i) {
    pCfg->mnodes.mnodeInfos[i].mnodeId = htonl(pCfg->mnodes.mnodeInfos[i].mnodeId);
    dDebug("mnode index:%d, mnode:%d:%s", i, pCfg->mnodes.mnodeInfos[i].mnodeId, pCfg->mnodes.mnodeInfos[i].mnodeEp);
  }

  return dnodeStartMnode(&pCfg->mnodes);
}
