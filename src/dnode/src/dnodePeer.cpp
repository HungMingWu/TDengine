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

/* this file is mainly responsible for the communication between DNODEs. Each 
 * dnode works as both server and client. Dnode may send status, grant, config
 * messages to mnode, mnode may send create/alter/drop table/vnode messages 
 * to dnode. All theses messages are handled from here
 */

#include "os.h"
#include "mnode.h"
#include "dnodeVMgmt.h"
#include "dnodeVWrite.h"
#include "dnodeMPeer.h"
#include "dnodeMInfos.h"
#include "dnodeStep.h"

static void dnodeProcessReqMsgFromDnode(SRpcMsg *pMsg, SRpcEpSet *);
static void dnodeProcessRspFromDnode(SRpcMsg *pMsg, SRpcEpSet *pEpSet);
static void *tsServerRpc = NULL;
static SRpcInfo *tsClientRpc = NULL;

extern void dnodeProcessStatusRsp(SRpcMsg *pMsg);

int32_t dnodeInitServer() {
  SRpcInit rpcInit;
  memset(&rpcInit, 0, sizeof(rpcInit));
  rpcInit.localPort    = tsDnodeDnodePort;
  rpcInit.label        = "DND-S";
  rpcInit.numOfThreads = 1;
  rpcInit.cfp          = dnodeProcessReqMsgFromDnode;
  rpcInit.sessions     = TSDB_MAX_VNODES << 4;
  rpcInit.connType     = TAOS_CONN_SERVER;
  rpcInit.idleTime     = tsShellActivityTimer * 1000;

  tsServerRpc = rpcOpen(rpcInit);
  if (tsServerRpc == NULL) {
    dError("failed to init inter-dnodes RPC server");
    return -1;
  }

  dInfo("dnode inter-dnodes RPC server is initialized");
  return 0;
}

void dnodeCleanupServer() {
  if (tsServerRpc) {
    rpcClose(tsServerRpc);
    tsServerRpc = NULL;
    dInfo("inter-dnodes RPC server is closed");
  }
}

static void dnodeProcessReqMsgFromDnode(SRpcMsg *pMsg, SRpcEpSet *pEpSet) {
  SRpcMsg rspMsg;
  rspMsg.handle = pMsg->handle;
  rspMsg.pCont = NULL;
  rspMsg.contLen = 0;

  if (pMsg->pCont == NULL) return;
  if (pMsg->msgType == TSDB_MSG_TYPE_NETWORK_TEST) return dnodeSendStartupStep(pMsg);

  if (dnodeGetRunStatus() != TSDB_RUN_STATUS_RUNING) {
    rspMsg.code = TSDB_CODE_APP_NOT_READY;
    rpcSendResponse(&rspMsg);
    rpcFreeCont(pMsg->pCont);
    dTrace("RPC %p, msg:%s is ignored since dnode not running", pMsg->handle, taosMsg[pMsg->msgType]);
    return;
  }

  if (pMsg->pCont == NULL) {
    rspMsg.code = TSDB_CODE_DND_INVALID_MSG_LEN;
    rpcSendResponse(&rspMsg);
    return;
  }
  if (pMsg->msgType == TSDB_MSG_TYPE_MD_CREATE_TABLE ||
      pMsg->msgType == TSDB_MSG_TYPE_MD_DROP_TABLE ||
      pMsg->msgType == TSDB_MSG_TYPE_MD_ALTER_TABLE ||
      pMsg->msgType == TSDB_MSG_TYPE_MD_DROP_STABLE) {
    dnodeDispatchToVWriteQueue(pMsg);
  } else if (pMsg->msgType == TSDB_MSG_TYPE_MD_CREATE_VNODE ||
      pMsg->msgType == TSDB_MSG_TYPE_MD_ALTER_VNODE ||
      pMsg->msgType == TSDB_MSG_TYPE_MD_DROP_VNODE ||
      pMsg->msgType == TSDB_MSG_TYPE_MD_ALTER_STREAM ||
      pMsg->msgType == TSDB_MSG_TYPE_MD_CONFIG_DNODE ||
      pMsg->msgType == TSDB_MSG_TYPE_MD_CREATE_MNODE) {
    dnodeDispatchToVMgmtQueue(pMsg);
  } else if (pMsg->msgType == TSDB_MSG_TYPE_DM_CONFIG_TABLE ||
      pMsg->msgType == TSDB_MSG_TYPE_DM_CONFIG_VNODE ||
      pMsg->msgType == TSDB_MSG_TYPE_DM_AUTH ||
      pMsg->msgType == TSDB_MSG_TYPE_DM_GRANT ||
      pMsg->msgType == TSDB_MSG_TYPE_DM_STATUS) {
    dnodeDispatchToMPeerQueue(pMsg);
  } else {
    dDebug("RPC %p, message:%s not processed", pMsg->handle, taosMsg[pMsg->msgType]);
    rspMsg.code = TSDB_CODE_DND_MSG_NOT_PROCESSED;
    rpcSendResponse(&rspMsg);
    rpcFreeCont(pMsg->pCont);
  }
}

int32_t dnodeInitClient() {
  char secret[TSDB_KEY_LEN] = "secret";
  SRpcInit rpcInit;
  memset(&rpcInit, 0, sizeof(rpcInit));
  rpcInit.label        = "DND-C";
  rpcInit.numOfThreads = 1;
  rpcInit.cfp          = dnodeProcessRspFromDnode;
  rpcInit.sessions     = TSDB_MAX_VNODES << 4;
  rpcInit.connType     = TAOS_CONN_CLIENT;
  rpcInit.idleTime     = tsShellActivityTimer * 1000;
  rpcInit.user         = "t";
  rpcInit.ckey         = "key";
  rpcInit.secret       = secret;

  tsClientRpc = rpcOpen(rpcInit);
  if (tsClientRpc == NULL) {
    dError("failed to init mnode rpc client");
    return -1;
  }

  dInfo("dnode inter-dnodes rpc client is initialized");
  return 0;
}

void dnodeCleanupClient() {
  if (tsClientRpc) {
    rpcClose(tsClientRpc);
    tsClientRpc = NULL;
    dInfo("dnode inter-dnodes rpc client is closed");
  }
}

static void dnodeProcessRspFromDnode(SRpcMsg *pMsg, SRpcEpSet *pEpSet) {
  if (dnodeGetRunStatus() == TSDB_RUN_STATUS_STOPPED) {
    if (pMsg == NULL || pMsg->pCont == NULL) return;
    dTrace("msg:%p is ignored since dnode is stopping", pMsg);
    rpcFreeCont(pMsg->pCont);
    return;
  }

  if (pMsg->msgType == TSDB_MSG_TYPE_DM_STATUS_RSP && pEpSet) {
    dnodeUpdateEpSetForPeer(pEpSet);
  }

  if (pMsg->msgType == TSDB_MSG_TYPE_DM_STATUS_RSP) {    
    dnodeProcessStatusRsp(pMsg);
  } else {
    mnodeProcessPeerRsp(pMsg);
  }
  rpcFreeCont(pMsg->pCont);
}

void dnodeSendMsgToDnode(SRpcEpSet *epSet, SRpcMsg *rpcMsg) {
  rpcSendRequest(tsClientRpc, epSet, rpcMsg);
}

void dnodeSendMsgToMnodeRecv(SRpcMsg *rpcMsg, SRpcMsg *rpcRsp) {
  SRpcEpSet epSet = {0};
  dnodeGetEpSetForPeer(&epSet);

  assert(tsClientRpc != 0);
  rpcSendRecv(tsClientRpc, &epSet, rpcMsg, rpcRsp);
}

void dnodeSendMsgToDnodeRecv(SRpcMsg *rpcMsg, SRpcMsg *rpcRsp, SRpcEpSet *epSet) {
  rpcSendRecv(tsClientRpc, epSet, rpcMsg, rpcRsp);
}
