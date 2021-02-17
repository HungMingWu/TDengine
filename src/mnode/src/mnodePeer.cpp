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

#include "os.h"
#include "taoserror.h"
#include "tsched.h"
#include "tsystem.h"
#include "tutil.h"
#include "tgrant.h"
#include "tbn.h"
#include "tglobal.h"
#include "mnode.h"
#include "dnode.h"
#include "mnodeDef.h"
#include "mnodeInt.h"
#include "mnodeDb.h"
#include "mnodeMnode.h"
#include "mnodeProfile.h"
#include "mnodeShow.h"
#include "mnodeSdb.h"
#include "mnodeTable.h"
#include "mnodeVgroup.h"

extern int32_t mnodeProcessDnodeStatusMsg(SMnodeMsg *pMsg);
extern int32_t mnodeProcessTableCfgMsg(SMnodeMsg *pMsg);
extern int32_t mnodeProcessAuthMsg(SMnodeMsg *pMsg);
extern int32_t mnodeProcessVnodeCfgMsg(SMnodeMsg *pMsg);
extern void    mnodeProcessCfgDnodeMsgRsp(SRpcMsg *rpcMsg);
extern void    mnodeProcessCreateChildTableRsp(SRpcMsg *rpcMsg);
extern void    mnodeProcessDropChildTableRsp(SRpcMsg *rpcMsg);
extern void    mnodeProcessDropSuperTableRsp(SRpcMsg *rpcMsg);
extern void    mnodeProcessAlterTableRsp(SRpcMsg *rpcMsg);
extern void    mnodeProcessCreateVnodeRsp(SRpcMsg *rpcMsg);
extern void    mnodeProcessAlterVnodeRsp(SRpcMsg *rpcMsg);
extern void    mnodeProcessDropVnodeRsp(SRpcMsg *rpcMsg);

int32_t mnodeProcessPeerReq(SMnodeMsg *pMsg) {
  if (pMsg->rpcMsg.pCont == NULL) {
    mError("msg:%p, ahandle:%p type:%s in mpeer queue, content is null", pMsg, pMsg->rpcMsg.ahandle, taosMsg[pMsg->rpcMsg.msgType]);
    return TSDB_CODE_MND_INVALID_MSG_LEN;
  }

  if (!sdbIsMaster()) {
    SMnodeRsp *rpcRsp = &pMsg->rpcRsp;
    SRpcEpSet *epSet = static_cast<SRpcEpSet *>(rpcMallocCont(sizeof(SRpcEpSet)));
    mnodeGetMnodeEpSetForPeer(epSet, true);
    rpcRsp->rsp = epSet;
    rpcRsp->len = sizeof(SRpcEpSet);

    mDebug("msg:%p, ahandle:%p type:%s in mpeer queue is redirected, numOfEps:%d inUse:%d", pMsg, pMsg->rpcMsg.ahandle,
           taosMsg[pMsg->rpcMsg.msgType], epSet->numOfEps, epSet->inUse);

    return TSDB_CODE_RPC_REDIRECT;
  }

  if (pMsg->rpcMsg.msgType == TSDB_MSG_TYPE_DM_STATUS) {
    return mnodeProcessDnodeStatusMsg(pMsg);
  } else if (pMsg->rpcMsg.msgType == TSDB_MSG_TYPE_DM_CONFIG_TABLE) {
    return mnodeProcessTableCfgMsg(pMsg);
  } else if (pMsg->rpcMsg.msgType == TSDB_MSG_TYPE_DM_CONFIG_TABLE) {
    return mnodeProcessAuthMsg(pMsg);
  } else if (pMsg->rpcMsg.msgType == TSDB_MSG_TYPE_DM_CONFIG_VNODE) {
    return mnodeProcessVnodeCfgMsg(pMsg);
  } else {
    mError("msg:%p, ahandle:%p type:%s in mpeer queue, not processed", pMsg, pMsg->rpcMsg.ahandle,
           taosMsg[pMsg->rpcMsg.msgType]);
    return TSDB_CODE_MND_MSG_NOT_PROCESSED;
  }

  
}

void mnodeProcessPeerRsp(SRpcMsg *pMsg) {
  if (!sdbIsMaster()) {
    mError("msg:%p, ahandle:%p type:%s  is not processed for it is not master", pMsg, pMsg->ahandle,
           taosMsg[pMsg->msgType]);
    return;
  }

  if (pMsg->msgType == TSDB_MSG_TYPE_MD_CONFIG_DNODE_RSP) {
    mnodeProcessCfgDnodeMsgRsp(pMsg);
  } else if (pMsg->msgType == TSDB_MSG_TYPE_MD_CREATE_TABLE_RSP) {
    mnodeProcessCreateChildTableRsp(pMsg);
  } else if (pMsg->msgType == TSDB_MSG_TYPE_MD_DROP_TABLE_RSP) {
    mnodeProcessDropChildTableRsp(pMsg);
  } else if (pMsg->msgType == TSDB_MSG_TYPE_MD_DROP_STABLE_RSP) {
    mnodeProcessDropSuperTableRsp(pMsg);
  } else if (pMsg->msgType == TSDB_MSG_TYPE_MD_ALTER_TABLE_RSP) {
    mnodeProcessAlterTableRsp(pMsg);
  } else if (pMsg->msgType == TSDB_MSG_TYPE_MD_CREATE_VNODE_RSP) {
    mnodeProcessCreateVnodeRsp(pMsg);
  } else if (pMsg->msgType == TSDB_MSG_TYPE_MD_ALTER_VNODE_RSP) {
    mnodeProcessAlterVnodeRsp(pMsg);
  } else if (pMsg->msgType == TSDB_MSG_TYPE_MD_DROP_VNODE_RSP) {
    mnodeProcessDropVnodeRsp(pMsg);
  } else {
    mError("msg:%p, ahandle:%p type:%s is not processed", pMsg, pMsg->ahandle, taosMsg[pMsg->msgType]);
  }
}
