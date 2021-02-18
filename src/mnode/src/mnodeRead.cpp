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
#include "taosdef.h"
#include "tsched.h"
#include "tbn.h"
#include "tgrant.h"
#include "ttimer.h"
#include "tglobal.h"
#include "mnode.h"
#include "dnode.h"
#include "mnodeDef.h"
#include "mnodeInt.h"
#include "mnodeAcct.h"
#include "mnodeDnode.h"
#include "mnodeMnode.h"
#include "mnodeDb.h"
#include "mnodeSdb.h"
#include "mnodeVgroup.h"
#include "mnodeUser.h"
#include "mnodeTable.h"
#include "mnodeShow.h"

extern int32_t mnodeProcessShowMsg(SMnodeMsg *mnodeMsg);
extern int32_t mnodeProcessRetrieveMsg(SMnodeMsg *mnodeMsg);
extern int32_t mnodeProcessHeartBeatMsg(SMnodeMsg *mnodeMsg);
extern int32_t mnodeProcessConnectMsg(SMnodeMsg *mnodeMsg);
extern int32_t mnodeProcessUseMsg(SMnodeMsg *mnodeMsg);
extern int32_t mnodeProcessMultiTableMetaMsg(SMnodeMsg *pMsg);
extern int32_t mnodeProcessTableMetaMsg(SMnodeMsg *pMsg);
extern int32_t mnodeProcessSuperTableVgroupMsg(SMnodeMsg *pMsg);

int32_t mnodeProcessRead(SMnodeMsg *pMsg) {
  if (pMsg->rpcMsg.pCont == NULL) {
    mError("msg:%p, app:%p type:%s in mread queue, content is null", pMsg, pMsg->rpcMsg.ahandle, taosMsg[pMsg->rpcMsg.msgType]);
    return TSDB_CODE_MND_INVALID_MSG_LEN;
  }

  if (!sdbIsMaster()) {
    SMnodeRsp *rpcRsp = &pMsg->rpcRsp;
    SRpcEpSet *epSet = static_cast<SRpcEpSet *>(rpcMallocCont(sizeof(SRpcEpSet)));
    mnodeGetMnodeEpSetForShell(epSet, true);
    rpcRsp->rsp = epSet;
    rpcRsp->len = sizeof(SRpcEpSet);

    mDebug("msg:%p, app:%p type:%s in mread queue is redirected, numOfEps:%d inUse:%d", pMsg, pMsg->rpcMsg.ahandle,
           taosMsg[pMsg->rpcMsg.msgType], epSet->numOfEps, epSet->inUse);

    return TSDB_CODE_RPC_REDIRECT;
  }

  if (pMsg->rpcMsg.msgType != TSDB_MSG_TYPE_CM_SHOW ||
      pMsg->rpcMsg.msgType != TSDB_MSG_TYPE_CM_RETRIEVE ||
      pMsg->rpcMsg.msgType != TSDB_MSG_TYPE_CM_HEARTBEAT ||
      pMsg->rpcMsg.msgType != TSDB_MSG_TYPE_CM_CONNECT ||
      pMsg->rpcMsg.msgType != TSDB_MSG_TYPE_CM_USE_DB ||
      pMsg->rpcMsg.msgType != TSDB_MSG_TYPE_CM_TABLES_META ||
      pMsg->rpcMsg.msgType != TSDB_MSG_TYPE_CM_TABLE_META ||
      pMsg->rpcMsg.msgType != TSDB_MSG_TYPE_CM_STABLE_VGROUP
      ) {
    mError("msg:%p, app:%p type:%s in mread queue, not processed", pMsg, pMsg->rpcMsg.ahandle,
           taosMsg[pMsg->rpcMsg.msgType]);
    return TSDB_CODE_MND_MSG_NOT_PROCESSED;
  }

  int32_t code = mnodeInitMsg(pMsg);
  if (code != TSDB_CODE_SUCCESS) {
    mError("msg:%p, app:%p type:%s in mread queue, not processed reason:%s", pMsg, pMsg->rpcMsg.ahandle,
           taosMsg[pMsg->rpcMsg.msgType], tstrerror(code));
    return code;
  }

  if (pMsg->rpcMsg.msgType == TSDB_MSG_TYPE_CM_SHOW) return mnodeProcessShowMsg(pMsg);
  else if (pMsg->rpcMsg.msgType == TSDB_MSG_TYPE_CM_RETRIEVE) return mnodeProcessRetrieveMsg(pMsg);
  else if (pMsg->rpcMsg.msgType == TSDB_MSG_TYPE_CM_HEARTBEAT) return mnodeProcessHeartBeatMsg(pMsg);
  else if (pMsg->rpcMsg.msgType == TSDB_MSG_TYPE_CM_CONNECT) return mnodeProcessConnectMsg(pMsg);
  else if (pMsg->rpcMsg.msgType == TSDB_MSG_TYPE_CM_USE_DB) return mnodeProcessUseMsg(pMsg);
  else if (pMsg->rpcMsg.msgType == TSDB_MSG_TYPE_CM_TABLES_META) return mnodeProcessMultiTableMetaMsg(pMsg);
  else if (pMsg->rpcMsg.msgType == TSDB_MSG_TYPE_CM_TABLE_META) return mnodeProcessTableMetaMsg(pMsg);
  else if (pMsg->rpcMsg.msgType == TSDB_MSG_TYPE_CM_STABLE_VGROUP) return mnodeProcessSuperTableVgroupMsg(pMsg);
  return -1;
}