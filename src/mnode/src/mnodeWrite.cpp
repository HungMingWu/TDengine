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
#include "tglobal.h"
#include "trpc.h"
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

extern int32_t mnodeProcessKillQueryMsg(SMnodeMsg *pMsg);
extern int32_t mnodeProcessKillStreamMsg(SMnodeMsg *pMsg);
extern int32_t mnodeProcessKillConnectionMsg(SMnodeMsg *pMsg);
extern int32_t mnodeProcessCreateDbMsg(SMnodeMsg *pMsg);
extern int32_t mnodeProcessAlterDbMsg(SMnodeMsg *pMsg);
extern int32_t mnodeProcessDropDbMsg(SMnodeMsg *pMsg);
extern int32_t mnodeProcessCreateDnodeMsg(SMnodeMsg *pMsg);
extern int32_t mnodeProcessDropDnodeMsg(SMnodeMsg *pMsg);
extern int32_t mnodeProcessCfgDnodeMsg(SMnodeMsg *pMsg);
extern int32_t mnodeProcessCreateTableMsg(SMnodeMsg *pMsg);
extern int32_t mnodeProcessDropTableMsg(SMnodeMsg *pMsg);
extern int32_t mnodeProcessAlterTableMsg(SMnodeMsg *pMsg);
extern int32_t mnodeProcessCreateUserMsg(SMnodeMsg *pMsg);
extern int32_t mnodeProcessAlterUserMsg(SMnodeMsg *pMsg);
extern int32_t mnodeProcessDropUserMsg(SMnodeMsg *pMsg);

int32_t mnodeProcessWrite(SMnodeMsg *pMsg) {
  if (pMsg->rpcMsg.pCont == NULL) {
    mError("msg:%p, app:%p type:%s content is null", pMsg, pMsg->rpcMsg.ahandle, taosMsg[pMsg->rpcMsg.msgType]);
    return TSDB_CODE_MND_INVALID_MSG_LEN;
  }

  if (!sdbIsMaster()) {
    SMnodeRsp *rpcRsp = &pMsg->rpcRsp;
    SRpcEpSet *epSet = static_cast<SRpcEpSet *>(rpcMallocCont(sizeof(SRpcEpSet)));
    mnodeGetMnodeEpSetForShell(epSet, true);
    rpcRsp->rsp = epSet;
    rpcRsp->len = sizeof(SRpcEpSet);

    mDebug("msg:%p, app:%p type:%s in write queue, is redirected, numOfEps:%d inUse:%d", pMsg,
           pMsg->rpcMsg.ahandle, taosMsg[pMsg->rpcMsg.msgType], epSet->numOfEps, epSet->inUse);

    return TSDB_CODE_RPC_REDIRECT;
  }

  if (pMsg->rpcMsg.msgType != TSDB_MSG_TYPE_CM_KILL_QUERY ||
      pMsg->rpcMsg.msgType != TSDB_MSG_TYPE_CM_KILL_STREAM ||
      pMsg->rpcMsg.msgType != TSDB_MSG_TYPE_CM_KILL_CONN ||
      pMsg->rpcMsg.msgType != TSDB_MSG_TYPE_CM_CREATE_DB ||
      pMsg->rpcMsg.msgType != TSDB_MSG_TYPE_CM_ALTER_DB ||
      pMsg->rpcMsg.msgType != TSDB_MSG_TYPE_CM_DROP_DB ||
      pMsg->rpcMsg.msgType != TSDB_MSG_TYPE_CM_CREATE_DNODE || 
      pMsg->rpcMsg.msgType != TSDB_MSG_TYPE_CM_DROP_DNODE ||
      pMsg->rpcMsg.msgType != TSDB_MSG_TYPE_CM_CONFIG_DNODE ||
      pMsg->rpcMsg.msgType != TSDB_MSG_TYPE_CM_CREATE_TABLE ||
      pMsg->rpcMsg.msgType != TSDB_MSG_TYPE_CM_DROP_TABLE ||
      pMsg->rpcMsg.msgType != TSDB_MSG_TYPE_CM_ALTER_TABLE ||
      pMsg->rpcMsg.msgType != TSDB_MSG_TYPE_CM_CREATE_USER ||
      pMsg->rpcMsg.msgType != TSDB_MSG_TYPE_CM_ALTER_USER ||
      pMsg->rpcMsg.msgType != TSDB_MSG_TYPE_CM_DROP_USER
      ) {
    mError("msg:%p, app:%p type:%s not processed", pMsg, pMsg->rpcMsg.ahandle, taosMsg[pMsg->rpcMsg.msgType]);
    return TSDB_CODE_MND_MSG_NOT_PROCESSED;
  }

  int32_t code = mnodeInitMsg(pMsg);
  if (code != TSDB_CODE_SUCCESS) {
    mError("msg:%p, app:%p type:%s not processed, reason:%s", pMsg, pMsg->rpcMsg.ahandle, taosMsg[pMsg->rpcMsg.msgType],
           tstrerror(code));
    return code;
  }

  if (!pMsg->pUser->writeAuth) {
    mError("msg:%p, app:%p type:%s not processed, no write auth", pMsg, pMsg->rpcMsg.ahandle,
           taosMsg[pMsg->rpcMsg.msgType]);
    return TSDB_CODE_MND_NO_RIGHTS;
  }

  if (pMsg->rpcMsg.msgType == TSDB_MSG_TYPE_CM_KILL_QUERY) return mnodeProcessKillQueryMsg(pMsg);
  else if (pMsg->rpcMsg.msgType == TSDB_MSG_TYPE_CM_KILL_STREAM) return mnodeProcessKillStreamMsg(pMsg);
  else if (pMsg->rpcMsg.msgType == TSDB_MSG_TYPE_CM_KILL_CONN) return mnodeProcessKillConnectionMsg(pMsg);
  else if (pMsg->rpcMsg.msgType == TSDB_MSG_TYPE_CM_CREATE_DB) return mnodeProcessCreateDbMsg(pMsg);
  else if (pMsg->rpcMsg.msgType == TSDB_MSG_TYPE_CM_ALTER_DB) return mnodeProcessAlterDbMsg(pMsg);
  else if (pMsg->rpcMsg.msgType == TSDB_MSG_TYPE_CM_DROP_DB) return mnodeProcessDropDbMsg(pMsg);
  else if (pMsg->rpcMsg.msgType == TSDB_MSG_TYPE_CM_CREATE_DNODE) return mnodeProcessCreateDnodeMsg(pMsg);
  else if (pMsg->rpcMsg.msgType == TSDB_MSG_TYPE_CM_DROP_DNODE) return mnodeProcessDropDnodeMsg(pMsg);
  else if (pMsg->rpcMsg.msgType == TSDB_MSG_TYPE_CM_CONFIG_DNODE) return mnodeProcessCfgDnodeMsg(pMsg);
  else if (pMsg->rpcMsg.msgType == TSDB_MSG_TYPE_CM_CREATE_TABLE) return mnodeProcessCreateTableMsg(pMsg);
  else if (pMsg->rpcMsg.msgType == TSDB_MSG_TYPE_CM_DROP_TABLE) return mnodeProcessDropTableMsg(pMsg);
  else if (pMsg->rpcMsg.msgType == TSDB_MSG_TYPE_CM_ALTER_TABLE) return mnodeProcessAlterTableMsg(pMsg);
  else if (pMsg->rpcMsg.msgType == TSDB_MSG_TYPE_CM_CREATE_USER) return mnodeProcessCreateUserMsg(pMsg);
  else if (pMsg->rpcMsg.msgType == TSDB_MSG_TYPE_CM_ALTER_USER) return mnodeProcessAlterUserMsg(pMsg);
  else if (pMsg->rpcMsg.msgType == TSDB_MSG_TYPE_CM_DROP_USER) return mnodeProcessDropUserMsg(pMsg);
  return -1;
}
