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

#ifndef TDENGINE_DNODE_H
#define TDENGINE_DNODE_H

#include <initializer_list>
#include <memory>
#include "trpc.h"
#include "taosmsg.h"
struct STaosQueue;

typedef struct {
  int32_t queryReqNum;
  int32_t submitReqNum;
  int32_t httpReqNum;
} SStatisInfo;

SStatisInfo dnodeGetStatisInfo();

bool    dnodeIsFirstDeploy();
bool    dnodeIsMasterEp(char *ep);
void    dnodeGetEpSetForPeer(SRpcEpSet *epSet);
void    dnodeGetEpSetForShell(SRpcEpSet *epSet);
int32_t dnodeGetDnodeId();
void    dnodeGetClusterId(char *clusterId);

void    dnodeUpdateEp(int32_t dnodeId, char *ep, char *fqdn, uint16_t *port);
bool    dnodeCheckEpChanged(int32_t dnodeId, char *epstr);
int32_t dnodeStartMnode(SMInfos *pMinfos);

void  dnodeAddClientRspHandle(uint8_t msgType, void (*fp)(SRpcMsg *rpcMsg));
void  dnodeSendMsgToDnode(SRpcEpSet *epSet, SRpcMsg *rpcMsg);
void  dnodeSendMsgToMnodeRecv(SRpcMsg *rpcMsg, SRpcMsg *rpcRsp);
void  dnodeSendMsgToDnodeRecv(SRpcMsg *rpcMsg, SRpcMsg *rpcRsp, SRpcEpSet *epSet);
void *dnodeSendCfgTableToRecv(int32_t vgId, int32_t tid);

std::unique_ptr<STaosQueue> dnodeAllocVWriteQueue(void *pVnode);
void  dnodeSendRpcVWriteRsp(void *pVnode, void *pWrite, int32_t code);
STaosQueue *dnodeAllocVQueryQueue(void *pVnode);
STaosQueue *dnodeAllocVFetchQueue(void *pVnode);

int32_t dnodeAllocateMPeerQueue();
void    dnodeFreeMPeerQueue();
int32_t dnodeAllocMReadQueue();
void    dnodeFreeMReadQueue();
int32_t dnodeAllocMWritequeue();
void    dnodeFreeMWritequeue();
void    dnodeSendRpcMWriteRsp(void *pMsg, int32_t code);
void    dnodeReprocessMWriteMsg(void *pMsg);
void    dnodeDelayReprocessMWriteMsg(void *pMsg);

void    dnodeSendStatusMsgToMnode();

struct SStep {
  char *name;
  int32_t (*initFp)();
  void (*cleanupFp)();
};

int32_t dnodeStepInit(const std::initializer_list<SStep> &steps);
void    dnodeStepCleanup(const std::initializer_list<SStep> &steps);
void    dnodeReportStep(char *name, char *desc, int8_t finished);

#endif
