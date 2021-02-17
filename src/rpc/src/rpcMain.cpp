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
#include "tmd5.h"
#include "tmempool.h"
#include "ttimer.h"
#include "tutil.h"
#include "lz4.h"
#include "tref.h"
#include "taoserror.h"
#include "tsocket.h"
#include "tglobal.h"
#include "taosmsg.h"
#include "trpc.h"
#include "hash.h"
#include "rpcLog.h"
#include "rpcUdp.h"
#include "rpcCache.h"
#include "rpcTcp.h"
#include "rpcHead.h"

#define RPC_MSG_OVERHEAD (sizeof(SRpcReqContext) + sizeof(SRpcHead) + sizeof(SRpcDigest)) 
#define rpcHeadFromCont(cont) ((SRpcHead *) ((char*)cont - sizeof(SRpcHead)))
#define rpcContFromHead(msg) (msg + sizeof(SRpcHead))
#define rpcMsgLenFromCont(contLen) (contLen + sizeof(SRpcHead))
#define rpcContLenFromMsg(msgLen) (msgLen - sizeof(SRpcHead))
#define rpcIsReq(type) (type & 1U)

int tsRpcMaxUdpSize = 15000;  // bytes
int tsProgressTimer = 100;
// not configurable
int tsRpcMaxRetry;
int tsRpcHeadSize;
int tsRpcOverhead;

static int     tsRpcRefId = -1;

SRpcInfo::SRpcInfo()
{
    tsRpcNum++;
}

SRpcInfo::~SRpcInfo()
{
    tsRpcNum--;
}

std::atomic<int32_t> SRpcInfo::tsRpcNum{0};

//static pthread_once_t tsRpcInit = PTHREAD_ONCE_INIT;

// server:0 client:1  tcp:2 udp:0
#define RPC_CONN_UDPS   0
#define RPC_CONN_UDPC   1
#define RPC_CONN_TCPS   2
#define RPC_CONN_TCPC   3

void *(*taosInitConn[])(uint32_t ip, uint16_t port, char *label, int threads, void *fp, void *shandle) = {
    taosInitUdpConnection,
    taosInitUdpConnection,
    taosInitTcpServer, 
    taosInitTcpClient
};

void (*taosCleanUpConn[])(void *thandle) = {
    taosCleanUpUdpConnection, 
    taosCleanUpUdpConnection, 
    taosCleanUpTcpServer,
    taosCleanUpTcpClient
};

void (*taosStopConn[])(void *thandle) = {
    taosStopUdpConnection, 
    taosStopUdpConnection, 
    taosStopTcpServer,
    taosStopTcpClient,
};

int (*taosSendData[])(uint32_t ip, uint16_t port, void *data, int len, void *chandle) = {
    taosSendUdpData, 
    taosSendUdpData, 
    taosSendTcpData, 
    taosSendTcpData
};

void *(*taosOpenConn[])(void *shandle, void *thandle, uint32_t ip, uint16_t port) = {
    taosOpenUdpConnection,
    taosOpenUdpConnection,
    NULL,
    taosOpenTcpClientConnection,
};

void (*taosCloseConn[])(void *chandle) = {
    NULL, 
    NULL, 
    taosCloseTcpConnection, 
    taosCloseTcpConnection
};

static SRpcConn *rpcOpenConn(SRpcInfo *pRpc, char *peerFqdn, uint16_t peerPort, int8_t connType);
static void      rpcCloseConn(void *thandle);
static SRpcConn *rpcSetupConnToServer(SRpcReqContext *pContext);
static SRpcConn *rpcAllocateClientConn(SRpcInfo *pRpc);
static SRpcConn *rpcAllocateServerConn(SRpcInfo *pRpc, SRecvInfo *pRecv);
static SRpcConn *rpcGetConnObj(SRpcInfo *pRpc, int sid, SRecvInfo *pRecv);

static void  rpcSendReqToServer(SRpcInfo *pRpc, SRpcReqContext *pContext);
static void  rpcSendQuickRsp(SRpcConn *pConn, int32_t code);
static void  rpcSendErrorMsgToPeer(SRecvInfo *pRecv, int32_t code);
static void  rpcSendMsgToPeer(SRpcConn *pConn, void *data, int dataLen);
static void  rpcSendReqHead(SRpcConn *pConn);

static void *rpcProcessMsgFromPeer(SRecvInfo *pRecv);
static void  rpcProcessIncomingMsg(SRpcConn *pConn, SRpcHead *pHead, SRpcReqContext *pContext);
static void  rpcProcessIdleTimer(SRpcConn *pConn, void *tmrId);
static void  rpcProcessProgressTimer(SRpcConn *pConn, void *tmrId);

static void  rpcFreeMsg(void *msg);
static int32_t rpcCompressRpcMsg(char* pCont, int32_t contLen);
static SRpcHead *rpcDecompressRpcMsg(SRpcHead *pHead);
static int   rpcAddAuthPart(SRpcConn *pConn, char *msg, int msgLen);
static int   rpcCheckAuthentication(SRpcConn *pConn, char *msg, int msgLen);
static void  rpcLockConn(SRpcConn *pConn);
static void  rpcUnlockConn(SRpcConn *pConn);
static void  rpcAddRef(SRpcInfo *pRpc);
static void  rpcDecRef(SRpcInfo *pRpc);

static void rpcFree(void *p) {
  tTrace("free mem: %p", p);
  free(p);
}

int32_t rpcInit(void) {
  tsProgressTimer = tsRpcTimer/2; 
  tsRpcMaxRetry = tsRpcMaxTime * 1000/tsProgressTimer;
  tsRpcHeadSize = RPC_MSG_OVERHEAD; 
  tsRpcOverhead = sizeof(SRpcReqContext);

  tsRpcRefId = taosOpenRef(200, rpcFree);

  return 0;
}
 
void rpcCleanup(void) {
  taosCloseRef(tsRpcRefId);
  tsRpcRefId = -1;
}
 
SRpcInfo *rpcOpen(const SRpcInit &init) {
  //pthread_once(&tsRpcInit, rpcInit);

  auto pRpc = new SRpcInfo;
  if (pRpc == NULL) return NULL;

  if (init.label) tstrncpy(pRpc->label, init.label, sizeof(pRpc->label));
  pRpc->connType = init.connType;
  pRpc->idleTime = init.idleTime;
  pRpc->numOfThreads = init.numOfThreads>TSDB_MAX_RPC_THREADS ? TSDB_MAX_RPC_THREADS:init.numOfThreads;
  pRpc->localPort = init.localPort;
  pRpc->afp = init.afp;
  pRpc->sessions = init.sessions+1;
  if (init.user) tstrncpy(pRpc->user, init.user, sizeof(pRpc->user));
  if (init.secret) memcpy(pRpc->secret, init.secret, sizeof(pRpc->secret));
  if (init.ckey) tstrncpy(pRpc->ckey, init.ckey, sizeof(pRpc->ckey));
  pRpc->spi = init.spi;
  pRpc->cfp = init.cfp;
  pRpc->afp = init.afp;
  pRpc->refCount = 1;

  pRpc->connList.resize(pRpc->sessions);

  pRpc->idPool.reset(new id_pool_t(pRpc->sessions - 1));
  pRpc->tmrCtrl = taosTmrInit(pRpc->sessions*2 + 1, 50, 10000, pRpc->label);
  if (pRpc->tmrCtrl == NULL) {
    tError("%s failed to init timers", pRpc->label);
    rpcClose(pRpc);
    return NULL;
  }

  if (pRpc->connType == TAOS_CONN_SERVER) {
    pRpc->hash = taosHashInit(pRpc->sessions, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), true, HASH_ENTRY_LOCK);
    if (pRpc->hash == NULL) {
      tError("%s failed to init string hash", pRpc->label);
      rpcClose(pRpc);
      return NULL;
    }
  } else {
    pRpc->pCache = rpcOpenConnCache(pRpc->sessions, rpcCloseConn, pRpc->tmrCtrl, pRpc->idleTime); 
    if ( pRpc->pCache == NULL ) {
      tError("%s failed to init connection cache", pRpc->label);
      rpcClose(pRpc);
      return NULL;
    }
  }

  pRpc->tcphandle = (*taosInitConn[pRpc->connType|RPC_CONN_TCP])(0, pRpc->localPort, pRpc->label, 
                                                  pRpc->numOfThreads, (void *)rpcProcessMsgFromPeer, pRpc);
  pRpc->udphandle = (*taosInitConn[pRpc->connType])(0, pRpc->localPort, pRpc->label, 
                                                  pRpc->numOfThreads, (void *)rpcProcessMsgFromPeer, pRpc);

  if (pRpc->tcphandle == NULL || pRpc->udphandle == NULL) {
    tError("%s failed to init network, port:%d", pRpc->label, pRpc->localPort);
    rpcClose(pRpc);
    return NULL;
  }

  tDebug("%s rpc is opened, threads:%d sessions:%d", pRpc->label, pRpc->numOfThreads, init.sessions);

  return pRpc;
}

void rpcClose(void *param) {
  SRpcInfo *pRpc = (SRpcInfo *)param;

  // stop connection to outside first
  (*taosStopConn[pRpc->connType | RPC_CONN_TCP])(pRpc->tcphandle);
  (*taosStopConn[pRpc->connType])(pRpc->udphandle);

  // close all connections 
  for (int i = 0; i < pRpc->sessions; ++i) {
    if (pRpc->connList[i].user[0]) {
      rpcCloseConn((void *)(&pRpc->connList[i]));
    }
  }

  // clean up
  (*taosCleanUpConn[pRpc->connType | RPC_CONN_TCP])(pRpc->tcphandle);
  (*taosCleanUpConn[pRpc->connType])(pRpc->udphandle);

  tDebug("%s rpc is closed", pRpc->label);
  rpcDecRef(pRpc);
}

void *rpcMallocCont(int contLen) {
  int size = contLen + RPC_MSG_OVERHEAD;

  char *start = (char *)calloc(1, (size_t)size);
  if (start == NULL) {
    tError("failed to malloc msg, size:%d", size);
    return NULL;
  } else {
    tTrace("malloc mem:%p size:%d", start, size);
  }

  return start + sizeof(SRpcReqContext) + sizeof(SRpcHead);
}

void rpcFreeCont(void *cont) {
  if (cont) {
    char *temp = ((char *)cont) - sizeof(SRpcHead) - sizeof(SRpcReqContext);
    free(temp);
    tTrace("free mem: %p", temp);
  }
}

void *rpcReallocCont(void *ptr, int contLen) {
  if (ptr == NULL) return rpcMallocCont(contLen);

  char *start = ((char *)ptr) - sizeof(SRpcReqContext) - sizeof(SRpcHead);
  if (contLen == 0 ) {
    free(start); 
    return NULL;
  }

  int size = contLen + RPC_MSG_OVERHEAD;
  start = (char *)realloc(start, size);
  if (start == NULL) {
    tError("failed to realloc cont, size:%d", size);
    return NULL;
  } 

  return start + sizeof(SRpcReqContext) + sizeof(SRpcHead);
}

int64_t rpcSendRequest(SRpcInfo *pRpc, const SRpcEpSet *pEpSet, SRpcMsg *pMsg) {
  SRpcReqContext *pContext;

  int contLen = rpcCompressRpcMsg((char *)pMsg->pCont, pMsg->contLen);
  pContext = (SRpcReqContext *) ((char*)pMsg->pCont-sizeof(SRpcHead)-sizeof(SRpcReqContext));
  pContext->ahandle = pMsg->ahandle;
  pContext->pRpc = pRpc;
  pContext->epSet = *pEpSet;
  pContext->contLen = contLen;
  pContext->pCont = (uint8_t *)pMsg->pCont;
  pContext->msgType = pMsg->msgType;
  pContext->oldInUse = pEpSet->inUse;

  pContext->connType = RPC_CONN_UDPC; 
  if (contLen > tsRpcMaxUdpSize) pContext->connType = RPC_CONN_TCPC;

  // connection type is application specific. 
  // for TDengine, all the query, show commands shall have TCP connection
  char type = pMsg->msgType;
  if (type == TSDB_MSG_TYPE_QUERY || type == TSDB_MSG_TYPE_CM_RETRIEVE
    || type == TSDB_MSG_TYPE_FETCH || type == TSDB_MSG_TYPE_CM_STABLE_VGROUP
    || type == TSDB_MSG_TYPE_CM_TABLES_META || type == TSDB_MSG_TYPE_CM_TABLE_META
    || type == TSDB_MSG_TYPE_CM_SHOW || type == TSDB_MSG_TYPE_DM_STATUS)
    pContext->connType = RPC_CONN_TCPC;
  
  pContext->rid = taosAddRef(tsRpcRefId, pContext);

  rpcSendReqToServer(pRpc, pContext);
  return pContext->rid;
}

void rpcSendResponse(const SRpcMsg *pRsp) {
  int        msgLen = 0;
  SRpcConn  *pConn = (SRpcConn *)pRsp->handle;
  SRpcMsg    rpcMsg = *pRsp;
  SRpcMsg   *pMsg = &rpcMsg;
  SRpcInfo  *pRpc = pConn->pRpc;

  if ( pMsg->pCont == NULL ) {
    pMsg->pCont = rpcMallocCont(0);
    pMsg->contLen = 0;
  }

  SRpcHead  *pHead = rpcHeadFromCont(pMsg->pCont);
  char      *msg = (char *)pHead;

  pMsg->contLen = rpcCompressRpcMsg((char *)pMsg->pCont, pMsg->contLen);
  msgLen = rpcMsgLenFromCont(pMsg->contLen);

  rpcLockConn(pConn);

  if ( pConn->inType == 0 || pConn->user[0] == 0 ) {
    tError("%s, connection is already released, rsp wont be sent", pConn->info);
    rpcUnlockConn(pConn);
    rpcFreeCont(pMsg->pCont);
    rpcDecRef(pRpc);
    return;
  }

  // set msg header
  pHead->version = 1;
  pHead->msgType = pConn->inType+1;
  pHead->spi = pConn->spi;
  pHead->encrypt = pConn->encrypt;
  pHead->tranId = pConn->inTranId;
  pHead->sourceId = pConn->ownId;
  pHead->destId = pConn->peerId;
  pHead->linkUid = pConn->linkUid;
  pHead->port = htons(pConn->localPort);
  pHead->code = htonl(pMsg->code);
  pHead->ahandle = (uint64_t) pConn->ahandle;
 
  // set pConn parameters
  pConn->inType = 0;

  // response message is released until new response is sent
  rpcFreeMsg(pConn->pRspMsg); 
  pConn->pRspMsg = msg;
  pConn->rspMsgLen = msgLen;
  if (pMsg->code == TSDB_CODE_RPC_ACTION_IN_PROGRESS) pConn->inTranId--;

  // stop the progress timer
  taosTmrStopA(&pConn->pTimer);

  // set the idle timer to monitor the activity
  taosTmrReset([pConn](void *tmrId) { rpcProcessIdleTimer(pConn, tmrId); }, pRpc->idleTime,  pRpc->tmrCtrl,
               &pConn->pIdleTimer);
  rpcSendMsgToPeer(pConn, msg, msgLen);

  // if not set to secured, set it expcet NOT_READY case, since client wont treat it as secured
  if (pConn->secured == 0 && pMsg->code != TSDB_CODE_RPC_NOT_READY)
    pConn->secured = 1; // connection shall be secured

  if (pConn->pReqMsg) rpcFreeCont(pConn->pReqMsg);
  pConn->pReqMsg = NULL;
  pConn->reqMsgLen = 0;

  rpcUnlockConn(pConn);
  rpcDecRef(pRpc);    // decrease the referene count

  return;
}

void rpcSendRedirectRsp(void *thandle, const SRpcEpSet *pEpSet) {
  SRpcMsg  rpcMsg; 
  memset(&rpcMsg, 0, sizeof(rpcMsg));
  
  rpcMsg.contLen = sizeof(SRpcEpSet);
  rpcMsg.pCont = rpcMallocCont(rpcMsg.contLen);
  if (rpcMsg.pCont == NULL) return;

  memcpy(rpcMsg.pCont, pEpSet, sizeof(SRpcEpSet));

  rpcMsg.code = TSDB_CODE_RPC_REDIRECT;
  rpcMsg.handle = thandle;

  rpcSendResponse(&rpcMsg);

  return;
}

int rpcGetConnInfo(void *thandle, SRpcConnInfo *pInfo) {
  SRpcConn  *pConn = (SRpcConn *)thandle;
  if (pConn->user[0] == 0) return -1;

  pInfo->clientIp = pConn->peerIp;
  pInfo->clientPort = pConn->peerPort;
  // pInfo->serverIp = pConn->destIp;

  tstrncpy(pInfo->user, pConn->user, sizeof(pInfo->user));
  return 0;
}

void rpcSendRecv(SRpcInfo *pRpc, SRpcEpSet *pEpSet, SRpcMsg *pMsg, SRpcMsg *pRsp) {
  SRpcReqContext *pContext;
  pContext = (SRpcReqContext *) ((char*)pMsg->pCont-sizeof(SRpcHead)-sizeof(SRpcReqContext));

  memset(pRsp, 0, sizeof(SRpcMsg));
  
  tsem_t   sem;        
  tsem_init(&sem, 0, 0);
  pContext->pSem = &sem;
  pContext->pRsp = pRsp;
  pContext->pSet = pEpSet;

  rpcSendRequest(pRpc, pEpSet, pMsg);

  tsem_wait(&sem);
  tsem_destroy(&sem);

  return;
}

// this API is used by server app to keep an APP context in case connection is broken
int rpcReportProgress(void *handle, char *pCont, int contLen) {
  SRpcConn *pConn = (SRpcConn *)handle;
  int code = 0;

  rpcLockConn(pConn);

  if (pConn->user[0]) {
    // pReqMsg and reqMsgLen is re-used to store the context from app server
    pConn->pReqMsg = pCont;     
    pConn->reqMsgLen = contLen;
  } else {
    tDebug("%s, rpc connection is already released", pConn->info);
    rpcFreeCont(pCont);
    code = -1;
  }

  rpcUnlockConn(pConn);
  return code;
}

void rpcCancelRequest(int64_t rid) {

  SRpcReqContext *pContext = (SRpcReqContext*)taosAcquireRef(tsRpcRefId, rid);
  if (pContext == NULL) return;

  rpcCloseConn(pContext->pConn);

  taosReleaseRef(tsRpcRefId, rid);
}

static void rpcFreeMsg(void *msg) {
  if ( msg ) {
    char *temp = (char *)msg - sizeof(SRpcReqContext);
    free(temp);
    tTrace("free mem: %p", temp);
  }
}

static SRpcConn *rpcOpenConn(SRpcInfo *pRpc, char *peerFqdn, uint16_t peerPort, int8_t connType) {
  SRpcConn *pConn;

  uint32_t peerIp = taosGetIpv4FromFqdn(peerFqdn);
  if (peerIp == 0xFFFFFFFF) {
    tError("%s, failed to resolve FQDN:%s", pRpc->label, peerFqdn); 
    terrno = TSDB_CODE_RPC_FQDN_ERROR; 
    return NULL;
  }

  pConn = rpcAllocateClientConn(pRpc); 

  if (pConn) { 
    tstrncpy(pConn->peerFqdn, peerFqdn, sizeof(pConn->peerFqdn));
    pConn->peerIp = peerIp;
    pConn->peerPort = peerPort;
    tstrncpy(pConn->user, pRpc->user, sizeof(pConn->user));
    pConn->connType = connType;

    if (taosOpenConn[connType]) {
      void *shandle = (connType & RPC_CONN_TCP)? pRpc->tcphandle:pRpc->udphandle;
      pConn->chandle = (*taosOpenConn[connType])(shandle, pConn, pConn->peerIp, pConn->peerPort);
      if (pConn->chandle == NULL) {
        tError("failed to connect to:0x%x:%d", pConn->peerIp, pConn->peerPort);

        terrno = TSDB_CODE_RPC_NETWORK_UNAVAIL;
        rpcCloseConn(pConn);
        pConn = NULL;
      }
    }
  }

  return pConn;
}

static void rpcReleaseConn(SRpcConn *pConn) {
  SRpcInfo *pRpc = pConn->pRpc;
  if (pConn->user[0] == 0) return;

  pConn->user[0] = 0;
  if (taosCloseConn[pConn->connType]) (*taosCloseConn[pConn->connType])(pConn->chandle);

  taosTmrStopA(&pConn->pTimer);
  taosTmrStopA(&pConn->pIdleTimer);

  if ( pRpc->connType == TAOS_CONN_SERVER) {
    char hashstr[40] = {0};
    size_t size = snprintf(hashstr, sizeof(hashstr), "%x:%x:%x:%d", pConn->peerIp, pConn->linkUid, pConn->peerId, pConn->connType);
    taosHashRemove(pRpc->hash, hashstr, size);
    rpcFreeMsg(pConn->pRspMsg); // it may have a response msg saved, but not request msg
    pConn->pRspMsg = NULL;
  
    // if server has ever reported progress, free content
    if (pConn->pReqMsg) rpcFreeCont(pConn->pReqMsg);  // do not use rpcFreeMsg
  } else {
    // if there is an outgoing message, free it
    if (pConn->outType && pConn->pReqMsg) {
      SRpcReqContext *pContext = pConn->pContext;
      if (pContext) {
        if (pContext->pRsp) {   
        // for synchronous API, post semaphore to unblock app
          pContext->pRsp->code = TSDB_CODE_RPC_APP_ERROR;
          pContext->pRsp->pCont = NULL;
          pContext->pRsp->contLen = 0;
          tsem_post(pContext->pSem);
        }
        pContext->pConn = NULL; 
        taosRemoveRef(tsRpcRefId, pContext->rid);
      } else {
        assert(0); 
      }
    }
  }

  // memset could not be used, since lockeBy can not be reset
  pConn->inType = 0;
  pConn->outType = 0;
  pConn->inTranId = 0;
  pConn->outTranId = 0;
  pConn->secured = 0;
  pConn->peerId = 0;
  pConn->peerIp = 0;
  pConn->peerPort = 0;
  pConn->pReqMsg = NULL;
  pConn->reqMsgLen = 0;
  pConn->pContext = NULL;
  pConn->chandle = NULL;

  pRpc->idPool->dealloc(pConn->sid);
  tDebug("%s, rpc connection is released", pConn->info);
}

static void rpcCloseConn(void *thandle) {
  SRpcConn *pConn = (SRpcConn *)thandle;
  if (pConn == NULL) return;

  rpcLockConn(pConn);

  if (pConn->user[0])
    rpcReleaseConn(pConn);

  rpcUnlockConn(pConn);
}

static SRpcConn *rpcAllocateClientConn(SRpcInfo *pRpc) {
  SRpcConn *pConn = NULL;

  int sid = pRpc->idPool->alloc();
  if (sid <= 0) {
    tError("%s maximum number of sessions:%d is reached", pRpc->label, pRpc->sessions);
    terrno = TSDB_CODE_RPC_MAX_SESSIONS;
  } else {
    pConn = &pRpc->connList[sid];

    pConn->pRpc = pRpc;
    pConn->sid = sid;
    pConn->tranId = (uint16_t)(taosRand() & 0xFFFF);
    pConn->ownId = htonl(pConn->sid);
    pConn->linkUid = (uint32_t)((int64_t)pConn + (int64_t)getpid() + (int64_t)pConn->tranId);
    pConn->spi = pRpc->spi;
    pConn->encrypt = pRpc->encrypt;
    if (pConn->spi) memcpy(pConn->secret, pRpc->secret, TSDB_KEY_LEN);
    tDebug("%s %p client connection is allocated, uid:0x%x", pRpc->label, pConn, pConn->linkUid);
  }

  return pConn;
}

static SRpcConn *rpcAllocateServerConn(SRpcInfo *pRpc, SRecvInfo *pRecv) {
  SRpcConn *pConn = NULL;
  char      hashstr[40] = {0};
  SRpcHead *pHead = (SRpcHead *)pRecv->msg;

  size_t size = snprintf(hashstr, sizeof(hashstr), "%x:%x:%x:%d", pRecv->ip, pHead->linkUid, pHead->sourceId, pRecv->connType);
 
  // check if it is already allocated
  SRpcConn **ppConn = (SRpcConn **)(taosHashGet(pRpc->hash, hashstr, size));
  if (ppConn) pConn = *ppConn;
  if (pConn) {
    pConn->secured = 0;
    return pConn;
  }

  // if code is not 0, it means it is simple reqhead, just ignore
  if (pHead->code != 0) {
    terrno = TSDB_CODE_RPC_ALREADY_PROCESSED;
    return NULL;
  }

  int sid = pRpc->idPool->alloc();
  if (sid <= 0) {
    tError("%s maximum number of sessions:%d is reached", pRpc->label, pRpc->sessions);
    terrno = TSDB_CODE_RPC_MAX_SESSIONS;
  } else {
    pConn = &pRpc->connList[sid];
    memcpy(pConn->user, pHead->user, tListLen(pConn->user));
    pConn->pRpc = pRpc;
    pConn->sid = sid;
    pConn->tranId = (uint16_t)(rand() & 0xFFFF);
    pConn->ownId = htonl(pConn->sid);
    pConn->linkUid = pHead->linkUid;
    if (pRpc->afp) {
      if (pConn->user[0] == 0) {
        terrno = TSDB_CODE_RPC_AUTH_REQUIRED;
      } else {
        terrno = (*pRpc->afp)(pConn->user, &pConn->spi, &pConn->encrypt, pConn->secret, pConn->ckey);
      }

      if (terrno != 0) {
        pRpc->idPool->dealloc(sid);  // sid shall be released
        pConn = NULL;
      }
    }
  }

  if (pConn) {
    if (pRecv->connType == RPC_CONN_UDPS && pRpc->numOfThreads > 1) {
      // UDP server, assign to new connection
      pRpc->index = (pRpc->index + 1) % pRpc->numOfThreads;
      pConn->localPort = (pRpc->localPort + pRpc->index);
    }

    taosHashPut(pRpc->hash, hashstr, size, (char *)&pConn, POINTER_BYTES);
    tDebug("%s %p server connection is allocated, uid:0x%x sid:%d key:%s", pRpc->label, pConn, pConn->linkUid, sid, hashstr);
  }

  return pConn;
}

static SRpcConn *rpcGetConnObj(SRpcInfo *pRpc, int sid, SRecvInfo *pRecv) {
  SRpcConn *pConn = NULL;  
  SRpcHead *pHead = (SRpcHead *)pRecv->msg;

  if (sid) {
    pConn = &pRpc->connList[sid];
    if (pConn->user[0] == 0) pConn = NULL;
  } 

  if (pConn == NULL) { 
    if (pRpc->connType == TAOS_CONN_SERVER) {
      pConn = rpcAllocateServerConn(pRpc, pRecv);
    } else {
      terrno = TSDB_CODE_RPC_UNEXPECTED_RESPONSE;
    }
  }

  if (pConn) {
    if (pConn->linkUid != pHead->linkUid) {
      terrno = TSDB_CODE_RPC_MISMATCHED_LINK_ID;
      tDebug("%s %p %p, linkUid:0x%x is not matched with received:0x%x", pRpc->label, pConn, (void *)pHead->ahandle,
             pConn->linkUid, pHead->linkUid);
      pConn = NULL;
    }
  }

  return pConn;
}

static SRpcConn *rpcSetupConnToServer(SRpcReqContext *pContext) {
  SRpcConn   *pConn;
  SRpcInfo   *pRpc = pContext->pRpc;
  SRpcEpSet  *pEpSet = &pContext->epSet;

  pConn = (SRpcConn*)rpcGetConnFromCache(pRpc->pCache, pEpSet->fqdn[pEpSet->inUse], pEpSet->port[pEpSet->inUse], pContext->connType);
  if ( pConn == NULL || pConn->user[0] == 0) {
    pConn = rpcOpenConn(pRpc, pEpSet->fqdn[pEpSet->inUse], pEpSet->port[pEpSet->inUse], pContext->connType);
  } 

  if (pConn) {
    pConn->tretry = 0;
    pConn->ahandle = pContext->ahandle;
    snprintf(pConn->info, sizeof(pConn->info), "%s %p %p", pRpc->label, pConn, pConn->ahandle);
    pConn->tretry = 0;
  } else {
    tError("%s %p, failed to set up connection(%s)", pRpc->label, pContext->ahandle, tstrerror(terrno));
  }

  return pConn;
}

static int rpcProcessReqHead(SRpcConn *pConn, SRpcHead *pHead) {

    if (pConn->peerId == 0) {
      pConn->peerId = pHead->sourceId;
    } else {
      if (pConn->peerId != pHead->sourceId) {
        tDebug("%s, source Id is changed, old:0x%08x new:0x%08x", pConn->info, 
               pConn->peerId, pHead->sourceId);
        return TSDB_CODE_RPC_INVALID_VALUE;
      }
    }

    if (pConn->inTranId == pHead->tranId) {
      if (pConn->inType == pHead->msgType) {
        if (pHead->code == 0) {
          tDebug("%s, %s is retransmitted", pConn->info, taosMsg[pHead->msgType]);
          rpcSendQuickRsp(pConn, TSDB_CODE_RPC_ACTION_IN_PROGRESS);
        } else {
          // do nothing, it is heart beat from client
        }
      } else if (pConn->inType == 0) {
        tDebug("%s, %s is already processed, tranId:%d", pConn->info, taosMsg[pHead->msgType], pConn->inTranId);
        rpcSendMsgToPeer(pConn, pConn->pRspMsg, pConn->rspMsgLen); // resend the response
      } else {
        tDebug("%s, mismatched message %s and tranId", pConn->info, taosMsg[pHead->msgType]);
      }

      // do not reply any message
      return TSDB_CODE_RPC_ALREADY_PROCESSED;
    }

    if (pConn->inType != 0) {
      tDebug("%s, last session is not finished, inTranId:%d tranId:%d", pConn->info, 
              pConn->inTranId, pHead->tranId);
      return TSDB_CODE_RPC_LAST_SESSION_NOT_FINISHED;
    }

    if (rpcContLenFromMsg(pHead->msgLen) <= 0) {
      tDebug("%s, message body is empty, ignore", pConn->info);
      return TSDB_CODE_RPC_APP_ERROR;
    }

    pConn->inTranId = pHead->tranId;
    pConn->inType = pHead->msgType;

    // start the progress timer to monitor the response from server app
    if (pConn->connType != RPC_CONN_TCPS) 
      pConn->pTimer = taosTmrStart([pConn](void *tmrId) { rpcProcessProgressTimer(pConn, tmrId); }, tsProgressTimer,
                                   pConn->pRpc->tmrCtrl);
 
    return 0;
}

static int rpcProcessRspHead(SRpcConn *pConn, SRpcHead *pHead) {
  SRpcInfo *pRpc = pConn->pRpc;
  pConn->peerId = pHead->sourceId;

  if (pConn->outType == 0 || pConn->pContext == NULL) {
    return TSDB_CODE_RPC_UNEXPECTED_RESPONSE;
  }

  if (pHead->tranId != pConn->outTranId) {
    return TSDB_CODE_RPC_INVALID_TRAN_ID;
  }

  if (pHead->msgType != pConn->outType + 1) {
    return TSDB_CODE_RPC_INVALID_RESPONSE_TYPE;
  }

  taosTmrStopA(&pConn->pTimer);
  pConn->retry = 0;

  if (pHead->code == TSDB_CODE_RPC_AUTH_REQUIRED && pRpc->spi) {
    tDebug("%s, authentication shall be restarted", pConn->info);
    pConn->secured = 0;
    rpcSendMsgToPeer(pConn, pConn->pReqMsg, pConn->reqMsgLen);      
    if (pConn->connType != RPC_CONN_TCPC)
      pConn->pTimer =
          taosTmrStart([pConn](void *tmrId) { pConn->processRetryTimer(tmrId); }, tsRpcTimer, pRpc->tmrCtrl);
    return TSDB_CODE_RPC_ALREADY_PROCESSED;
  }

  if (pHead->code == TSDB_CODE_RPC_MISMATCHED_LINK_ID) {
    tDebug("%s, mismatched linkUid, link shall be restarted", pConn->info);
    pConn->secured = 0;
    ((SRpcHead *)pConn->pReqMsg)->destId = 0;
    rpcSendMsgToPeer(pConn, pConn->pReqMsg, pConn->reqMsgLen);      
    if (pConn->connType != RPC_CONN_TCPC)
      pConn->pTimer =
          taosTmrStart([pConn](void *tmrId) { pConn->processRetryTimer(tmrId); }, tsRpcTimer, pRpc->tmrCtrl);
    return TSDB_CODE_RPC_ALREADY_PROCESSED;
  }

  if (pHead->code == TSDB_CODE_RPC_ACTION_IN_PROGRESS) {
    if (pConn->tretry <= tsRpcMaxRetry) {
      tDebug("%s, peer is still processing the transaction, retry:%d", pConn->info, pConn->tretry);
      pConn->tretry++;
      rpcSendReqHead(pConn);
      if (pConn->connType != RPC_CONN_TCPC)
        pConn->pTimer =
            taosTmrStart([pConn](void *tmrId) { pConn->processRetryTimer(tmrId); }, tsRpcTimer,
                                     pRpc->tmrCtrl);
      return TSDB_CODE_RPC_ALREADY_PROCESSED;
    } else {
      // peer still in processing, give up
      tDebug("%s, server processing takes too long time, give up", pConn->info);
      pHead->code = TSDB_CODE_RPC_TOO_SLOW;
    }
  }

  pConn->outType = 0;
  pConn->pReqMsg = NULL;
  pConn->reqMsgLen = 0;
  SRpcReqContext *pContext = pConn->pContext;

  if (pHead->code == TSDB_CODE_RPC_REDIRECT) { 
    if (rpcContLenFromMsg(pHead->msgLen) < sizeof(SRpcEpSet)) {
      // if EpSet is not included in the msg, treat it as NOT_READY
      pHead->code = TSDB_CODE_RPC_NOT_READY; 
    } else {
      pContext->redirect++;
      if (pContext->redirect > TSDB_MAX_REPLICA) {
        pHead->code = TSDB_CODE_RPC_NETWORK_UNAVAIL; 
        tWarn("%s, too many redirects, quit", pConn->info);
      }
    }
  } 

  return TSDB_CODE_SUCCESS;
}

static SRpcConn *rpcProcessMsgHead(SRpcInfo *pRpc, SRecvInfo *pRecv, SRpcReqContext **ppContext) {
  int32_t    sid;
  SRpcConn  *pConn = NULL;

  SRpcHead *pHead = (SRpcHead *)pRecv->msg;

  sid = htonl(pHead->destId);
  *ppContext = NULL;

  if (pHead->msgType >= TSDB_MSG_TYPE_MAX || pHead->msgType <= 0) {
    tDebug("%s sid:%d, invalid message type:%d", pRpc->label, sid, pHead->msgType);
    terrno = TSDB_CODE_RPC_INVALID_MSG_TYPE; return NULL;
  }

  if (sid < 0 || sid >= pRpc->sessions) {
    tDebug("%s sid:%d, sid is out of range, max sid:%d, %s discarded", pRpc->label, sid,
           pRpc->sessions, taosMsg[pHead->msgType]);
    terrno = TSDB_CODE_RPC_INVALID_SESSION_ID; return NULL;
  }

  if (rpcIsReq(pHead->msgType) && htonl(pHead->msgVer) != tsVersion >> 8) {
    tDebug("%s sid:%d, invalid client version:%x/%x %s", pRpc->label, sid, htonl(pHead->msgVer), tsVersion, taosMsg[pHead->msgType]);
    terrno = TSDB_CODE_RPC_INVALID_VERSION; return NULL;
  }

  pConn = rpcGetConnObj(pRpc, sid, pRecv);
  if (pConn == NULL) {
    tDebug("%s %p, failed to get connection obj(%s)", pRpc->label, (void *)pHead->ahandle, tstrerror(terrno)); 
    return NULL;
  } 

  rpcLockConn(pConn);

  if (rpcIsReq(pHead->msgType)) {
    pConn->ahandle = (void *)pHead->ahandle;
    snprintf(pConn->info, sizeof(pConn->info), "%s %p %p", pRpc->label, pConn, pConn->ahandle);
  }

  sid = pConn->sid;
  if (pConn->chandle == NULL) pConn->chandle = pRecv->chandle;
  pConn->peerIp = pRecv->ip; 
  pConn->peerPort = pRecv->port;
  if (pHead->port) pConn->peerPort = htons(pHead->port); 

  terrno = rpcCheckAuthentication(pConn, (char *)pHead, pRecv->msgLen);

  // code can be transformed only after authentication
  pHead->code = htonl(pHead->code);

  if (terrno == 0) {
    if (pHead->encrypt) {
      // decrypt here
    }

    if ( rpcIsReq(pHead->msgType) ) {
      terrno = rpcProcessReqHead(pConn, pHead);
      pConn->connType = pRecv->connType;

      // stop idle timer
      taosTmrStopA(&pConn->pIdleTimer);  

      // client shall send the request within tsRpcTime again for UDP, double it 
      if (pConn->connType != RPC_CONN_TCPS)
        pConn->pIdleTimer = taosTmrStart([pConn](void *tmrId) { rpcProcessIdleTimer(pConn, tmrId); }, tsRpcTimer * 2,
                                         pRpc->tmrCtrl);
    } else {
      terrno = rpcProcessRspHead(pConn, pHead);
      *ppContext = pConn->pContext;
    }
  }

  rpcUnlockConn(pConn);

  return pConn;
}

static void rpcReportBrokenLinkToServer(SRpcConn *pConn) {
  SRpcInfo *pRpc = pConn->pRpc;
  if (pConn->pReqMsg == NULL) return;

  // if there are pending request, notify the app
  rpcAddRef(pRpc);
  tDebug("%s, notify the server app, connection is gone", pConn->info);

  SRpcMsg rpcMsg;
  rpcMsg.pCont = pConn->pReqMsg;     // pReqMsg is re-used to store the APP context from server
  rpcMsg.contLen = pConn->reqMsgLen; // reqMsgLen is re-used to store the APP context length
  rpcMsg.ahandle = pConn->ahandle;
  rpcMsg.handle = pConn;
  rpcMsg.msgType = pConn->inType;
  rpcMsg.code = TSDB_CODE_RPC_NETWORK_UNAVAIL; 
  pConn->pReqMsg = NULL;
  pConn->reqMsgLen = 0;
  if (pRpc->cfp) (*(pRpc->cfp))(&rpcMsg, NULL);
}

static void rpcProcessBrokenLink(SRpcConn *pConn) {
  if (pConn == NULL) return;
  SRpcInfo *pRpc = pConn->pRpc;
  tDebug("%s, link is broken", pConn->info);

  rpcLockConn(pConn);

  if (pConn->outType) {
    SRpcReqContext *pContext = pConn->pContext;
    pContext->code = TSDB_CODE_RPC_NETWORK_UNAVAIL;
    pContext->pConn = NULL;
    pConn->pReqMsg = NULL;
    taosTmrStart([pContext](void *tmrId) { pContext->processConnError(tmrId); }, 0, pRpc->tmrCtrl);
  }

  if (pConn->inType) rpcReportBrokenLinkToServer(pConn); 

  rpcReleaseConn(pConn);
  rpcUnlockConn(pConn);
}

static void *rpcProcessMsgFromPeer(SRecvInfo *pRecv) {
  SRpcHead  *pHead = (SRpcHead *)pRecv->msg;
  SRpcInfo  *pRpc = (SRpcInfo *)pRecv->shandle;
  SRpcConn  *pConn = (SRpcConn *)pRecv->thandle;

  tDump(pRecv->msg, pRecv->msgLen);

  // underlying UDP layer does not know it is server or client
  pRecv->connType = pRecv->connType | pRpc->connType;  

  if (pRecv->msg == NULL) {
    rpcProcessBrokenLink(pConn);
    return NULL;
  }

  terrno = 0;
  SRpcReqContext *pContext;
  pConn = rpcProcessMsgHead(pRpc, pRecv, &pContext);

  if (pHead->msgType >= 1 && pHead->msgType < TSDB_MSG_TYPE_MAX) {
    tDebug("%s %p %p, %s received from 0x%x:%hu, parse code:0x%x len:%d sig:0x%08x:0x%08x:%d code:0x%x", pRpc->label,
           pConn, (void *)pHead->ahandle, taosMsg[pHead->msgType], pRecv->ip, pRecv->port, terrno, pRecv->msgLen,
           pHead->sourceId, pHead->destId, pHead->tranId, pHead->code);
  } else {
    tDebug("%s %p %p, %d received from 0x%x:%hu, parse code:0x%x len:%d sig:0x%08x:0x%08x:%d code:0x%x", pRpc->label,
           pConn, (void *)pHead->ahandle, pHead->msgType, pRecv->ip, pRecv->port, terrno, pRecv->msgLen,
           pHead->sourceId, pHead->destId, pHead->tranId, pHead->code);
  }

  int32_t code = terrno;
  if (code != TSDB_CODE_RPC_ALREADY_PROCESSED) {
    if (code != 0) { // parsing error
      if (rpcIsReq(pHead->msgType)) {
        rpcSendErrorMsgToPeer(pRecv, code);
        if (code == TSDB_CODE_RPC_INVALID_TIME_STAMP || code == TSDB_CODE_RPC_AUTH_FAILURE) {
          rpcCloseConn(pConn);
        }
        if (pHead->msgType + 1 > 1 && pHead->msgType+1 < TSDB_MSG_TYPE_MAX) {
          tDebug("%s %p %p, %s is sent with error code:0x%x", pRpc->label, pConn, (void *)pHead->ahandle, taosMsg[pHead->msgType+1], code);
        } else {
          tError("%s %p %p, %s is sent with error code:0x%x", pRpc->label, pConn, (void *)pHead->ahandle, taosMsg[pHead->msgType], code);
        }     
      } 
    } else { // msg is passed to app only parsing is ok 
      rpcProcessIncomingMsg(pConn, pHead, pContext);
    }
  }

  if (code) rpcFreeMsg(pRecv->msg); // parsing failed, msg shall be freed
  return pConn;
}

static void rpcNotifyClient(SRpcReqContext *pContext, SRpcMsg *pMsg) {
  SRpcInfo       *pRpc = pContext->pRpc;

  pContext->pConn = NULL;
  if (pContext->pRsp) { 
    // for synchronous API
    memcpy(pContext->pSet, &pContext->epSet, sizeof(SRpcEpSet));
    memcpy(pContext->pRsp, pMsg, sizeof(SRpcMsg));
    tsem_post(pContext->pSem);
  } else {
    // for asynchronous API 
    SRpcEpSet *pEpSet = NULL;
    if (pContext->epSet.inUse != pContext->oldInUse || pContext->redirect) 
      pEpSet = &pContext->epSet;  

    (*pRpc->cfp)(pMsg, pEpSet);  
  }

  // free the request message
  taosRemoveRef(tsRpcRefId, pContext->rid); 
}

static void rpcProcessIncomingMsg(SRpcConn *pConn, SRpcHead *pHead, SRpcReqContext *pContext) {

  SRpcInfo *pRpc = pConn->pRpc;
  SRpcMsg   rpcMsg;

  pHead = rpcDecompressRpcMsg(pHead);
  rpcMsg.contLen = rpcContLenFromMsg(pHead->msgLen);
  rpcMsg.pCont = pHead->content;
  rpcMsg.msgType = pHead->msgType;
  rpcMsg.code = pHead->code; 
   
  if ( rpcIsReq(pHead->msgType) ) {
    rpcMsg.ahandle = pConn->ahandle;
    rpcMsg.handle = pConn;
    rpcAddRef(pRpc);  // add the refCount for requests

    // notify the server app
    (*(pRpc->cfp))(&rpcMsg, NULL);
  } else {
    // it's a response
    rpcMsg.handle = pContext;
    rpcMsg.ahandle = pContext->ahandle;
    pContext->pConn = NULL;

    // for UDP, port may be changed by server, the port in epSet shall be used for cache
    if (pHead->code != TSDB_CODE_RPC_TOO_SLOW) {
      rpcAddConnIntoCache(pRpc->pCache, pConn, pConn->peerFqdn, pContext->epSet.port[pContext->epSet.inUse], pConn->connType);    
    } else {
      rpcCloseConn(pConn);
    }

    if (pHead->code == TSDB_CODE_RPC_REDIRECT) {
      pContext->numOfTry = 0;
      SRpcEpSet *pEpSet = (SRpcEpSet*)pHead->content;
      if (pEpSet->numOfEps > 0) {
        memcpy(&pContext->epSet, pHead->content, sizeof(pContext->epSet));
        tDebug("%s, redirect is received, numOfEps:%d inUse:%d", pConn->info, pContext->epSet.numOfEps,
               pContext->epSet.inUse);
        for (int i = 0; i < pContext->epSet.numOfEps; ++i) {
          pContext->epSet.port[i] = htons(pContext->epSet.port[i]);
          tDebug("%s, redirect is received, index:%d ep:%s:%u", pConn->info, i, pContext->epSet.fqdn[i],
                 pContext->epSet.port[i]);
        }
      }
      rpcSendReqToServer(pRpc, pContext);
      rpcFreeCont(rpcMsg.pCont);
    } else if (pHead->code == TSDB_CODE_RPC_NOT_READY || pHead->code == TSDB_CODE_APP_NOT_READY) {
      pContext->code = pHead->code;
      pContext->processConnError(NULL);
      rpcFreeCont(rpcMsg.pCont);
    } else {
      rpcNotifyClient(pContext, &rpcMsg);
    }
  }
}

static void rpcSendQuickRsp(SRpcConn *pConn, int32_t code) {
  char       msg[RPC_MSG_OVERHEAD];
  SRpcHead  *pHead;

  // set msg header
  memset(msg, 0, sizeof(SRpcHead));
  pHead = (SRpcHead *)msg;
  pHead->version = 1;
  pHead->msgType = pConn->inType+1;
  pHead->spi = pConn->spi;
  pHead->encrypt = 0;
  pHead->tranId = pConn->inTranId;
  pHead->sourceId = pConn->ownId;
  pHead->destId = pConn->peerId;
  pHead->linkUid = pConn->linkUid;
  pHead->ahandle = (uint64_t)pConn->ahandle;
  memcpy(pHead->user, pConn->user, tListLen(pHead->user));
  pHead->code = htonl(code);

  rpcSendMsgToPeer(pConn, msg, sizeof(SRpcHead));
  pConn->secured = 1; // connection shall be secured
}

static void rpcSendReqHead(SRpcConn *pConn) {
  char       msg[RPC_MSG_OVERHEAD];
  SRpcHead  *pHead;

  // set msg header
  memset(msg, 0, sizeof(SRpcHead));
  pHead = (SRpcHead *)msg;
  pHead->version = 1;
  pHead->msgType = pConn->outType;
  pHead->msgVer = htonl(tsVersion >> 8);
  pHead->spi = pConn->spi;
  pHead->encrypt = 0;
  pHead->tranId = pConn->outTranId;
  pHead->sourceId = pConn->ownId;
  pHead->destId = pConn->peerId;
  pHead->linkUid = pConn->linkUid;
  pHead->ahandle = (uint64_t)pConn->ahandle;
  memcpy(pHead->user, pConn->user, tListLen(pHead->user));
  pHead->code = 1;

  rpcSendMsgToPeer(pConn, msg, sizeof(SRpcHead));
}

static void rpcSendErrorMsgToPeer(SRecvInfo *pRecv, int32_t code) {
  SRpcHead  *pRecvHead, *pReplyHead;
  char       msg[sizeof(SRpcHead) + sizeof(SRpcDigest) + sizeof(uint32_t) ];
  uint32_t   timeStamp;
  int        msgLen;

  pRecvHead = (SRpcHead *)pRecv->msg;
  pReplyHead = (SRpcHead *)msg;

  memset(msg, 0, sizeof(SRpcHead));
  pReplyHead->version = pRecvHead->version;
  pReplyHead->msgType = (char)(pRecvHead->msgType + 1);
  pReplyHead->spi = 0;
  pReplyHead->encrypt = pRecvHead->encrypt;
  pReplyHead->tranId = pRecvHead->tranId;
  pReplyHead->sourceId = pRecvHead->destId;
  pReplyHead->destId = pRecvHead->sourceId;
  pReplyHead->linkUid = pRecvHead->linkUid;
  pReplyHead->ahandle = pRecvHead->ahandle;

  pReplyHead->code = htonl(code);
  msgLen = sizeof(SRpcHead);

  if (code == TSDB_CODE_RPC_INVALID_TIME_STAMP) {
    // include a time stamp if client's time is not synchronized well
    uint8_t *pContent = pReplyHead->content;
    timeStamp = htonl(taosGetTimestampSec());
    memcpy(pContent, &timeStamp, sizeof(timeStamp));
    msgLen += sizeof(timeStamp);
  }

  pReplyHead->msgLen = (int32_t)htonl((uint32_t)msgLen);
  (*taosSendData[pRecv->connType])(pRecv->ip, pRecv->port, msg, msgLen, pRecv->chandle);

  return; 
}

static void rpcSendReqToServer(SRpcInfo *pRpc, SRpcReqContext *pContext) {
  SRpcHead  *pHead = rpcHeadFromCont(pContext->pCont);
  char      *msg = (char *)pHead;
  int        msgLen = rpcMsgLenFromCont(pContext->contLen);
  char       msgType = pContext->msgType;

  pContext->numOfTry++;
  SRpcConn *pConn = rpcSetupConnToServer(pContext);
  if (pConn == NULL) {
    pContext->code = terrno;
    taosTmrStart([pContext](void *tmrId) { pContext->processConnError(tmrId); }, 0, pRpc->tmrCtrl);
    return;
  }

  pContext->pConn = pConn;
  pConn->ahandle = pContext->ahandle;
  rpcLockConn(pConn);

  // set the message header  
  pHead->version = 1;
  pHead->msgVer = htonl(tsVersion >> 8);
  pHead->msgType = msgType;
  pHead->encrypt = 0;
  pConn->tranId++;
  if ( pConn->tranId == 0 ) pConn->tranId++;
  pHead->tranId = pConn->tranId;
  pHead->sourceId = pConn->ownId;
  pHead->destId = pConn->peerId;
  pHead->port = 0;
  pHead->linkUid = pConn->linkUid;
  pHead->ahandle = (uint64_t)pConn->ahandle;
  memcpy(pHead->user, pConn->user, tListLen(pHead->user));

  // set the connection parameters
  pConn->outType = msgType;
  pConn->outTranId = pHead->tranId;
  pConn->pReqMsg = msg;
  pConn->reqMsgLen = msgLen;
  pConn->pContext = pContext;

  rpcSendMsgToPeer(pConn, msg, msgLen);
  if (pConn->connType != RPC_CONN_TCPC)
    taosTmrReset([pConn](void *tmrId) { pConn->processRetryTimer(tmrId); }, tsRpcTimer, pRpc->tmrCtrl,
                 &pConn->pTimer);

  rpcUnlockConn(pConn);
}

static void rpcSendMsgToPeer(SRpcConn *pConn, void *msg, int msgLen) {
  int        writtenLen = 0;
  SRpcHead  *pHead = (SRpcHead *)msg;

  msgLen = rpcAddAuthPart(pConn, (char *)msg, msgLen);

  if ( rpcIsReq(pHead->msgType)) {
    tDebug("%s, %s is sent to %s:%hu, len:%d sig:0x%08x:0x%08x:%d",
           pConn->info, taosMsg[pHead->msgType], pConn->peerFqdn, pConn->peerPort, 
           msgLen, pHead->sourceId, pHead->destId, pHead->tranId);
  } else {
    if (pHead->code == 0) pConn->secured = 1; // for success response, set link as secured
    tDebug("%s, %s is sent to 0x%x:%hu, code:0x%x len:%d sig:0x%08x:0x%08x:%d",
           pConn->info, taosMsg[pHead->msgType], pConn->peerIp, pConn->peerPort, 
           htonl(pHead->code), msgLen, pHead->sourceId, pHead->destId, pHead->tranId);
  }

  //tTrace("connection type is: %d", pConn->connType);
  writtenLen = (*taosSendData[pConn->connType])(pConn->peerIp, pConn->peerPort, pHead, msgLen, pConn->chandle);

  if (writtenLen != msgLen) {
    tError("%s, failed to send, msgLen:%d written:%d, reason:%s", pConn->info, msgLen, writtenLen, strerror(errno));
  }
 
  tDump(msg, msgLen);
}

void SRpcReqContext::processConnError(void *id) {
  SRpcMsg         rpcMsg;
 
  if (pRpc == NULL) {
    return;
  }
  
  tDebug("%s %p, connection error happens", pRpc->label, ahandle);

  if (numOfTry >= epSet.numOfEps) {
    rpcMsg.msgType = msgType+1;
    rpcMsg.ahandle = ahandle;
    rpcMsg.code = code;
    rpcMsg.pCont = NULL;
    rpcMsg.contLen = 0;

    rpcNotifyClient(this, &rpcMsg);
  } else {
    // move to next IP 
    epSet.inUse++;
    epSet.inUse = epSet.inUse % epSet.numOfEps;
    rpcSendReqToServer(pRpc, this);
  }
}

void SRpcConn::processRetryTimer(void *tmrId) {
  rpcLockConn(this);

  if (outType && user[0]) {
    tDebug("%s, expected %s is not received", info, taosMsg[(int)outType + 1]);
    pTimer = NULL;
    retry++;

    if (retry < 4) {
      tDebug("%s, re-send msg:%s to %s:%hu", info, taosMsg[outType], peerFqdn, peerPort);
      rpcSendMsgToPeer(this, pReqMsg, reqMsgLen);      
      pTimer = taosTmrStart([this](void *tmrId) { processRetryTimer(tmrId); }, tsRpcTimer, pRpc->tmrCtrl);
    } else {
      // close the connection
      tDebug("%s, failed to send msg:%s to %s:%hu", info, taosMsg[outType], peerFqdn, peerPort);
      if (pContext) {
        pContext->code = TSDB_CODE_RPC_NETWORK_UNAVAIL;
        pContext->pConn = NULL;
        pReqMsg = NULL;
        taosTmrStart([this](void *tmrId) { pContext->processConnError(tmrId); }, 1,
                     pRpc->tmrCtrl);
        rpcReleaseConn(this);
      }
    }
  } else {
    tDebug("%s, retry timer not processed", info);
  }

  rpcUnlockConn(this);
}

static void rpcProcessIdleTimer(SRpcConn *pConn, void *tmrId) {
  rpcLockConn(pConn);

  if (pConn->user[0]) {
    tDebug("%s, close the connection since no activity", pConn->info);
    if (pConn->inType) rpcReportBrokenLinkToServer(pConn); 
    rpcReleaseConn(pConn);
  } else {
    tDebug("%s, idle timer:%p not processed", pConn->info, tmrId);
  }

  rpcUnlockConn(pConn);
}

static void rpcProcessProgressTimer(SRpcConn *pConn, void *tmrId) {
  SRpcInfo *pRpc = pConn->pRpc;

  rpcLockConn(pConn);

  if (pConn->inType && pConn->user[0]) {
    tDebug("%s, progress timer expired, send progress", pConn->info);
    rpcSendQuickRsp(pConn, TSDB_CODE_RPC_ACTION_IN_PROGRESS);
    pConn->pTimer =
        taosTmrStart([pConn](void *tmrId) { rpcProcessProgressTimer(pConn, tmrId); }, tsProgressTimer, pRpc->tmrCtrl);
  } else {
    tDebug("%s, progress timer:%p not processed", pConn->info, tmrId);
  }

  rpcUnlockConn(pConn);
}

static int32_t rpcCompressRpcMsg(char* pCont, int32_t contLen) {
  SRpcHead  *pHead = rpcHeadFromCont(pCont);
  int32_t    finalLen = 0;
  int        overhead = sizeof(SRpcComp);
  
  if (!NEEDTO_COMPRESSS_MSG(contLen)) {
    return contLen;
  }
  
  char *buf = (char *)malloc (contLen + overhead + 8);  // 8 extra bytes
  if (buf == NULL) {
    tError("failed to allocate memory for rpc msg compression, contLen:%d", contLen);
    return contLen;
  }
  
  int32_t compLen = LZ4_compress_default(pCont, buf, contLen, contLen + overhead);
  tDebug("compress rpc msg, before:%d, after:%d, overhead:%d", contLen, compLen, overhead);
  
  /*
   * only the compressed size is less than the value of contLen - overhead, the compression is applied
   * The first four bytes is set to 0, the second four bytes are utilized to keep the original length of message
   */
  if (compLen < contLen - overhead) {
    SRpcComp *pComp = (SRpcComp *)pCont;
    pComp->reserved = 0; 
    pComp->contLen = htonl(contLen); 
    memcpy(pCont + overhead, buf, compLen);
    
    pHead->comp = 1;
    tDebug("compress rpc msg, before:%d, after:%d", contLen, compLen);
    finalLen = compLen + overhead;
  } else {
    finalLen = contLen;
  }

  free(buf);
  return finalLen;
}

static SRpcHead *rpcDecompressRpcMsg(SRpcHead *pHead) {
  int overhead = sizeof(SRpcComp);
  SRpcHead   *pNewHead = NULL;  
  uint8_t    *pCont = pHead->content;
  SRpcComp   *pComp = (SRpcComp *)pHead->content;

  if (pHead->comp) {
    // decompress the content
    assert(pComp->reserved == 0);
    int contLen = htonl(pComp->contLen);
  
    // prepare the temporary buffer to decompress message
    char *temp = (char *)malloc(contLen + RPC_MSG_OVERHEAD);
    pNewHead = (SRpcHead *)(temp + sizeof(SRpcReqContext)); // reserve SRpcReqContext
  
    if (pNewHead) {
      int compLen = rpcContLenFromMsg(pHead->msgLen) - overhead;
      int origLen = LZ4_decompress_safe((char*)(pCont + overhead), (char *)pNewHead->content, compLen, contLen);
      assert(origLen == contLen);
    
      memcpy(pNewHead, pHead, sizeof(SRpcHead));
      pNewHead->msgLen = rpcMsgLenFromCont(origLen);
      rpcFreeMsg(pHead); // free the compressed message buffer
      pHead = pNewHead; 
      tTrace("decomp malloc mem:%p", temp);
    } else {
      tError("failed to allocate memory to decompress msg, contLen:%d", contLen);
    }
  }

  return pHead;
}

static int rpcAuthenticateMsg(void *pMsg, int msgLen, void *pAuth, void *pKey) {
  MD5_CTX context;
  int     ret = -1;

  MD5Init(&context);
  MD5Update(&context, (uint8_t *)pKey, TSDB_KEY_LEN);
  MD5Update(&context, (uint8_t *)pMsg, msgLen);
  MD5Update(&context, (uint8_t *)pKey, TSDB_KEY_LEN);
  MD5Final(&context);

  if (memcmp(context.digest, pAuth, sizeof(context.digest)) == 0) ret = 0;

  return ret;
}

static void rpcBuildAuthHead(void *pMsg, int msgLen, void *pAuth, void *pKey) {
  MD5_CTX context;

  MD5Init(&context);
  MD5Update(&context, (uint8_t *)pKey, TSDB_KEY_LEN);
  MD5Update(&context, (uint8_t *)pMsg, msgLen);
  MD5Update(&context, (uint8_t *)pKey, TSDB_KEY_LEN);
  MD5Final(&context);

  memcpy(pAuth, context.digest, sizeof(context.digest));
}

static int rpcAddAuthPart(SRpcConn *pConn, char *msg, int msgLen) {
  SRpcHead *pHead = (SRpcHead *)msg;

  if (pConn->spi && pConn->secured == 0) {
    // add auth part
    pHead->spi = pConn->spi;
    SRpcDigest *pDigest = (SRpcDigest *)(msg + msgLen);
    pDigest->timeStamp = htonl(taosGetTimestampSec());
    msgLen += sizeof(SRpcDigest);
    pHead->msgLen = (int32_t)htonl((uint32_t)msgLen);
    rpcBuildAuthHead(pHead, msgLen - TSDB_AUTH_LEN, pDigest->auth, pConn->secret);
  } else {
    pHead->spi = 0;
    pHead->msgLen = (int32_t)htonl((uint32_t)msgLen);
  }

  return msgLen;
}

static int rpcCheckAuthentication(SRpcConn *pConn, char *msg, int msgLen) {
  SRpcHead *pHead = (SRpcHead *)msg;
  int       code = 0;

  if ((pConn->secured && pHead->spi == 0) || (pHead->spi == 0 && pConn->spi == 0)){
    // secured link, or no authentication 
    pHead->msgLen = (int32_t)htonl((uint32_t)pHead->msgLen);
    // tTrace("%s, secured link, no auth is required", pConn->info);
    return 0;
  }

  if ( !rpcIsReq(pHead->msgType) ) {
    // for response, if code is auth failure, it shall bypass the auth process
    code = htonl(pHead->code);
    if (code == TSDB_CODE_RPC_INVALID_TIME_STAMP || code == TSDB_CODE_RPC_AUTH_FAILURE ||
        code == TSDB_CODE_RPC_INVALID_VERSION ||
        code == TSDB_CODE_RPC_AUTH_REQUIRED || code == TSDB_CODE_MND_INVALID_USER || code == TSDB_CODE_RPC_NOT_READY) {
      pHead->msgLen = (int32_t)htonl((uint32_t)pHead->msgLen);
      // tTrace("%s, dont check authentication since code is:0x%x", pConn->info, code);
      return 0;
    }
  }
 
  code = 0;
  if (pHead->spi == pConn->spi) {
    // authentication
    SRpcDigest *pDigest = (SRpcDigest *)((char *)pHead + msgLen - sizeof(SRpcDigest));

    int32_t delta;
    delta = (int32_t)htonl(pDigest->timeStamp);
    delta -= (int32_t)taosGetTimestampSec();
    if (abs(delta) > 900) {
      tWarn("%s, time diff:%d is too big, msg discarded", pConn->info, delta);
      code = TSDB_CODE_RPC_INVALID_TIME_STAMP;
    } else {
      if (rpcAuthenticateMsg(pHead, msgLen-TSDB_AUTH_LEN, pDigest->auth, pConn->secret) < 0) {
        tDebug("%s, authentication failed, msg discarded", pConn->info);
        code = TSDB_CODE_RPC_AUTH_FAILURE;
      } else {
        pHead->msgLen = (int32_t)htonl((uint32_t)pHead->msgLen) - sizeof(SRpcDigest);
        if ( !rpcIsReq(pHead->msgType) ) pConn->secured = 1;  // link is secured for client
        // tTrace("%s, message is authenticated", pConn->info);
      }
    }
  } else {
    tDebug("%s, auth spi:%d not matched with received:%d", pConn->info, pConn->spi, pHead->spi);
    code = pHead->spi ? TSDB_CODE_RPC_AUTH_FAILURE : TSDB_CODE_RPC_AUTH_REQUIRED;
  }

  return code;
}

static void rpcLockConn(SRpcConn *pConn) {
  int64_t tid = taosGetSelfPthreadId();
  int     i = 0;
  while (atomic_val_compare_exchange_64(&(pConn->lockedBy), 0, tid) != 0) {
    if (++i % 1000 == 0) {
      sched_yield();
    }
  }
}

static void rpcUnlockConn(SRpcConn *pConn) {
  int64_t tid = taosGetSelfPthreadId();
  if (atomic_val_compare_exchange_64(&(pConn->lockedBy), tid, 0) != tid) {
    assert(false);
  }
}

static void rpcAddRef(SRpcInfo *pRpc)
{  
   atomic_add_fetch_32(&pRpc->refCount, 1);
}

static void rpcDecRef(SRpcInfo *pRpc)
{ 
  if (atomic_sub_fetch_32(&pRpc->refCount, 1) == 0) {
    rpcCloseConnCache(pRpc->pCache);
    taosHashCleanup(pRpc->hash);
    taosTmrCleanUp(pRpc->tmrCtrl);
    tDebug("%s rpc resources are released", pRpc->label);
    delete pRpc;
  }
}

