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
#ifndef TDENGINE_TRPC_H
#define TDENGINE_TRPC_H

#include <stdint.h>
#include <atomic>
#include <memory>
#include <vector>
#include "taosdef.h"
#include "tidpool.h"
#include "os.h"

#define TAOS_CONN_SERVER   0
#define TAOS_CONN_CLIENT   1

extern int tsRpcHeadSize;

typedef struct SRpcEpSet {
  int8_t    inUse; 
  int8_t    numOfEps;
  uint16_t  port[TSDB_MAX_REPLICA];
  char      fqdn[TSDB_MAX_REPLICA][TSDB_FQDN_LEN];
} SRpcEpSet;

typedef struct SRpcCorEpSet {
  int32_t version; 
  SRpcEpSet epSet; 
} SRpcCorEpSet;

typedef struct SRpcConnInfo {
  uint32_t  clientIp;
  uint16_t  clientPort;
  uint32_t  serverIp;
  char      user[TSDB_USER_LEN];
} SRpcConnInfo;

typedef struct SRpcMsg {
  uint8_t msgType;
  void   *pCont;
  int     contLen;
  int32_t code;
  void   *handle;   // rpc handle returned to app
  void   *ahandle;  // app handle set by client
} SRpcMsg;

typedef struct SRpcInit {
  uint16_t localPort; // local port
  char  *label;        // for debug purpose
  int    numOfThreads; // number of threads to handle connections
  int    sessions;     // number of sessions allowed
  int8_t connType;     // TAOS_CONN_UDP, TAOS_CONN_TCPC, TAOS_CONN_TCPS
  int    idleTime;     // milliseconds, 0 means idle timer is disabled

  // the following is for client app ecurity only
  char *user;         // user name
  char  spi;          // security parameter index
  char  encrypt;      // encrypt algorithm
  char *secret;       // key for authentication
  char *ckey;         // ciphering key

  // call back to process incoming msg, code shall be ignored by server app
  void (*cfp)(SRpcMsg *, SRpcEpSet *);  

  // call back to retrieve the client auth info, for server app only 
  int  (*afp)(char *tableId, char *spi, char *encrypt, char *secret, char *ckey);
} SRpcInit;

struct SRpcInfo;

struct SRpcReqContext {
  SRpcInfo *       pRpc;      // associated SRpcInfo
  SRpcEpSet        epSet;     // ip list provided by app
  void *           ahandle;   // handle provided by app
  struct SRpcConn *pConn;     // pConn allocated
  char             msgType;   // message type
  uint8_t *        pCont;     // content provided by app
  int32_t          contLen;   // content length
  int32_t          code;      // error code
  int16_t          numOfTry;  // number of try for different servers
  int8_t           oldInUse;  // server EP inUse passed by app
  int8_t           redirect;  // flag to indicate redirect
  int8_t           connType;  // connection type
  int64_t          rid;       // refId returned by taosAddRef
  SRpcMsg *        pRsp;      // for synchronous API
  tsem_t *         pSem;      // for synchronous API
  SRpcEpSet *      pSet;      // for synchronous API
  char             msg[0];    // RpcHead starts from here
};

struct SRpcConn {
  char            info[48];                 // debug info: label + pConn + ahandle
  int             sid;                      // session ID
  uint32_t        ownId;                    // own link ID
  uint32_t        peerId;                   // peer link ID
  char            user[TSDB_UNI_LEN];       // user ID for the link
  char            spi;                      // security parameter index
  char            encrypt;                  // encryption, 0:1
  char            secret[TSDB_KEY_LEN];     // secret for the link
  char            ckey[TSDB_KEY_LEN];       // ciphering key
  char            secured;                  // if set to 1, no authentication
  uint16_t        localPort;                // for UDP only
  uint32_t        linkUid;                  // connection unique ID assigned by client
  uint32_t        peerIp;                   // peer IP
  uint16_t        peerPort;                 // peer port
  char            peerFqdn[TSDB_FQDN_LEN];  // peer FQDN or ip string
  uint16_t        tranId;                   // outgoing transcation ID, for build message
  uint16_t        outTranId;                // outgoing transcation ID
  uint16_t        inTranId;                 // transcation ID for incoming msg
  uint8_t         outType;                  // message type for outgoing request
  uint8_t         inType;                   // message type for incoming request
  void *          chandle;                  // handle passed by TCP/UDP connection layer
  void *          ahandle;                  // handle provided by upper app layter
  int             retry;                    // number of retry for sending request
  int             tretry;                   // total retry
  void *          pTimer;                   // retry timer to monitor the response
  void *          pIdleTimer;               // idle timer
  char *          pRspMsg;                  // response message including header
  int             rspMsgLen;                // response messag length
  char *          pReqMsg;                  // request message including header
  int             reqMsgLen;                // request message length
  SRpcInfo *      pRpc;                     // the associated SRpcInfo
  int8_t          connType;                 // connection type
  int64_t         lockedBy;                 // lock for connection
  SRpcReqContext *pContext;                 // request context
};

struct SHashObj;

struct SRpcInfo {
  int      sessions;      // number of sessions allowed
  int      numOfThreads;  // number of threads to process incoming messages
  int      idleTime;      // milliseconds;
  uint16_t localPort;
  int8_t   connType;
  int      index;  // for UDP server only, round robin for multiple threads
  char     label[TSDB_LABEL_LEN];

  char user[TSDB_UNI_LEN];    // meter ID
  char spi;                   // security parameter index
  char encrypt;               // encrypt algorithm
  char secret[TSDB_KEY_LEN];  // secret for the link
  char ckey[TSDB_KEY_LEN];    // ciphering key

  void (*cfp)(SRpcMsg *, SRpcEpSet *);
  int (*afp)(char *user, char *spi, char *encrypt, char *secret, char *ckey);

  int32_t                     refCount;
  std::unique_ptr<id_pool_t>  idPool;     // handle to ID pool
  void *                      tmrCtrl;    // handle to timer
  SHashObj *                  hash;       // handle returned by hash utility
  void *                      tcphandle;  // returned handle from TCP initialization
  void *                      udphandle;  // returned handle from UDP initialization
  void *                      pCache;     // connection cache
  std::mutex                  mutex;
  std::vector<SRpcConn>       connList;  // connection list
  static std::atomic<int32_t> tsRpcNum;

 public:
  SRpcInfo();
  ~SRpcInfo();
};

int32_t rpcInit();
void  rpcCleanup();
SRpcInfo *rpcOpen(const SRpcInit &init);
void  rpcClose(void *);
void *rpcMallocCont(int contLen);
void  rpcFreeCont(void *pCont);
void *rpcReallocCont(void *ptr, int contLen);
void    rpcSendRequest(SRpcInfo *pRpc, const SRpcEpSet *pEpSet, SRpcMsg *pMsg, int64_t *rid);
void  rpcSendResponse(const SRpcMsg *pMsg);
void  rpcSendRedirectRsp(void *pConn, const SRpcEpSet *pEpSet); 
int   rpcGetConnInfo(void *thandle, SRpcConnInfo *pInfo);
void    rpcSendRecv(SRpcInfo *pRpc, SRpcEpSet *pEpSet, SRpcMsg *pReq, SRpcMsg *pRsp);
int   rpcReportProgress(void *pConn, char *pCont, int contLen);
void  rpcCancelRequest(int64_t rid);

#endif  // TDENGINE_TRPC_H
