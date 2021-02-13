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

#ifndef TDENGINE_SYNC_INT_H
#define TDENGINE_SYNC_INT_H

#include <functional>
#include <memory>
#include <mutex>
#include <vector>
#include "syncMsg.h"
#include "walInt.h"

#define sFatal(...) { if (sDebugFlag & DEBUG_FATAL) { taosPrintLog("SYN FATAL ", sDebugFlag, __VA_ARGS__); }}
#define sError(...) { if (sDebugFlag & DEBUG_ERROR) { taosPrintLog("SYN ERROR ", sDebugFlag, __VA_ARGS__); }}
#define sWarn(...)  { if (sDebugFlag & DEBUG_WARN)  { taosPrintLog("SYN WARN ", sDebugFlag, __VA_ARGS__); }}
#define sInfo(...)  { if (sDebugFlag & DEBUG_INFO)  { taosPrintLog("SYN ", sDebugFlag, __VA_ARGS__); }}
#define sDebug(...) { if (sDebugFlag & DEBUG_DEBUG) { taosPrintLog("SYN ", sDebugFlag, __VA_ARGS__); }}
#define sTrace(...) { if (sDebugFlag & DEBUG_TRACE) { taosPrintLog("SYN ", sDebugFlag, __VA_ARGS__); }}

#define SYNC_TCP_THREADS 2
#define SYNC_MAX_NUM 2

#define SYNC_MAX_SIZE (TSDB_MAX_WAL_SIZE + sizeof(SWalHead) + sizeof(SSyncHead) + 16)
static constexpr size_t SYNC_RECV_BUFFER_SIZE = 5 * 1024 * 1024;

#define SYNC_MAX_FWDS 512
#define SYNC_FWD_TIMER 300
#define SYNC_ROLE_TIMER 15000             // ms
#define SYNC_CHECK_INTERVAL 1000          // ms
#define SYNC_WAIT_AFTER_CHOOSE_MASTER 10  // ms

struct SRecvBuffer {
  std::vector<char> buffer;
  char *  offset;
  int32_t forwards = 0;
  int32_t code = 0;
  SRecvBuffer() : buffer(SYNC_RECV_BUFFER_SIZE), offset(buffer.data()) {}
  ~SRecvBuffer() = default;
};

typedef struct {
  uint64_t  version;
  void     *mhandle;
  int8_t    acks;
  int8_t    nacks;
  int8_t    confirmed;
  int32_t   code;
  int64_t   time;
} SFwdInfo;

struct SSyncFwds {
  int32_t  first = 0;
  int32_t  last = 0;
  int32_t  fwds = 0;  // number of forwards
  std::vector<SFwdInfo> fwdInfo;
 public:
  SSyncFwds(size_t infos) : fwdInfo(infos) {}
};

struct SConnObj;
struct SSyncNode;

struct SSyncPeer : public std::enable_shared_from_this<SSyncPeer> {
  int32_t  nodeId;
  uint32_t ip;
  uint16_t port;
  int8_t   role;
  int8_t   sstatus;               // sync status
  char     fqdn[TSDB_FQDN_LEN];   // peer ip string
  char     id[TSDB_EP_LEN + 32];  // peer vgId + end point
  uint64_t version;
  uint64_t sversion;        // track the peer version in retrieve process
  uint64_t lastFileVer;     // track the file version while retrieve
  uint64_t lastWalVer;      // track the wal version while retrieve
  int32_t  syncFd;
  int32_t  peerFd;          // forward FD
  int32_t  numOfRetrieves;  // number of retrieves tried
  int32_t  fileChanged;     // a flag to indicate file is changed during retrieving process
  int8_t   isArb;
  void *   timer;
  SConnObj*        pConn;
  std::weak_ptr<SSyncNode> pSyncNode;

 public:
  ~SSyncPeer();
  void recoverFromMaster();
  void closeConn();
  void restart();
};
using SSyncPeerPtr = std::shared_ptr<SSyncPeer>;

struct SSyncNode : public std::enable_shared_from_this<SSyncNode> {
  char         path[TSDB_FILENAME_LEN];
  int8_t       replica;
  int8_t       quorum;
  int8_t       selfIndex;
  uint32_t     vgId;
  SSyncPeerPtr peerInfo[TAOS_SYNC_MAX_REPLICA + 1];  // extra one for arbitrator
  SSyncPeerPtr pMaster;
  std::unique_ptr<SRecvBuffer> pRecv;
  std::unique_ptr<SSyncFwds>  pSyncFwds;  // saved forward info if quorum >1
  void *       pFwdTimer;
  void *       pRoleTimer;
  FGetFileInfo      getFileInfo;
  FGetWalInfo       getWalInfo;
  FWriteToCache     writeToCache;
  FConfirmForward   confirmForward;
  FNotifyRole       notifyRole;
  FNotifyFlowCtrl   notifyFlowCtrl;
  FNotifyFileSynced notifyFileSynced;
  FGetVersion       getVersion;
  std::mutex        mutex;

 protected:
  int32_t save(uint64_t version, void *mhandle) const;
 public:
  ~SSyncNode();
  void process(SFwdInfo *pFwdInfo, int32_t code) const;
  int8_t getRole() const { return peerInfo[selfIndex]->role; }
  void setRole(int8_t newRole) { peerInfo[selfIndex]->role = newRole; }
  uint64_t getVer() const { return peerInfo[selfIndex]->version; }
  void setVer(uint64_t newVersion) { peerInfo[selfIndex]->version = newVersion; }
  int8_t   getStatus() const { return peerInfo[selfIndex]->sstatus; }
  void     setStatus(int8_t newStatus) { peerInfo[selfIndex]->sstatus = newStatus; }
  int32_t  forwardToPeerImpl(void *data, void *mhandle, int32_t qtype);
  void     removeConfirmedFwdInfo();
  void     monitorFwdInfos(void *tmrId);
  void     monitorNodeRole(void *tmrId);
};

using SSyncNodePtr = std::shared_ptr<SSyncNode>;

typedef struct SThreadObj {
  pthread_t        thread;
  bool             stop;
  int32_t          pollFd;
  int32_t          numOfFds;
  struct SPoolObj *pPool;
} SThreadObj;

struct SPoolInfo {
  int32_t  numOfThreads;
  uint32_t serverIp;
  int16_t  port;
  int32_t  bufferSize;
  void (*processIncomingConn)(int32_t fd, uint32_t ip);
};

typedef struct SPoolObj {
  SPoolInfo    info;
  SThreadObj **pThread;
  pthread_t    thread;
  int32_t      nextId;
  int32_t      acceptFd;  // FD for accept new connection
} SPoolObj;

struct SConnObj {
  SThreadObj *                   pThread;
  int32_t                        fd;
  int32_t                        closedByApp;
  std::function<void(void)>      processBrokenLink;
  std::function<int32_t(void *)> processIncomingMsg;
};

// sync module global
extern int32_t tsSyncNum;
extern char    tsNodeFqdn[TSDB_FQDN_LEN];
extern const char *  syncStatus[];

void       syncRetrieveData(SSyncPeerPtr pPeer);
void       syncRestoreData(SSyncPeerPtr rid);
int32_t    syncSaveIntoBuffer(SSyncPeerPtr pPeer, SWalHead *pHead);
void       syncRestartConnection(SSyncPeerPtr pPeer);
void       syncBroadcastStatus(SSyncNodePtr pNode);

SSyncNodePtr syncStart(const SSyncInfo *);
void         syncStop(SSyncNodePtr pNode);
int32_t      syncReconfig(SSyncNodePtr pNode, const SSyncCfg *);
int32_t      syncForwardToPeer(SSyncNodePtr pNode, void *pHead, void *mhandle, int32_t qtype);
void         syncConfirmForward(SSyncNodePtr pNode, uint64_t version, int32_t code);
int32_t      syncGetNodesRole(SSyncNodePtr pNode, SNodesRole *);
#endif  // TDENGINE_VNODEPEER_H
