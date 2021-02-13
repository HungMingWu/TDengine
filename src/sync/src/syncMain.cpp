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

#include <thread>
#include "os.h"
#include "hash.h"
#include "tlog.h"
#include "tutil.h"
#include "ttimer.h"
#include "tref.h"
#include "tsocket.h"
#include "tglobal.h"
#include "taoserror.h"
#include "tqueue.h"
#include "tsync.h"
#include "syncTcp.h"
#include "syncInt.h"

int32_t tsSyncNum = 0;  // number of sync in process in whole system
char    tsNodeFqdn[TSDB_FQDN_LEN] = {0};

static void *  tsTcpPool = NULL;
static void *  tsSyncTmrCtrl = NULL;
static SHashObj *  tsVgIdHash = nullptr;

// local functions
static void    syncProcessSyncRequest(char *pMsg, SSyncPeerPtr pPeer);
static void    syncCheckPeerConnection(SSyncPeerPtr pPeer, void *tmrId);
static int32_t syncSendPeersStatusMsgToPeer(SSyncPeerPtr pPeer, char ack, int8_t type, uint16_t tranId);
static void    syncProcessBrokenLink(SSyncPeerPtr pPeer);
static int32_t syncProcessPeerMsg(SSyncPeerPtr pPeer, void *buffer);
static void    syncProcessIncommingConnection(int32_t connFd, uint32_t sourceIp);
static void    syncAddArbitrator(SSyncNodePtr pNode);
static void    syncMonitorFwdInfos(SSyncNodePtr pNode, void *tmrId);
static void    syncMonitorNodeRole(SSyncNodePtr pNode, void *tmrId);

static SSyncPeerPtr syncAddPeer(SSyncNodePtr pNode, const SNodeInfo *pInfo);
static void       syncStartCheckPeerConn(SSyncPeerPtr pPeer);
static void       syncStopCheckPeerConn(SSyncPeerPtr pPeer);

const char* syncRole[] = {
  "offline",
  "unsynced",
  "syncing",
  "slave",
  "master"
};

const char *syncStatus[] = {
  "init",
  "start",
  "file",
  "cache",
  "invalid"
};

int32_t syncInit() {
  SPoolInfo info = {0};

  info.numOfThreads = SYNC_TCP_THREADS;
  info.serverIp = 0;
  info.port = tsSyncPort;
  info.bufferSize = SYNC_MAX_SIZE;

  tsTcpPool = syncOpenTcpThreadPool(&info);
  if (tsTcpPool == NULL) {
    sError("failed to init tcpPool");
    syncCleanUp();
    return -1;
  }

  tsSyncTmrCtrl = taosTmrInit(1000, 50, 10000, "SYNC");
  if (tsSyncTmrCtrl == NULL) {
    sError("failed to init tmrCtrl");
    syncCleanUp();
    return -1;
  }

  tsVgIdHash = taosHashInit(TSDB_MIN_VNODES, taosGetDefaultHashFunction(TSDB_DATA_TYPE_INT), true, HASH_ENTRY_LOCK);
  if (tsVgIdHash == NULL) {
    sError("failed to init vgIdHash");
    syncCleanUp();
    return -1;
  }

  tstrncpy(tsNodeFqdn, tsLocalFqdn, sizeof(tsNodeFqdn));
  sInfo("sync module initialized successfully");

  return 0;
}

void syncCleanUp() {
  if (tsTcpPool != NULL) {
    syncCloseTcpThreadPool(tsTcpPool);
    tsTcpPool = NULL;
  }

  if (tsSyncTmrCtrl != NULL) {
    taosTmrCleanUp(tsSyncTmrCtrl);
    tsSyncTmrCtrl = NULL;
  }

  if (tsVgIdHash) {
    taosHashCleanup(tsVgIdHash);
    tsVgIdHash = NULL;
  }

  sInfo("sync module is cleaned up");
}

SSyncNodePtr syncStart(const SSyncInfo *pInfo) {
  const SSyncCfg *pCfg = &pInfo->syncCfg;

  auto pNode = std::make_shared<SSyncNode>();
  tstrncpy(pNode->path, pInfo->path, sizeof(pNode->path));

  pNode->getFileInfo = pInfo->getFileInfo;
  pNode->getWalInfo = pInfo->getWalInfo;
  pNode->writeToCache = pInfo->writeToCache;
  pNode->notifyRole = pInfo->notifyRole;
  pNode->confirmForward = pInfo->confirmForward;
  pNode->notifyFlowCtrl = pInfo->notifyFlowCtrl;
  pNode->notifyFileSynced = pInfo->notifyFileSynced;
  pNode->getVersion = pInfo->getVersion;

  pNode->selfIndex = -1;
  pNode->vgId = pInfo->vgId;
  pNode->replica = pCfg->replica;
  pNode->quorum = pCfg->quorum;
  if (pNode->quorum > pNode->replica) pNode->quorum = pNode->replica;

  for (int32_t index = 0; index < pCfg->replica; ++index) {
    const SNodeInfo *pNodeInfo = pCfg->nodeInfo + index;
    pNode->peerInfo[index] = syncAddPeer(pNode, pNodeInfo);
    if (pNode->peerInfo[index] == NULL) {
      sError("vgId:%d, node:%d fqdn:%s port:%u is not configured, stop taosd", pNode->vgId, pNodeInfo->nodeId,
             pNodeInfo->nodeFqdn, pNodeInfo->nodePort);
      syncStop(pNode);
      exit(1);
    }

    if ((strcmp(pNodeInfo->nodeFqdn, tsNodeFqdn) == 0) && (pNodeInfo->nodePort == tsSyncPort)) {
      pNode->selfIndex = index;
    }
  }

  if (pNode->selfIndex < 0) {
    sError("vgId:%d, this node is not configured", pNode->vgId);
    terrno = TSDB_CODE_SYN_INVALID_CONFIG;
    syncStop(pNode);
    return SSyncNodePtr();
  }

  pNode->setVer(pInfo->version);  // set the initial version
  pNode->setRole((pNode->replica > 1) ? TAOS_SYNC_ROLE_UNSYNCED : TAOS_SYNC_ROLE_MASTER);
  sInfo("vgId:%d, %d replicas are configured, quorum:%d role:%s", pNode->vgId, pNode->replica, pNode->quorum,
        syncRole[pNode->getRole()]);

  pNode->pSyncFwds.reset(new SSyncFwds(SYNC_MAX_FWDS));
  pNode->pFwdTimer = taosTmrStart([pNode](void *tmrId) { syncMonitorFwdInfos(pNode, tmrId); }, SYNC_FWD_TIMER,
                                   tsSyncTmrCtrl);
  if (pNode->pFwdTimer == NULL) {
    sError("vgId:%d, failed to allocate fwd timer", pNode->vgId);
    syncStop(pNode);
    return SSyncNodePtr();
  }

  pNode->pRoleTimer = taosTmrStart([pNode](void *tmrId) { syncMonitorNodeRole(pNode, tmrId); }, SYNC_ROLE_TIMER,
                                   tsSyncTmrCtrl);
  if (pNode->pRoleTimer == NULL) {
    sError("vgId:%d, failed to allocate role timer", pNode->vgId);
    syncStop(pNode);
    return SSyncNodePtr();
  }

  syncAddArbitrator(pNode);
  taosHashPut(tsVgIdHash, &pNode->vgId, sizeof(int32_t), &pNode, sizeof(SSyncNode *));

  if (pNode->notifyRole) {
    (*pNode->notifyRole)(pNode->vgId, pNode->getRole());
  }

  syncStartCheckPeerConn(pNode->peerInfo[TAOS_SYNC_MAX_REPLICA]);  // arb
  for (int32_t index = 0; index < pNode->replica; ++index) {
    syncStartCheckPeerConn(pNode->peerInfo[index]);
  }

  return pNode;
}

void syncStop(SSyncNodePtr pNode) {
  sInfo("vgId:%d, cleanup sync", pNode->vgId);

  pNode->mutex.lock();

  if (tsVgIdHash) taosHashRemove(tsVgIdHash, &pNode->vgId, sizeof(int32_t));
  if (pNode->pFwdTimer) taosTmrStop(pNode->pFwdTimer);
  if (pNode->pRoleTimer) taosTmrStop(pNode->pRoleTimer);

  for (int32_t index = 0; index < pNode->replica; ++index)
    pNode->peerInfo[index].reset();

  pNode->peerInfo[TAOS_SYNC_MAX_REPLICA].reset();

  pNode->mutex.unlock();
}

int32_t syncReconfig(SSyncNodePtr pNode, const SSyncCfg *pNewCfg) {
  int32_t i, j;

  sInfo("vgId:%d, reconfig, role:%s replica:%d old:%d", pNode->vgId, syncRole[pNode->getRole()], pNewCfg->replica,
        pNode->replica);

  pNode->mutex.lock();

  syncStopCheckPeerConn(pNode->peerInfo[TAOS_SYNC_MAX_REPLICA]);  // arb
  for (int32_t index = 0; index < pNode->replica; ++index) {
    syncStopCheckPeerConn(pNode->peerInfo[index]);
  }

  for (i = 0; i < pNode->replica; ++i) {
    for (j = 0; j < pNewCfg->replica; ++j) {
      if ((strcmp(pNode->peerInfo[i]->fqdn, pNewCfg->nodeInfo[j].nodeFqdn) == 0) &&
          (pNode->peerInfo[i]->port == pNewCfg->nodeInfo[j].nodePort))
        break;
    }

    if (j >= pNewCfg->replica) {
      pNode->peerInfo[i].reset();
    }
  }

  SSyncPeerPtr newPeers[TAOS_SYNC_MAX_REPLICA];
  for (i = 0; i < pNewCfg->replica; ++i) {
    const SNodeInfo *pNewNode = &pNewCfg->nodeInfo[i];

    for (j = 0; j < pNode->replica; ++j) {
      if (pNode->peerInfo[j] && (strcmp(pNode->peerInfo[j]->fqdn, pNewNode->nodeFqdn) == 0) &&
          (pNode->peerInfo[j]->port == pNewNode->nodePort))
        break;
    }

    if (j >= pNode->replica) {
      newPeers[i] = syncAddPeer(pNode, pNewNode);
    } else {
      newPeers[i] = pNode->peerInfo[j];
    }

    if (newPeers[i] == NULL) {
      sError("vgId:%d, failed to reconfig", pNode->vgId);
      return TSDB_CODE_SYN_INVALID_CONFIG;
    }

    if ((strcmp(pNewNode->nodeFqdn, tsNodeFqdn) == 0) && (pNewNode->nodePort == tsSyncPort)) {
      pNode->selfIndex = i;
    }
  }

  pNode->replica = pNewCfg->replica;
  pNode->quorum = pNewCfg->quorum;
  if (pNode->quorum > pNode->replica) pNode->quorum = pNode->replica;
  memcpy(pNode->peerInfo, newPeers, sizeof(SSyncPeer *) * pNewCfg->replica);

  for (i = pNewCfg->replica; i < TAOS_SYNC_MAX_REPLICA; ++i) {
    pNode->peerInfo[i] = NULL;
  }

  syncAddArbitrator(pNode);

  if (pNewCfg->replica <= 1) {
    sInfo("vgId:%d, no peers are configured, work as master!", pNode->vgId);
    pNode->setRole(TAOS_SYNC_ROLE_MASTER);
    (*pNode->notifyRole)(pNode->vgId, pNode->getRole());
  }

  syncStartCheckPeerConn(pNode->peerInfo[TAOS_SYNC_MAX_REPLICA]);  // arb
  for (int32_t index = 0; index < pNode->replica; ++index) {
    syncStartCheckPeerConn(pNode->peerInfo[index]);
  }

  pNode->mutex.unlock();

  sInfo("vgId:%d, %d replicas are configured, quorum:%d", pNode->vgId, pNode->replica, pNode->quorum);
  syncBroadcastStatus(pNode);

  return 0;
}

int32_t syncForwardToPeer(SSyncNodePtr pNode, void *data, void *mhandle, int32_t qtype) {
  return pNode->forwardToPeerImpl(data, mhandle, qtype);
}

void syncConfirmForward(SSyncNodePtr pNode, uint64_t version, int32_t code) {
  SSyncPeerPtr pPeer = pNode->pMaster;
  if (pPeer && pNode->quorum > 1) {
    SFwdRsp rsp(pNode->vgId, version, code);

    if (taosWriteMsg(pPeer->peerFd, &rsp, sizeof(SFwdRsp)) == sizeof(SFwdRsp)) {
      sTrace("%s, forward-rsp is sent, code:0x%x hver:%" PRIu64, pPeer->id, code, version);
    } else {
      sDebug("%s, failed to send forward-rsp, restart", pPeer->id);
      syncRestartConnection(pPeer);
    }
  }
}

#if 0
void syncRecover(int64_t rid) {
  SSyncPeer *pPeer;

  SSyncNode *pNode = syncAcquireNode(rid);
  if (pNode == NULL) return;

  // to do: add a few lines to check if recover is OK
  // if take this node to unsync state, the whole system may not work

  nodeRole = TAOS_SYNC_ROLE_UNSYNCED;
  (*pNode->notifyRole)(pNode->vgId, nodeRole);
  nodeVersion = 0;

  pNode->mutex.lock();

  for (int32_t i = 0; i < pNode->replica; ++i) {
    pPeer = pNode->peerInfo[i];
    if (pPeer->peerFd >= 0) {
      syncRestartConnection(pPeer);
    }
  }

  pNode->mutex.unlock();

  syncReleaseNode(pNode);
}
#endif

int32_t syncGetNodesRole(SSyncNodePtr pNode, SNodesRole *pNodesRole) {
  pNodesRole->selfIndex = pNode->selfIndex;
  for (int32_t i = 0; i < pNode->replica; ++i) {
    pNodesRole->nodeId[i] = pNode->peerInfo[i]->nodeId;
    pNodesRole->role[i] = pNode->peerInfo[i]->role;
  }
  return 0;
}

static void syncAddArbitrator(SSyncNodePtr pNode) {
  SSyncPeerPtr pPeer = pNode->peerInfo[TAOS_SYNC_MAX_REPLICA];

  // if not configured, return right away
  if (tsArbitrator[0] == 0) {
    pNode->peerInfo[TAOS_SYNC_MAX_REPLICA].reset();
    return;
  }

  SNodeInfo nodeInfo;
  nodeInfo.nodeId = 0;
  int32_t ret = taosGetFqdnPortFromEp(tsArbitrator, nodeInfo.nodeFqdn, &nodeInfo.nodePort);
  if (-1 == ret) {
    nodeInfo.nodePort = tsArbitratorPort;
  }

  if (pPeer) {
    if ((strcmp(nodeInfo.nodeFqdn, pPeer->fqdn) == 0) && (nodeInfo.nodePort == pPeer->port)) {
      return;
    } else {
      pNode->peerInfo[TAOS_SYNC_MAX_REPLICA].reset();
    }
  }

  pPeer = syncAddPeer(pNode, &nodeInfo);
  if (pPeer != NULL) {
    pPeer->isArb = 1;
    sInfo("%s, is added as arbitrator", pPeer->id);
  }

  pNode->peerInfo[TAOS_SYNC_MAX_REPLICA] = pPeer;
}

SSyncNode::~SSyncNode() {
  sDebug("vgId:%d, node is freed", vgId);
}

SSyncPeer::~SSyncPeer() {
  sDebug("%s, peer is freed", id);
}

void SSyncPeer::closeConn() {
  sDebug("%s, pfd:%d sfd:%d will be closed", id, peerFd, syncFd);

  taosTmrStopA(&timer);
  taosClose(syncFd);
  if (peerFd >= 0) {
    peerFd = -1;
    void *pConn = pConn;
    if (pConn != NULL) syncFreeTcpConn(pConn);
  }
}

static void syncStartCheckPeerConn(SSyncPeerPtr pPeer) {
  auto pNode = pPeer->pSyncNode.lock();

  int32_t ret = strcmp(pPeer->fqdn, tsNodeFqdn);
  if (pPeer->nodeId == 0 || (ret > 0) || (ret == 0 && pPeer->port > tsSyncPort)) {
    int32_t checkMs = 100 + (pNode->vgId * 10) % 100;

    sDebug("%s, check peer connection after %d ms", pPeer->id, checkMs);
    taosTmrReset([pPeer](void *tmrId) { syncCheckPeerConnection(pPeer, tmrId); }, checkMs,
                 tsSyncTmrCtrl, &pPeer->timer);
  }
}

static void syncStopCheckPeerConn(SSyncPeerPtr pPeer) {
  taosTmrStopA(&pPeer->timer);
  sDebug("%s, stop check peer connection", pPeer->id);
}

static SSyncPeerPtr syncAddPeer(SSyncNodePtr pNode, const SNodeInfo *pInfo) {
  uint32_t ip = taosGetIpv4FromFqdn(pInfo->nodeFqdn);
  if (ip == 0xFFFFFFFF) {
    sError("failed to add peer, can resolve fqdn:%s since %s", pInfo->nodeFqdn, strerror(errno));
    terrno = TSDB_CODE_RPC_FQDN_ERROR;
    return NULL;
  }

  auto pPeer = std::make_shared<SSyncPeer>();
  pPeer->nodeId = pInfo->nodeId;
  tstrncpy(pPeer->fqdn, pInfo->nodeFqdn, sizeof(pPeer->fqdn));
  pPeer->ip = ip;
  pPeer->port = pInfo->nodePort;
  pPeer->fqdn[sizeof(pPeer->fqdn) - 1] = 0;
  snprintf(pPeer->id, sizeof(pPeer->id), "vgId:%d, nodeId:%d", pNode->vgId, pPeer->nodeId);

  pPeer->peerFd = -1;
  pPeer->syncFd = -1;
  pPeer->role = TAOS_SYNC_ROLE_OFFLINE;
  pPeer->pSyncNode = pNode;

  sInfo("%s, %p it is configured, ep:%s:%u" PRId64, pPeer->id, pPeer, pPeer->fqdn, pPeer->port);

  return pPeer;
}

void syncBroadcastStatus(SSyncNodePtr pNode) {
  for (int32_t index = 0; index < pNode->replica; ++index) {
    if (index == pNode->selfIndex) continue;
    SSyncPeerPtr pPeer = pNode->peerInfo[index];
    syncSendPeersStatusMsgToPeer(pPeer, 1, SYNC_STATUS_BROADCAST, syncGenTranId());
  }
}

static void syncResetFlowCtrl(SSyncNodePtr pNode) {
  for (int32_t index = 0; index < pNode->replica; ++index) {
    pNode->peerInfo[index]->numOfRetrieves = 0;
  }

  if (pNode->notifyFlowCtrl) {
    (*pNode->notifyFlowCtrl)(pNode->vgId, 0);
  }
}

static void syncChooseMaster(SSyncNodePtr pNode) {
  SSyncPeerPtr pPeer;
  int32_t    onlineNum = 0;
  int32_t    index = -1;
  int32_t    replica = pNode->replica;

  for (int32_t i = 0; i < pNode->replica; ++i) {
    if (pNode->peerInfo[i]->role != TAOS_SYNC_ROLE_OFFLINE) {
      onlineNum++;
    }
  }

  if (onlineNum == pNode->replica) {
    // if all peers are online, peer with highest version shall be master
    index = 0;
    for (int32_t i = 1; i < pNode->replica; ++i) {
      if (pNode->peerInfo[i]->version > pNode->peerInfo[index]->version) {
        index = i;
      }
    }
    sDebug("vgId:%d, master:%s may be choosed, index:%d", pNode->vgId, pNode->peerInfo[index]->id, index);
  } else {
    sDebug("vgId:%d, no master election since onlineNum:%d replica:%d", pNode->vgId, onlineNum, pNode->replica);
  }

  // add arbitrator connection
  SSyncPeerPtr pArb = pNode->peerInfo[TAOS_SYNC_MAX_REPLICA];
  if (pArb) {
    if (pArb->role != TAOS_SYNC_ROLE_OFFLINE) {
      onlineNum++;
      replica = pNode->replica + 1;
      sDebug("vgId:%d, arb:%s is used while choose master", pNode->vgId, pArb->id);
    } else {
      sError("vgId:%d, arb:%s is not used while choose master for its offline", pNode->vgId, pArb->id);
    }
  }

  if (index < 0 && onlineNum > replica / 2.0) {
    // over half of nodes are online
    for (int32_t i = 0; i < pNode->replica; ++i) {
      // slave with highest version shall be master
      pPeer = pNode->peerInfo[i];
      if (pPeer->role == TAOS_SYNC_ROLE_SLAVE || pPeer->role == TAOS_SYNC_ROLE_MASTER) {
        if (index < 0 || pPeer->version > pNode->peerInfo[index]->version) {
          index = i;
        }
      }
    }

    if (index >= 0) {
      sDebug("vgId:%d, master:%s may be choosed, index:%d onlineNum(arb):%d replica:%d", pNode->vgId,
             pNode->peerInfo[index]->id, index, onlineNum, replica);
    }
  }

  if (index >= 0) {
    if (index == pNode->selfIndex) {
      sInfo("vgId:%d, start to work as master", pNode->vgId);
      pNode->setRole(TAOS_SYNC_ROLE_MASTER);

      // Wait for other nodes to receive status to avoid version inconsistency
      taosMsleep(SYNC_WAIT_AFTER_CHOOSE_MASTER);

      syncResetFlowCtrl(pNode);
      (*pNode->notifyRole)(pNode->vgId, pNode->getRole());
    } else {
      pPeer = pNode->peerInfo[index];
      sInfo("%s, it shall work as master", pPeer->id);
    }
  } else {
    sDebug("vgId:%d, failed to choose master", pNode->vgId);
  }
}

static SSyncPeerPtr syncCheckMaster(SSyncNodePtr pNode) {
  int32_t onlineNum = 0;
  int32_t masterIndex = -1;
  int32_t replica = pNode->replica;

  for (int32_t index = 0; index < pNode->replica; ++index) {
    if (pNode->peerInfo[index]->role != TAOS_SYNC_ROLE_OFFLINE) {
      onlineNum++;
    }
  }

  // add arbitrator connection
  SSyncPeerPtr pArb = pNode->peerInfo[TAOS_SYNC_MAX_REPLICA];
  if (pArb && pArb->role != TAOS_SYNC_ROLE_OFFLINE) {
    onlineNum++;
    replica = pNode->replica + 1;
  }

  if (onlineNum <= replica * 0.5) {
    if (pNode->getRole() != TAOS_SYNC_ROLE_UNSYNCED) {
       if (pNode->getRole() == TAOS_SYNC_ROLE_MASTER && onlineNum == replica * 0.5 && onlineNum >= 1) {
         sInfo("vgId:%d, self keep work as master, online:%d replica:%d", pNode->vgId, onlineNum, replica);
       } else {
        pNode->setRole(TAOS_SYNC_ROLE_UNSYNCED);
        sInfo("vgId:%d, self change to unsynced state, online:%d replica:%d", pNode->vgId, onlineNum, replica);
       }
      (*pNode->notifyRole)(pNode->vgId, pNode->getRole());
    }
  } else {
    for (int32_t index = 0; index < pNode->replica; ++index) {
      SSyncPeerPtr pTemp = pNode->peerInfo[index];
      if (pTemp->role != TAOS_SYNC_ROLE_MASTER) continue;
      if (masterIndex < 0) {
        masterIndex = index;
      } else {  // multiple masters, it shall not happen
        if (masterIndex == pNode->selfIndex) {
          sError("%s, peer is master, work as slave instead", pTemp->id);
          pNode->setRole(TAOS_SYNC_ROLE_SLAVE);
          (*pNode->notifyRole)(pNode->vgId, pNode->getRole());
        }
      }
    }
  }

  SSyncPeerPtr pMaster = (masterIndex >= 0) ? pNode->peerInfo[masterIndex] : SSyncPeerPtr();
  return pMaster;
}

static int32_t syncValidateMaster(SSyncPeerPtr pPeer) {
  auto pNode = pPeer->pSyncNode.lock();
  int32_t code = 0;

  if (pNode->getRole() == TAOS_SYNC_ROLE_MASTER && pNode->getVer() < pPeer->version) {
    sDebug("%s, peer has higher sver:%" PRIu64 ", restart all peer connections", pPeer->id, pPeer->version);
    pNode->setRole(TAOS_SYNC_ROLE_UNSYNCED);
    (*pNode->notifyRole)(pNode->vgId, pNode->getRole());
    code = -1;

    for (int32_t index = 0; index < pNode->replica; ++index) {
      if (index == pNode->selfIndex) continue;
      pNode->peerInfo[index]->restart();
    }
  }

  return code;
}

static void syncCheckRole(SSyncPeerPtr pPeer, SPeerStatus* peersStatus, int8_t newPeerRole) {
  auto pNode = pPeer->pSyncNode.lock();
  int8_t oldPeerRole = pPeer->role;
  int8_t oldSelfRole = pNode->getRole();
  int8_t syncRequired = 0;

  pPeer->role = newPeerRole;
  sDebug("%s, peer role:%s change to %s", pPeer->id, syncRole[oldPeerRole], syncRole[newPeerRole]);

  SSyncPeerPtr pMaster = syncCheckMaster(pNode);

  if (pMaster) {
    // master is there
    pNode->pMaster = pMaster;
    sDebug("%s, it is the master, replica:%d sver:%" PRIu64, pMaster->id, pNode->replica, pMaster->version);

    if (syncValidateMaster(pPeer) < 0) return;

    if (pNode->getRole() == TAOS_SYNC_ROLE_UNSYNCED) {
      if (pNode->getVer() < pMaster->version) {
        sDebug("%s, is master, sync required, self sver:%" PRIu64, pMaster->id, pNode->getVer());
        syncRequired = 1;
      } else {
        sInfo("%s, is master, work as slave, self sver:%" PRIu64, pMaster->id, pNode->getVer());
        pNode->setRole(TAOS_SYNC_ROLE_SLAVE);
        (*pNode->notifyRole)(pNode->vgId, pNode->getRole());
      }
    } else if (pNode->getRole() == TAOS_SYNC_ROLE_SLAVE && pMaster == pPeer) {
      sDebug("%s, is master, continue work as slave, self sver:%" PRIu64, pMaster->id, pNode->getVer());
    }
  } else {
    // master not there, if all peer's state and version are consistent, choose the master
    int32_t consistent = 0;
    int32_t index = 0;
    if (peersStatus != NULL) {
      for (index = 0; index < pNode->replica; ++index) {
        SSyncPeerPtr pTemp = pNode->peerInfo[index];
        if (pTemp->role != peersStatus[index].role) break;
        if ((pTemp->role != TAOS_SYNC_ROLE_OFFLINE) && (pTemp->version != peersStatus[index].version)) break;
      }

      if (index >= pNode->replica) consistent = 1;
    } else {
      if (pNode->replica == 2) consistent = 1;
    }

    if (consistent) {
      sDebug("vgId:%d, choose master, replica:%d", pNode->vgId, pNode->replica);
      syncChooseMaster(pNode);
    } else {
      sDebug("vgId:%d, cannot choose master since roles inequality, replica:%d", pNode->vgId, pNode->replica);
    }
  }

  if (syncRequired) {
    pMaster->recoverFromMaster();
  }

  if (oldPeerRole != newPeerRole || pNode->getRole() != oldSelfRole) {
    sDebug("vgId:%d, roles changed, broadcast status", pNode->vgId);
    syncBroadcastStatus(pNode);
  }

  if (pNode->getRole() != TAOS_SYNC_ROLE_MASTER) {
    syncResetFlowCtrl(pNode);
  }
}

void SSyncPeer::restart() {
  sDebug("%s, restart peer connection, last sstatus:%s", id, syncStatus[sstatus]);

  closeConn();

  sstatus = TAOS_SYNC_STATUS_INIT;
  sDebug("%s, peer conn is restart and set sstatus:%s", id, syncStatus[sstatus]);

  int32_t ret = strcmp(fqdn, tsNodeFqdn);
  if (ret > 0 || (ret == 0 && port > tsSyncPort)) {
    sDebug("%s, check peer connection in 1000 ms", id);
    auto self = shared_from_this();
    taosTmrReset([self](void *tmrId) { syncCheckPeerConnection(self, tmrId); }, SYNC_CHECK_INTERVAL,
                 tsSyncTmrCtrl, &timer);
  }
}

void syncRestartConnection(SSyncPeerPtr pPeer) {
  if (pPeer->ip == 0) return;

  pPeer->restart();
  syncCheckRole(pPeer, NULL, TAOS_SYNC_ROLE_OFFLINE);
}

static void syncProcessSyncRequest(char *msg, SSyncPeerPtr pPeer) {
  auto pNode = pPeer->pSyncNode.lock();
  sInfo("%s, sync-req is received", pPeer->id);

  if (pPeer->ip == 0) return;

  if (pNode->getRole() != TAOS_SYNC_ROLE_MASTER) {
    sError("%s, I am not master anymore", pPeer->id);
    taosClose(pPeer->syncFd);
    return;
  }

  if (pPeer->sstatus != TAOS_SYNC_STATUS_INIT) {
    sDebug("%s, sync is already started for sstatus:%s", pPeer->id, syncStatus[pPeer->sstatus]);
    return;  // already started
  }

  std::thread   thread(syncRetrieveData, pPeer);
  thread.detach();
  pPeer->sstatus = TAOS_SYNC_STATUS_START;
  sDebug("%s, sync retrieve thread:0x%08" PRIx64 " create successfully, set sstatus:%s", pPeer->id,
         thread.get_id(), syncStatus[pPeer->sstatus]);
}

static void syncNotStarted(SSyncPeerPtr pPeer, void *tmrId) {
  auto pNode = pPeer->pSyncNode.lock();

  pNode->mutex.lock();
  pPeer->timer = NULL;
  pPeer->sstatus = TAOS_SYNC_STATUS_INIT;
  sInfo("%s, sync conn is still not up, restart and set sstatus:%s", pPeer->id, syncStatus[pPeer->sstatus]);
  syncRestartConnection(pPeer);
  pNode->mutex.unlock();
}

static void syncTryRecoverFromMaster(SSyncPeerPtr pPeer, void *tmrId) {
  auto pNode = pPeer->pSyncNode.lock();

  pNode->mutex.lock();
  pPeer->recoverFromMaster();
  pNode->mutex.unlock();
}

void SSyncPeer::recoverFromMaster() {
  auto pNode = pSyncNode.lock();

  if (pNode->getStatus() != TAOS_SYNC_STATUS_INIT) {
    sDebug("%s, sync is already started since sstatus:%s", id, syncStatus[pNode->getStatus()]);
    return;
  }
  auto self = shared_from_this();
  taosTmrStopA(&timer);

  // Ensure the sync of mnode not interrupted
  if (pNode->vgId != 1 && tsSyncNum >= SYNC_MAX_NUM) {
    sInfo("%s, %d syncs are in process, try later", id, tsSyncNum);
    taosTmrReset([self](void *tmrId) { syncTryRecoverFromMaster(self, tmrId); }, 500 + (pNode->vgId * 10) % 200,
                 tsSyncTmrCtrl, &timer);
    return;
  }

  sDebug("%s, try to sync", id);

  SSyncMsg msg;
  syncBuildSyncReqMsg(&msg, pNode->vgId);

  taosTmrReset([self](void *tmrId) { syncNotStarted(self, tmrId); }, SYNC_CHECK_INTERVAL, tsSyncTmrCtrl,
               &timer);

  if (taosWriteMsg(peerFd, &msg, sizeof(SSyncMsg)) != sizeof(SSyncMsg)) {
    sError("%s, failed to send sync-req to peer", id);
  } else {
    sInfo("%s, sync-req is sent to peer, tranId:%u, sstatus:%s", id, msg.tranId, syncStatus[pNode->getStatus()]);
  }
}

static void syncProcessFwdResponse(SFwdRsp *pFwdRsp, SSyncPeerPtr pPeer) {
  auto pNode = pPeer->pSyncNode.lock();
  auto &pSyncFwds = pNode->pSyncFwds;
  SFwdInfo * pFwdInfo;

  sTrace("%s, forward-rsp is received, code:%x hver:%" PRIu64, pPeer->id, pFwdRsp->code, pFwdRsp->version);
  SFwdInfo *pFirst = &pSyncFwds->fwdInfo[pSyncFwds->first];

  if (pFirst->version <= pFwdRsp->version && pSyncFwds->fwds > 0) {
    // find the forwardInfo from first
    for (int32_t i = 0; i < pSyncFwds->fwds; ++i) {
      pFwdInfo = &pSyncFwds->fwdInfo[(i + pSyncFwds->first) % SYNC_MAX_FWDS];
      if (pFwdRsp->version == pFwdInfo->version) {
        pNode->process(pFwdInfo, pFwdRsp->code);
        pNode->removeConfirmedFwdInfo();
        return;
      }
    }
  }
}

static void syncProcessForwardFromPeer(char *cont, SSyncPeerPtr pPeer) {
  auto pNode = pPeer->pSyncNode.lock();
  SWalHead * pHead = (SWalHead *)(cont + sizeof(SSyncHead));

  sTrace("%s, forward is received, hver:%" PRIu64 ", len:%d", pPeer->id, pHead->version, pHead->len);

  if (pNode->getRole() == TAOS_SYNC_ROLE_SLAVE) {
    // nodeVersion = pHead->version;
    (*pNode->writeToCache)(pNode->vgId, pHead, TAOS_QTYPE_FWD, NULL);
  } else {
    if (pNode->getStatus() != TAOS_SYNC_STATUS_INIT) {
      syncSaveIntoBuffer(pPeer, pHead);
    } else {
      sError("%s, forward discarded since sstatus:%s, hver:%" PRIu64, pPeer->id, syncStatus[pNode->getStatus()],
             pHead->version);
    }
  }
}

static void syncProcessPeersStatusMsg(SPeersStatus *pPeersStatus, SSyncPeerPtr pPeer) {
  auto pNode = pPeer->pSyncNode.lock();

  sDebug("%s, status is received, self:%s:%s:%" PRIu64 ", peer:%s:%" PRIu64 ", ack:%d tranId:%u type:%s pfd:%d",
         pPeer->id, syncRole[pNode->getRole()], syncStatus[pNode->getStatus()], pNode->getVer(),
         syncRole[pPeersStatus->role],
         pPeersStatus->version, pPeersStatus->ack, pPeersStatus->tranId, statusType[pPeersStatus->type], pPeer->peerFd);

  pPeer->version = pPeersStatus->version;
  syncCheckRole(pPeer, pPeersStatus->peersStatus, pPeersStatus->role);

  if (pPeersStatus->ack) {
    syncSendPeersStatusMsgToPeer(pPeer, 0, pPeersStatus->type + 1, pPeersStatus->tranId);
  }
}

static int32_t syncReadPeerMsg(SSyncPeerPtr pPeer, SSyncHead *pHead) {
  if (pPeer->peerFd < 0) return -1;

  int32_t hlen = taosReadMsg(pPeer->peerFd, pHead, sizeof(SSyncHead));
  if (hlen != sizeof(SSyncHead)) {
    sDebug("%s, failed to read msg since %s, hlen:%d", pPeer->id, tstrerror(errno), hlen);
    return -1;
  }

  int32_t code = pHead->check();
  if (code != 0) {
    sError("%s, failed to check msg head since %s, type:%d", pPeer->id, tstrerror(code), pHead->type);
    return -1;
  }

  int32_t bytes = taosReadMsg(pPeer->peerFd, (char *)pHead + sizeof(SSyncHead), pHead->len);
  if (bytes != pHead->len) {
    sError("%s, failed to read, bytes:%d len:%d", pPeer->id, bytes, pHead->len);
    return -1;
  }

  return 0;
}

static int32_t syncProcessPeerMsg(SSyncPeerPtr pPeer, void *buffer) {
  SSyncHead *pHead = static_cast<SSyncHead *>(buffer);
  auto pNode = pPeer->pSyncNode.lock();
  
  pNode->mutex.lock();

  int32_t code = syncReadPeerMsg(pPeer, pHead);

  if (code == 0) {
    if (pHead->type == TAOS_SMSG_SYNC_FWD) {
      syncProcessForwardFromPeer(static_cast<char*>(buffer), pPeer);
    } else if (pHead->type == TAOS_SMSG_SYNC_FWD_RSP) {
      syncProcessFwdResponse(static_cast<SFwdRsp*>(buffer), pPeer);
    } else if (pHead->type == TAOS_SMSG_SYNC_REQ) {
      syncProcessSyncRequest(static_cast<char*>(buffer), pPeer);
    } else if (pHead->type == TAOS_SMSG_STATUS) {
      syncProcessPeersStatusMsg(static_cast<SPeersStatus*>(buffer), pPeer);
    }
  }

  pNode->mutex.unlock();
  return code;
}

static int32_t syncSendPeersStatusMsgToPeer(SSyncPeerPtr pPeer, char ack, int8_t type, uint16_t tranId) {
  if (pPeer->peerFd < 0 || pPeer->ip == 0) {
    sDebug("%s, failed to send status msg, restart fd:%d", pPeer->id, pPeer->peerFd);
    syncRestartConnection(pPeer);
    return -1;
  }

  auto pNode = pPeer->pSyncNode.lock();
  SPeersStatus msg;

  memset(&msg, 0, sizeof(SPeersStatus));
  syncBuildPeersStatus(&msg, pNode->vgId);

  msg.role = pNode->getRole();
  msg.ack = ack;
  msg.type = type;
  msg.tranId = tranId;
  msg.version = pNode->getVer();

  for (int32_t i = 0; i < pNode->replica; ++i) {
    msg.peersStatus[i].role = pNode->peerInfo[i]->role;
    msg.peersStatus[i].version = pNode->peerInfo[i]->version;
  }

  if (taosWriteMsg(pPeer->peerFd, &msg, sizeof(SPeersStatus)) == sizeof(SPeersStatus)) {
    sDebug("%s, status is sent, self:%s:%s:%" PRIu64 ", peer:%s:%s:%" PRIu64 ", ack:%d tranId:%u type:%s pfd:%d",
           pPeer->id, syncRole[pNode->getRole()], syncStatus[pNode->getStatus()], pNode->getVer(),
           syncRole[pPeer->role],
           syncStatus[pPeer->sstatus], pPeer->version, ack, tranId, statusType[type], pPeer->peerFd);
    return 0;
  } else {
    sDebug("%s, failed to send status msg, restart", pPeer->id);
    syncRestartConnection(pPeer);
    return -1;
  }
}

static void syncSetupPeerConnection(SSyncPeerPtr pPeer) {
  auto pNode = pPeer->pSyncNode.lock();

  taosTmrStopA(&pPeer->timer);
  if (pPeer->peerFd >= 0) {
    sDebug("%s, send role version to peer", pPeer->id);
    syncSendPeersStatusMsgToPeer(pPeer, 1, SYNC_STATUS_SETUP_CONN, syncGenTranId());
    return;
  }

  int32_t connFd = taosOpenTcpClientSocket(pPeer->ip, pPeer->port, 0);
  if (connFd < 0) {
    sDebug("%s, failed to open tcp socket since %s", pPeer->id, strerror(errno));
    taosTmrReset([pPeer](void *tmrId) { syncCheckPeerConnection(pPeer, tmrId); }, SYNC_CHECK_INTERVAL,
                 tsSyncTmrCtrl, &pPeer->timer);
    return;
  }

  SSyncMsg msg;
  syncBuildSyncSetupMsg(&msg, pPeer->nodeId ? pNode->vgId : 0);

  if (taosWriteMsg(connFd, &msg, sizeof(SSyncMsg)) == sizeof(SSyncMsg)) {
    sDebug("%s, connection to peer server is setup, pfd:%d sfd:%d tranId:%u", pPeer->id, connFd, pPeer->syncFd, msg.tranId);
    pPeer->peerFd = connFd;
    pPeer->role = TAOS_SYNC_ROLE_UNSYNCED;
    pPeer->pConn = syncAllocateTcpConn(tsTcpPool, connFd);
    pPeer->pConn->processBrokenLink = [pPeer] { syncProcessBrokenLink(pPeer); };
    pPeer->pConn->processIncomingMsg = [pPeer](void *buffer) { return syncProcessPeerMsg(pPeer, buffer); };
    if (pPeer->isArb) tsArbOnline = 1;
  } else {
    sDebug("%s, failed to setup peer connection to server since %s, try later", pPeer->id, strerror(errno));
    taosClose(connFd);
    taosTmrReset([pPeer](void *tmrId) { syncCheckPeerConnection(pPeer, tmrId); }, SYNC_CHECK_INTERVAL,
                 tsSyncTmrCtrl, &pPeer->timer);
  }
}

static void syncCheckPeerConnection(SSyncPeerPtr pPeer, void *tmrId) {
  auto pNode = pPeer->pSyncNode.lock();

  pNode->mutex.lock();

  sDebug("%s, check peer connection", pPeer->id);
  syncSetupPeerConnection(pPeer);

  pNode->mutex.unlock();
}

static void syncCreateRestoreDataThread(SSyncPeerPtr pPeer) {
  taosTmrStopA(&pPeer->timer);

  std::thread thread(syncRestoreData, pPeer);
  thread.detach();

  sInfo("%s, sync restore " PRIx64 " create successfully" PRId64, pPeer->id, thread.get_id());
}

static void syncProcessIncommingConnection(int32_t connFd, uint32_t sourceIp) {
  char    ipstr[24];
  int32_t i;

  tinet_ntoa(ipstr, sourceIp);
  sDebug("peer TCP connection from ip:%s", ipstr);

  SSyncMsg msg;
  if (taosReadMsg(connFd, &msg, sizeof(SSyncMsg)) != sizeof(SSyncMsg)) {
    sError("failed to read peer sync msg from ip:%s since %s", ipstr, strerror(errno));
    taosCloseSocket(connFd);
    return;
  }

  int32_t code = ((SSyncHead *)(&msg))->check();
  if (code != 0) {
    sError("failed to check peer sync msg from ip:%s since %s", ipstr, strerror(code));
    taosCloseSocket(connFd);
    return;
  }

  int32_t     vgId = msg.head.vgId;
  SSyncNode **ppNode = static_cast<SSyncNode**>(taosHashGet(tsVgIdHash, &vgId, sizeof(int32_t)));
  if (ppNode == NULL || *ppNode == NULL) {
    sError("vgId:%d, vgId could not be found", vgId);
    taosCloseSocket(connFd);
    return;
  }

  sDebug("vgId:%d, sync connection is incoming, tranId:%u", vgId, msg.tranId);

  SSyncNode *pNode = *ppNode;
  std::lock_guard<std::mutex> lock(pNode->mutex);

  SSyncPeerPtr pPeer;
  for (i = 0; i < pNode->replica; ++i) {
    pPeer = pNode->peerInfo[i];
    if (pPeer && (strcmp(pPeer->fqdn, msg.fqdn) == 0) && (pPeer->port == msg.port)) break;
  }

  pPeer = (i < pNode->replica) ? pNode->peerInfo[i] : SSyncPeerPtr();
  if (pPeer == NULL) {
    sError("vgId:%d, peer:%s:%u not configured", pNode->vgId, msg.fqdn, msg.port);
    taosCloseSocket(connFd);
    // syncSendVpeerCfgMsg(sync);
  } else {
    // first packet tells what kind of link
    if (msg.head.type == TAOS_SMSG_SYNC_DATA) {
      pPeer->syncFd = connFd;
      pNode->setStatus(TAOS_SYNC_STATUS_START);
      sInfo("%s, sync-data msg from master is received, tranId:%u, set sstatus:%s", pPeer->id, msg.tranId,
            syncStatus[pNode->getStatus()]);
      syncCreateRestoreDataThread(pPeer);
    } else {
      sDebug("%s, TCP connection is up, pfd:%d sfd:%d, old pfd:%d", pPeer->id, connFd, pPeer->syncFd, pPeer->peerFd);
      pPeer->closeConn();
      pPeer->peerFd = connFd;
      pPeer->pConn = syncAllocateTcpConn(tsTcpPool, connFd);
      pPeer->pConn->processBrokenLink = [pPeer] { syncProcessBrokenLink(pPeer); };
      pPeer->pConn->processIncomingMsg = [pPeer](void *buffer) { return syncProcessPeerMsg(pPeer, buffer); };
      sDebug("%s, ready to exchange data", pPeer->id);
      syncSendPeersStatusMsgToPeer(pPeer, 1, SYNC_STATUS_EXCHANGE_DATA, syncGenTranId());
    }
  }
}

static void syncProcessBrokenLink(SSyncPeerPtr pPeer) {
  auto pNode = pPeer->pSyncNode.lock();

  pNode->mutex.lock();

  sDebug("%s, TCP link is broken since %s, pfd:%d sfd:%d", pPeer->id, strerror(errno), pPeer->peerFd, pPeer->syncFd);
  pPeer->peerFd = -1;
  if (pPeer->isArb) {
    tsArbOnline = 0;
  }

  syncRestartConnection(pPeer);
  pNode->mutex.unlock();
}

int32_t SSyncNode::save(uint64_t version, void *mhandle) const {
  int64_t    time = taosGetTimestampMs();

  if (pSyncFwds->fwds >= SYNC_MAX_FWDS) {
    // pSyncFwds->first = (pSyncFwds->first + 1) % SYNC_MAX_FWDS;
    // pSyncFwds->fwds--;
    sError("vgId:%d, failed to save fwd info, hver:%" PRIu64 " fwds:%d", vgId, version, pSyncFwds->fwds);
    return TSDB_CODE_SYN_TOO_MANY_FWDINFO;
  }

  if (pSyncFwds->fwds > 0) {
    pSyncFwds->last = (pSyncFwds->last + 1) % SYNC_MAX_FWDS;
  }

  SFwdInfo *pFwdInfo = &pSyncFwds->fwdInfo[pSyncFwds->last];
  memset(pFwdInfo, 0, sizeof(SFwdInfo));
  pFwdInfo->version = version;
  pFwdInfo->mhandle = mhandle;
  pFwdInfo->time = time;

  pSyncFwds->fwds++;
  sTrace("vgId:%d, fwd info is saved, hver:%" PRIu64 " fwds:%d ", vgId, version, pSyncFwds->fwds);

  return 0;
}

void SSyncNode::removeConfirmedFwdInfo() {
  int32_t fwds = pSyncFwds->fwds;
  for (int32_t i = 0; i < fwds; ++i) {
    SFwdInfo *pFwdInfo = &pSyncFwds->fwdInfo[pSyncFwds->first];
    if (pFwdInfo->confirmed == 0) break;

    pSyncFwds->first = (pSyncFwds->first + 1) % SYNC_MAX_FWDS;
    pSyncFwds->fwds--;
    if (pSyncFwds->fwds == 0) pSyncFwds->first = pSyncFwds->last;
    sTrace("vgId:%d, fwd info is removed, hver:%" PRIu64 " fwds:%d", vgId, pFwdInfo->version, pSyncFwds->fwds);
    memset(pFwdInfo, 0, sizeof(SFwdInfo));
  }
}

void SSyncNode::process(SFwdInfo *pFwdInfo, int32_t code) const {
  int32_t confirm = 0;
  if (pFwdInfo->code == 0) pFwdInfo->code = code;

  if (code == 0) {
    pFwdInfo->acks++;
    if (pFwdInfo->acks >= quorum - 1) {
      confirm = 1;
    }
  } else {
    pFwdInfo->nacks++;
    if (pFwdInfo->nacks > replica - quorum) {
      confirm = 1;
    }
  }

  if (confirm && pFwdInfo->confirmed == 0) {
    sTrace("vgId:%d, forward is confirmed, hver:%" PRIu64 " code:0x%x", vgId, pFwdInfo->version, pFwdInfo->code);
    (*confirmForward)(vgId, pFwdInfo->mhandle, pFwdInfo->code);
    pFwdInfo->confirmed = 1;
  }
}

void SSyncNode::monitorNodeRole(void *tmrId) 
{
  for (int32_t index = 0; index < replica; index++) {
    if (index == selfIndex) continue;

    SSyncPeerPtr pPeer = peerInfo[index];
    if (/*pPeer->role > TAOS_SYNC_ROLE_UNSYNCED && */ getRole() > TAOS_SYNC_ROLE_UNSYNCED) continue;
    if (/*pPeer->sstatus > TAOS_SYNC_STATUS_INIT || */ getStatus() > TAOS_SYNC_STATUS_INIT) continue;

    sDebug("%s, check roles since self:%s sstatus:%s, peer:%s sstatus:%s", pPeer->id, syncRole[pPeer->role],
           syncStatus[pPeer->sstatus], syncRole[getRole()], syncStatus[getStatus()]);
    syncSendPeersStatusMsgToPeer(pPeer, 1, SYNC_STATUS_CHECK_ROLE, syncGenTranId());
    break;
  }

  auto self = shared_from_this();
  pRoleTimer = taosTmrStart([self](void *tmrId) { syncMonitorNodeRole(self, tmrId); }, SYNC_ROLE_TIMER,
                            tsSyncTmrCtrl);
}

static void syncMonitorNodeRole(SSyncNodePtr pNode, void *tmrId) {
  pNode->monitorNodeRole(tmrId);
}

void SSyncNode::monitorFwdInfos(void* tmrId) 
{
  if (pSyncFwds) {
    int64_t time = taosGetTimestampMs();

    if (pSyncFwds->fwds > 0) {
      std::lock_guard<std::mutex> lock(mutex);
      for (int32_t i = 0; i < pSyncFwds->fwds; ++i) {
        SFwdInfo *pFwdInfo = &pSyncFwds->fwdInfo[(pSyncFwds->first + i) % SYNC_MAX_FWDS];
        if (ABS(time - pFwdInfo->time) < 2000) break;

        sDebug("vgId:%d, forward info expired, hver:%" PRIu64 " curtime:%" PRIu64 " savetime:%" PRIu64, vgId,
               pFwdInfo->version, time, pFwdInfo->time);
        process(pFwdInfo, TSDB_CODE_SYN_CONFIRM_EXPIRED);
      }

      removeConfirmedFwdInfo();
    }

    auto self = shared_from_this();
    pFwdTimer = taosTmrStart([self](void *tmrId) { syncMonitorFwdInfos(self, tmrId); }, SYNC_FWD_TIMER,
                             tsSyncTmrCtrl);
  }
}

static void syncMonitorFwdInfos(SSyncNodePtr pNode, void *tmrId) {
  pNode->monitorFwdInfos(tmrId);
}

int32_t SSyncNode::forwardToPeerImpl(void *data, void *mhandle, int32_t qtype) {
  SSyncPeerPtr pPeer;
  SSyncHead *pSyncHead;
  SWalHead * pWalHead = static_cast<SWalHead *>(data);
  int32_t    fwdLen;
  int32_t    code = 0;

  if (pWalHead->version > getVer() + 1) {
    sError("vgId:%d, hver:%" PRIu64 ", inconsistent with sver:%" PRIu64, vgId, pWalHead->version, getVer());
    if (getRole() == TAOS_SYNC_ROLE_SLAVE) {
      sInfo("vgId:%d, restart connection", vgId);
      for (int32_t i = 0; i < replica; ++i) {
        pPeer = peerInfo[i];
        syncRestartConnection(pPeer);
      }
    }

    if (replica != 1) {
      return TSDB_CODE_SYN_INVALID_VERSION;
    }
  }

  // always update version
  sTrace("vgId:%d, update version, replica:%d role:%s qtype:%s hver:%" PRIu64, vgId, replica,
         syncRole[getRole()], qtypeStr[qtype], pWalHead->version);
  setVer(pWalHead->version);

  if (replica == 1 || getRole() != TAOS_SYNC_ROLE_MASTER) return 0;

  // only msg from RPC or CQ can be forwarded
  if (qtype != TAOS_QTYPE_RPC && qtype != TAOS_QTYPE_CQ) return 0;

    // a hacker way to improve the performance
  pSyncHead = (SSyncHead *)(((char *)pWalHead) - sizeof(SSyncHead));
  pSyncHead->buildFwdMsg(vgId, sizeof(SWalHead) + pWalHead->len);
  fwdLen = pSyncHead->len + sizeof(SSyncHead);  // include the WAL and SYNC head

  std::lock_guard<std::mutex> lock(mutex);

  for (int32_t i = 0; i < replica; ++i) {
    pPeer = peerInfo[i];
    if (pPeer == NULL || pPeer->peerFd < 0) continue;
    if (pPeer->role != TAOS_SYNC_ROLE_SLAVE && pPeer->sstatus != TAOS_SYNC_STATUS_CACHE) continue;

    if (quorum > 1 && code == 0) {
      code = save(pWalHead->version, mhandle);
      if (code >= 0) code = 1;
    }

    int32_t retLen = taosWriteMsg(pPeer->peerFd, pSyncHead, fwdLen);
    if (retLen == fwdLen) {
      sTrace("%s, forward is sent, role:%s sstatus:%s hver:%" PRIu64 " contLen:%d", pPeer->id, syncRole[pPeer->role],
             syncStatus[pPeer->sstatus], pWalHead->version, pWalHead->len);
    } else {
      sError("%s, failed to forward, role:%s sstatus:%s hver:%" PRIu64 " retLen:%d", pPeer->id, syncRole[pPeer->role],
             syncStatus[pPeer->sstatus], pWalHead->version, retLen);
      syncRestartConnection(pPeer);
    }
  }

  return code;
}
