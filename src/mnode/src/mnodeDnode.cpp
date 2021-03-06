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

#include <mutex>
#include "os.h"
#include "tgrant.h"
#include "tbn.h"
#include "tglobal.h"
#include "tconfig.h"
#include "tutil.h"
#include "tsocket.h"
#include "tbn.h"
#include "tsync.h"
#include "tdataformat.h"
#include "mnode.h"
#include "dnode.h"
#include "mnodeDef.h"
#include "mnodeInt.h"
#include "mnodeDnode.h"
#include "mnodeMnode.h"
#include "mnodeSdb.h"
#include "mnodeShow.h"
#include "mnodeUser.h"
#include "mnodeVgroup.h"
#include "mnodeWrite.h"
#include "mnodePeer.h"
#include "mnodeCluster.h"
#include "dnodeEps.h"
#include "SdbMgmt.h"

int32_t tsAccessSquence = 0;
static std::shared_ptr<SSdbTable> tsDnodeSdb;
static int32_t tsDnodeUpdateSize = 0;
extern void *  tsMnodeSdb;
extern void *  tsVgroupSdb;

static SDnodeEps*tsDnodeEps;
static int32_t   tsDnodeEpsSize;
static std::mutex tsDnodeEpsMutex;

static int32_t mnodeCreateDnode(char *ep, SMnodeMsg *pMsg);
static void    mnodeUpdateDnodeEps();

static const char* offlineReason[] = {
  "",
  "status msg timeout",
  "status not received",
  "status reset by mnode",
  "version not match",
  "dnodeId not match",
  "clusterId not match",
  "numOfMnodes not match",
  "balance not match",
  "mnEqualVn not match",
  "offThreshold not match",
  "interval not match",
  "maxTabPerVn not match",
  "maxVgPerDb not match",
  "arbitrator not match",
  "timezone not match",
  "locale not match",
  "charset not match",
  "unknown",
};

int32_t SDnodeObj::insert() {
  if (status != TAOS_DN_STATUS_DROPPING) {
    status = TAOS_DN_STATUS_OFFLINE;
    lastAccess = tsAccessSquence;
    offlineReason = TAOS_DN_OFF_STATUS_NOT_RECEIVED;
  }

  dnodeUpdateEp(dnodeId, dnodeEp, dnodeFqdn, &dnodePort);
  mnodeUpdateDnodeEps();

  mInfo("dnode:%d, fqdn:%s ep:%s port:%d is created", dnodeId, dnodeFqdn, dnodeEp, dnodePort);
  return TSDB_CODE_SUCCESS;
}

int32_t SDnodeObj::remove() {
  mnodeDropMnodeLocal(dnodeId);
  bnNotify();
  mnodeUpdateDnodeEps();
  mDebug("dnode:%d, all vgroups is dropped from sdb", dnodeId);
  return TSDB_CODE_SUCCESS;
}

int32_t SDnodeObj::update() {
    #if 0
  SDnodeObj *pDnode = static_cast<SDnodeObj *>(mnodeGetDnode(dnodeId));
  if (pDnode != NULL && this != pDnode) {
    memcpy(pDnode, this, sizeof(SDnodeObj));
    delete this;
  }
  #endif
  mnodeUpdateDnodeEps();
  return TSDB_CODE_SUCCESS;
}

int32_t SDnodeObj::encode(binser::memory_output_archive<> &out) {
  out(dnodeId, openVnodes, createdTime, resever0, customScore,
      lastAccess, numOfCores, dnodePort, dnodeFqdn, dnodeEp,
      alternativeRole, status, isMgmt, reserve1);
  return TSDB_CODE_SUCCESS;
}

class DnodeTable : public SSdbTable {
 public:
  using SSdbTable::SSdbTable;
  int32_t decode(SSdbRow *pRow) override {
    auto pDnode = std::make_shared<SDnodeObj>();
    memcpy(pDnode.get(), pRow->rowData, tsDnodeUpdateSize);
    pRow->pObj = pDnode;
    return TSDB_CODE_SUCCESS;
  }
  int32_t restore() override {
    int32_t numOfRows = getNumOfRows();
    if (numOfRows <= 0 && dnodeIsFirstDeploy()) {
      mInfo("dnode first deploy, create dnode:%s", tsLocalEp);
      mnodeCreateDnode(tsLocalEp, NULL);
      SDnodeObj *pDnode = mnodeGetDnodeByEp(tsLocalEp);
      if (pDnode != NULL) {
        mnodeCreateMnode(pDnode->dnodeId, pDnode->dnodeEp, false);
      }
    }

    mnodeUpdateDnodeEps();
    return TSDB_CODE_SUCCESS;
  }
};

int32_t mnodeInitDnodes() {
  SDnodeObj tObj;
  tsDnodeUpdateSize = (int8_t *)tObj.updateEnd - (int8_t *)&tObj;

  SSdbTableDesc desc;
  desc.id = SDB_TABLE_DNODE;
  desc.name = "dnodes";
  desc.hashSessions = TSDB_DEFAULT_DNODES_HASH_SIZE;
  desc.maxRowSize = tsDnodeUpdateSize;
  desc.keyType = SDB_KEY_AUTO;

  tsDnodeSdb = SSdbMgmt::instance().openTable<DnodeTable>(desc);
  if (tsDnodeSdb == NULL) {
    mError("failed to init dnodes data");
    return -1;
  }
 
  mDebug("table:dnodes table is created");
  return 0;
}

void mnodeCleanupDnodes() {
  tsDnodeSdb.reset();
  free(tsDnodeEps);
  tsDnodeEps = NULL;
}

void *mnodeGetNextDnode(void *pIter, SDnodeObj **pDnode) { 
  return tsDnodeSdb->fetchRow(pIter, (void **)pDnode); 
}

void mnodeCancelGetNextDnode(void *pIter) {
  tsDnodeSdb->freeIter(pIter);
}

int32_t mnodeGetDnodesNum() {
  return tsDnodeSdb->getNumOfRows();
}

int32_t mnodeGetOnlinDnodesCpuCoreNum() {
  SDnodeObj *pDnode = NULL;
  void *     pIter = NULL;
  int32_t    cpuCores = 0;

  while (1) {
    pIter = mnodeGetNextDnode(pIter, &pDnode);
    if (pDnode == NULL) break;
    if (pDnode->status != TAOS_DN_STATUS_OFFLINE) {
      cpuCores += pDnode->numOfCores;
    }
  }

  if (cpuCores < 2) cpuCores = 2;
  return cpuCores;
}

int32_t mnodeGetOnlineDnodesNum() {
  SDnodeObj *pDnode = NULL;
  void *     pIter = NULL;
  int32_t    onlineDnodes = 0;

  while (1) {
    pIter = mnodeGetNextDnode(pIter, &pDnode);
    if (pDnode == NULL) break;
    if (pDnode->status != TAOS_DN_STATUS_OFFLINE) onlineDnodes++;
  }

  return onlineDnodes;
}

void *mnodeGetDnode(int32_t dnodeId) {
  return tsDnodeSdb->getRow(&dnodeId);
}

SDnodeObj *mnodeGetDnodeByEp(char *ep) {
  SDnodeObj *pDnode = NULL;
  void *     pIter = NULL;

  while (1) {
    pIter = mnodeGetNextDnode(pIter, &pDnode);
    if (pDnode == NULL) break;
    if (strcmp(ep, pDnode->dnodeEp) == 0) {
      mnodeCancelGetNextDnode(pIter);
      return pDnode;
    }
  }


  return NULL;
}

void SDnodeObj::update(int status) {
  SSdbRow row;
  row.type = SDB_OPER_GLOBAL;
  row.pTable = tsDnodeSdb.get();
  row.pObj = shared_from_this();

  this->status = status;
  int32_t code = row.Update();
  if (code != TSDB_CODE_SUCCESS && code != TSDB_CODE_MND_ACTION_IN_PROGRESS) {
    mError("dnode:%d, failed update", dnodeId);
  }
}

int32_t mnodeProcessCfgDnodeMsg(SMnodeMsg *pMsg) {
  if (strcmp(pMsg->pUser->user, TSDB_DEFAULT_USER) != 0) {
    mError("failed to cfg dnode, no rights");
    return TSDB_CODE_MND_NO_RIGHTS;
  }
  
  SCfgDnodeMsg *pCmCfgDnode = static_cast<SCfgDnodeMsg *>(pMsg->rpcMsg.pCont);
  if (pCmCfgDnode->ep[0] == 0) {
    tstrncpy(pCmCfgDnode->ep, tsLocalEp, TSDB_EP_LEN);
  }

  SDnodeObj *pDnode = mnodeGetDnodeByEp(pCmCfgDnode->ep);
  if (pDnode == NULL) {
    int32_t dnodeId = strtol(pCmCfgDnode->ep, NULL, 10);
    if (dnodeId <= 0 || dnodeId > 65536) {
      mError("failed to cfg dnode, invalid dnodeEp:%s", pCmCfgDnode->ep);
      return TSDB_CODE_MND_DNODE_NOT_EXIST;
    }

    pDnode = static_cast<SDnodeObj *>(mnodeGetDnode(dnodeId));
    if (pDnode == NULL) {
      mError("failed to cfg dnode, invalid dnodeId:%d", dnodeId);
      return TSDB_CODE_MND_DNODE_NOT_EXIST;
    }
  }

  SRpcEpSet epSet = mnodeGetEpSetFromIp(pDnode->dnodeEp);

  if (strncasecmp(pCmCfgDnode->config, "balance", 7) == 0) {
    int32_t vnodeId = 0;
    int32_t dnodeId = 0;
    bool parseOk = taosCheckBalanceCfgOptions(pCmCfgDnode->config + 8, &vnodeId, &dnodeId);
    if (!parseOk) {
      return TSDB_CODE_MND_INVALID_DNODE_CFG_OPTION;
    }

    int32_t code = bnAlterDnode(pDnode, vnodeId, dnodeId);
    return code;
  } else {
    SCfgDnodeMsg *pMdCfgDnode = static_cast<SCfgDnodeMsg *>(rpcMallocCont(sizeof(SCfgDnodeMsg)));
    strcpy(pMdCfgDnode->ep, pCmCfgDnode->ep);
    strcpy(pMdCfgDnode->config, pCmCfgDnode->config);

    SRpcMsg rpcMdCfgDnodeMsg;
    rpcMdCfgDnodeMsg.ahandle = 0;
    rpcMdCfgDnodeMsg.code = 0;
    rpcMdCfgDnodeMsg.msgType = TSDB_MSG_TYPE_MD_CONFIG_DNODE;
    rpcMdCfgDnodeMsg.pCont = pMdCfgDnode;
    rpcMdCfgDnodeMsg.contLen = sizeof(SCfgDnodeMsg);

    mInfo("dnode:%s, is configured by %s", pCmCfgDnode->ep, pMsg->pUser->user);
    dnodeSendMsgToDnode(&epSet, &rpcMdCfgDnodeMsg);
    return TSDB_CODE_SUCCESS;
  }
}

void mnodeProcessCfgDnodeMsgRsp(SRpcMsg *rpcMsg) {
  mInfo("cfg dnode rsp is received");
}

static int32_t mnodeCheckClusterCfgPara(const SClusterCfg *clusterCfg) {
  if (clusterCfg->numOfMnodes != htonl(tsNumOfMnodes)) {
    mError("\"numOfMnodes\"[%d - %d] cfg parameters inconsistent", clusterCfg->numOfMnodes, htonl(tsNumOfMnodes));
    return TAOS_DN_OFF_NUM_OF_MNODES_NOT_MATCH;
  }
  if (clusterCfg->mnodeEqualVnodeNum != htonl(tsMnodeEqualVnodeNum)) {
    mError("\"mnodeEqualVnodeNum\"[%d - %d] cfg parameters inconsistent", clusterCfg->mnodeEqualVnodeNum,
           htonl(tsMnodeEqualVnodeNum));
    return TAOS_DN_OFF_MN_EQUAL_VN_NOT_MATCH;
  }
  if (clusterCfg->offlineThreshold != htonl(tsOfflineThreshold)) {
    mError("\"offlineThreshold\"[%d - %d] cfg parameters inconsistent", clusterCfg->offlineThreshold,
           htonl(tsOfflineThreshold));
    return TAOS_DN_OFF_OFFLINE_THRESHOLD_NOT_MATCH;
  }
  if (clusterCfg->statusInterval != htonl(tsStatusInterval)) {
    mError("\"statusInterval\"[%d - %d] cfg parameters inconsistent", clusterCfg->statusInterval,
           htonl(tsStatusInterval));
    return TAOS_DN_OFF_STATUS_INTERVAL_NOT_MATCH;
  }
  if (clusterCfg->maxtablesPerVnode != htonl(tsMaxTablePerVnode)) {
    mError("\"maxTablesPerVnode\"[%d - %d] cfg parameters inconsistent", clusterCfg->maxtablesPerVnode,
           htonl(tsMaxTablePerVnode));
    return TAOS_DN_OFF_MAX_TAB_PER_VN_NOT_MATCH;
  }
  if (clusterCfg->maxVgroupsPerDb != htonl(tsMaxVgroupsPerDb)) {
    mError("\"maxVgroupsPerDb\"[%d - %d]  cfg parameters inconsistent", clusterCfg->maxVgroupsPerDb,
           htonl(tsMaxVgroupsPerDb));
    return TAOS_DN_OFF_MAX_VG_PER_DB_NOT_MATCH;
  }
  if (0 != strncasecmp(clusterCfg->arbitrator, tsArbitrator, strlen(tsArbitrator))) {
    mError("\"arbitrator\"[%s - %s]  cfg parameters inconsistent", clusterCfg->arbitrator, tsArbitrator);
    return TAOS_DN_OFF_ARBITRATOR_NOT_MATCH;
  }

  int64_t checkTime = 0;
  char    timestr[32] = "1970-01-01 00:00:00.00";
  (void)taosParseTime(timestr, &checkTime, strlen(timestr), TSDB_TIME_PRECISION_MILLI, 0);
  if ((0 != strncasecmp(clusterCfg->timezone, tsTimezone, strlen(tsTimezone))) &&
      (checkTime != clusterCfg->checkTime)) {
    mError("\"timezone\"[%s - %s] [%" PRId64 " - %" PRId64 "] cfg parameters inconsistent", clusterCfg->timezone,
           tsTimezone, clusterCfg->checkTime, checkTime);
    return TAOS_DN_OFF_TIME_ZONE_NOT_MATCH;
  }

  if (0 != strncasecmp(clusterCfg->locale, tsLocale, strlen(tsLocale))) {
    mError("\"locale\"[%s - %s]  cfg parameters inconsistent", clusterCfg->locale, tsLocale);
    return TAOS_DN_OFF_LOCALE_NOT_MATCH;
  }
  if (0 != strncasecmp(clusterCfg->charset, tsCharset, strlen(tsCharset))) {
    mError("\"charset\"[%s - %s] cfg parameters inconsistent.", clusterCfg->charset, tsCharset);
    return TAOS_DN_OFF_CHARSET_NOT_MATCH;
  }

  if (clusterCfg->enableBalance != tsEnableBalance) {
    mError("\"balance\"[%d - %d] cfg parameters inconsistent", clusterCfg->enableBalance, tsEnableBalance);
    return TAOS_DN_OFF_ENABLE_BALANCE_NOT_MATCH;
  }
  if (clusterCfg->flowCtrl != tsEnableFlowCtrl) {
    mError("\"flowCtrl\"[%d - %d] cfg parameters inconsistent", clusterCfg->flowCtrl, tsEnableFlowCtrl);
    return TAOS_DN_OFF_FLOW_CTRL_NOT_MATCH;
  }
  if (clusterCfg->slaveQuery != tsEnableSlaveQuery) {
    mError("\"slaveQuery\"[%d - %d] cfg parameters inconsistent", clusterCfg->slaveQuery, tsEnableSlaveQuery);
    return TAOS_DN_OFF_SLAVE_QUERY_NOT_MATCH;
  }
  if (clusterCfg->adjustMaster != tsEnableAdjustMaster) {
    mError("\"adjustMaster\"[%d - %d] cfg parameters inconsistent", clusterCfg->adjustMaster, tsEnableAdjustMaster);
    return TAOS_DN_OFF_ADJUST_MASTER_NOT_MATCH;
  }

  return 0;
}

static int32_t mnodeGetDnodeEpsSize() {
  std::lock_guard<std::mutex> _(tsDnodeEpsMutex);
  return tsDnodeEpsSize;
}

static void mnodeGetDnodeEpsData(SDnodeEps *pEps, int32_t epsSize) {
  std::lock_guard<std::mutex> _(tsDnodeEpsMutex);
  if (epsSize == tsDnodeEpsSize) {
    memcpy(pEps, tsDnodeEps, tsDnodeEpsSize);
  }
}

static void mnodeUpdateDnodeEps() {
  std::lock_guard<std::mutex> _(tsDnodeEpsMutex);

  int32_t totalDnodes = mnodeGetDnodesNum();
  tsDnodeEpsSize = sizeof(SDnodeEps) + totalDnodes * sizeof(SDnodeEp);
  free(tsDnodeEps);
  tsDnodeEps = static_cast<SDnodeEps *>(calloc(1, tsDnodeEpsSize));
  tsDnodeEps->dnodeNum = htonl(totalDnodes);

  SDnodeObj *pDnode = NULL;
  void *     pIter = NULL;
  int32_t    dnodesNum = 0;

  while (1) {
    pIter = mnodeGetNextDnode(pIter, &pDnode);
    if (pDnode == NULL) break;
    if (dnodesNum >= totalDnodes) {
      mnodeCancelGetNextDnode(pIter);
      break;
    }

    SDnodeEp *pEp = &tsDnodeEps->dnodeEps[dnodesNum];
    dnodesNum++;
    pEp->dnodeId = htonl(pDnode->dnodeId);
    pEp->dnodePort = htons(pDnode->dnodePort);
    tstrncpy(pEp->dnodeFqdn, pDnode->dnodeFqdn, TSDB_FQDN_LEN);
  }
}

int32_t mnodeProcessDnodeStatusMsg(SMnodeMsg *pMsg) {
  SDnodeObj *pDnode     = NULL;
  SStatusMsg *pStatus = static_cast<SStatusMsg *>(pMsg->rpcMsg.pCont);
  pStatus->dnodeId      = htonl(pStatus->dnodeId);
  pStatus->moduleStatus = htonl(pStatus->moduleStatus);
  pStatus->lastReboot   = htonl(pStatus->lastReboot);
  pStatus->numOfCores   = htons(pStatus->numOfCores);

  uint32_t version = htonl(pStatus->version);
  if (version != tsVersion) {
    pDnode = static_cast<SDnodeObj *>(mnodeGetDnodeByEp(pStatus->dnodeEp));
    if (pDnode != NULL && pDnode->status != TAOS_DN_STATUS_READY) {
      pDnode->offlineReason = TAOS_DN_OFF_VERSION_NOT_MATCH;
    }
    mError("dnode:%d, status msg version:%d not equal with cluster:%d", pStatus->dnodeId, version, tsVersion);
    return TSDB_CODE_MND_INVALID_MSG_VERSION;
  }

  if (pStatus->dnodeId == 0) {
    pDnode = mnodeGetDnodeByEp(pStatus->dnodeEp);
    if (pDnode == NULL) {
      mDebug("dnode %s not created", pStatus->dnodeEp);
      return TSDB_CODE_MND_DNODE_NOT_EXIST;
    }
  } else {
    pDnode = static_cast<SDnodeObj *>(mnodeGetDnode(pStatus->dnodeId));
    if (pDnode == NULL) {
      pDnode = mnodeGetDnodeByEp(pStatus->dnodeEp);
      if (pDnode != NULL && pDnode->status != TAOS_DN_STATUS_READY) {
        pDnode->offlineReason = TAOS_DN_OFF_DNODE_ID_NOT_MATCH;
      }
      mError("dnode:%d, %s not exist", pStatus->dnodeId, pStatus->dnodeEp);
      return TSDB_CODE_MND_DNODE_NOT_EXIST;
    }
  }

  pDnode->lastReboot       = pStatus->lastReboot;
  pDnode->numOfCores       = pStatus->numOfCores;
  pDnode->diskAvailable    = pStatus->diskAvailable;
  pDnode->alternativeRole  = pStatus->alternativeRole;
  pDnode->moduleStatus     = pStatus->moduleStatus;

  if (pStatus->dnodeId == 0) {
    mDebug("dnode:%d %s, first access, set clusterId %s", pDnode->dnodeId, pDnode->dnodeEp, mnodeGetClusterId());
  } else {
    if (strncmp(pStatus->clusterId, mnodeGetClusterId(), TSDB_CLUSTER_ID_LEN - 1) != 0) {
      if (pDnode != NULL && pDnode->status != TAOS_DN_STATUS_READY) {
        pDnode->offlineReason = TAOS_DN_OFF_CLUSTER_ID_NOT_MATCH;
      }
      mError("dnode:%d, input clusterId %s not match with exist %s", pDnode->dnodeId, pStatus->clusterId,
             mnodeGetClusterId());
      return TSDB_CODE_MND_INVALID_CLUSTER_ID;
    } else {
      mTrace("dnode:%d, status received, access times %d openVnodes:%d:%d", pDnode->dnodeId, pDnode->lastAccess,
             htons(pStatus->openVnodes), pDnode->openVnodes.load());
    }
  }

  int32_t openVnodes = htons(pStatus->openVnodes);
  int32_t epsSize = mnodeGetDnodeEpsSize();
  int32_t vgAccessSize = openVnodes * sizeof(SVgroupAccess);
  int32_t contLen = sizeof(SStatusRsp) + vgAccessSize + epsSize;

  SStatusRsp *pRsp = static_cast<SStatusRsp *>(rpcMallocCont(contLen));
  if (pRsp == NULL) {
    return TSDB_CODE_MND_OUT_OF_MEMORY;
  }

  pRsp->dnodeCfg.dnodeId = htonl(pDnode->dnodeId);
  pRsp->dnodeCfg.moduleStatus = htonl((int32_t)pDnode->isMgmt);
  pRsp->dnodeCfg.numOfVnodes = htonl(openVnodes);
  tstrncpy(pRsp->dnodeCfg.clusterId, mnodeGetClusterId(), TSDB_CLUSTER_ID_LEN);
  SVgroupAccess *pAccess = (SVgroupAccess *)((char *)pRsp + sizeof(SStatusRsp));
  
  for (int32_t j = 0; j < openVnodes; ++j) {
    SVnodeLoad *pVload = &pStatus->load[j];
    pVload->vgId = htonl(pVload->vgId);
    pVload->dbCfgVersion = htonl(pVload->dbCfgVersion);
    pVload->vgCfgVersion = htonl(pVload->vgCfgVersion);
    pVload->vnodeVersion = htobe64(pVload->vnodeVersion);

    SVgObj *pVgroup = mnodeGetVgroup(pVload->vgId);
    if (pVgroup == NULL) {
      SRpcEpSet epSet = mnodeGetEpSetFromIp(pDnode->dnodeEp);
      mInfo("dnode:%d, vgId:%d not exist in mnode, drop it", pDnode->dnodeId, pVload->vgId);
      mnodeSendDropVnodeMsg(pVload->vgId, &epSet, NULL);
    } else {
      mnodeUpdateVgroupStatus(pVgroup, pDnode, pVload);
      pAccess->vgId = htonl(pVload->vgId);
      pAccess->accessState = pVgroup->accessState;
      pAccess++;
    }
  }

  if (pDnode->status == TAOS_DN_STATUS_OFFLINE) {
    // Verify whether the cluster parameters are consistent when status change from offline to ready
    int32_t ret = mnodeCheckClusterCfgPara(&(pStatus->clusterCfg));
    if (0 != ret) {
      pDnode->offlineReason = ret;
      rpcFreeCont(pRsp);
      mError("dnode:%d, %s cluster cfg parameters inconsistent, reason:%s", pDnode->dnodeId, pStatus->dnodeEp,
             offlineReason[ret]);
      return TSDB_CODE_MND_CLUSTER_CFG_INCONSISTENT;
    }

    mInfo("dnode:%d, from offline to online", pDnode->dnodeId);
    pDnode->status = TAOS_DN_STATUS_READY;
    pDnode->offlineReason = TAOS_DN_OFF_ONLINE;
    bnCheckModules();
    bnNotify();
  }

  if (openVnodes != pDnode->openVnodes) {
    mnodeCheckUnCreatedVgroup(pDnode, pStatus->load, openVnodes);
  }

  pDnode->lastAccess = tsAccessSquence;

  //this func should be called after sdb replica changed
  mnodeGetMnodeInfos(&pRsp->mnodes);
  
  SDnodeEps *pEps = (SDnodeEps *)((char *)pRsp + sizeof(SStatusRsp) + vgAccessSize);
  mnodeGetDnodeEpsData(pEps, epsSize);

  pMsg->rpcRsp.len = contLen;
  pMsg->rpcRsp.rsp =  pRsp;

  return TSDB_CODE_SUCCESS;
}

static int32_t mnodeCreateDnode(char *ep, SMnodeMsg *pMsg) {
  int32_t grantCode = grantCheck(TSDB_GRANT_DNODE);
  if (grantCode != TSDB_CODE_SUCCESS) {
    return grantCode;
  }

  char dnodeEp[TSDB_EP_LEN] = {0};
  tstrncpy(dnodeEp, ep, TSDB_EP_LEN);
  strtrim(dnodeEp);

  char *temp = strchr(dnodeEp, ':');
  if (!temp) {
    int len = strlen(dnodeEp);
    if (dnodeEp[len - 1] == ';') dnodeEp[len - 1] = 0;
    len = strlen(dnodeEp);
    snprintf(dnodeEp + len, TSDB_EP_LEN - len, ":%d", tsServerPort);
  }
  ep = dnodeEp;

  SDnodeObj *pDnode = mnodeGetDnodeByEp(ep);
  if (pDnode != NULL) {
    mError("dnode:%d, already exist, %s:%d", pDnode->dnodeId, pDnode->dnodeFqdn, pDnode->dnodePort);
    return TSDB_CODE_MND_DNODE_ALREADY_EXIST;
  }

  pDnode = (SDnodeObj *) calloc(1, sizeof(SDnodeObj));
  pDnode->createdTime = taosGetTimestampMs();
  pDnode->status = TAOS_DN_STATUS_OFFLINE; 
  pDnode->offlineReason = TAOS_DN_OFF_STATUS_NOT_RECEIVED;
  tstrncpy(pDnode->dnodeEp, ep, TSDB_EP_LEN);
  taosGetFqdnPortFromEp(ep, pDnode->dnodeFqdn, &pDnode->dnodePort);

  SSdbRow row;
  row.type = SDB_OPER_GLOBAL;
  row.pTable = tsDnodeSdb.get();
  row.pObj.reset(pDnode);
  row.rowSize = sizeof(SDnodeObj);
  row.pMsg = pMsg;

  int32_t code = row.Insert();
  if (code != TSDB_CODE_SUCCESS && code != TSDB_CODE_MND_ACTION_IN_PROGRESS) {
    int dnodeId = pDnode->dnodeId;
    tfree(pDnode);
    mError("failed to create dnode:%d, reason:%s", dnodeId, tstrerror(code));
  } else {
    mLInfo("dnode:%d is created", pDnode->dnodeId);
  }

  return code;
}

int32_t SDnodeObj::drop(void *pMsg) {
  SSdbRow row;
  row.type = SDB_OPER_GLOBAL;
  row.pTable = tsDnodeSdb.get();
  row.pObj = shared_from_this();
  row.pMsg = static_cast<SMnodeMsg *>(pMsg);

  int32_t code = row.Delete();
  if (code != TSDB_CODE_SUCCESS && code != TSDB_CODE_MND_ACTION_IN_PROGRESS) {
    mError("dnode:%d, failed to drop from cluster, result:%s", dnodeId, tstrerror(code));
  } else {
    mLInfo("dnode:%d, is dropped from cluster", dnodeId);
  }

  return code;
}

static int32_t mnodeDropDnodeByEp(char *ep, SMnodeMsg *pMsg) {
  SDnodeObj *pDnode = mnodeGetDnodeByEp(ep);
  if (pDnode == NULL) {
    int32_t dnodeId = (int32_t)strtol(ep, NULL, 10);
    pDnode = static_cast<SDnodeObj *>(mnodeGetDnode(dnodeId));
    if (pDnode == NULL) {
      mError("dnode:%s, is not exist", ep);
      return TSDB_CODE_MND_DNODE_NOT_EXIST;
    }
  }

  if (strcmp(pDnode->dnodeEp, mnodeGetMnodeMasterEp()) == 0) {
    mError("dnode:%d, can't drop dnode:%s which is master", pDnode->dnodeId, ep);
    return TSDB_CODE_MND_NO_REMOVE_MASTER;
  }

  mInfo("dnode:%d, start to drop it", pDnode->dnodeId);

  int32_t code = bnDropDnode(pDnode);
  return code;
}

int32_t mnodeProcessCreateDnodeMsg(SMnodeMsg *pMsg) {
  SCreateDnodeMsg *pCreate = static_cast<SCreateDnodeMsg * >(pMsg->rpcMsg.pCont);

  if (strcmp(pMsg->pUser->user, TSDB_DEFAULT_USER) != 0) {
    return TSDB_CODE_MND_NO_RIGHTS;
  } else {
    return mnodeCreateDnode(pCreate->ep, pMsg);
  }
}

int32_t mnodeProcessDropDnodeMsg(SMnodeMsg *pMsg) {
  SDropDnodeMsg *pDrop = static_cast<SDropDnodeMsg *>(pMsg->rpcMsg.pCont);

  if (strcmp(pMsg->pUser->user, TSDB_DEFAULT_USER) != 0) {
    return TSDB_CODE_MND_NO_RIGHTS;
  } else {
    return mnodeDropDnodeByEp(pDrop->ep, pMsg);
  }
}

int32_t mnodeGetDnodeMeta(STableMetaMsg *pMeta, SShowObj *pShow, void *pConn) {
  SUserObj *pUser = mnodeGetUserFromConn(pConn);
  if (pUser == NULL) return 0;

  if (strcmp(pUser->pAcct->user, TSDB_DEFAULT_USER) != 0) {
    return TSDB_CODE_MND_NO_RIGHTS;
  }

  int32_t  cols = 0;
  SSchema *pSchema = pMeta->schema;

  pShow->bytes[cols] = 2;
  pSchema[cols].type = TSDB_DATA_TYPE_SMALLINT;
  strcpy(pSchema[cols].name, "id");
  pSchema[cols].bytes = htons(pShow->bytes[cols]);
  cols++;

  pShow->bytes[cols] = 40 + VARSTR_HEADER_SIZE;
  pSchema[cols].type = TSDB_DATA_TYPE_BINARY;
  strcpy(pSchema[cols].name, "end_point");
  pSchema[cols].bytes = htons(pShow->bytes[cols]);
  cols++;

  pShow->bytes[cols] = 2;
  pSchema[cols].type = TSDB_DATA_TYPE_SMALLINT;
  strcpy(pSchema[cols].name, "vnodes");
  pSchema[cols].bytes = htons(pShow->bytes[cols]);
  cols++;

  pShow->bytes[cols] = 2;
  pSchema[cols].type = TSDB_DATA_TYPE_SMALLINT;
  strcpy(pSchema[cols].name, "cores");
  pSchema[cols].bytes = htons(pShow->bytes[cols]);
  cols++;

  pShow->bytes[cols] = 10 + VARSTR_HEADER_SIZE;
  pSchema[cols].type = TSDB_DATA_TYPE_BINARY;
  strcpy(pSchema[cols].name, "status");
  pSchema[cols].bytes = htons(pShow->bytes[cols]);
  cols++;

  pShow->bytes[cols] = 5 + VARSTR_HEADER_SIZE;
  pSchema[cols].type = TSDB_DATA_TYPE_BINARY;
  strcpy(pSchema[cols].name, "role");
  pSchema[cols].bytes = htons(pShow->bytes[cols]);
  cols++;

  pShow->bytes[cols] = 8;
  pSchema[cols].type = TSDB_DATA_TYPE_TIMESTAMP;
  strcpy(pSchema[cols].name, "create_time");
  pSchema[cols].bytes = htons(pShow->bytes[cols]);
  cols++;

  pShow->bytes[cols] = 24 + VARSTR_HEADER_SIZE;
  pSchema[cols].type = TSDB_DATA_TYPE_BINARY;
  strcpy(pSchema[cols].name, "offline reason");
  pSchema[cols].bytes = htons(pShow->bytes[cols]);
  cols++;

  pMeta->numOfColumns = htons(cols);
  pShow->numOfColumns = cols;

  pShow->offset[0] = 0;
  for (int32_t i = 1; i < cols; ++i) {
    pShow->offset[i] = pShow->offset[i - 1] + pShow->bytes[i - 1];
  }

  pShow->numOfRows = mnodeGetDnodesNum();
  if (tsArbitrator[0] != 0) {
    pShow->numOfRows++;
  }

  pShow->rowSize = pShow->offset[cols - 1] + pShow->bytes[cols - 1];
  pShow->pIter = NULL;

  return 0;
}

int32_t mnodeRetrieveDnodes(SShowObj *pShow, char *data, int32_t rows, void *pConn) {
  int32_t    numOfRows = 0;
  int32_t    cols      = 0;
  SDnodeObj *pDnode    = NULL;
  char      *pWrite;

  while (numOfRows < rows) {
    pShow->pIter = mnodeGetNextDnode(pShow->pIter, &pDnode);
    if (pDnode == NULL) break;

    cols = 0;

    pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
    *(int16_t *)pWrite = pDnode->dnodeId;
    cols++;

    pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
    STR_WITH_MAXSIZE_TO_VARSTR(pWrite, pDnode->dnodeEp, pShow->bytes[cols]);
    cols++;

    pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
    *(int16_t *)pWrite = pDnode->openVnodes;
    cols++;

    pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
    *(int16_t *)pWrite = pDnode->numOfCores;
    cols++;
    
    pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;  
    const char* status = dnodeStatus[pDnode->status];
    STR_TO_VARSTR(pWrite, status);
    cols++;

    pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;  
    const char* role = dnodeRoles[pDnode->alternativeRole];
    STR_TO_VARSTR(pWrite, role);
    cols++;

    pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
    *(int64_t *)pWrite = pDnode->createdTime;
    cols++;

    pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
    STR_TO_VARSTR(pWrite, offlineReason[pDnode->offlineReason]);
    cols++;

    numOfRows++;
  }

  if (tsArbitrator[0] != 0) {
    cols = 0;

    pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
    *(int16_t *)pWrite = 0;
    cols++;

    pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
    STR_WITH_MAXSIZE_TO_VARSTR(pWrite, tsArbitrator, pShow->bytes[cols]);
    cols++;

    pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
    *(int16_t *)pWrite = 0;
    cols++;

    pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
    *(int16_t *)pWrite = 0;
    cols++;

    pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
    const char *status = dnodeStatus[tsArbOnline > 0 ? TAOS_DN_STATUS_READY : TAOS_DN_STATUS_OFFLINE];
    STR_TO_VARSTR(pWrite, status);
    cols++;

    pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
    STR_TO_VARSTR(pWrite, "arb");
    cols++;

    pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
    *(int64_t *)pWrite = 0;
    cols++;

    pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
    STR_TO_VARSTR(pWrite, "-");
    cols++;

    numOfRows++;
  }

  mnodeVacuumResult(data, pShow->numOfColumns, numOfRows, rows, pShow);
  pShow->numOfReads += numOfRows;
  return numOfRows;
}

static bool mnodeCheckModuleInDnode(SDnodeObj *pDnode, int32_t moduleType) {
  uint32_t status = pDnode->moduleStatus & (1u << moduleType);
  return status > 0;
}

int32_t mnodeGetModuleMeta(STableMetaMsg *pMeta, SShowObj *pShow, void *pConn) {
  int32_t cols = 0;

  SUserObj *pUser = mnodeGetUserFromConn(pConn);
  if (pUser == NULL) return 0;

  if (strcmp(pUser->user, TSDB_DEFAULT_USER) != 0)  {
    return TSDB_CODE_MND_NO_RIGHTS;
  }

  SSchema *pSchema = pMeta->schema;

  pShow->bytes[cols] = 2;
  pSchema[cols].type = TSDB_DATA_TYPE_SMALLINT;
  strcpy(pSchema[cols].name, "id");
  pSchema[cols].bytes = htons(pShow->bytes[cols]);
  cols++;

  pShow->bytes[cols] = 40 + VARSTR_HEADER_SIZE;
  pSchema[cols].type = TSDB_DATA_TYPE_BINARY;
  strcpy(pSchema[cols].name, "end_point");
  pSchema[cols].bytes = htons(pShow->bytes[cols]);
  cols++;

  pShow->bytes[cols] = 8 + VARSTR_HEADER_SIZE;
  pSchema[cols].type = TSDB_DATA_TYPE_BINARY;
  strcpy(pSchema[cols].name, "module");
  pSchema[cols].bytes = htons(pShow->bytes[cols]);
  cols++;

  pShow->bytes[cols] = 8 + VARSTR_HEADER_SIZE;
  pSchema[cols].type = TSDB_DATA_TYPE_BINARY;
  strcpy(pSchema[cols].name, "status");
  pSchema[cols].bytes = htons(pShow->bytes[cols]);
  cols++;

  pMeta->numOfColumns = htons(cols);
  pShow->numOfColumns = cols;

  pShow->offset[0] = 0;
  for (int32_t i = 1; i < cols; ++i) {
    pShow->offset[i] = pShow->offset[i - 1] + pShow->bytes[i - 1];
  }

  pShow->numOfRows = mnodeGetDnodesNum() * TSDB_MOD_MAX;
  pShow->rowSize = pShow->offset[cols - 1] + pShow->bytes[cols - 1];
  pShow->pIter = NULL;

  return 0;
}

int32_t mnodeRetrieveModules(SShowObj *pShow, char *data, int32_t rows, void *pConn) {
  int32_t numOfRows = 0;

  char* pWrite;
  const char* moduleName[5] = { "MNODE", "HTTP", "MONITOR", "MQTT", "UNKNOWN" };
  int32_t cols;

  while (numOfRows < rows) {
    SDnodeObj *pDnode = NULL;
    pShow->pIter = mnodeGetNextDnode(pShow->pIter, (SDnodeObj **)&pDnode);
    if (pDnode == NULL) break;

    for (int32_t moduleType = 0; moduleType < TSDB_MOD_MAX; ++moduleType) {
      cols = 0;

      pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
      *(int16_t *)pWrite = pDnode->dnodeId;
      cols++;

      pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
      STR_WITH_MAXSIZE_TO_VARSTR(pWrite, pDnode->dnodeEp, pShow->bytes[cols] - 1);
      cols++;

      pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
      STR_WITH_MAXSIZE_TO_VARSTR(pWrite, moduleName[moduleType], pShow->bytes[cols]);
      cols++;

      pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
      bool enable = mnodeCheckModuleInDnode(pDnode, moduleType);

      const char* v = enable? "enable":"disable";
      STR_TO_VARSTR(pWrite, v);
      cols++;

      numOfRows++;
    }
  }

  mnodeVacuumResult(data, pShow->numOfColumns, numOfRows, rows, pShow);
  pShow->numOfReads += numOfRows;
  return numOfRows;
}

static bool mnodeCheckConfigShow(SGlobalCfg *cfg) {
  if (!(cfg->cfgType & TSDB_CFG_CTYPE_B_SHOW))
    return false;
  return true;
}

int32_t mnodeGetConfigMeta(STableMetaMsg *pMeta, SShowObj *pShow, void *pConn) {
  int32_t cols = 0;

  SUserObj *pUser = mnodeGetUserFromConn(pConn);
  if (pUser == NULL) return 0;

  if (strcmp(pUser->user, TSDB_DEFAULT_USER) != 0)  {
    return TSDB_CODE_MND_NO_RIGHTS;
  }

  SSchema *pSchema = pMeta->schema;

  pShow->bytes[cols] = TSDB_CFG_OPTION_LEN + VARSTR_HEADER_SIZE;
  pSchema[cols].type = TSDB_DATA_TYPE_BINARY;
  tstrncpy(pSchema[cols].name, "name", sizeof(pSchema[cols].name));
  pSchema[cols].bytes = htons(pShow->bytes[cols]);
  cols++;

  pShow->bytes[cols] = TSDB_CFG_VALUE_LEN + VARSTR_HEADER_SIZE;
  pSchema[cols].type = TSDB_DATA_TYPE_BINARY;
  tstrncpy(pSchema[cols].name, "value", sizeof(pSchema[cols].name));
  pSchema[cols].bytes = htons(pShow->bytes[cols]);
  cols++;

  pMeta->numOfColumns = htons(cols);
  pShow->numOfColumns = cols;

  pShow->offset[0] = 0;
  for (int32_t i = 1; i < cols; ++i) pShow->offset[i] = pShow->offset[i - 1] + pShow->bytes[i - 1];

  pShow->numOfRows = 0;
  for (auto it = tsGlobalConfig.rbegin(); it != tsGlobalConfig.rend(); it++) {
    SGlobalCfg &cfg = *it;
    if (!mnodeCheckConfigShow(&cfg)) continue;
    pShow->numOfRows++;
  }

  pShow->rowSize = pShow->offset[cols - 1] + pShow->bytes[cols - 1];
  pShow->pIter = NULL;

  return 0;
}

int32_t mnodeRetrieveConfigs(SShowObj *pShow, char *data, int32_t rows, void *pConn) {
  int32_t numOfRows = 0;

  for (auto it = tsGlobalConfig.rbegin(); it != tsGlobalConfig.rend() && numOfRows < rows; it++) {
    SGlobalCfg &cfg = *it;
    if (!mnodeCheckConfigShow(&cfg)) continue;

    char *pWrite;
    int32_t   cols = 0;

    pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
    STR_WITH_MAXSIZE_TO_VARSTR(pWrite, cfg.option, TSDB_CFG_OPTION_LEN);

    cols++;
    int32_t t = 0;

    pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
    switch (cfg.valType) {
      case TAOS_CFG_VTYPE_INT8:
        t = snprintf(static_cast<char *>(varDataVal(pWrite)), TSDB_CFG_VALUE_LEN, "%d", *((int8_t *)cfg.ptr));
        varDataSetLen(pWrite, t);
        numOfRows++;
        break;
      case TAOS_CFG_VTYPE_INT16:
        t = snprintf(static_cast<char *>(varDataVal(pWrite)), TSDB_CFG_VALUE_LEN, "%d", *((int16_t *)cfg.ptr));
        varDataSetLen(pWrite, t);
        numOfRows++;
        break;
      case TAOS_CFG_VTYPE_INT32:
        t = snprintf(static_cast<char *>(varDataVal(pWrite)), TSDB_CFG_VALUE_LEN, "%d", *((int32_t *)cfg.ptr));
        varDataSetLen(pWrite, t);
        numOfRows++;
        break;
      case TAOS_CFG_VTYPE_FLOAT:
        t = snprintf(static_cast<char *>(varDataVal(pWrite)), TSDB_CFG_VALUE_LEN, "%f", *((float *)cfg.ptr));
        varDataSetLen(pWrite, t);
        numOfRows++;
        break;
      case TAOS_CFG_VTYPE_STRING:
      case TAOS_CFG_VTYPE_IPSTR:
      case TAOS_CFG_VTYPE_DIRECTORY:
        STR_WITH_MAXSIZE_TO_VARSTR(pWrite, static_cast<const char *>(cfg.ptr), TSDB_CFG_VALUE_LEN);
        numOfRows++;
        break;
      default:
        break;
    }
  }

  mnodeVacuumResult(data, pShow->numOfColumns, numOfRows, rows, pShow);
  pShow->numOfReads += numOfRows;
  return numOfRows;
}

int32_t mnodeGetVnodeMeta(STableMetaMsg *pMeta, SShowObj *pShow, void *pConn) {
  int32_t cols = 0;
  SUserObj *pUser = mnodeGetUserFromConn(pConn);
  if (pUser == NULL) return 0;
  
  if (strcmp(pUser->user, TSDB_DEFAULT_USER) != 0)  {
    return TSDB_CODE_MND_NO_RIGHTS;
  }

  SSchema *pSchema = pMeta->schema;

  pShow->bytes[cols] = 4;
  pSchema[cols].type = TSDB_DATA_TYPE_INT;
  strcpy(pSchema[cols].name, "vnode");
  pSchema[cols].bytes = htons(pShow->bytes[cols]);
  cols++;

  pShow->bytes[cols] = 12 + VARSTR_HEADER_SIZE;
  pSchema[cols].type = TSDB_DATA_TYPE_BINARY;
  strcpy(pSchema[cols].name, "status");
  pSchema[cols].bytes = htons(pShow->bytes[cols]);
  cols++;

  pMeta->numOfColumns = htons(cols);
  pShow->numOfColumns = cols;

  pShow->offset[0] = 0;
  for (int32_t i = 1; i < cols; ++i) pShow->offset[i] = pShow->offset[i - 1] + pShow->bytes[i - 1];

  SDnodeObj *pDnode = NULL;
  if (pShow->payloadLen > 0 ) {
    pDnode = mnodeGetDnodeByEp(pShow->payload);
  } else {
    void *pIter = mnodeGetNextDnode(NULL, (SDnodeObj **)&pDnode);
    mnodeCancelGetNextDnode(pIter);
  }

  if (pDnode != NULL) {
    pShow->numOfRows += pDnode->openVnodes;
  }

  pShow->rowSize = pShow->offset[cols - 1] + pShow->bytes[cols - 1];
  pShow->pIter = pDnode;

  return 0;
}

int32_t mnodeRetrieveVnodes(SShowObj *pShow, char *data, int32_t rows, void *pConn) {
  int32_t    numOfRows = 0;
  SDnodeObj *pDnode = NULL;
  char *     pWrite;
  int32_t    cols = 0;

  if (0 == rows) return 0;

  pDnode = (SDnodeObj *)(pShow->pIter);
  if (pDnode != NULL) {
    void *pIter = NULL;
    SVgObj *pVgroup;
    while (1) {
      pIter = mnodeGetNextVgroup(pIter, &pVgroup);
      if (pVgroup == NULL) break;

      for (int32_t i = 0; i < pVgroup->numOfVnodes; ++i) {
        SVnodeGid *pVgid = &pVgroup->vnodeGid[i];
        if (pVgid->pDnode == pDnode) {
          cols = 0;

          pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
          *(uint32_t *)pWrite = pVgroup->vgId;
          cols++;

          pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
          strcpy(pWrite, syncRole[pVgid->role]);
          cols++;
        }
      }
    }
  } else {
    numOfRows = 0;
  }

  mnodeVacuumResult(data, pShow->numOfColumns, numOfRows, rows, pShow);
  pShow->numOfReads += numOfRows;
  return numOfRows;
}

const char* dnodeStatus[] = {
  "offline",
  "dropping",
  "balancing",
  "ready",
  "undefined"
};

const char* dnodeRoles[] = {
  "any",
  "mnode",
  "vnode",
  "any"
};
