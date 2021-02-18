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

#define _DEFAULT_SOURCE
#include "os.h"
#include "taoserror.h"
#include "taosmsg.h"
#include "tglobal.h"
// #include "tfs.h"
#include "query.h"
#include "dnode.h"
#include "vnodeCfg.h"
#include "vnodeStatus.h"
#include "vnodeSync.h"
#include "vnodeVersion.h"
#include "vnodeMgmt.h"
#include "vnodeWorker.h"
#include "vnodeMain.h"
#include "vnodeWrite.h"
#include "tqueue.h"

static int32_t vnodeProcessTsdbStatus(void *arg, int32_t status, int32_t eno);

int32_t vnodeCreate(SCreateVnodeMsg *pVnodeCfg) {
  int32_t code;

  SVnodeObj *pVnode = vnodeAcquire(pVnodeCfg->cfg.vgId);
  if (pVnode != NULL) {
    vDebug("vgId:%d, vnode already exist, refCount:%d pVnode:%p", pVnodeCfg->cfg.vgId, pVnode->refCount, pVnode);
    pVnode->Release();
    return TSDB_CODE_SUCCESS;
  }

  if (mkdir(tsVnodeDir, 0755) != 0 && errno != EEXIST) {
    vError("vgId:%d, failed to create vnode, reason:%s dir:%s", pVnodeCfg->cfg.vgId, strerror(errno), tsVnodeDir);
    if (errno == EACCES) {
      return TSDB_CODE_VND_NO_DISK_PERMISSIONS;
    } else if (errno == ENOSPC) {
      return TSDB_CODE_VND_NO_DISKSPACE;
    } else if (errno == ENOENT) {
      return TSDB_CODE_VND_NO_SUCH_FILE_OR_DIR;
    } else {
      return TSDB_CODE_VND_INIT_FAILED;
    }
  }

  char rootDir[TSDB_FILENAME_LEN] = {0};
  sprintf(rootDir, "%s/vnode%d", tsVnodeDir, pVnodeCfg->cfg.vgId);
  if (mkdir(rootDir, 0755) != 0 && errno != EEXIST) {
    vError("vgId:%d, failed to create vnode, reason:%s dir:%s", pVnodeCfg->cfg.vgId, strerror(errno), rootDir);
    if (errno == EACCES) {
      return TSDB_CODE_VND_NO_DISK_PERMISSIONS;
    } else if (errno == ENOSPC) {
      return TSDB_CODE_VND_NO_DISKSPACE;
    } else if (errno == ENOENT) {
      return TSDB_CODE_VND_NO_SUCH_FILE_OR_DIR;
    } else {
      return TSDB_CODE_VND_INIT_FAILED;
    }
  }

  code = vnodeWriteCfg(pVnodeCfg);
  if (code != TSDB_CODE_SUCCESS) {
    vError("vgId:%d, failed to save vnode cfg, reason:%s", pVnodeCfg->cfg.vgId, tstrerror(code));
    return code;
  }

  STsdbCfg tsdbCfg = {0};
  tsdbCfg.tsdbId              = pVnodeCfg->cfg.vgId;
  tsdbCfg.cacheBlockSize      = pVnodeCfg->cfg.cacheBlockSize;
  tsdbCfg.totalBlocks         = pVnodeCfg->cfg.totalBlocks;
  tsdbCfg.daysPerFile         = pVnodeCfg->cfg.daysPerFile;
  tsdbCfg.keep                = pVnodeCfg->cfg.daysToKeep;
  tsdbCfg.minRowsPerFileBlock = pVnodeCfg->cfg.minRowsPerFileBlock;
  tsdbCfg.maxRowsPerFileBlock = pVnodeCfg->cfg.maxRowsPerFileBlock;
  tsdbCfg.precision           = pVnodeCfg->cfg.precision;
  tsdbCfg.compression         = pVnodeCfg->cfg.compression;
  tsdbCfg.update              = pVnodeCfg->cfg.update;
  tsdbCfg.cacheLastRow        = pVnodeCfg->cfg.cacheLastRow;

  char tsdbDir[TSDB_FILENAME_LEN] = {0};
  sprintf(tsdbDir, "%s/vnode%d/tsdb", tsVnodeDir, pVnodeCfg->cfg.vgId);
  if (tsdbCreateRepo(tsdbDir, &tsdbCfg) < 0) {
    vError("vgId:%d, failed to create tsdb in vnode, reason:%s", pVnodeCfg->cfg.vgId, tstrerror(terrno));
    return TSDB_CODE_VND_INIT_FAILED;
  }

  vInfo("vgId:%d, vnode dir is created, walLevel:%d fsyncPeriod:%d", pVnodeCfg->cfg.vgId, pVnodeCfg->cfg.walLevel,
        pVnodeCfg->cfg.fsyncPeriod);
  code = vnodeOpen(pVnodeCfg->cfg.vgId);

  return code;
}

int32_t vnodeDrop(int32_t vgId) {
  SVnodeObj *pVnode = vnodeAcquire(vgId);
  if (pVnode == NULL) {
    vDebug("vgId:%d, failed to drop, vnode not find", vgId);
    return TSDB_CODE_VND_INVALID_VGROUP_ID;
  }

  vInfo("vgId:%d, vnode will be dropped, refCount:%d pVnode:%p", pVnode->vgId, pVnode->refCount, pVnode);
  pVnode->dropped = 1;

  // remove from hash, so new messages wont be consumed
  pVnode->RemoveFromHash();
  pVnode->Release();
  pVnode->WriteIntoMWorker(VNODE_WORKER_ACTION_CLEANUP);

  return TSDB_CODE_SUCCESS;
}

static int32_t vnodeAlterImp(SVnodeObj *pVnode, SCreateVnodeMsg *pVnodeCfg) {
  int32_t code = vnodeWriteCfg(pVnodeCfg);
  if (code != TSDB_CODE_SUCCESS) {
    return code; 
  }

  code = vnodeReadCfg(pVnode);
  if (code != TSDB_CODE_SUCCESS) {
    return code; 
  }

  code = pVnode->wal->alter(&pVnode->walCfg);
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }

  code = syncReconfig(pVnode->sync, &pVnode->syncCfg);
  if (code != TSDB_CODE_SUCCESS) {
    return code; 
  }

  if (pVnode->tsdb) {
    code = tsdbConfigRepo(pVnode->tsdb, &pVnode->tsdbCfg);
    if (code != TSDB_CODE_SUCCESS) {
      return code;
    }
  }

  return 0;
}

int32_t vnodeAlter(SVnodeObj *pVnode, SCreateVnodeMsg *pVnodeCfg) {
  if (pVnode->dbCfgVersion == pVnodeCfg->cfg.dbCfgVersion && pVnode->vgCfgVersion == pVnodeCfg->cfg.vgCfgVersion) {
    vDebug("vgId:%d, dbCfgVersion:%d and vgCfgVersion:%d not change", pVnode->vgId, pVnode->dbCfgVersion,
           pVnode->vgCfgVersion);
    return TSDB_CODE_SUCCESS;
  }

  // vnode in non-ready state and still needs to return success instead of TSDB_CODE_VND_INVALID_STATUS
  // dbCfgVersion can be corrected by status msg
  if (!pVnode->SetStatus(TAOS_VN_STATUS_UPDATING)) {
    vDebug("vgId:%d, vnode is not ready, do alter operation later", pVnode->vgId);
    return TSDB_CODE_SUCCESS;
  }

  int32_t code = vnodeAlterImp(pVnode, pVnodeCfg);
  pVnode->SetStatus(TAOS_VN_STATUS_READY);

  if (code != 0) {
    vError("vgId:%d, failed to alter vnode, code:0x%x", pVnode->vgId, code);
  } else {
    vDebug("vgId:%d, vnode is altered", pVnode->vgId);
  }

  return code;
}

int32_t vnodeOpen(int32_t vgId) {
  char temp[TSDB_FILENAME_LEN * 3];
  char rootDir[TSDB_FILENAME_LEN * 2];
  snprintf(rootDir, TSDB_FILENAME_LEN * 2, "%s/vnode%d", tsVnodeDir, vgId);

  auto pVnode = new (std::nothrow) SVnodeObj{};
  if (pVnode == NULL) {
    vError("vgId:%d, failed to open vnode since no enough memory", vgId);
    return TAOS_SYSTEM_ERROR(errno);
  }

  atomic_add_fetch_32(&pVnode->refCount, 1);

  pVnode->vgId     = vgId;
  pVnode->tsdbCfg.tsdbId = pVnode->vgId;
  pVnode->rootDir = strdup(rootDir);
  pVnode->accessState = TSDB_VN_ALL_ACCCESS;
  tsem_init(&pVnode->sem, 0, 0);

  int32_t code = vnodeReadCfg(pVnode);
  if (code != TSDB_CODE_SUCCESS) {
    pVnode->CleanUp();
    return code;
  } 

  code = vnodeReadVersion(pVnode);
  if (code != TSDB_CODE_SUCCESS) {
    vError("vgId:%d, failed to read file version, generate it from data file", pVnode->vgId);
    // Allow vnode start even when read file version fails, set file version as wal version or zero
    // pVnode->CleanUp();
    // return code;
  }

  pVnode->fversion = pVnode->version;
  
  pVnode->wqueue = dnodeAllocVWriteQueue(pVnode);
  pVnode->qqueue.reset(dnodeAllocVQueryQueue(pVnode));
  pVnode->fqueue.reset(dnodeAllocVFetchQueue(pVnode));
  if (pVnode->wqueue == NULL || pVnode->qqueue == NULL || pVnode->fqueue == NULL) {
    pVnode->CleanUp();
    return terrno;
  }

  if (tsEnableStream) {
    SCqCfg cqCfg = {0};
    sprintf(cqCfg.user, "_root");
    strcpy(cqCfg.pass, tsInternalPass);
    strcpy(cqCfg.db, pVnode->db);
    cqCfg.vgId = vgId;
    cqCfg.cqWrite = vnodeWriteToCache;
    pVnode->cq = cqOpen(pVnode, &cqCfg);
    if (pVnode->cq == NULL) {
      pVnode->CleanUp();
      return terrno;
    }
  }

  STsdbAppH appH = {0};
  appH.appH = (void *)pVnode;
  appH.notifyStatus = vnodeProcessTsdbStatus;
  appH.cqH = pVnode->cq;
  appH.cqCreateFunc = cqCreate;
  appH.cqDropFunc = cqDrop;
  sprintf(temp, "%s/tsdb", rootDir);

  terrno = 0;
  pVnode->tsdb = tsdbOpenRepo(temp, &appH);
  if (pVnode->tsdb == NULL) {
    pVnode->CleanUp();
    return terrno;
  } else if (terrno != TSDB_CODE_SUCCESS) {
    vError("vgId:%d, failed to open tsdb, replica:%d reason:%s", pVnode->vgId, pVnode->syncCfg.replica,
           tstrerror(terrno));
    if (pVnode->syncCfg.replica <= 1) {
      pVnode->CleanUp();
      return terrno;
    } else {
      pVnode->fversion = 0;
      pVnode->version = 0;
    }
  }

  sprintf(temp, "%s/wal", rootDir);
  pVnode->walCfg.vgId = pVnode->vgId;
  pVnode->wal = walOpen(temp, &pVnode->walCfg);
  if (pVnode->wal == NULL) { 
    pVnode->CleanUp();
    return terrno;
  }

  pVnode->wal->restore(pVnode, vnodeProcessWrite);
  if (pVnode->version == 0) {
    pVnode->fversion = 0;
    pVnode->version = pVnode->wal->getVersion();
  }

  code = tsdbSyncCommit(pVnode->tsdb);
  if (code != 0) {
    vError("vgId:%d, failed to commit after restore from wal since %s", pVnode->vgId, tstrerror(code));
    pVnode->CleanUp();
    return code;
  }

  pVnode->wal->removeAllOldFiles();
  pVnode->wal->renew();

  pVnode->qMgmt = qOpenQueryMgmt(pVnode->vgId);
  if (pVnode->qMgmt == NULL) {
    pVnode->CleanUp();
    return terrno;
  }

  pVnode->events = NULL;

  vDebug("vgId:%d, vnode is opened in %s, pVnode:%p", pVnode->vgId, rootDir, pVnode);
  tsdbIncCommitRef(pVnode->vgId);

  pVnode->AddIntoHash();

  SSyncInfo syncInfo;
  syncInfo.vgId = pVnode->vgId;
  syncInfo.version = pVnode->version;
  syncInfo.syncCfg = pVnode->syncCfg;
  tstrncpy(syncInfo.path, rootDir, TSDB_FILENAME_LEN);
  syncInfo.getWalInfo = vnodeGetWalInfo;
  syncInfo.getFileInfo = vnodeGetFileInfo;
  syncInfo.writeToCache = vnodeWriteToCache;
  syncInfo.confirmForward = vnodeConfirmForard; 
  syncInfo.notifyRole = vnodeNotifyRole;
  syncInfo.notifyFlowCtrl = vnodeCtrlFlow;
  syncInfo.notifyFileSynced = vnodeNotifyFileSynced;
  syncInfo.getVersion = vnodeGetVersion;
  pVnode->sync = syncStart(&syncInfo);

  if (pVnode->sync <= 0) {
    vError("vgId:%d, failed to open sync, replica:%d reason:%s", pVnode->vgId, pVnode->syncCfg.replica,
           tstrerror(terrno));
    pVnode->RemoveFromHash();
    pVnode->CleanUp();
    return terrno;
  }

  pVnode->SetStatus(TAOS_VN_STATUS_READY);
  return TSDB_CODE_SUCCESS;
}

int32_t vnodeClose(int32_t vgId) {
  SVnodeObj *pVnode = vnodeAcquire(vgId);
  if (pVnode == NULL) return 0;

  vDebug("vgId:%d, vnode will be closed, pVnode:%p", pVnode->vgId, pVnode);
  pVnode->RemoveFromHash();
  pVnode->Release();
  pVnode->CleanUp();

  return 0;
}

void SVnodeObj::Destroy() {
  int32_t code = 0;
  int32_t vgId = vgId;

  if (qMgmt) {
    qCleanupQueryMgmt(qMgmt);
  }

  if (wal) {
    wal->stop();
  }

  if (tsdb) {
    code = tsdbCloseRepo(tsdb, 1);
  }

  // stop continuous query
  if (cq) {
    cqClose(cq);
  }

  if (wal) {
    if (code != 0) {
      vError("vgId:%d, failed to commit while close tsdb repo, keep wal", vgId);
    } else {
      wal->removeAllOldFiles();
    }
    wal->stop();
  }

  wqueue.reset();
  qqueue.reset();
  fqueue.reset();

  tfree(rootDir);

  if (dropped) {
    char rootDir[TSDB_FILENAME_LEN] = {0};    
    char newDir[TSDB_FILENAME_LEN] = {0};
    sprintf(rootDir, "%s/vnode%d", tsVnodeDir, vgId);
    sprintf(newDir, "%s/vnode%d", tsVnodeBakDir, vgId);

    if (0 == tsEnableVnodeBak) {
      vInfo("vgId:%d, vnode backup not enabled", vgId);
    } else {
      taosRemoveDir(newDir);
      taosRename(rootDir, newDir);
    }

    taosRemoveDir(rootDir);
    dnodeSendStatusMsgToMnode();
  }

  tsem_destroy(&sem);
  tsdbDecCommitRef(vgId);
  delete this;
}

void SVnodeObj::CleanUp() {
  vDebug("vgId:%d, vnode will cleanup, refCount:%d pVnode:%p", vgId, refCount, this);

  vnodeSetClosingStatus(this);

  // stop replication module
  if (sync) {
    syncStop(sync);
    sync.reset();
  }

  vDebug("vgId:%d, vnode is cleaned, refCount:%d pVnode:%p", vgId, refCount, this);
  Release();
}

static int32_t vnodeProcessTsdbStatus(void *arg, int32_t status, int32_t eno) {
  SVnodeObj *pVnode = (SVnodeObj*)arg;

  if (eno != TSDB_CODE_SUCCESS) {
    vError("vgId:%d, failed to commit since %s, fver:%" PRIu64 " vver:%" PRIu64, pVnode->vgId, tstrerror(eno),
           pVnode->fversion, pVnode->version);
    pVnode->isCommiting = 0;
    pVnode->isFull = 1;
    return 0;
  }

  if (status == TSDB_STATUS_COMMIT_START) {
    pVnode->isCommiting = 1;
    pVnode->cversion = pVnode->version;
    vDebug("vgId:%d, start commit, fver:%" PRIu64 " vver:%" PRIu64, pVnode->vgId, pVnode->fversion, pVnode->version);
    if (!pVnode->InStatus(TAOS_VN_STATUS_INIT)) {
      return pVnode->wal->renew();
    }
    return 0;
  }

  if (status == TSDB_STATUS_COMMIT_OVER) {
    pVnode->isCommiting = 0;
    pVnode->isFull = 0;
    pVnode->fversion = pVnode->cversion;
    vDebug("vgId:%d, commit over, fver:%" PRIu64 " vver:%" PRIu64, pVnode->vgId, pVnode->fversion, pVnode->version);
    if (!pVnode->InStatus(TAOS_VN_STATUS_INIT)) {
      pVnode->wal->removeOneOldFile();
    }
    return vnodeSaveVersion(pVnode);
  }

  return 0;
}

int32_t vnodeReset(SVnodeObj *pVnode) {
  char rootDir[128] = "\0";
  sprintf(rootDir, "%s/tsdb", pVnode->rootDir);

  if (!vnodeSetResetStatus(pVnode)) {
    return -1;
  }

  void *tsdb = pVnode->tsdb;
  pVnode->tsdb = NULL;
  
  // acquire vnode
  int32_t refCount = atomic_add_fetch_32(&pVnode->refCount, 1);

  if (refCount > 3) {
    tsem_wait(&pVnode->sem);
  }

  // close tsdb, then open tsdb
  tsdbCloseRepo(tsdb, 0);
  STsdbAppH appH = {0};
  appH.appH = (void *)pVnode;
  appH.notifyStatus = vnodeProcessTsdbStatus;
  appH.cqH = pVnode->cq;
  appH.cqCreateFunc = cqCreate;
  appH.cqDropFunc = cqDrop;
  pVnode->tsdb = tsdbOpenRepo(rootDir, &appH);

  pVnode->SetStatus(TAOS_VN_STATUS_READY);
  pVnode->Release();

  return 0;
}