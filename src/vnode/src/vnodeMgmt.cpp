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
#include <initializer_list>
#include "os.h"
#include "dnode.h"
#include "vnodeStatus.h"
#include "vnodeWorker.h"
#include "vnodeRead.h"
#include "vnodeWrite.h"
#include "vnodeMain.h"

static SHashObj *tsVnodesHash = NULL;

static int32_t vnodeInitHash(void);
static void    vnodeCleanupHash(void);
static void    vnodeIncRef(void *ptNode);

static std::initializer_list<SStep> tsVnodeSteps = {
  {"vnode-worker", vnodeInitMWorker,    vnodeCleanupMWorker},
  {"vnode-write",  vnodeInitWrite,      vnodeCleanupWrite},
  {"vnode-read",   vnodeInitRead,       vnodeCleanupRead},
  {"vnode-hash",   vnodeInitHash,       vnodeCleanupHash},
  {"tsdb-queue",   tsdbInitCommitQueue, tsdbDestroyCommitQueue}
};

int32_t vnodeInitMgmt() {
  return dnodeStepInit(tsVnodeSteps);
}

void vnodeCleanupMgmt() {
  dnodeStepCleanup(tsVnodeSteps);
}

static int32_t vnodeInitHash() {
  tsVnodesHash = taosHashInit(TSDB_MIN_VNODES, taosGetDefaultHashFunction(TSDB_DATA_TYPE_INT), true, HASH_ENTRY_LOCK);
  if (tsVnodesHash == NULL) {
    vError("failed to init vnode mgmt");
    return -1;
  }

  return 0;
}

static void vnodeCleanupHash() {
  if (tsVnodesHash != NULL) {
    vDebug("vnode mgmt is cleanup");
    taosHashCleanup(tsVnodesHash);
    tsVnodesHash = NULL;
  }
}

SWal *vnodeGetWal(void *pVnode) {
  return ((SVnodeObj *)pVnode)->wal;
}

void SVnodeObj::AddIntoHash() {
  taosHashPut(tsVnodesHash, &vgId, sizeof(int32_t), this, sizeof(SVnodeObj *));
}

void SVnodeObj::RemoveFromHash() { 
  taosHashRemove(tsVnodesHash, &vgId, sizeof(int32_t));
}

static void vnodeIncRef(void *ptNode) {
  assert(ptNode != NULL);

  SVnodeObj **ppVnode = (SVnodeObj **)ptNode;
  assert(ppVnode);
  assert(*ppVnode);

  SVnodeObj *pVnode = *ppVnode;
  atomic_add_fetch_32(&pVnode->refCount, 1);
  vTrace("vgId:%d, get vnode, refCount:%d pVnode:%p", pVnode->vgId, pVnode->refCount, pVnode);
}

SVnodeObj *vnodeAcquire(int32_t vgId) {
  SVnodeObj **ppVnode = NULL;
  if (tsVnodesHash != NULL) {
    ppVnode = (SVnodeObj**)taosHashGetClone(tsVnodesHash, &vgId, sizeof(int32_t), vnodeIncRef, NULL, sizeof(void *));
  }

  if (ppVnode == NULL || *ppVnode == NULL) {
    terrno = TSDB_CODE_VND_INVALID_VGROUP_ID;
    vDebug("vgId:%d, not exist", vgId);
    return NULL;
  }

  return *ppVnode;
}

void SVnodeObj::Release() {
  int32_t refCount = atomic_sub_fetch_32(&refCount, 1);
  vTrace("vgId:%d, release vnode, refCount:%d pVnode:%p", vgId, refCount, this);
  assert(refCount >= 0);

  if (refCount > 0) {
    if (InStatus(TAOS_VN_STATUS_RESET) && refCount <= 3) {
      tsem_post(&sem);
    }
  } else {
    vDebug("vgId:%d, vnode will be destroyed, refCount:%d pVnode:%p", vgId, refCount, this);
    WriteIntoMWorker(VNODE_WORKER_ACTION_DESTROUY);
    int32_t count = taosHashGetSize(tsVnodesHash);
    vDebug("vgId:%d, vnode is destroyed, vnodes:%d", vgId, count);
  }
}

static void vnodeBuildVloadMsg(SVnodeObj *pVnode, SStatusMsg *pStatus) {
  int64_t totalStorage = 0;
  int64_t compStorage = 0;
  int64_t pointsWritten = 0;

  if (!pVnode->InStatus(TAOS_VN_STATUS_READY)) return;
  if (pStatus->openVnodes >= TSDB_MAX_VNODES) return;

  if (pVnode->tsdb) {
    tsdbReportStat(pVnode->tsdb, &pointsWritten, &totalStorage, &compStorage);
  }

  SVnodeLoad *pLoad = &pStatus->load[pStatus->openVnodes++];
  pLoad->vgId = htonl(pVnode->vgId);
  pLoad->dbCfgVersion = htonl(pVnode->dbCfgVersion);
  pLoad->vgCfgVersion = htonl(pVnode->vgCfgVersion);
  pLoad->totalStorage = htobe64(totalStorage);
  pLoad->compStorage = htobe64(compStorage);
  pLoad->pointsWritten = htobe64(pointsWritten);
  pLoad->vnodeVersion = htobe64(pVnode->version);
  pLoad->status = pVnode->status;
  pLoad->role = pVnode->role;
  pLoad->replica = pVnode->syncCfg.replica;  
}

void vnodeGetVnodeList(int32_t vnodeList[], int32_t *numOfVnodes) {
  void *pIter = taosHashIterate(tsVnodesHash, NULL);
  while (pIter) {
    SVnodeObj **pVnode = (SVnodeObj**)pIter;
    if (*pVnode) {

    (*numOfVnodes)++;
    if (*numOfVnodes >= TSDB_MAX_VNODES) {
      vError("vgId:%d, too many open vnodes, exist:%d max:%d", (*pVnode)->vgId, *numOfVnodes, TSDB_MAX_VNODES);
      continue;
    } else {
      vnodeList[*numOfVnodes - 1] = (*pVnode)->vgId;
    }

    }

    pIter = taosHashIterate(tsVnodesHash, pIter);    
  }
}

void vnodeBuildStatusMsg(void *param) {
  SStatusMsg *pStatus = (SStatusMsg *)param;

  void *pIter = taosHashIterate(tsVnodesHash, NULL);
  while (pIter) {
    SVnodeObj **pVnode = (SVnodeObj**)pIter;
    if (*pVnode) {
      vnodeBuildVloadMsg(*pVnode, pStatus);
    }
    pIter = taosHashIterate(tsVnodesHash, pIter);
  }
}

void vnodeSetAccess(SVgroupAccess *pAccess, int32_t numOfVnodes) {
  for (int32_t i = 0; i < numOfVnodes; ++i) {
    pAccess[i].vgId = htonl(pAccess[i].vgId);
    SVnodeObj *pVnode = vnodeAcquire(pAccess[i].vgId);
    if (pVnode != NULL) {
      pVnode->accessState = pAccess[i].accessState;
      if (pVnode->accessState != TSDB_VN_ALL_ACCCESS) {
        vDebug("vgId:%d, access state is set to %d", pAccess[i].vgId, pVnode->accessState);
      }
      pVnode->Release();
    }
  }
}