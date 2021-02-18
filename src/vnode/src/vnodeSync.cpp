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
#include "taosmsg.h"
#include "query.h"
#include "dnode.h"
#include "vnodeVersion.h"
#include "vnodeMain.h"
#include "vnodeMgmt.h"
#include "vnodeWrite.h"

uint32_t vnodeGetFileInfo(int32_t vgId, char *name, uint32_t *index, uint32_t eindex, int64_t *size, uint64_t *fver) {
  SVnodeObj *pVnode = vnodeAcquire(vgId);
  if (pVnode == NULL) {
    vError("vgId:%d, vnode not found while get file info", vgId);
    return 0;
  }

  *fver = pVnode->fversion;
  uint32_t ret = tsdbGetFileInfo(pVnode->tsdb, name, index, eindex, size);

  pVnode->Release();
  return ret;
}

int32_t vnodeGetWalInfo(int32_t vgId, char *fileName, int64_t *fileId) {
  SVnodeObj *pVnode = vnodeAcquire(vgId);
  if (pVnode == NULL) {
    vError("vgId:%d, vnode not found while get wal info", vgId);
    return -1;
  }

  int32_t code = pVnode->wal->getWalFile(fileName, fileId);

  pVnode->Release();
  return code;
}

void vnodeNotifyRole(int32_t vgId, int8_t role) {
  SVnodeObj *pVnode = vnodeAcquire(vgId);
  if (pVnode == NULL) {
    vTrace("vgId:%d, vnode not found while notify role", vgId);
    return;
  }

  vInfo("vgId:%d, sync role changed from %s to %s", pVnode->vgId, syncRole[pVnode->role], syncRole[role]);
  pVnode->role = role;
  dnodeSendStatusMsgToMnode();

  if (pVnode->role == TAOS_SYNC_ROLE_MASTER) {
    cqStart(pVnode->cq);
  } else {
    cqStop(pVnode->cq);
  }

  pVnode->Release();
}

void vnodeCtrlFlow(int32_t vgId, int32_t level) {
  SVnodeObj *pVnode = vnodeAcquire(vgId);
  if (pVnode == NULL) {
    vTrace("vgId:%d, vnode not found while flow ctrl", vgId);
    return;
  }

  if (pVnode->flowctrlLevel != level) {
    vDebug("vgId:%d, set flowctrl level from %d to %d", pVnode->vgId, pVnode->flowctrlLevel, level);
    pVnode->flowctrlLevel = level;
  }

  pVnode->Release();
}

int32_t vnodeNotifyFileSynced(int32_t vgId, uint64_t fversion) {
  SVnodeObj *pVnode = vnodeAcquire(vgId);
  if (pVnode == NULL) {
    vError("vgId:%d, vnode not found while notify file synced", vgId);
    return 0;
  }

  pVnode->fversion = fversion;
  pVnode->version = fversion;
  vnodeSaveVersion(pVnode);

  vDebug("vgId:%d, data file is synced, fver:%" PRIu64 " vver:%" PRIu64, vgId, fversion, fversion);
  int32_t code = vnodeReset(pVnode);

  pVnode->Release();
  return code;
}

void vnodeConfirmForard(int32_t vgId, void *wparam, int32_t code) {
  SVnodeObj *pVnode = vnodeAcquire(vgId);
  if (pVnode == NULL) {
    vError("vgId:%d, vnode not found while confirm forward", vgId);
    return;
  }

  dnodeSendRpcVWriteRsp(pVnode, wparam, code);
  pVnode->Release();
}

int32_t vnodeWriteToCache(int32_t vgId, SWalHead *pHead, int32_t qtype, void *rparam) {
  SVnodeObj *pVnode = vnodeAcquire(vgId);
  if (pVnode == NULL) {
    vError("vgId:%d, vnode not found while write to cache", vgId);
    return TSDB_CODE_VND_INVALID_VGROUP_ID;
  }

  int32_t code = vnodeWriteToWQueue(pVnode, pHead, qtype, rparam);

  pVnode->Release();
  return code;
}

int32_t vnodeGetVersion(int32_t vgId, uint64_t *fver, uint64_t *wver) {
  SVnodeObj *pVnode = vnodeAcquire(vgId);
  if (pVnode == NULL) {
    vError("vgId:%d, vnode not found while write to cache", vgId);
    return -1;
  }

  int32_t code = 0;
  if (pVnode->isCommiting) {
    vDebug("vgId:%d, vnode is commiting while get version", vgId);
    code = -1;
  } else {
    *fver = pVnode->fversion;
    *wver = pVnode->version;
  }

  pVnode->Release();
  return code;
}

void vnodeConfirmForward(void *vparam, uint64_t version, int32_t code) {
  SVnodeObj *pVnode = (SVnodeObj*)vparam;
  syncConfirmForward(pVnode->sync, version, code);
}