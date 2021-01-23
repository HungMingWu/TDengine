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
#include "vnodeStatus.h"
#include "vnodeRead.h"
#include "vnodeWrite.h"

char* vnodeStatus[] = {
  "init",
  "ready",
  "closing",
  "updating",
  "reset"
};

bool SVnodeObj::SetStatus(EVnodeStatus status) {
  std::lock_guard<std::mutex> lock(statusMutex);
  if (status == TAOS_VN_STATUS_READY) {
    this->status = TAOS_VN_STATUS_READY;
    qQueryMgmtReOpen(this->qMgmt);
    return true;
  } else if (status == TAOS_VN_STATUS_UPDATING) {
    bool ret = this->status == TAOS_VN_STATUS_READY;
    this->status = TAOS_VN_STATUS_UPDATING;
    return ret;
  }
}

static bool vnodeSetClosingStatusImp(SVnodeObj* pVnode) {
  bool set = false;
  std::lock_guard<std::mutex> lock(pVnode->statusMutex);

  if (pVnode->status == TAOS_VN_STATUS_READY || pVnode->status == TAOS_VN_STATUS_INIT) {
    pVnode->status = TAOS_VN_STATUS_CLOSING;
    set = true;
  }
  return set;
}

bool vnodeSetClosingStatus(SVnodeObj* pVnode) {
  while (!vnodeSetClosingStatusImp(pVnode)) {
    taosMsleep(1);
  }

  // release local resources only after cutting off outside connections
  qQueryMgmtNotifyClosed(pVnode->qMgmt);
  vnodeWaitReadCompleted(pVnode);
  vnodeWaitWriteCompleted(pVnode);

  return true;
}

static bool vnodeSetResetStatusImp(SVnodeObj* pVnode) {
  bool set = false;
  std::lock_guard<std::mutex> lock(pVnode->statusMutex);

  if (pVnode->status == TAOS_VN_STATUS_READY || pVnode->status == TAOS_VN_STATUS_INIT) {
    pVnode->status = TAOS_VN_STATUS_RESET;
    set = true;
  }

  return set;
}

bool vnodeSetResetStatus(SVnodeObj* pVnode) {
  while (!vnodeSetResetStatusImp(pVnode)) {
    taosMsleep(1);
  }

  // release local resources only after cutting off outside connections
  qQueryMgmtNotifyClosed(pVnode->qMgmt);
  vnodeWaitReadCompleted(pVnode);
  vnodeWaitWriteCompleted(pVnode);

  return true;
}

bool vnodeInReadyOrUpdatingStatus(SVnodeObj* pVnode) {
  std::lock_guard<std::mutex> lock(pVnode->statusMutex);
  return (pVnode->status == TAOS_VN_STATUS_READY || pVnode->status == TAOS_VN_STATUS_UPDATING);
}

bool SVnodeObj::InStatus(EVnodeStatus status) {
  std::lock_guard<std::mutex> lock(statusMutex);
  return this->status == status;
}
