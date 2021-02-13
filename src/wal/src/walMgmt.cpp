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
#include "taoserror.h"
#include "tref.h"
#include "tfile.h"
#include "walInt.h"

static int32_t walInitObj(SWal *pWal);
static void    walFreeObj(void *pWal);

struct SWalMgmt 
{
  int32_t   refId = 0;
  int32_t   seq = 0;
  int8_t    stop = 0;
  std::thread thread;

 protected:
  bool needFsync(SWal *pWal);
  void updateSeq();
  void fsyncAll();
 public:
  SWalMgmt();
  ~SWalMgmt();
};

SWalMgmt::SWalMgmt() {
  refId = taosOpenRef(TSDB_MIN_VNODES, walFreeObj);
  thread = std::thread([this] { 
      while (1) {
        updateSeq();
        fsyncAll();
        if (stop) break;
      }
  });
}

SWalMgmt::~SWalMgmt() {
  stop = 1;
  thread.join();

  wDebug("wal thread is stopped");
  taosCloseRef(refId);
  wInfo("wal module is cleaned up");
}

static SWalMgmt tsWal;

void SWalMgmt::updateSeq() {
  taosMsleep(WAL_REFRESH_MS);
  if (++seq <= 0) {
    seq = 1;
  }
}

bool SWalMgmt::needFsync(SWal *pWal) {
  if (pWal->fsyncPeriod <= 0 || pWal->level != TAOS_WAL_FSYNC) {
    return false;
  }

  if (seq % pWal->fsyncSeq == 0) {
    return true;
  }

  return false;
}

void SWalMgmt::fsyncAll() {
  SWal *pWal = (SWal *)taosIterateRef(refId, 0);
  while (pWal) {
    if (needFsync(pWal)) {
      wTrace("vgId:%d, do fsync, level:%d seq:%d rseq:%d", pWal->vgId, pWal->level, pWal->fsyncSeq, tsWal.seq);
      int32_t code = pWal->tfd->fsync();
      if (code != 0) {
        wError("vgId:%d, file:%s, failed to fsync since %s", pWal->vgId, pWal->name, strerror(code));
      }
    }
    pWal = (SWal *)taosIterateRef(refId, pWal->rid);
  }
}

SWal *walOpen(char *path, SWalCfg *pCfg) {
  auto pWal = new SWal;
  if (pWal == NULL) {
    terrno = TAOS_SYSTEM_ERROR(errno);
    return NULL;
  }

  pWal->vgId = pCfg->vgId;
  pWal->fileId = -1;
  pWal->level = pCfg->walLevel;
  pWal->keep = pCfg->keep;
  pWal->fsyncPeriod = pCfg->fsyncPeriod;
  tstrncpy(pWal->path, path, sizeof(pWal->path));

  pWal->fsyncSeq = pCfg->fsyncPeriod / 1000;
  if (pWal->fsyncSeq <= 0) pWal->fsyncSeq = 1;

  if (walInitObj(pWal) != TSDB_CODE_SUCCESS) {
    walFreeObj(pWal);
    return NULL;
  }

   pWal->rid = taosAddRef(tsWal.refId, pWal);
   if (pWal->rid < 0) {
    walFreeObj(pWal);
    return NULL;
  }

  wDebug("vgId:%d, wal:%p is opened, level:%d fsyncPeriod:%d", pWal->vgId, pWal, pWal->level, pWal->fsyncPeriod);

  return pWal;
}

int32_t SWal::alter(SWalCfg *pCfg) {
  if (level == pCfg->walLevel && fsyncPeriod == pCfg->fsyncPeriod) {
    wDebug("vgId:%d, old walLevel:%d fsync:%d, new walLevel:%d fsync:%d not change", vgId, level,
           fsyncPeriod, pCfg->walLevel, pCfg->fsyncPeriod);
    return TSDB_CODE_SUCCESS;
  }

  wInfo("vgId:%d, change old walLevel:%d fsync:%d, new walLevel:%d fsync:%d", vgId, level,
        fsyncPeriod, pCfg->walLevel, pCfg->fsyncPeriod);

  level = pCfg->walLevel;
  fsyncPeriod = pCfg->fsyncPeriod;
  fsyncSeq = pCfg->fsyncPeriod % 1000;
  if (fsyncSeq <= 0) fsyncSeq = 1;

  return TSDB_CODE_SUCCESS;
}

void SWal::stop() {
  mutex.lock();
  stop_ = 1;
  mutex.unlock();
  wDebug("vgId:%d, stop write wal", vgId);
}

void SWal::close() {
  mutex.lock();
  tfd.reset();
  mutex.unlock();
  taosRemoveRef(tsWal.refId, rid);
}

static int32_t walInitObj(SWal *pWal) {
  if (taosMkDir(pWal->path, 0755) != 0) {
    wError("vgId:%d, path:%s, failed to create directory since %s", pWal->vgId, pWal->path, strerror(errno));
    return TAOS_SYSTEM_ERROR(errno);
  }

  wDebug("vgId:%d, object is initialized", pWal->vgId);
  return TSDB_CODE_SUCCESS;
}

static void walFreeObj(void *wal) {
  SWal *pWal = (SWal*)wal;
  wDebug("vgId:%d, wal:%p is freed", pWal->vgId, pWal);

  pWal->tfd.reset();
  tfree(pWal);
}
