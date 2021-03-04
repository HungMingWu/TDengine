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

#include "tsdb.h"
#include "tsdbMain.h"

static STsdbBufBlock *tsdbNewBufBlock(int bufBlockSize);
static void           tsdbFreeBufBlock(STsdbBufBlock *pBufBlock);

// ---------------- INTERNAL FUNCTIONS ----------------
STsdbBufPool::~STsdbBufPool() {
  ASSERT(bufBlockList.empty());
}

int tsdbOpenBufPool(STsdbRepo *pRepo) {
  STsdbCfg *    pCfg = &(pRepo->config);
  auto &pool = pRepo->pool;

  pool.bufBlockSize = pCfg->cacheBlockSize * 1024 * 1024;  // MB
  pool.tBufBlocks = pCfg->totalBlocks;
  pool.nBufBlocks = 0;
  pool.index = 0;

  for (int i = 0; i < pCfg->totalBlocks; i++) {
    STsdbBufBlock *pBufBlock = tsdbNewBufBlock(pool.bufBlockSize);
    if (pBufBlock == NULL) goto _err;

    pool.bufBlockList.push_back(pBufBlock);


    pool.nBufBlocks++;
  }

  tsdbDebug("vgId:%d buffer pool is opened! bufBlockSize:%d tBufBlocks:%d nBufBlocks:%d", REPO_ID(pRepo),
            pool.bufBlockSize, pool.tBufBlocks, pool.nBufBlocks);

  return 0;

_err:
  tsdbCloseBufPool(pRepo);
  return -1;
}

void tsdbCloseBufPool(STsdbRepo *pRepo) {
  if (pRepo == NULL) return;

  auto &pool = pRepo->pool;

  for (auto &pBufBlock : pool.bufBlockList)
      tsdbFreeBufBlock(pBufBlock);

  tsdbDebug("vgId:%d, buffer pool is closed", REPO_ID(pRepo));
}

STsdbBufBlock *tsdbAllocBufBlockFromPool(STsdbRepo *pRepo) {
  ASSERT(pRepo != NULL);

  auto &pool = pRepo->pool;
  std::unique_lock<std::mutex> lock(pRepo->mutex);
  while (pool.bufBlockList.empty()) {
    pool.poolNotEmpty.wait(lock);
  }

  auto pBufBlock = pool.bufBlockList.front();
  pool.bufBlockList.pop_front();
  pBufBlock->blockId = pool.index++;
  pBufBlock->offset = 0;
  pBufBlock->remain = pool.bufBlockSize;

  tsdbDebug("vgId:%d, buffer block is allocated, blockId:%" PRId64, REPO_ID(pRepo), pBufBlock->blockId);
  return pBufBlock;
}

// ---------------- LOCAL FUNCTIONS ----------------
static STsdbBufBlock *tsdbNewBufBlock(int bufBlockSize) {
  STsdbBufBlock *pBufBlock = (STsdbBufBlock *)malloc(sizeof(*pBufBlock) + bufBlockSize);
  if (pBufBlock == NULL) {
    terrno = TSDB_CODE_TDB_OUT_OF_MEMORY;
    goto _err;
  }

  pBufBlock->blockId = 0;
  pBufBlock->offset = 0;
  pBufBlock->remain = bufBlockSize;

  return pBufBlock;

_err:
  tsdbFreeBufBlock(pBufBlock);
  return NULL;
}

static void tsdbFreeBufBlock(STsdbBufBlock *pBufBlock) { tfree(pBufBlock); }