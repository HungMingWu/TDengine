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
STsdbBufPool *tsdbNewBufPool() {
  auto pBufPool = new (std::nothrow) STsdbBufPool;
  if (pBufPool == NULL) {
    terrno = TSDB_CODE_TDB_OUT_OF_MEMORY;
    tsdbFreeBufPool(pBufPool);
    return NULL;
  }

  int code = pthread_cond_init(&(pBufPool->poolNotEmpty), NULL);
  if (code != 0) {
    terrno = TAOS_SYSTEM_ERROR(code);
    tsdbFreeBufPool(pBufPool);
    return NULL;
  }

  return pBufPool;
}

void tsdbFreeBufPool(STsdbBufPool *pBufPool) {
  if (pBufPool) {
    ASSERT(pBufPool->bufBlockList.empty());

    pthread_cond_destroy(&pBufPool->poolNotEmpty);

    delete pBufPool;
  }
}

int tsdbOpenBufPool(STsdbRepo *pRepo) {
  STsdbCfg *    pCfg = &(pRepo->config);
  STsdbBufPool *pPool = pRepo->pPool;

  ASSERT(pPool != NULL);

  pPool->bufBlockSize = pCfg->cacheBlockSize * 1024 * 1024; // MB
  pPool->tBufBlocks = pCfg->totalBlocks;
  pPool->nBufBlocks = 0;
  pPool->index = 0;

  for (int i = 0; i < pCfg->totalBlocks; i++) {
    STsdbBufBlock *pBufBlock = tsdbNewBufBlock(pPool->bufBlockSize);
    if (pBufBlock == NULL) goto _err;

    pPool->bufBlockList.push_back(pBufBlock);


    pPool->nBufBlocks++;
  }

  tsdbDebug("vgId:%d buffer pool is opened! bufBlockSize:%d tBufBlocks:%d nBufBlocks:%d", REPO_ID(pRepo),
            pPool->bufBlockSize, pPool->tBufBlocks, pPool->nBufBlocks);

  return 0;

_err:
  tsdbCloseBufPool(pRepo);
  return -1;
}

void tsdbCloseBufPool(STsdbRepo *pRepo) {
  if (pRepo == NULL) return;

  STsdbBufPool * pBufPool = pRepo->pPool;

  if (pBufPool) {
    for (auto &pBufBlock : pBufPool->bufBlockList)
      tsdbFreeBufBlock(pBufBlock);
  }

  tsdbDebug("vgId:%d, buffer pool is closed", REPO_ID(pRepo));
}

STsdbBufBlock *tsdbAllocBufBlockFromPool(STsdbRepo *pRepo) {
  ASSERT(pRepo != NULL && pRepo->pPool != NULL);
  ASSERT(IS_REPO_LOCKED(pRepo));

  STsdbBufPool *pBufPool = pRepo->pPool;

  while (pBufPool->bufBlockList.empty()) {
    pRepo->repoLocked = false;
    pthread_cond_wait(&(pBufPool->poolNotEmpty), &(pRepo->mutex));
    pRepo->repoLocked = true;
  }

  auto pBufBlock = pBufPool->bufBlockList.front();
  pBufPool->bufBlockList.pop_front();
  pBufBlock->blockId = pBufPool->index++;
  pBufBlock->offset = 0;
  pBufBlock->remain = pBufPool->bufBlockSize;

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