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
#include "tulog.h"
#include "tmempool.h"
#include "tutil.h"

struct pool_t {
  int             numOfFree;  /* number of free slots */
  int             first;      /* the first free slot  */
  int             numOfBlock; /* the number of blocks */
  int             blockSize;  /* block size in bytes  */
  int *           freeList;   /* the index list       */
  char *          pool;       /* the actual mem block */
  std::mutex      mutex;
};

mpool_h taosMemPoolInit(int numOfBlock, int blockSize) {
  int     i;

  if (numOfBlock <= 1 || blockSize <= 1) {
    uError("invalid parameter in memPoolInit\n");
    return NULL;
  }

  auto pool_p = new pool_t;
  pool_p->blockSize = blockSize;
  pool_p->numOfBlock = numOfBlock;
  pool_p->pool = (char *)malloc((size_t)(blockSize * numOfBlock));
  pool_p->freeList = (int *)malloc(sizeof(int) * (size_t)numOfBlock);

  if (pool_p->pool == NULL || pool_p->freeList == NULL) {
    uError("failed to allocate memory\n");
    tfree(pool_p->freeList);
    tfree(pool_p->pool);
    tfree(pool_p);
    return NULL;
  }

  memset(pool_p->pool, 0, (size_t)(blockSize * numOfBlock));
  for (i = 0; i < pool_p->numOfBlock; ++i) pool_p->freeList[i] = i;

  pool_p->first = 0;
  pool_p->numOfFree = pool_p->numOfBlock;

  return (mpool_h)pool_p;
}

char *taosMemPoolMalloc(mpool_h handle) {
  char *  pos = NULL;
  pool_t *pool_p = (pool_t *)handle;

  std::lock_guard<std::mutex> _(pool_p->mutex);

  if (pool_p->numOfFree > 0) {
    pos = pool_p->pool + pool_p->blockSize * (pool_p->freeList[pool_p->first]);
    pool_p->first++;
    pool_p->first = pool_p->first % pool_p->numOfBlock;
    pool_p->numOfFree--;
  }

  if (pos == NULL) uDebug("mempool: out of memory");
  return pos;
}

void taosMemPoolFree(mpool_h handle, char *pMem) {
  int     index;
  pool_t *pool_p = (pool_t *)handle;

  if (pMem == NULL) return;

  index = (int)(pMem - pool_p->pool) % pool_p->blockSize;
  if (index != 0) {
    uError("invalid free address:%p\n", pMem);
    return;
  }

  index = (int)((pMem - pool_p->pool) / pool_p->blockSize);
  if (index < 0 || index >= pool_p->numOfBlock) {
    uError("mempool: error, invalid address:%p\n", pMem);
    return;
  }

  memset(pMem, 0, (size_t)pool_p->blockSize);

  std::lock_guard<std::mutex> _(pool_p->mutex);

  pool_p->freeList[(pool_p->first + pool_p->numOfFree) % pool_p->numOfBlock] = index;
  pool_p->numOfFree++;
}

void taosMemPoolCleanUp(mpool_h handle) {
  pool_t *pool_p = (pool_t *)handle;

  if (pool_p->pool) free(pool_p->pool);
  if (pool_p->freeList) free(pool_p->freeList);
  delete pool_p;
}
