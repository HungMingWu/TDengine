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

#include "os.h"
#include "tulog.h"
#include "tqueue.h"
#include "tworker.h"

int32_t tWorkerInit(SWorkerPool *pPool) {
  pPool->qset.reset(new STaosQset());
  pPool->worker = (SWorker*)calloc(sizeof(SWorker), pPool->max);
  for (int i = 0; i < pPool->max; ++i) {
    SWorker *pWorker = pPool->worker + i;
    pWorker->id = i;
    pWorker->pPool = pPool;
  }

  uInfo("worker:%s is initialized, min:%d max:%d", pPool->name, pPool->min, pPool->max);
  return 0;
}

void tWorkerCleanup(SWorkerPool *pPool) {
  for (int i = 0; i < pPool->max; ++i) {
    SWorker *pWorker = pPool->worker + i;
    if(taosCheckPthreadValid(pWorker->thread)) {
      pPool->qset->threadResume();
    }
  }

  for (int i = 0; i < pPool->max; ++i) {
    SWorker *pWorker = pPool->worker + i;
    if (taosCheckPthreadValid(pWorker->thread)) {
      pthread_join(pWorker->thread, NULL);
    }
  }

  free(pPool->worker);
  pPool->qset.reset();

  uInfo("worker:%s is closed", pPool->name);
}

STaosQueue *tWorkerAllocQueue(SWorkerPool *pPool, void *ahandle) {
  std::lock_guard<std::mutex> lock(pPool->mutex);
  auto pQueue = new STaosQueue;

  pPool->qset->addIntoQset(pQueue, ahandle);

  // spawn a thread to process queue
  if (pPool->num < pPool->max) {
    do {
      SWorker *pWorker = pPool->worker + pPool->num;

      pthread_attr_t thAttr;
      pthread_attr_init(&thAttr);
      pthread_attr_setdetachstate(&thAttr, PTHREAD_CREATE_JOINABLE);

      if (pthread_create(&pWorker->thread, &thAttr, pPool->workerFp, pWorker) != 0) {
        uError("worker:%s:%d failed to create thread to process since %s", pPool->name, pWorker->id, strerror(errno));
      }

      pthread_attr_destroy(&thAttr);
      pPool->num++;
      uDebug("worker:%s:%d is launched, total:%d", pPool->name, pWorker->id, pPool->num);
    } while (pPool->num < pPool->min);
  }

  uDebug("worker:%s, queue:%p is allocated, ahandle:%p", pPool->name, pQueue, ahandle);

  return pQueue;
}