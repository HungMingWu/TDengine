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

#ifndef TDENGINE_TWORKER_H
#define TDENGINE_TWORKER_H

#include <mutex>
typedef void *(*FWorkerThread)(void *pWorker);
struct SWorkerPool;

typedef struct {
  pthread_t thread;  // thread
  int32_t   id;      // worker ID
  struct SWorkerPool *pPool;
} SWorker;

struct SWorkerPool {
  int32_t  max;  // max number of workers
  int32_t  min;  // min number of workers
  int32_t  num;  // current number of workers
  void *   qset;
  char *   name;
  SWorker *worker;
  FWorkerThread   workerFp;
  std::mutex mutex;
};

int32_t tWorkerInit(SWorkerPool *pPool);
void    tWorkerCleanup(SWorkerPool *pPool);
STaosQueue* tWorkerAllocQueue(SWorkerPool *pPool, void *ahandle);

#endif
