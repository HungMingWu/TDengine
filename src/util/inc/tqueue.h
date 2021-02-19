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

#ifndef TAOS_QUEUE_H
#define TAOS_QUEUE_H

#include <mutex>

/*

This set of API for queue is designed specially for vnode/mnode. The main purpose is to 
consume all the items instead of one item from a queue by one single read. Also, it can 
combine multiple queues into a queue set, a consumer thread can consume a queue set via 
a single API instead of looping every queue by itself.

Notes:
1: taosOpenQueue/taosCloseQueue, taosOpenQset/taosCloseQset is NOT multi-thread safe 
2: after taosCloseQueue/taosCloseQset is called, read/write operation APIs are not safe.
3: read/write operation APIs are multi-thread safe

To remove the limitation and make this set of queue APIs multi-thread safe, REF(tref.c)
shall be used to set up the protection. 

*/

struct STaosQueue;
struct STaosQall;

struct STaosQset {
  STaosQueue *head;
  STaosQueue *current;
  std::mutex  mutex;
  int32_t     numOfQueues;
  int32_t     numOfItems;
  tsem_t      sem;

 public:
  STaosQset();
  ~STaosQset();
  void threadResume();
  int  getQueueNumber() { return numOfQueues; }
  int  addIntoQset(STaosQueue *, void *ahandle);
  void removeFromQset(STaosQueue *);
  int  readQitem(int *type, void **pitem, void **handle);
  int  readAllQitems(STaosQall *qall, void **handle);
};

typedef struct STaosQnode {
  int                type;
  struct STaosQnode *next;
  char               item[];
} STaosQnode;

struct STaosQall {
  STaosQnode *current;
  STaosQnode *start;
  int32_t     itemSize;
  int32_t     numOfItems;

 public:
  int getQitem(int *type, void **pitem);
  void resetQitems();
};

struct STaosQueue {
  int32_t            itemSize;
  int32_t            numOfItems;
  struct STaosQnode *head;
  struct STaosQnode *tail;
  struct STaosQueue *next;     // for queue set
  struct STaosQset * qset;     // for queue set
  void *             ahandle;  // for queue set
  std::mutex         mutex;

 public:
  ~STaosQueue();
  int writeQitem(int type, void *item);
  int readQitem(int *type, void **pitem);
  int readAllQitems(STaosQall *);
  int getQueueItemsNumber() const { return numOfItems; };
};

void *taosAllocateQitem(int size);
void  taosFreeQitem(void *item);

#endif


