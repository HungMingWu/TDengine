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
#include "taoserror.h"
#include "tqueue.h"
  
STaosQueue::~STaosQueue() 
{
  STaosQnode *pNode = head;  
  if (qset) qset->removeFromQset(this); 
  while (pNode) {
    auto pTemp = pNode;
    pNode = pNode->next;
    free (pTemp);
  }
}

void *taosAllocateQitem(int size) {
  STaosQnode *pNode = (STaosQnode *)calloc(sizeof(STaosQnode) + size, 1);
  
  if (pNode == NULL) return NULL;
  uTrace("item:%p, node:%p is allocated", pNode->item, pNode);
  return (void *)pNode->item;
}

void taosFreeQitem(void *param) {
  if (param == NULL) return;

  char *temp = (char *)param;
  temp -= sizeof(STaosQnode);
  uTrace("item:%p, node:%p is freed", param, temp);
  free(temp);
}

int STaosQueue::writeQitem(int type, void *item) {
  STaosQnode *pNode = (STaosQnode *)(((char *)item) - sizeof(STaosQnode));
  pNode->type = type;
  pNode->next = NULL;

  mutex.lock();

  if (tail) {
    tail->next = pNode;
    tail = pNode;
  } else {
    head = pNode;
    tail = pNode; 
  }

  numOfItems++;
  if (qset) atomic_add_fetch_32(&qset->numOfItems, 1);
  uTrace("item:%p is put into queue:%p, type:%d items:%d", item, this, type, numOfItems);

  mutex.unlock();

  if (qset) tsem_post(&qset->sem);

  return 0;
}

int STaosQueue::readQitem(int *type, void **pitem) {
  STaosQnode *pNode = NULL;
  int         code = 0;

  std::lock_guard<std::mutex> lock(mutex);

  if (head) {
      pNode = head;
      *pitem = pNode->item;
      *type = pNode->type;
      head = pNode->next;
      if (head == NULL) 
        tail = NULL;
      numOfItems--;
      if (qset) atomic_sub_fetch_32(&qset->numOfItems, 1);
      code = 1;
      uDebug("item:%p is read out from queue:%p, type:%d items:%d", *pitem, this, *type, numOfItems);
  } 

  return code;
}

int STaosQueue::readAllQitems(STaosQall *qall) {
  int         code = 0;

  std::lock_guard<std::mutex> lock(mutex);

  if (head) {
    memset(qall, 0, sizeof(STaosQall));
    qall->current = head;
    qall->start = head;
    qall->numOfItems = numOfItems;
    qall->itemSize = itemSize;
    code = qall->numOfItems;

    head = NULL;
    tail = NULL;
    numOfItems = 0;
    if (qset) atomic_sub_fetch_32(&qset->numOfItems, qall->numOfItems);
  } 
  
  return code; 
}

int STaosQall::getQitem(int *type, void **pitem) {
  STaosQnode *pNode;
  int         num = 0;

  pNode = current;
  if (pNode)
    current = pNode->next;
 
  if (pNode) {
    *pitem = pNode->item;
    *type = pNode->type;
    num = 1;
    uTrace("item:%p is fetched, type:%d", *pitem, *type);
  }

  return num;
}

void STaosQall::resetQitems() {
  current = start;
}

STaosQset::STaosQset() {
  tsem_init(&sem, 0, 0);
  uTrace("qset:%p is opened", this);
}

STaosQset::~STaosQset() {
  tsem_destroy(&sem);
  uTrace("qset:%p is closed", this);
}

// tsem_post 'qset->sem', so that reader threads waiting for it
// resumes execution and return, should only be used to signal the
// thread to exit.
void STaosQset::threadResume() {
  uDebug("qset:%p, it will exit", this);
  tsem_post(&sem);
}

int STaosQset::addIntoQset(STaosQueue *queue, void *ahandle) {
  if (queue->qset) return -1; 

  std::lock_guard<std::mutex> _(mutex);

  queue->next = head;
  queue->ahandle = ahandle;
  head = queue;
  numOfQueues++;

  queue->mutex.lock();
  atomic_add_fetch_32(&numOfItems, queue->numOfItems);
  queue->qset = this;
  queue->mutex.unlock();

  uTrace("queue:%p is added into qset:%p", queue, this);
  return 0;
}

void STaosQset::removeFromQset(STaosQueue *queue) {
  STaosQueue *tqueue = NULL;

  std::lock_guard<std::mutex> _(mutex);

  if (head) {
    if (head == queue) {
      head = head->next;
      tqueue = queue;
    } else {
      STaosQueue *prev = head;
      tqueue = head->next;
      while (tqueue) {
        assert(tqueue->qset);
        if (tqueue== queue) {
          prev->next = tqueue->next;
          break;
        } else {
          prev = tqueue;
          tqueue = tqueue->next;
        }
      }
    }

    if (tqueue) {
      if (current == queue) current = tqueue->next;
      numOfQueues--;

      std::lock_guard<std::mutex> lock(queue->mutex);
      atomic_sub_fetch_32(&numOfItems, queue->numOfItems);
      queue->qset = NULL;
      queue->next = NULL;
    }
  } 

  uTrace("queue:%p is removed from qset:%p", queue, this);
}

int STaosQset::readQitem(int *type, void **pitem, void **phandle) {
  STaosQnode *pNode = NULL;
  int         code = 0;
   
  tsem_wait(&sem);

  std::lock_guard<std::mutex> _(mutex);

  for(int i=0; i<numOfQueues; ++i) {
    if (current == NULL) 
      current = head;   
    STaosQueue *queue = current;
    if (queue) current = queue->next;
    if (queue == NULL) break;
    if (queue->head == NULL) continue;

    std::lock_guard<std::mutex> lock(queue->mutex);

    if (queue->head) {
        pNode = queue->head;
        *pitem = pNode->item;
        if (type) *type = pNode->type;
        if (phandle) *phandle = queue->ahandle;
        queue->head = pNode->next;
        if (queue->head == NULL) 
          queue->tail = NULL;
        queue->numOfItems--;
        atomic_sub_fetch_32(&numOfItems, 1);
        code = 1;
        uTrace("item:%p is read out from queue:%p, type:%d items:%d", *pitem, queue, pNode->type, queue->numOfItems);
    } 

    if (pNode) break;
  }

  return code; 
}

int STaosQset::readAllQitems(STaosQall *qall, void **phandle) {
  STaosQueue *queue;
  int         code = 0;

  tsem_wait(&sem);
  std::lock_guard<std::mutex> _(mutex);

  for(int i=0; i<numOfQueues; ++i) {
    if (current == NULL) 
      current = head;   
    queue = current;
    if (queue) current = queue->next;
    if (queue == NULL) break;
    if (queue->head == NULL) continue;

    std::lock_guard<std::mutex> lock(queue->mutex);

    if (queue->head) {
      qall->current = queue->head;
      qall->start = queue->head;
      qall->numOfItems = queue->numOfItems;
      qall->itemSize = queue->itemSize;
      code = qall->numOfItems;
      *phandle = queue->ahandle;
          
      queue->head = NULL;
      queue->tail = NULL;
      queue->numOfItems = 0;
      atomic_sub_fetch_32(&numOfItems, qall->numOfItems);
      for (int j=1; j<qall->numOfItems; ++j) tsem_wait(&sem);
    } 

    if (code != 0) break;  
  }

  return code;
}