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

#include "tidpool.h"

id_pool_t::id_pool_t(int maxId) : freeList(maxId), numOfFree(maxId), freeSlot(0) {}

int id_pool_t::alloc() {
  int slot = -1;
  std::lock_guard<std::mutex> lock(mutex);

  if (numOfFree > 0) {
    const auto maxId = freeList.size();
    for (int i = 0; i < freeList.size(); ++i) {
      slot = (i + freeSlot) % maxId;
      if (!freeList[slot]) {
        freeList[slot] = true;
        freeSlot = slot + 1;
        numOfFree--;
        break;
      }
    }
  }

  return slot + 1;
}

void id_pool_t::dealloc(int id) {
  std::lock_guard<std::mutex> lock(mutex);

  int slot = (id - 1) % freeList.size();
  if (freeList[slot]) {
    freeList[slot] = false;
    numOfFree++;
  }
}

int id_pool_t::numOfUsed() { return freeList.size() - numOfFree; }

void id_pool_t::markStatus(int id) {
  std::lock_guard<std::mutex> lock(mutex);

  int slot = (id - 1) % freeList.size();
  if (!freeList[slot]) {
    freeList[slot] = true;
    numOfFree--;
  }
}

int id_pool_t::update(int maxId) {
  if (maxId <= freeList.size()) {
    return 0;
  }

  std::lock_guard<std::mutex> lock(mutex);
  numOfFree += maxId - freeList.size();
  freeList.resize(maxId);
  return 0;
}