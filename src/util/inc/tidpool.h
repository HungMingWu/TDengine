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

#ifndef TDENGINE_TIDPOOL_H
#define TDENGINE_TIDPOOL_H

#include <mutex>
#include <deque>

struct id_pool_t {
  int             numOfFree;
  int             freeSlot;
  std::deque<bool> freeList;
  std::mutex mutex;

 public:
  id_pool_t(int maxId);
  ~id_pool_t() = default;
  int update(int maxId);
  int alloc();
  void dealloc(int id);
  int  MaxSize() const { return freeList.size(); }
  void markStatus(int id);
  int  numOfUsed();
};

#endif
