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
#ifndef _TD_KVSTORE_H_
#define _TD_KVSTORE_H_

#include <stdint.h>
#include <unordered_map>

#define KVSTORE_FILE_VERSION ((uint32_t)0)

typedef int (*iterFunc)(void *, void *cont, int contLen);
typedef void (*afterFunc)(void *);

typedef struct {
  int64_t  size;  // including 512 bytes of header size
  int64_t  tombSize;
  int64_t  nRecords;
  int64_t  nDels;
  uint32_t magic;
} SStoreInfo;

struct SKVRecord {
  uint64_t uid;
  int64_t  offset;
  int64_t  size;
};

struct SKVStore {
  std::string fname;
  int        fd;
  std::string fsnap;
  int        sfd;
  std::string fnew;
  int        nfd;
  std::unordered_map<uint64_t, SKVRecord> map;
  iterFunc   iFunc;
  afterFunc  aFunc;
  void *     appH;
  SStoreInfo info;

 public:
  ~SKVStore();
};

#define KVSTORE_MAGIC(s) (s)->info.magic

int       tdCreateKVStore(const char *fname);
int       tdDestroyKVStore(char *fname);
SKVStore *tdOpenKVStore(const char *fname, iterFunc iFunc, afterFunc aFunc, void *appH);
int       tdKVStoreStartCommit(SKVStore *pStore);
int       tdUpdateKVStoreRecord(SKVStore *pStore, uint64_t uid, void *cont, int contLen);
int       tdDropKVStoreRecord(SKVStore *pStore, uint64_t uid);
int       tdKVStoreEndCommit(SKVStore *pStore);
void      tsdbGetStoreInfo(const char *fname, uint32_t *magic, int64_t *size);

#endif