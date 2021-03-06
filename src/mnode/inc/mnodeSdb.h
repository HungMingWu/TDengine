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

#ifndef TDENGINE_MNODE_SDB_H
#define TDENGINE_MNODE_SDB_H

#include <memory>
#include <mutex>
#include "mnode.h"
#include "walInt.h"
#include "object.h"
#include "taoserror.h"

typedef enum {
  SDB_TABLE_CLUSTER = 0,
  SDB_TABLE_DNODE   = 1,
  SDB_TABLE_MNODE   = 2,
  SDB_TABLE_ACCOUNT = 3,
  SDB_TABLE_USER    = 4,
  SDB_TABLE_DB      = 5,
  SDB_TABLE_VGROUP  = 6,
  SDB_TABLE_STABLE  = 7,
  SDB_TABLE_CTABLE  = 8,
  SDB_TABLE_MAX     = 9
} ESdbTable;

typedef enum {
  SDB_KEY_STRING     = 0, 
  SDB_KEY_INT        = 1,
  SDB_KEY_AUTO       = 2,
  SDB_KEY_VAR_STRING = 3,
} ESdbKey;

typedef enum {
  SDB_OPER_GLOBAL = 0,
  SDB_OPER_LOCAL  = 1
} ESdbOper;

struct SSdbTable;
struct SSdbRow {
  ESdbOper   type;
  std::atomic<int32_t> processedCount{1};  // for sync fwd callback
  int32_t    code;            // for callback in sdb queue
  int32_t    rowSize;
  void *     rowData;
  ObjectPtr  pObj;
  SSdbTable *pTable;
  SMnodeMsg *pMsg;
  int32_t  (*fpReq)(SMnodeMsg *pMsg) = nullptr;
  int32_t  (*fpRsp)(SMnodeMsg *pMsg, int32_t code) = nullptr;
  char       reserveForSync[24];
  SWalHead   pHead;
  std::vector<uint8_t> serializeRow;
 public:
  int32_t Insert();
  int32_t Delete();
  int32_t Update();
};
using SSdbRowPtr = std::shared_ptr<SSdbRow>;

struct SSdbTableDesc {
  char *    name;
  int32_t   hashSessions;
  int32_t   maxRowSize;
  ESdbTable id;
  ESdbKey   keyType;
};

#define SDB_TABLE_LEN 12

struct SSdbTable {
  char      name[SDB_TABLE_LEN];
  ESdbTable id;
  ESdbKey   keyType;
  int32_t   hashSessions;
  int32_t   maxRowSize;
  int32_t   autoIndex;
  std::atomic<int64_t>  numOfRows;
  void *    iHandle;
  std::mutex mutex;

 public:
  SSdbTable(const SSdbTableDesc &desc);
  virtual ~SSdbTable();
  void *getRow(void *key);
  void *fetchRow(void *pIter, void **ppRow);
  int64_t getNumOfRows() const;
  void    freeIter(void *pIter);
  int32_t Id() const { return autoIndex; }
  void *  getRowMetaFromObj(void *key);
  objectBase *getRowMeta(void *key);
  char *  getRowStr(void *key);
  int32_t insertHash(SSdbRow *pRow);
  int32_t updateHash(SSdbRow *pRow);
  int32_t deleteHash(SSdbRow *pRow);
  void *  getRowFromObj(void *key);
  void *  getObjKey(void *key);
  virtual int32_t decode(SSdbRow *pRow) = 0;
  virtual int32_t restore() = 0;
};

template <class K, class V> 
class SdbHashTable : public SSdbTable {
 protected:
  std::unordered_map<K, std::shared_ptr<V>> objects;

 public:
  using SSdbTable::SSdbTable;

  int32_t insert(const K& key, std::shared_ptr<V> obj) {
    //sdbTrace("vgId:1, sdb:%s, insert key:%s to hash, rows:%" PRId64 "", name, key.c_str(), numOfRows.load());

    int32_t code = obj->insert();
    if (code != TSDB_CODE_SUCCESS) {
      //sdbError("vgId:1, sdb:%s, failed to insert key:%s to hash, remove it", name, key.c_str());
      obj->remove();
      return code;
    }

    std::lock_guard<std::mutex> lock(mutex);
    auto it = objects.find(key);
    if (it != objects.end()) {
      //sdbError("vgId:1, sdb:%s, failed to insert:%s since it exist", name, key.c_str());
      return TSDB_CODE_MND_SDB_OBJ_ALREADY_THERE;
    }
    objects[key] = obj;
    atomic_add_fetch_32(&autoIndex, 1);

    return code;
  }
};

int32_t sdbInit();
void    sdbCleanUp();
bool    sdbIsMaster();
bool    sdbIsServing();
void    sdbUpdateMnodeRoles();
int32_t sdbGetReplicaNum();

int32_t sdbInsertRowToQueue(SSdbRowPtr pRow);

uint64_t sdbGetVersion();
bool     sdbCheckRowDeleted(objectBase *pRow);

#endif