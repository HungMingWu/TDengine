#pragma once
#include <stdint.h>
#include <mutex>
#include "tsync.h"
#include "syncInt.h"
#include "mnodeSdb.h"

typedef enum { SDB_STATUS_OFFLINE = 0, SDB_STATUS_SERVING = 1, SDB_STATUS_CLOSING = 2 } ESdbStatus;

struct SSdbMgmt {
  ESyncRole  role = TAOS_SYNC_ROLE_OFFLINE;
  ESdbStatus status;
  uint64_t   version;
  SSyncNodePtr sync;
  SWal*      wal;
  SSyncCfg   cfg;
  std::atomic<int32_t>   queuedMsg;
  int32_t    numOfTables;
  SSdbTable *tableList[SDB_TABLE_MAX];
  std::mutex mutex;

 public:
  static SSdbMgmt& instance() { 
    static SSdbMgmt instance_;
    return instance_;
  }
  uint64_t getVersion() const { return version; }
  bool     isMaster() const { return role == TAOS_SYNC_ROLE_MASTER; }
  bool     isServing() const { return status == SDB_STATUS_SERVING; }
  SSdbTable* getTable(int32_t tableId) { return tableList[tableId]; }

  int32_t update(SWalHead* pHead) 
  {
      std::unique_lock<std::mutex> _(mutex);

      if (pHead->version == 0) {
        // assign version
        pHead->version = version++;
      } else {
        // for data from WAL or forward, version may be smaller
        if (pHead->version <= version) {
            #if 0
          sdbDebug("vgId:1, sdb:%s, failed to restore %s key:%s from source(%d), hver:%" PRIu64
                   " too large, mver:%" PRIu64,
                   pTable->name, actStr[action], sdbGetKeyStr(pTable, pHead->cont.data()), qtype, pHead->version,
                   version);
          #endif
          return TSDB_CODE_SUCCESS;
        } else if (pHead->version != version + 1) {
            #if 0
          sdbError("vgId:1, sdb:%s, failed to restore %s key:%s from source(%d), hver:%" PRIu64
                   " too large, mver:%" PRIu64,
                   pTable->name, actStr[action], sdbGetKeyStr(pTable, pHead->cont.data()), qtype, pHead->version,
                   version);
          #endif
          return TSDB_CODE_SYN_INVALID_VERSION;
        } else {
          version = pHead->version;
        }
      }

      return wal->write(pHead);
  }

  template <typename T>
  std::shared_ptr<T> openTable(const SSdbTableDesc& desc) 
  {
    auto pTable = std::make_shared<T>(desc);
    SSdbMgmt::instance().numOfTables++;
    SSdbMgmt::instance().tableList[pTable->id] = pTable.get();
    return pTable;
  }
};