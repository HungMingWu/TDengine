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
  int32_t    queuedMsg;
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

  template <typename T>
  std::shared_ptr<T> openTable(const SSdbTableDesc& desc) 
  {
    auto pTable = std::make_shared<T>(desc);
    SSdbMgmt::instance().numOfTables++;
    SSdbMgmt::instance().tableList[pTable->id] = pTable.get();
    return pTable;
  }
};