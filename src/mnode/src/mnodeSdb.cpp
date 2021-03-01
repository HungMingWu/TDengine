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

#include <memory>
#include <mutex>
#include <thread>
#include "workpool.h"
#include "os.h"
#include "taoserror.h"
#include "hash.h"
#include "tutil.h"
#include "tref.h"
#include "tbn.h"
#include "tqueue.h"
#include "tsync.h"
#include "ttimer.h"
#include "tglobal.h"
#include "dnode.h"
#include "mnode.h"
#include "mnodeDef.h"
#include "mnodeInt.h"
#include "mnodeMnode.h"
#include "mnodeDnode.h"
#include "mnodeCluster.h"
#include "mnodeSdb.h"
#include "syncInt.h"
#include "walInt.h"
#include "SdbMgmt.h"

#define MAX_QUEUED_MSG_NUM 10000

typedef enum {
  SDB_ACTION_INSERT = 0,
  SDB_ACTION_DELETE = 1,
  SDB_ACTION_UPDATE = 2
} ESdbAction;

const char *actStr[] = {
  "insert",
  "delete",
  "update",
  "invalid"
};

static void sdbConfirmForward(int32_t vgId, void *wparam, int32_t code);
static int32_t sdbProcessWrite(void *pRow, SWalHead *pHead, int32_t qtype, void *unused);

using SDBItem = std::pair<int, SSdbRowPtr>;

class SdbPool : public workpool<SDBItem, SdbPool> {
 public:
  void process(std::vector<SDBItem> &&items) {
    for (auto &item : items) {
      auto qtype = item.first;
      auto pRow = item.second;
      sdbTrace("vgId:1, msg:%p, row:%p hver:%" PRIu64 ", will be processed in sdb queue", pRow->pMsg, pRow->pObj,
               pRow->pHead.version);

      pRow->code = sdbProcessWrite((qtype == TAOS_QTYPE_RPC) ? pRow.get() : NULL, &pRow->pHead, qtype, NULL);
      if (pRow->code > 0) pRow->code = 0;

      sdbTrace("vgId:1, msg:%p is processed in sdb queue, code:%x", pRow->pMsg, pRow->code);
    }

    SSdbMgmt::instance().wal->fsync(true);

    // browse all items, and process them one by one
    for (const auto &item : items) {
      const auto qtype = item.first;
      const auto pRow = item.second;

      if (qtype == TAOS_QTYPE_RPC) {
        sdbConfirmForward(1, pRow.get(), pRow->code);
      } else {
        if (qtype == TAOS_QTYPE_FWD) {
          syncConfirmForward(SSdbMgmt::instance().sync, pRow->pHead.version, pRow->code);
        }
        --SSdbMgmt::instance().queuedMsg;
      }
    }
  }
  using workpool<SDBItem, SdbPool>::workpool;
};

static std::shared_ptr<SdbPool> tsSdbPool;

extern void *     tsMnodeTmr;
static void *     tsSdbTmr;

static int32_t sdbWriteFwdToQueue(int32_t vgId, SWalHead *pHead, int32_t qtype, void *rparam);
static int32_t sdbWriteRowToQueue(SSdbRowPtr pRow, int32_t action);
static void    sdbFreeQueue();

int64_t SSdbTable::getNumOfRows() const { return numOfRows.load(); }

uint64_t sdbGetVersion() { return SSdbMgmt::instance().getVersion(); }

bool sdbIsMaster() { return SSdbMgmt::instance().isMaster(); }

bool sdbIsServing() { return SSdbMgmt::instance().isServing(); }

void *SSdbTable::getObjKey(void *key) {
  if (keyType == SDB_KEY_VAR_STRING) {
    return *(char **)key;
  }

  return key;
}

static char *sdbGetKeyStr(SSdbTable *pTable, void *key) {
  static char str[16];
  switch (pTable->keyType) {
    case SDB_KEY_STRING:
    case SDB_KEY_VAR_STRING:
      return (char *)key;
    case SDB_KEY_INT:
    case SDB_KEY_AUTO:
      sprintf(str, "%d", *(int32_t *)key);
      return str;
    default:
      return "invalid";
  }
}

char *SSdbTable::getRowStr(void *key) { return sdbGetKeyStr(this, getObjKey(key)); }

static int32_t sdbInitWal() {
  SWalCfg walCfg;
  walCfg.vgId = 1;
  walCfg.walLevel = TAOS_WAL_FSYNC;
  walCfg.keep = TAOS_WAL_KEEP;
  walCfg.fsyncPeriod = 0;

  char    temp[TSDB_FILENAME_LEN] = {0};
  sprintf(temp, "%s/wal", tsMnodeDir);
  SSdbMgmt::instance().wal = walOpen(temp, &walCfg);
  if (SSdbMgmt::instance().wal == NULL) {
    sdbError("vgId:1, failed to open wal in %s", tsMnodeDir);
    return -1;
  }

  sdbInfo("vgId:1, open sdb wal for restore");
  int32_t code = SSdbMgmt::instance().wal->restore(NULL, sdbProcessWrite);
  if (code != TSDB_CODE_SUCCESS) {
    sdbError("vgId:1, failed to open wal for restore since %s", tstrerror(code));
    return -1;
  }

  sdbInfo("vgId:1, sdb wal load success");
  return 0;
}

static void sdbRestoreTables() {
  int32_t totalRows = 0;
  int32_t numOfTables = 0;

  sdbInfo("vgId:1, sdb start to check for integrity");

  for (int32_t tableId = 0; tableId < SDB_TABLE_MAX; ++tableId) {
    SSdbTable *pTable = SSdbMgmt::instance().getTable(tableId);
    if (pTable == NULL) continue;
    pTable->restore();

    totalRows += pTable->numOfRows;
    numOfTables++;
    sdbInfo("vgId:1, sdb:%s is checked, rows:%" PRId64, pTable->name, pTable->numOfRows.load());
  }

  sdbInfo("vgId:1, sdb is restored, mver:%" PRIu64 " rows:%d tables:%d", SSdbMgmt::instance().version, totalRows,
          numOfTables);
}

void sdbUpdateMnodeRoles() {
  if (SSdbMgmt::instance().sync <= 0) return;

  SNodesRole roles = {0};
  if (syncGetNodesRole(SSdbMgmt::instance().sync, &roles) != 0) return;

  sdbInfo("vgId:1, update mnodes role, replica:%d", SSdbMgmt::instance().cfg.replica);
  for (int32_t i = 0; i < SSdbMgmt::instance().cfg.replica; ++i) {
    SMnodeObj *pMnode = static_cast<SMnodeObj *>(mnodeGetMnode(roles.nodeId[i]));
    if (pMnode != NULL) {
      if (pMnode->role != roles.role[i]) {
        bnNotify();
      }

      pMnode->role = roles.role[i];
      sdbInfo("vgId:1, mnode:%d, role:%s", pMnode->mnodeId, syncRole[pMnode->role]);
      if (pMnode->mnodeId == dnodeGetDnodeId()) SSdbMgmt::instance().role = static_cast<ESyncRole>(pMnode->role);
    } else {
      sdbDebug("vgId:1, mnode:%d not found", roles.nodeId[i]);
    }
  }

  mnodeUpdateClusterId();
  mnodeUpdateMnodeEpSet(NULL);
}

static uint32_t sdbGetFileInfo(int32_t vgId, char *name, uint32_t *index, uint32_t eindex, int64_t *size, uint64_t *fversion) {
  sdbUpdateMnodeRoles();
  return 0;
}

static int32_t sdbGetWalInfo(int32_t vgId, char *fileName, int64_t *fileId) {
  return SSdbMgmt::instance().wal->getWalFile(fileName, fileId);
}

static void sdbNotifyRole(int32_t vgId, int8_t role) {
  sdbInfo("vgId:1, mnode role changed from %s to %s", syncRole[SSdbMgmt::instance().role], syncRole[role]);

  if (role == TAOS_SYNC_ROLE_MASTER && SSdbMgmt::instance().role != TAOS_SYNC_ROLE_MASTER) {
    bnReset();
  }
  SSdbMgmt::instance().role = static_cast<ESyncRole>(role);

  sdbUpdateMnodeRoles();
}

static int32_t sdbNotifyFileSynced(int32_t vgId, uint64_t fversion) { return 0; }

static void sdbNotifyFlowCtrl(int32_t vgId, int32_t level) {}

static int32_t sdbGetSyncVersion(int32_t vgId, uint64_t *fver, uint64_t *vver) {
  *fver = 0;
  *vver = 0;
  return 0;
}

// failed to forward, need revert insert
static void sdbHandleFailedConfirm(SSdbRow *pRow) {
  SWalHead *pHead = &pRow->pHead;
  int32_t   action = pHead->msgType % 10;

  sdbError("vgId:1, row:%p:%s hver:%" PRIu64 " action:%s, failed to foward since %s", pRow->pObj,
           sdbGetKeyStr(static_cast<SSdbTable *>(pRow->pTable), pHead->cont.data()), pHead->version, actStr[action],
           tstrerror(pRow->code));

  // It's better to create a table in two stages, create it first and then set it success
  if (action == SDB_ACTION_INSERT) {
    SSdbRow row;
    row.type = SDB_OPER_GLOBAL;
    row.pTable = pRow->pTable;
    row.pObj = pRow->pObj;
    row.Delete();
  }
}

FORCE_INLINE
static void sdbConfirmForward(int32_t vgId, void *wparam, int32_t code) {
  if (wparam == NULL) return;
  SSdbRow *pRow = static_cast<SSdbRow *>(wparam);
  SMnodeMsg * pMsg = pRow->pMsg;

  if (code <= 0) pRow->code = code;
  int32_t count = ++pRow->processedCount;
  if (count <= 1) {
    if (pMsg != NULL) sdbTrace("vgId:1, msg:%p waiting for confirm, count:%d code:%x", pMsg, count, code);
    return;
  } else {
    if (pMsg != NULL) sdbTrace("vgId:1, msg:%p is confirmed, code:%x", pMsg, code);
  }

  if (pRow->code != TSDB_CODE_SUCCESS) sdbHandleFailedConfirm(pRow);

  if (pRow->fpRsp != NULL) {
    pRow->code = (*pRow->fpRsp)(pMsg, pRow->code);
  }

  dnodeSendRpcMWriteRsp(pMsg, pRow->code);
  --SSdbMgmt::instance().queuedMsg;
}

static void sdbUpdateSyncTmrFp(void *tmrId) { sdbUpdateSync(NULL); }

void sdbUpdateAsync() {
  taosTmrReset(sdbUpdateSyncTmrFp, 200, tsMnodeTmr, &tsSdbTmr);
}

int32_t sdbUpdateSync(void *pMnodes) {
  SMInfos *pMinfos = static_cast<SMInfos *>(pMnodes);
  if (!mnodeIsRunning()) {
    mDebug("vgId:1, mnode not start yet, update sync config later");
    return TSDB_CODE_MND_MNODE_IS_RUNNING;
  }

  mDebug("vgId:1, update sync config, pMnodes:%p", pMnodes);

  SSyncCfg syncCfg = {0};
  int32_t  index = 0;

  if (pMinfos == NULL) {
    mDebug("vgId:1, mInfos not input, use mInfos in sdb, numOfMnodes:%d", syncCfg.replica);

    void *pIter = NULL;
    while (1) {
      SMnodeObj *pMnode = NULL;
      pIter = mnodeGetNextMnode(pIter, &pMnode);
      if (pMnode == NULL) break;

      syncCfg.nodeInfo[index].nodeId = pMnode->mnodeId;

      SDnodeObj *pDnode = static_cast<SDnodeObj *>(mnodeGetDnode(pMnode->mnodeId));
      if (pDnode != NULL) {
        syncCfg.nodeInfo[index].nodePort = pDnode->dnodePort + TSDB_PORT_SYNC;
        tstrncpy(syncCfg.nodeInfo[index].nodeFqdn, pDnode->dnodeFqdn, TSDB_FQDN_LEN);
        index++;
      }

    }
    syncCfg.replica = index;
  } else {
    mDebug("vgId:1, mInfos input, numOfMnodes:%d", pMinfos->mnodeNum);

    for (index = 0; index < pMinfos->mnodeNum; ++index) {
      SMInfo *node = &pMinfos->mnodeInfos[index];
      syncCfg.nodeInfo[index].nodeId = node->mnodeId;
      taosGetFqdnPortFromEp(node->mnodeEp, syncCfg.nodeInfo[index].nodeFqdn, &syncCfg.nodeInfo[index].nodePort);
      syncCfg.nodeInfo[index].nodePort += TSDB_PORT_SYNC;
    }
    syncCfg.replica = index;
    mnodeUpdateMnodeEpSet(static_cast<SMInfos*>(pMnodes));
  }

  syncCfg.quorum = (syncCfg.replica == 1) ? 1 : 2;

  bool hasThisDnode = false;
  for (int32_t i = 0; i < syncCfg.replica; ++i) {
    if (syncCfg.nodeInfo[i].nodeId == dnodeGetDnodeId()) {
      hasThisDnode = true;
      break;
    }
  }

  if (!hasThisDnode) {
    sdbDebug("vgId:1, update sync config, this dnode not exist");
    return TSDB_CODE_MND_FAILED_TO_CONFIG_SYNC;
  }

  if (memcmp(&syncCfg, &SSdbMgmt::instance().cfg, sizeof(SSyncCfg)) == 0) {
    sdbDebug("vgId:1, update sync config, info not changed");
    return TSDB_CODE_SUCCESS;
  }

  sdbInfo("vgId:1, work as mnode, replica:%d", syncCfg.replica);
  for (int32_t i = 0; i < syncCfg.replica; ++i) {
    sdbInfo("vgId:1, mnode:%d, %s:%d", syncCfg.nodeInfo[i].nodeId, syncCfg.nodeInfo[i].nodeFqdn,
            syncCfg.nodeInfo[i].nodePort);
  }

  SSyncInfo syncInfo = {0};
  syncInfo.vgId = 1;
  syncInfo.version = sdbGetVersion();
  syncInfo.syncCfg = syncCfg;
  sprintf(syncInfo.path, "%s", tsMnodeDir);
  syncInfo.getFileInfo = sdbGetFileInfo;
  syncInfo.getWalInfo = sdbGetWalInfo;
  syncInfo.writeToCache = sdbWriteFwdToQueue;
  syncInfo.confirmForward = sdbConfirmForward;
  syncInfo.notifyRole = sdbNotifyRole;
  syncInfo.notifyFileSynced = sdbNotifyFileSynced;
  syncInfo.notifyFlowCtrl = sdbNotifyFlowCtrl;
  syncInfo.getVersion = sdbGetSyncVersion;
  SSdbMgmt::instance().cfg = syncCfg;

  if (SSdbMgmt::instance().sync) {
    int32_t code = syncReconfig(SSdbMgmt::instance().sync, &syncCfg);
    if (code != 0) return code;
  } else {
    SSdbMgmt::instance().sync = syncStart(&syncInfo);
    if (SSdbMgmt::instance().sync <= 0) return TSDB_CODE_MND_FAILED_TO_START_SYNC;
  }

  sdbUpdateMnodeRoles();
  return TSDB_CODE_SUCCESS;
}

int32_t sdbInit() {
  tsSdbPool = std::make_shared<SdbPool>(1);

  if (sdbInitWal() != 0) {
    return -1;
  }

  sdbRestoreTables();

  if (mnodeGetMnodesNum() == 1) {
    SSdbMgmt::instance().role = TAOS_SYNC_ROLE_MASTER;
  }

  SSdbMgmt::instance().status = SDB_STATUS_SERVING;
  return TSDB_CODE_SUCCESS;
}

void sdbCleanUp() {
  if (SSdbMgmt::instance().status != SDB_STATUS_SERVING) return;

  SSdbMgmt::instance().status = SDB_STATUS_CLOSING;

  tsSdbPool->stop();
  tsSdbPool.reset();
  sdbDebug("vgId:1, sdb will be closed, mver:%" PRIu64, SSdbMgmt::instance().version);

  if (SSdbMgmt::instance().sync) {
    syncStop(SSdbMgmt::instance().sync);
    SSdbMgmt::instance().sync.reset();
  }

  if (SSdbMgmt::instance().wal) {
    SSdbMgmt::instance().wal->close();
    SSdbMgmt::instance().wal = NULL;
  }
}

objectBase *SSdbTable::getRowMeta(void *key) {
  int32_t keySize = sizeof(int32_t);
  if (keyType == SDB_KEY_STRING || keyType == SDB_KEY_VAR_STRING) {
    keySize = strlen((char *)key);
  }

  objectBase **ppRow = (objectBase **)taosHashGet(static_cast<SHashObj *>(iHandle), key, keySize);
  if (ppRow != NULL) return *ppRow;

  return NULL;
}

void *SSdbTable::getRowMetaFromObj(void *key) { return getRowMeta(getObjKey(key)); }

void *SSdbTable::getRow(void *key) {
  std::lock_guard<std::mutex> lock(mutex);
  objectBase *pRow = getRowMeta(key);

  return pRow;
}

void *SSdbTable::getRowFromObj(void *key) { return getRow(getObjKey(key)); }

int32_t SSdbTable::insertHash(SSdbRow *pRow) {
  void *  key = getObjKey(pRow->pObj.get());
  int32_t keySize = sizeof(int32_t);

  if (keyType == SDB_KEY_STRING || keyType == SDB_KEY_VAR_STRING) {
    keySize = strlen((char *)key);
  }

  mutex.lock();
  taosHashPut(static_cast<SHashObj *>(iHandle), key, keySize, &pRow->pObj, sizeof(int64_t));
  mutex.unlock();

  numOfRows++;

  if (keyType == SDB_KEY_AUTO) {
    autoIndex = MAX(autoIndex, *((uint32_t *)pRow->pObj.get()));
  } else {
    atomic_add_fetch_32(&autoIndex, 1);
  }

  sdbTrace("vgId:1, sdb:%s, insert key:%s to hash, rowSize:%d rows:%" PRId64 ", msg:%p", name,
           getRowStr(pRow->pObj.get()), pRow->rowSize, numOfRows.load(), pRow->pMsg);

  int32_t code = pRow->pObj->insert();
  if (code != TSDB_CODE_SUCCESS) {
    sdbError("vgId:1, sdb:%s, failed to insert key:%s to hash, remove it", name,
             getRowStr(pRow->pObj.get()));
    deleteHash(pRow);
  }

  return TSDB_CODE_SUCCESS;
}

int32_t SSdbTable::deleteHash(SSdbRow *pRow) {
  int32_t *updateEnd = &pRow->pObj->updateEnd;
  bool set = atomic_val_compare_exchange_32(updateEnd, 0, 1) == 0;
  if (!set) {
    sdbError("vgId:1, sdb:%s, failed to delete key:%s from hash, for it already removed", name,
             getRowStr(pRow->pObj.get()));
    return TSDB_CODE_MND_SDB_OBJ_NOT_THERE;
  }

  pRow->pObj->remove();
  
  void *  key = getObjKey(pRow->pObj.get());
  int32_t keySize = sizeof(int32_t);
  if (keyType == SDB_KEY_STRING || keyType == SDB_KEY_VAR_STRING) {
    keySize = strlen((char *)key);
  }

  mutex.lock();
  taosHashRemove(static_cast<SHashObj *>(iHandle), key, keySize);
  mutex.unlock();

  numOfRows--;

  sdbTrace("vgId:1, sdb:%s, delete key:%s from hash, numOfRows:%" PRId64 ", msg:%p", name,
           getRowStr(pRow->pObj.get()), numOfRows.load(), pRow->pMsg);

  return TSDB_CODE_SUCCESS;
}

int32_t SSdbTable::updateHash(SSdbRow *pRow) {
  sdbTrace("vgId:1, sdb:%s, update key:%s in hash, numOfRows:%" PRId64 ", msg:%p", name,
           getRowStr(pRow->pObj.get()), numOfRows.load(), pRow->pMsg);

  pRow->pObj->update();
  return TSDB_CODE_SUCCESS;
}

static int32_t sdbPerformInsertAction(SWalHead *pHead, SSdbTable *pTable) {
  SSdbRow row = {.rowSize = pHead->len, .rowData = pHead->cont.data(), .pTable = pTable};
  pTable->decode(&row);
  return pTable->insertHash(&row);
}

static int32_t sdbPerformDeleteAction(SWalHead *pHead, SSdbTable *pTable) {
  objectBase *pObj = pTable->getRowMeta(pHead->cont.data());
  if (pObj == NULL) {
    sdbDebug("vgId:1, sdb:%s, object:%s not exist in hash, ignore delete action", pTable->name,
             sdbGetKeyStr(pTable, pHead->cont.data()));
    return TSDB_CODE_SUCCESS;
  }
  SSdbRow row;
  row.pTable = pTable;
  row.pObj.reset(pObj);
  return pTable->deleteHash(&row);
}

static int32_t sdbPerformUpdateAction(SWalHead *pHead, SSdbTable *pTable) {
  objectBase *pObj = pTable->getRowMeta(pHead->cont.data());
  if (pObj == NULL) {
    sdbDebug("vgId:1, sdb:%s, object:%s not exist in hash, ignore update action", pTable->name,
             sdbGetKeyStr(pTable, pHead->cont.data()));
    return TSDB_CODE_SUCCESS;
  }
  SSdbRow row = {.rowSize = pHead->len, .rowData = pHead->cont.data(), .pTable = pTable};
  pTable->decode(&row);
  return pTable->updateHash(&row);
}

static int32_t sdbProcessWrite(void *wparam, SWalHead *pHead, int32_t qtype, void *unused) {
  SSdbRow * pRow = static_cast<SSdbRow *>(wparam);
  int32_t tableId = pHead->msgType / 10;
  int32_t action = pHead->msgType % 10;

  SSdbTable *pTable = SSdbMgmt::instance().getTable(tableId);
  assert(pTable != NULL);

  if (!mnodeIsRunning() && SSdbMgmt::instance().getVersion() % 100000 == 0) {
    char stepDesc[TSDB_STEP_DESC_LEN] = {0};
    snprintf(stepDesc, TSDB_STEP_DESC_LEN, "%" PRIu64 " rows have been restored", SSdbMgmt::instance().version);
    dnodeReportStep("mnode-sdb", stepDesc, 0);
  }

  if (qtype == TAOS_QTYPE_QUERY) return sdbPerformDeleteAction(pHead, pTable);

  int32_t code = SSdbMgmt::instance().update(pHead);
  if (code < 0) {
    return code;
  }

  // from app, row is created
  if (pRow != NULL) {
    // forward to peers
    pRow->processedCount = 0;
    int32_t syncCode = SSdbMgmt::instance().sync->forwardToPeerImpl(pHead, pRow, TAOS_QTYPE_RPC);
    if (syncCode <= 0) pRow->processedCount = 1;

    if (syncCode < 0) {
      sdbError("vgId:1, sdb:%s, failed to forward req since %s action:%s key:%s hver:%" PRIu64 ", msg:%p", pTable->name,
               tstrerror(syncCode), actStr[action], sdbGetKeyStr(pTable, pHead->cont.data()), pHead->version, pRow->pMsg);
    } else if (syncCode > 0) {
      sdbDebug("vgId:1, sdb:%s, forward req is sent, action:%s key:%s hver:%" PRIu64 ", msg:%p", pTable->name,
               actStr[action], sdbGetKeyStr(pTable, pHead->cont.data()), pHead->version, pRow->pMsg);
    } else {
      sdbTrace("vgId:1, sdb:%s, no need to send fwd req, action:%s key:%s hver:%" PRIu64 ", msg:%p", pTable->name,
               actStr[action], sdbGetKeyStr(pTable, pHead->cont.data()), pHead->version, pRow->pMsg);
    }
    return syncCode;
  }

  sdbTrace("vgId:1, sdb:%s, record from %s is disposed, action:%s key:%s hver:%" PRIu64, pTable->name, qtypeStr[qtype],
           actStr[action], sdbGetKeyStr(pTable, pHead->cont.data()), pHead->version);

  // even it is WAL/FWD, it shall be called to update version in sync
  SSdbMgmt::instance().sync->forwardToPeerImpl(pHead, pRow, TAOS_QTYPE_RPC);

  // from wal or forward msg, row not created, should add into hash
  if (action == SDB_ACTION_INSERT) {
    return sdbPerformInsertAction(pHead, pTable);
  } else if (action == SDB_ACTION_DELETE) {
    if (qtype == TAOS_QTYPE_FWD) {
      // Drop database/stable may take a long time and cause a timeout, so we confirm first then reput it into queue
      sdbWriteFwdToQueue(1, pHead, TAOS_QTYPE_QUERY, unused);
      return TSDB_CODE_SUCCESS;
    } else {
      return sdbPerformDeleteAction(pHead, pTable);
    }
  } else if (action == SDB_ACTION_UPDATE) {
    return sdbPerformUpdateAction(pHead, pTable);
  } else {
    return TSDB_CODE_MND_INVALID_MSG_TYPE;
  }
}

int32_t SSdbRow::Insert() {
  if (pTable == NULL) return TSDB_CODE_MND_SDB_INVALID_TABLE_TYPE;

  if (pTable->getRowFromObj(pObj.get())) {
    sdbError("vgId:1, sdb:%s, failed to insert:%s since it exist", pTable->name, pTable->getRowStr(pObj.get()));
    return TSDB_CODE_MND_SDB_OBJ_ALREADY_THERE;
  }

  if (pTable->keyType == SDB_KEY_AUTO) {
    *((uint32_t *)pObj.get()) = atomic_add_fetch_32(&pTable->autoIndex, 1);

    // let vgId increase from 2
    if (pTable->autoIndex == 1 && pTable->id == SDB_TABLE_VGROUP) {
      *((uint32_t *)pObj.get()) = atomic_add_fetch_32(&pTable->autoIndex, 1);
    }
  }

  int32_t code = pTable->insertHash(this);
  if (code != TSDB_CODE_SUCCESS) {
    sdbError("vgId:1, sdb:%s, failed to insert:%s into hash", pTable->name, pTable->getRowStr(pObj.get()));
    return code;
  }

  // just insert data into memory
  if (type != SDB_OPER_GLOBAL) {
    return TSDB_CODE_SUCCESS;
  }

  if (fpReq) {
    return (*fpReq)(pMsg);
  } else {
    return 0;
    //sdbWriteRowToQueue(this, SDB_ACTION_INSERT);
  }
}

bool sdbCheckRowDeleted(objectBase *pRow) {
  int32_t *updateEnd = &pRow->updateEnd;
  return atomic_val_compare_exchange_32(updateEnd, 1, 1) == 1;
}

int32_t SSdbRow::Delete() {
  if (pTable == NULL) return TSDB_CODE_MND_SDB_INVALID_TABLE_TYPE;

  void *pObj1 = pTable->getRowMetaFromObj(pObj.get());
  if (pObj1 == NULL) {
    sdbDebug("vgId:1, sdb:%s, record is not there, delete failed", pTable->name);
    return TSDB_CODE_MND_SDB_OBJ_NOT_THERE;
  }

  int32_t code = pTable->deleteHash(this);
  if (code != TSDB_CODE_SUCCESS) {
    sdbError("vgId:1, sdb:%s, failed to delete from hash", pTable->name);
    return code;
  }

  // just delete data from memory
  if (type != SDB_OPER_GLOBAL) {
    return TSDB_CODE_SUCCESS;
  }

  if (fpReq) {
    return (*fpReq)(pMsg);
  } else {
    return 0;
    //sdbWriteRowToQueue(this, SDB_ACTION_DELETE);
  }
}

int32_t SSdbRow::Update() {
  if (pTable == NULL) return TSDB_CODE_MND_SDB_INVALID_TABLE_TYPE;

  void *pObj1 = pTable->getRowMetaFromObj(pObj.get());
  if (pObj1 == NULL) {
    sdbDebug("vgId:1, sdb:%s, record is not there, update failed", pTable->name);
    return TSDB_CODE_MND_SDB_OBJ_NOT_THERE;
  }

  int32_t code = pTable->updateHash(this);
  if (code != TSDB_CODE_SUCCESS) {
    sdbError("vgId:1, sdb:%s, failed to update hash", pTable->name);
    return code;
  }

  // just update data in memory
  if (type != SDB_OPER_GLOBAL) {
    return TSDB_CODE_SUCCESS;
  }

  if (fpReq) {
    return (*fpReq)(pMsg);
  } else {
    return 0;
    //sdbWriteRowToQueue(this, SDB_ACTION_UPDATE);
  }
}

void *SSdbTable::fetchRow(void *pIter, void **ppRow) {
  *ppRow = NULL;
  pIter = taosHashIterate(static_cast<SHashObj *>(iHandle), pIter);
  if (pIter == NULL) return NULL;

  objectBase **ppMetaRow = static_cast<objectBase **>(pIter);
  if (ppMetaRow == NULL) {
    taosHashCancelIterate(static_cast<SHashObj *>(iHandle), pIter);
    return NULL;
  }

  *ppRow = *ppMetaRow;

  return pIter;
}

void SSdbTable::freeIter(void *pIter) {
  if (pIter == NULL) return;

  taosHashCancelIterate(static_cast<SHashObj *>(iHandle), pIter);
}

SSdbTable::SSdbTable(const SSdbTableDesc& desc) 
{
  tstrncpy(name, desc.name, SDB_TABLE_LEN);
  keyType = desc.keyType;
  id = desc.id;
  hashSessions = desc.hashSessions;
  maxRowSize = desc.maxRowSize;

  _hash_fn_t hashFp = taosGetDefaultHashFunction(TSDB_DATA_TYPE_INT);
  if (keyType == SDB_KEY_STRING || keyType == SDB_KEY_VAR_STRING) {
    hashFp = taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY);
  }
  iHandle = taosHashInit(hashSessions, hashFp, true, HASH_ENTRY_LOCK);
}

SSdbTable::~SSdbTable() { 
  SSdbMgmt::instance().numOfTables--;
  SSdbMgmt::instance().tableList[id] = nullptr;

  void *pIter = taosHashIterate(static_cast<SHashObj *>(iHandle), NULL);
  while (pIter) {
    objectBase **ppRow = static_cast<objectBase **>(pIter);
    pIter = taosHashIterate(static_cast<SHashObj *>(iHandle), pIter);
    if (ppRow == NULL) continue;

    delete *ppRow;
  }

  taosHashCancelIterate(static_cast<SHashObj *>(iHandle), pIter);
  taosHashCleanup(static_cast<SHashObj *>(iHandle));

  sdbDebug("vgId:1, sdb:%s, is closed, numOfTables:%d", name, SSdbMgmt::instance().numOfTables);
}

static int32_t sdbWriteToQueue(SSdbRowPtr pRow, int32_t qtype) {
  SWalHead *pHead = &pRow->pHead;

  if (pHead->len > TSDB_MAX_WAL_SIZE) {
    sdbError("vgId:1, wal len:%d exceeds limit, hver:%" PRIu64, pHead->len, pHead->version);
    return TSDB_CODE_WAL_SIZE_LIMIT;
  }

  int32_t queued = ++SSdbMgmt::instance().queuedMsg;
  if (queued > MAX_QUEUED_MSG_NUM) {
    sdbDebug("vgId:1, too many msg:%d in sdb queue, flow control", queued);
    taosMsleep(1);
  }

  sdbTrace("vgId:1, msg:%p qtype:%s write into to sdb queue, queued:%d", pRow->pMsg, qtypeStr[qtype], queued);
  tsSdbPool->put(std::make_pair(qtype, pRow));

  return TSDB_CODE_MND_ACTION_IN_PROGRESS;
}

static void sdbFreeFromQueue(SSdbRow *pRow) {
  int32_t queued = --SSdbMgmt::instance().queuedMsg;
  sdbTrace("vgId:1, msg:%p free from sdb queue, queued:%d", pRow->pMsg, queued);

  taosFreeQitem(pRow);
}

static int32_t sdbWriteFwdToQueue(int32_t vgId, SWalHead *pHead, int32_t qtype, void *rparam) {
  int32_t  size = sizeof(SSdbRow) + sizeof(SWalHead) + pHead->len;
  auto pRow = std::make_shared<SSdbRow>();
  memcpy(&pRow->pHead, pHead, sizeof(SWalHead) + pHead->len);
  pRow->rowData = pRow->pHead.cont.data();

  int32_t code = sdbWriteToQueue(pRow, qtype);
  if (code == TSDB_CODE_MND_ACTION_IN_PROGRESS) code = 0;

  return code;
}

static int32_t sdbWriteRowToQueue(SSdbRowPtr pRow, int32_t action) {
  const SSdbTable *pTable = pRow->pTable;
  if (pTable == NULL) return TSDB_CODE_MND_SDB_INVALID_TABLE_TYPE;

  SWalHead *pHead = &pRow->pHead;
  pRow->rowData = pHead->cont.data();
  std::vector<uint8_t> output;
  binser::memory_output_archive<> out(output);
  pRow->pObj->encode(out);

  pHead->len = pRow->rowSize;
  pHead->msgType = pTable->id * 10 + action;

  return sdbWriteToQueue(pRow, TAOS_QTYPE_RPC);
}

int32_t sdbInsertRowToQueue(SSdbRowPtr pRow) { return sdbWriteRowToQueue(pRow, SDB_ACTION_INSERT); }

int32_t sdbGetReplicaNum() { return SSdbMgmt::instance().cfg.replica; }