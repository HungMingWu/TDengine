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
#include "taoserror.h"
#include "tutil.h"
#include "tsocket.h"
#include "tidpool.h"
#include "tsync.h"
#include "tbn.h"
#include "tglobal.h"
#include "tdataformat.h"
#include "dnode.h"
#include "mnode.h"
#include "mnodeDef.h"
#include "mnodeInt.h"
#include "mnodeDb.h"
#include "mnodeDnode.h"
#include "mnodeMnode.h"
#include "mnodeProfile.h"
#include "mnodeSdb.h"
#include "mnodeShow.h"
#include "mnodeTable.h"
#include "mnodeVgroup.h"
#include "mnodePeer.h"
#include "SdbMgmt.h"

typedef enum {
  TAOS_VG_STATUS_READY,
  TAOS_VG_STATUS_DROPPING,
  TAOS_VG_STATUS_CREATING,
  TAOS_VG_STATUS_UPDATING,
} EVgroupStatus;

char* vgroupStatus[] = {
  "ready",
  "dropping",
  "creating",
  "updating"
};

static std::shared_ptr<SSdbTable> tsVgroupSdb;
static int32_t tsVgUpdateSize = 0;

static int32_t mnodeAllocVgroupIdPool(SVgObj *pInputVgroup);
static void    mnodeSendDropVgroupMsg(SVgObj *pVgroup, void *ahandle);

int32_t SVgObj::insert() {
  // refer to db
  SDbObj *pDb = mnodeGetDb(dbName);
  if (pDb == NULL) {
    mError("vgId:%d, db:%s is not exist while insert into hash", vgId, dbName);
    return TSDB_CODE_MND_INVALID_DB;
  }
  
  if (pDb->status != TSDB_DB_STATUS_READY) {
    mError("vgId:%d, db:%s status:%d, in dropping", vgId, pDb->name, pDb->status);
    return TSDB_CODE_MND_DB_IN_DROPPING;
  }

  this->pDb = pDb;
  status = TAOS_VG_STATUS_CREATING;
  accessState = TSDB_VN_ALL_ACCCESS;
  if (mnodeAllocVgroupIdPool(this) < 0) {
    mError("vgId:%d, failed to init idpool for vgroups", vgId);
    return -1;
  }

  for (int32_t i = 0; i < numOfVnodes; ++i) {
    SDnodeObj *pDnode = static_cast<SDnodeObj *>(mnodeGetDnode(vnodeGid[i].dnodeId));
    if (pDnode != NULL) {
      vnodeGid[i].pDnode = pDnode;
      pDnode->openVnodes++;
    }
  }

  mnodeAddVgroupIntoDb(this);

  return TSDB_CODE_SUCCESS;
}

int32_t SVgObj::remove() {
  if (pDb == NULL) {
    mError("vgId:%d, db:%s is not exist while insert into hash", vgId, dbName);
    return TSDB_CODE_MND_VGROUP_NOT_EXIST;
  }

  mnodeRemoveVgroupFromDb(this);

  for (int32_t i = 0; i < numOfVnodes; ++i) {
    SDnodeObj *pDnode = static_cast<SDnodeObj *>(mnodeGetDnode(vnodeGid[i].dnodeId));
    if (pDnode != NULL) {
      pDnode->openVnodes--;
    }
  }

  return TSDB_CODE_SUCCESS;
}

int32_t SVgObj::update() {
  SVgObj *pVgroup = mnodeGetVgroup(vgId);

  if (pVgroup != this) {
    for (int32_t i = 0; i < pVgroup->numOfVnodes; ++i) {
      SDnodeObj *pDnode = pVgroup->vnodeGid[i].pDnode;
      if (pDnode != NULL) {
        pDnode->openVnodes--;
      }
    }

    memcpy(pVgroup, this, tsVgUpdateSize);

    for (int32_t i = 0; i < pVgroup->numOfVnodes; ++i) {
      SDnodeObj *pDnode = static_cast<SDnodeObj *>(mnodeGetDnode(pVgroup->vnodeGid[i].dnodeId));
      pVgroup->vnodeGid[i].pDnode = pDnode;
      if (pDnode != NULL) {
        pDnode->openVnodes++;
      }
    }

    delete this;
  }


  // reset vgid status on vgroup changed
  mDebug("vgId:%d, reset sync status to offline", pVgroup->vgId);
  for (int32_t v = 0; v < pVgroup->numOfVnodes; ++v) {
    pVgroup->vnodeGid[v].role = TAOS_SYNC_ROLE_OFFLINE;
  }

  mDebug("vgId:%d, is updated, numOfVnode:%d", pVgroup->vgId, pVgroup->numOfVnodes);
  return TSDB_CODE_SUCCESS;
}

int32_t SVgObj::encode(binser::memory_output_archive<> &out) {
  out(vgId, numOfVnodes, createdTime, lbDnodeId, lbTime,
      dbName, inUse, accessState, status, reserved0, 
      vnodeGid, vgCfgVersion, reserved1);
  #if 0
  SVgObj *pTmpVgroup = static_cast<SVgObj *>(pRow->rowData);
  for (int32_t i = 0; i < TSDB_MAX_REPLICA; ++i) {
    pTmpVgroup->vnodeGid[i].pDnode = NULL;
    pTmpVgroup->vnodeGid[i].role = 0;
    memset(pTmpVgroup->vnodeGid[i].vver, 0, sizeof(pTmpVgroup->vnodeGid[i].vver));
  }
  #endif
  return TSDB_CODE_SUCCESS;
}

class VgroupTable : public SSdbTable {
 public:
  using SSdbTable::SSdbTable;
  int32_t decode(SSdbRow *pRow) override {
    auto pVgroup = std::make_shared<SVgObj>();
    memcpy(pVgroup.get(), pRow->rowData, tsVgUpdateSize);
    pRow->pObj = pVgroup;
    return TSDB_CODE_SUCCESS;
  }
  int32_t restore() override { return 0; }
};

int32_t mnodeInitVgroups() {
  SVgObj tObj;
  tsVgUpdateSize = (int8_t *)tObj.updateEnd - (int8_t *)&tObj;

  SSdbTableDesc desc;
  desc.id = SDB_TABLE_VGROUP;
  desc.name = "vgroups";
  desc.hashSessions = TSDB_DEFAULT_VGROUPS_HASH_SIZE;
  desc.maxRowSize = tsVgUpdateSize;
  desc.keyType = SDB_KEY_AUTO;

  tsVgroupSdb = SSdbMgmt::instance().openTable<VgroupTable>(desc);
  if (tsVgroupSdb == NULL) {
    mError("failed to init vgroups data");
    return -1;
  }

  mDebug("table:vgroups is created");
  
  return 0;
}

SVgObj *mnodeGetVgroup(int32_t vgId) {
  return (SVgObj *)tsVgroupSdb->getRow(&vgId);
}

void mnodeUpdateVgroup(SVgObj *pVgroup) {
  pVgroup->vgCfgVersion++;

  SSdbRow row;
  row.type = SDB_OPER_GLOBAL;
  row.pTable = tsVgroupSdb.get();
  row.pObj.reset(pVgroup);

  int32_t code = row.Update();
  if (code != TSDB_CODE_SUCCESS && code != TSDB_CODE_MND_ACTION_IN_PROGRESS) {
    mError("vgId:%d, failed to update vgroup", pVgroup->vgId);
  }
  mnodeSendAlterVgroupMsg(pVgroup);
}

/*
  Traverse all vgroups on mnode, if there no such vgId on a dnode, so send msg to this dnode for re-creating this vgId/vnode 
*/
void mnodeCheckUnCreatedVgroup(SDnodeObj *pDnode, SVnodeLoad *pVloads, int32_t openVnodes) {
  void *pIter = NULL;
  while (1) {
    SVgObj *pVgroup;
    pIter = mnodeGetNextVgroup(pIter, &pVgroup);
    if (pVgroup == NULL) break;

    for (int v = 0; v < pVgroup->numOfVnodes; ++v) {
      if (pVgroup->vnodeGid[v].dnodeId == pDnode->dnodeId) {
        // vgroup should have a vnode on this dnode
        bool have = false;
        for (int32_t i = 0; i < openVnodes; ++i) {
          SVnodeLoad *pVload = pVloads + i;
          if (pVgroup->vgId == pVload->vgId) {
            have = true;
            break;
          }
        }

        if (have) continue;

        if (pVgroup->status == TAOS_VG_STATUS_CREATING || pVgroup->status == TAOS_VG_STATUS_DROPPING) {
          mDebug("vgId:%d, not exist in dnode:%d and status is %s, do nothing", pVgroup->vgId, pDnode->dnodeId,
                 vgroupStatus[pVgroup->status]);
        } else {
          mDebug("vgId:%d, not exist in dnode:%d and status is %s, send create msg", pVgroup->vgId, pDnode->dnodeId,
                 vgroupStatus[pVgroup->status]);
          mnodeSendCreateVgroupMsg(pVgroup, NULL);
        }
      }
    }
  }

  mnodeCancelGetNextVgroup(pIter);
}

void mnodeUpdateVgroupStatus(SVgObj *pVgroup, SDnodeObj *pDnode, SVnodeLoad *pVload) {
  bool dnodeExist = false;
  for (int32_t i = 0; i < pVgroup->numOfVnodes; ++i) {
    SVnodeGid *pVgid = &pVgroup->vnodeGid[i];
    if (pVgid->pDnode == pDnode) {
      mTrace("vgId:%d, receive vnode status from dnode:%d, status:%s last:%s vver:%" PRIu64, pVgroup->vgId,
             pDnode->dnodeId, syncRole[pVload->role], syncRole[pVgid->role], pVload->vnodeVersion);
      pVgid->role = pVload->role;
      mnodeSetVgidVer(pVgid->vver, pVload->vnodeVersion);
      if (pVload->role == TAOS_SYNC_ROLE_MASTER) {
        pVgroup->inUse = i;
      }
      dnodeExist = true;
      break;
    }
  }

  if (!dnodeExist) {
    SRpcEpSet epSet = mnodeGetEpSetFromIp(pDnode->dnodeEp);
    mError("vgId:%d, dnode:%d not exist in mnode, drop it", pVload->vgId, pDnode->dnodeId);
    mnodeSendDropVnodeMsg(pVload->vgId, &epSet, NULL);
    return;
  }

  if (pVload->role == TAOS_SYNC_ROLE_MASTER) {
    pVgroup->totalStorage = htobe64(pVload->totalStorage);
    pVgroup->compStorage = htobe64(pVload->compStorage);
    pVgroup->pointsWritten = htobe64(pVload->pointsWritten);
  }

  if (pVload->dbCfgVersion != pVgroup->pDb->dbCfgVersion || pVload->replica != pVgroup->numOfVnodes ||
      pVload->vgCfgVersion != pVgroup->vgCfgVersion) {
    mError("dnode:%d, vgId:%d, vnode cfgVersion:%d:%d repica:%d not match with mnode cfgVersion:%d:%d replica:%d",
           pDnode->dnodeId, pVload->vgId, pVload->dbCfgVersion, pVload->vgCfgVersion, pVload->replica,
           pVgroup->pDb->dbCfgVersion, pVgroup->vgCfgVersion, pVgroup->numOfVnodes);
    mnodeSendAlterVgroupMsg(pVgroup);
  }
}

static int32_t mnodeAllocVgroupIdPool(SVgObj *pInputVgroup) {
  SDbObj *pDb = pInputVgroup->pDb;
  if (pDb == NULL) return TSDB_CODE_MND_APP_ERROR;

  int32_t minIdPoolSize = TSDB_MAX_TABLES;
  int32_t maxIdPoolSize = tsMinTablePerVnode;
  for (int32_t v = 0; v < pDb->numOfVgroups; ++v) {
    SVgObj *pVgroup = pDb->vgList[v];
    if (pVgroup == NULL) continue;

    int32_t idPoolSize = pVgroup->idPool->MaxSize();
    minIdPoolSize = MIN(minIdPoolSize, idPoolSize);
    maxIdPoolSize = MAX(maxIdPoolSize, idPoolSize);
  }

  // new vgroup
  if (pInputVgroup->idPool == NULL) {
    pInputVgroup->idPool.reset(new id_pool_t(maxIdPoolSize));
    if (pInputVgroup->idPool == NULL) {
      mError("vgId:%d, failed to init idPool for vgroup, size:%d", pInputVgroup->vgId, maxIdPoolSize);
      return TSDB_CODE_MND_OUT_OF_MEMORY;
    } else {
      mDebug("vgId:%d, init idPool for vgroup, size:%d", pInputVgroup->vgId, maxIdPoolSize);
      return TSDB_CODE_SUCCESS;
    }
  }

  // realloc all vgroups in db
  int32_t newIdPoolSize;
  if (minIdPoolSize * 4 < tsTableIncStepPerVnode) {
    newIdPoolSize = minIdPoolSize * 4;
  } else {
    newIdPoolSize = ((minIdPoolSize / tsTableIncStepPerVnode) + 1) * tsTableIncStepPerVnode;
  }

  if (newIdPoolSize > tsMaxTablePerVnode) {
    if (minIdPoolSize >= tsMaxTablePerVnode) {
      mError("db:%s, minIdPoolSize:%d newIdPoolSize:%d larger than maxTablesPerVnode:%d", pDb->name, minIdPoolSize, newIdPoolSize,
             tsMaxTablePerVnode);
      return TSDB_CODE_MND_NO_ENOUGH_DNODES;
    } else {
      newIdPoolSize = tsMaxTablePerVnode;
    }
  }

  for (int32_t v = 0; v < pDb->numOfVgroups; ++v) {
    SVgObj *pVgroup = pDb->vgList[v];
    if (pVgroup == NULL) continue;

    int32_t oldIdPoolSize = pVgroup->idPool->MaxSize();
    if (newIdPoolSize == oldIdPoolSize) continue;

    if (pVgroup->idPool->update(newIdPoolSize) < 0) {
      mError("vgId:%d, failed to update idPoolSize from %d to %d", pVgroup->vgId, oldIdPoolSize, newIdPoolSize);
      return TSDB_CODE_MND_NO_ENOUGH_DNODES;
    } else {
      mDebug("vgId:%d, idPoolSize update from %d to %d", pVgroup->vgId, oldIdPoolSize, newIdPoolSize);
    }
  }

  return TSDB_CODE_SUCCESS;
}

int32_t mnodeGetAvailableVgroup(SMnodeMsg *pMsg, SVgObj **ppVgroup, int32_t *pSid) {
  SDbObj *pDb = pMsg->pDb;
  std::lock_guard<std::mutex> _(pDb->mutex);
  
  for (int32_t v = 0; v < pDb->numOfVgroups; ++v) {
    int vgIndex = (v + pDb->vgListIndex) % pDb->numOfVgroups;
    SVgObj *pVgroup = pDb->vgList[vgIndex];
    if (pVgroup == NULL) {
      mError("db:%s, index:%d vgroup is null", pDb->name, vgIndex);
      return TSDB_CODE_MND_APP_ERROR;
    }

    int32_t sid = pVgroup->idPool->alloc();
    if (sid <= 0) {
      mDebug("msg:%p, app:%p db:%s, no enough sid in vgId:%d", pMsg, pMsg->rpcMsg.ahandle, pDb->name, pVgroup->vgId);
      continue;
    }

    *pSid = sid;
    *ppVgroup = pVgroup;
    pDb->vgListIndex = vgIndex;

    return TSDB_CODE_SUCCESS;
  }

  int maxVgroupsPerDb = tsMaxVgroupsPerDb;
  if (maxVgroupsPerDb <= 0) {
    maxVgroupsPerDb = mnodeGetOnlinDnodesCpuCoreNum();
    maxVgroupsPerDb = MAX(maxVgroupsPerDb, TSDB_MIN_VNODES_PER_DB);
    maxVgroupsPerDb = MIN(maxVgroupsPerDb, TSDB_MAX_VNODES_PER_DB);
  }

  int32_t code = TSDB_CODE_MND_NO_ENOUGH_DNODES;
  if (pDb->numOfVgroups < maxVgroupsPerDb) {
    mDebug("msg:%p, app:%p db:%s, try to create a new vgroup, numOfVgroups:%d maxVgroupsPerDb:%d", pMsg,
           pMsg->rpcMsg.ahandle, pDb->name, pDb->numOfVgroups, maxVgroupsPerDb);
    code = mnodeCreateVgroup(pMsg);
    if (code == TSDB_CODE_MND_ACTION_IN_PROGRESS) {
      return code;
    }
  }

  if (pDb->numOfVgroups < 1) {
    mDebug("msg:%p, app:%p db:%s, failed create new vgroup since:%s, numOfVgroups:%d maxVgroupsPerDb:%d ", pMsg,
           pMsg->rpcMsg.ahandle, pDb->name, tstrerror(code), pDb->numOfVgroups, maxVgroupsPerDb);
    return code;
  }

  SVgObj *pVgroup = pDb->vgList[0];
  if (pVgroup == NULL) {
    return code;
  }

  code = mnodeAllocVgroupIdPool(pVgroup);
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }

  int32_t sid = pVgroup->idPool->alloc();
  if (sid <= 0) {
    mError("msg:%p, app:%p db:%s, no enough sid in vgId:%d", pMsg, pMsg->rpcMsg.ahandle, pDb->name, pVgroup->vgId);
    return TSDB_CODE_MND_NO_ENOUGH_DNODES;
  }

  *pSid = sid;
  *ppVgroup = pVgroup;
  pDb->vgListIndex = 0;

  return TSDB_CODE_SUCCESS;
}

void *mnodeGetNextVgroup(void *pIter, SVgObj **pVgroup) { 
  return tsVgroupSdb->fetchRow(pIter, (void **)pVgroup); 
}

void mnodeCancelGetNextVgroup(void *pIter) {
  tsVgroupSdb->freeIter(pIter);
}

static int32_t mnodeCreateVgroupFp(SMnodeMsg *pMsg) {
  SVgObj *pVgroup = pMsg->pVgroup;
  SDbObj *pDb = pMsg->pDb;
  assert(pVgroup);

  mInfo("msg:%p, app:%p vgId:%d, is created in mnode, db:%s replica:%d", pMsg, pMsg->rpcMsg.ahandle, pVgroup->vgId,
        pDb->name, pVgroup->numOfVnodes);
  for (int32_t i = 0; i < pVgroup->numOfVnodes; ++i) {
    mInfo("msg:%p, app:%p vgId:%d, index:%d, dnode:%d", pMsg, pMsg->rpcMsg.ahandle, pVgroup->vgId, i,
          pVgroup->vnodeGid[i].dnodeId);
  }

  pMsg->expected = pVgroup->numOfVnodes;
  pMsg->successed = 0;
  pMsg->received = 0;
  mnodeSendCreateVgroupMsg(pVgroup, pMsg);

  return TSDB_CODE_MND_ACTION_IN_PROGRESS;
}

static int32_t mnodeCreateVgroupCb(SMnodeMsg *pMsg, int32_t code) {
  std::shared_ptr<SVgObj> pVgroup(pMsg->pVgroup);
  SDbObj *pDb = pMsg->pDb;
  assert(pVgroup);

  if (code != TSDB_CODE_SUCCESS) {
    mError("msg:%p, app:%p vgId:%d, failed to create in sdb, reason:%s", pMsg, pMsg->rpcMsg.ahandle, pVgroup->vgId,
           tstrerror(code));
    SSdbRow desc = {.type = SDB_OPER_GLOBAL, .pObj = pVgroup, .pTable = tsVgroupSdb.get()};
    desc.Delete();
    return code;
  } else {
    mInfo("msg:%p, app:%p vgId:%d, is created in sdb, db:%s replica:%d", pMsg, pMsg->rpcMsg.ahandle, pVgroup->vgId,
          pDb->name, pVgroup->numOfVnodes);
    pVgroup->status = TAOS_VG_STATUS_READY;
    SSdbRow desc = {.type = SDB_OPER_GLOBAL, .pObj = pVgroup, .pTable = tsVgroupSdb.get()};
    (void)desc.Update();

    dnodeReprocessMWriteMsg(pMsg);
    return TSDB_CODE_MND_ACTION_IN_PROGRESS;
    // if (pVgroup->status == TAOS_VG_STATUS_CREATING || pVgroup->status == TAOS_VG_STATUS_READY) {
    //   mInfo("msg:%p, app:%p vgId:%d, is created in sdb, db:%s replica:%d", pMsg, pMsg->rpcMsg.ahandle, pVgroup->vgId,
    //         pDb->name, pVgroup->numOfVnodes);
    //   pVgroup->status = TAOS_VG_STATUS_READY;
    //   SSdbRow desc = {.type = SDB_OPER_GLOBAL, .pObj = pVgroup, .pTable = tsVgroupSdb};
    //   (void)sdbUpdateRow(&desc);
    //   dnodeReprocessMWriteMsg(pMsg);
    //   return TSDB_CODE_MND_ACTION_IN_PROGRESS;
    // } else {
    //   mError("msg:%p, app:%p vgId:%d, is created in sdb, db:%s replica:%d, but vgroup is dropping", pMsg->rpcMsg.ahandle,
    //          pMsg, pVgroup->vgId, pDb->name, pVgroup->numOfVnodes);
    //   return TSDB_CODE_MND_VGROUP_NOT_EXIST;
    // }
  }
}

int32_t mnodeCreateVgroup(SMnodeMsg *pMsg) {
  if (pMsg == NULL) return TSDB_CODE_MND_APP_ERROR;
  SDbObj *pDb = pMsg->pDb;

  auto pVgroup = std::make_shared<SVgObj>();
  tstrncpy(pVgroup->dbName, pDb->name, TSDB_ACCT_ID_LEN + TSDB_DB_NAME_LEN);
  pVgroup->numOfVnodes = pDb->cfg.replications;
  pVgroup->createdTime = taosGetTimestampMs();
  pVgroup->accessState = TSDB_VN_ALL_ACCCESS;
  int32_t code = bnAllocVnodes(pVgroup.get());
  if (code != TSDB_CODE_SUCCESS) {
    mError("db:%s, no enough dnode to alloc %d vnodes to vgroup, reason:%s", pDb->name, pVgroup->numOfVnodes,
           tstrerror(code));
    return code;
  }

  if (pMsg->pVgroup != NULL) {
  }

  pMsg->pVgroup = pVgroup.get();

  SSdbRow row;
  row.type = SDB_OPER_GLOBAL;
  row.pTable = tsVgroupSdb.get();
  row.pObj = pVgroup;
  row.rowSize = sizeof(SVgObj);
  row.pMsg = pMsg;
  row.fpReq = mnodeCreateVgroupFp;

  code = row.Insert();
  if (code != TSDB_CODE_SUCCESS && code != TSDB_CODE_MND_ACTION_IN_PROGRESS) {
    pMsg->pVgroup = NULL;
  }

  return code;
}

void mnodeDropVgroup(SVgObj *pVgroup, void *ahandle) {
  if (ahandle != NULL) {
    mnodeSendDropVgroupMsg(pVgroup, ahandle);
  } else {
    mDebug("vgId:%d, replica:%d is deleting from sdb", pVgroup->vgId, pVgroup->numOfVnodes);
    mnodeSendDropVgroupMsg(pVgroup, NULL);
    SSdbRow row;
    row.type = SDB_OPER_GLOBAL;
    row.pTable = tsVgroupSdb.get();
    row.pObj.reset(pVgroup);
    row.Delete();
  }
}

void mnodeCleanupVgroups() {
  tsVgroupSdb.reset();
}

int64_t mnodeGetVgroupNum() {
  return tsVgroupSdb->getNumOfRows();
}

int32_t mnodeGetVgroupMeta(STableMetaMsg *pMeta, SShowObj *pShow, void *pConn) {
  SDbObj *pDb = mnodeGetDb(pShow->db);
  if (pDb == NULL) {
    return TSDB_CODE_MND_DB_NOT_SELECTED;
  }
  
  if (pDb->status != TSDB_DB_STATUS_READY) {
    mError("db:%s, status:%d, in dropping", pDb->name, pDb->status);
    return TSDB_CODE_MND_DB_IN_DROPPING;
  }

  int32_t cols = 0;
  SSchema *pSchema = pMeta->schema;

  pShow->bytes[cols] = 4;
  pSchema[cols].type = TSDB_DATA_TYPE_INT;
  strcpy(pSchema[cols].name, "vgId");
  pSchema[cols].bytes = htons(pShow->bytes[cols]);
  cols++;

  pShow->bytes[cols] = 4;
  pSchema[cols].type = TSDB_DATA_TYPE_INT;
  strcpy(pSchema[cols].name, "tables");
  pSchema[cols].bytes = htons(pShow->bytes[cols]);
  cols++;

  pShow->bytes[cols] = 8 + VARSTR_HEADER_SIZE;
  pSchema[cols].type = TSDB_DATA_TYPE_BINARY;
  strcpy(pSchema[cols].name, "status");
  pSchema[cols].bytes = htons(pShow->bytes[cols]);
  cols++;

  pShow->bytes[cols] = 4;
  pSchema[cols].type = TSDB_DATA_TYPE_INT;
  strcpy(pSchema[cols].name, "onlines");
  pSchema[cols].bytes = htons(pShow->bytes[cols]);
  cols++;

  pShow->maxReplica = 1;
  for (int32_t v = 0; v < pDb->numOfVgroups; ++v) {
    SVgObj *pVgroup = pDb->vgList[v];
    if (pVgroup != NULL) {
      pShow->maxReplica = pVgroup->numOfVnodes > pShow->maxReplica ? pVgroup->numOfVnodes : pShow->maxReplica;
    }
  }

  for (int32_t i = 0; i < pShow->maxReplica; ++i) {
    pShow->bytes[cols] = 2;
    pSchema[cols].type = TSDB_DATA_TYPE_SMALLINT;
    snprintf(pSchema[cols].name, TSDB_COL_NAME_LEN, "v%d_dnode", i + 1);
    pSchema[cols].bytes = htons(pShow->bytes[cols]);
    cols++;

    pShow->bytes[cols] = 9 + VARSTR_HEADER_SIZE;
    pSchema[cols].type = TSDB_DATA_TYPE_BINARY;
    snprintf(pSchema[cols].name, TSDB_COL_NAME_LEN, "v%d_status", i + 1);
    pSchema[cols].bytes = htons(pShow->bytes[cols]);
    cols++;
  }

  pMeta->numOfColumns = htons(cols);
  pShow->numOfColumns = cols;

  pShow->offset[0] = 0;
  for (int32_t i = 1; i < cols; ++i) {
    pShow->offset[i] = pShow->offset[i - 1] + pShow->bytes[i - 1];
  }

  pShow->numOfRows = pDb->numOfVgroups;
  pShow->rowSize = pShow->offset[cols - 1] + pShow->bytes[cols - 1];

  return 0;
}

static bool mnodeFilterVgroups(SVgObj *pVgroup, STableObj *pTable) {
  if (NULL == pTable || pTable->info.type == TSDB_SUPER_TABLE) {
    return true;
  }

  SCTableObj *pCTable = (SCTableObj *)pTable;
  if (pVgroup->vgId == pCTable->vgId) {
    return true;
  } else {
    return false;
  }
}

int32_t mnodeRetrieveVgroups(SShowObj *pShow, char *data, int32_t rows, void *pConn) {
  int32_t numOfRows = 0;
  SVgObj *pVgroup = NULL;
  int32_t cols = 0;
  char *  pWrite;

  SDbObj *pDb = mnodeGetDb(pShow->db);
  if (pDb == NULL) return 0;

  if (pDb->status != TSDB_DB_STATUS_READY) {
    mError("db:%s, status:%d, in dropping", pDb->name, pDb->status);
    return 0;
  }

  STableObj *pTable = NULL;
  if (pShow->payloadLen > 0 ) {
    pTable = static_cast<STableObj *>(mnodeGetTable(pShow->payload));
  }

  while (numOfRows < rows) {
    pShow->pIter = mnodeGetNextVgroup(pShow->pIter, &pVgroup);
    if (pVgroup == NULL) break;

    if (pVgroup->pDb != pDb) {
      continue;
    }

    if (!mnodeFilterVgroups(pVgroup, pTable)) {
      continue;
    }

    cols = 0;

    pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
    *(int32_t *) pWrite = pVgroup->vgId;
    cols++;

    pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
    *(int32_t *) pWrite = pVgroup->numOfTables;
    cols++;

    pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;  
    char* status = vgroupStatus[pVgroup->status];
    STR_TO_VARSTR(pWrite, status);
    cols++;

    int32_t onlineVnodes = 0;
    for (int32_t i = 0; i < pShow->maxReplica; ++i) {
      if (pVgroup->vnodeGid[i].role == TAOS_SYNC_ROLE_SLAVE || pVgroup->vnodeGid[i].role == TAOS_SYNC_ROLE_MASTER) {
        onlineVnodes++;
      }
    }

    pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
    *(int32_t *)pWrite = onlineVnodes;
    cols++;

    for (int32_t i = 0; i < pShow->maxReplica; ++i) {
      pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
      *(int16_t *) pWrite = pVgroup->vnodeGid[i].dnodeId;
      cols++;

      SDnodeObj * pDnode = pVgroup->vnodeGid[i].pDnode;
      const char *role = "NULL";
      if (pDnode != NULL) {
        role = syncRole[pVgroup->vnodeGid[i].role];
      }

      pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
      STR_WITH_MAXSIZE_TO_VARSTR(pWrite, role, pShow->bytes[cols]);
      cols++;
    }

    numOfRows++;
  }

  mnodeVacuumResult(data, pShow->numOfColumns, numOfRows, rows, pShow);

  pShow->numOfReads += numOfRows;

  return numOfRows;
}

void mnodeAddTableIntoVgroup(SVgObj *pVgroup, SCTableObj *pTable) {
  int32_t idPoolSize = pVgroup->idPool->MaxSize();
  if (pTable->tid > idPoolSize) {
    mnodeAllocVgroupIdPool(pVgroup);
  }

  if (pTable->tid >= 1) {
    pVgroup->idPool->markStatus(pTable->tid);
    pVgroup->numOfTables++;
    // The create vgroup message may be received later than the create table message
    // and the writing order in sdb is therefore uncertain
    // which will cause the reference count of the vgroup to be incorrect when restarting
    // mnodeIncVgroupRef(pVgroup);
  }
}

void mnodeRemoveTableFromVgroup(SVgObj *pVgroup, SCTableObj *pTable) {
  if (pTable->tid >= 1) {
    pVgroup->idPool->dealloc(pTable->tid);
    pVgroup->numOfTables--;
    // The create vgroup message may be received later than the create table message
    // and the writing order in sdb is therefore uncertain
    // which will cause the reference count of the vgroup to be incorrect when restarting
  }
}

static SCreateVnodeMsg *mnodeBuildVnodeMsg(SVgObj *pVgroup) {
  SDbObj *pDb = pVgroup->pDb;
  if (pDb == NULL) return NULL;

  SCreateVnodeMsg *pVnode = static_cast<SCreateVnodeMsg *>(rpcMallocCont(sizeof(SCreateVnodeMsg)));
  if (pVnode == NULL) return NULL;

  strcpy(pVnode->db, pVgroup->dbName);
  int32_t maxTables = pVgroup->idPool->MaxSize();
  //TODO: dynamic alloc tables in tsdb
  maxTables = MAX(10000, tsMaxTablePerVnode);

  SVnodeCfg *pCfg = &pVnode->cfg;
  pCfg->vgId                = htonl(pVgroup->vgId);
  pCfg->dbCfgVersion        = htonl(pDb->dbCfgVersion);
  pCfg->vgCfgVersion        = htonl(pVgroup->vgCfgVersion);
  pCfg->cacheBlockSize      = htonl(pDb->cfg.cacheBlockSize);
  pCfg->totalBlocks         = htonl(pDb->cfg.totalBlocks);
  pCfg->maxTables           = htonl(maxTables + 1);
  pCfg->daysPerFile         = htonl(pDb->cfg.daysPerFile);
  pCfg->daysToKeep          = htonl(pDb->cfg.daysToKeep);
  pCfg->daysToKeep1         = htonl(pDb->cfg.daysToKeep1);
  pCfg->daysToKeep2         = htonl(pDb->cfg.daysToKeep2);  
  pCfg->minRowsPerFileBlock = htonl(pDb->cfg.minRowsPerFileBlock);
  pCfg->maxRowsPerFileBlock = htonl(pDb->cfg.maxRowsPerFileBlock);
  pCfg->fsyncPeriod         = htonl(pDb->cfg.fsyncPeriod);
  pCfg->commitTime          = htonl(pDb->cfg.commitTime);
  pCfg->precision           = pDb->cfg.precision;
  pCfg->compression         = pDb->cfg.compression;
  pCfg->walLevel            = pDb->cfg.walLevel;
  pCfg->vgReplica           = (int8_t) pVgroup->numOfVnodes;
  pCfg->wals                = 3;
  pCfg->quorum              = pDb->cfg.quorum;
  pCfg->update              = pDb->cfg.update;
  pCfg->cacheLastRow        = pDb->cfg.cacheLastRow;
  pCfg->dbReplica           = pDb->cfg.replications;
  
  SVnodeDesc *pNodes = pVnode->nodes;
  for (int32_t j = 0; j < pVgroup->numOfVnodes; ++j) {
    SDnodeObj *pDnode = pVgroup->vnodeGid[j].pDnode;
    if (pDnode != NULL) {
      pNodes[j].nodeId = htonl(pDnode->dnodeId);
      strcpy(pNodes[j].nodeEp, pDnode->dnodeEp);
    }
  }

  return pVnode;
}

SRpcEpSet mnodeGetEpSetFromVgroup(SVgObj *pVgroup) {
  SRpcEpSet epSet;
  epSet.numOfEps = pVgroup->numOfVnodes;
  epSet.inUse = pVgroup->inUse;
  for (int i = 0; i < pVgroup->numOfVnodes; ++i) {
    strcpy(epSet.fqdn[i], pVgroup->vnodeGid[i].pDnode->dnodeFqdn);
    epSet.port[i] = pVgroup->vnodeGid[i].pDnode->dnodePort + TSDB_PORT_DNODEDNODE;
  }
  return epSet;
}

SRpcEpSet mnodeGetEpSetFromIp(char *ep) {
  SRpcEpSet epSet;

  epSet.numOfEps = 1;
  epSet.inUse = 0;
  taosGetFqdnPortFromEp(ep, epSet.fqdn[0], &epSet.port[0]);
  epSet.port[0] += TSDB_PORT_DNODEDNODE;
  return epSet;
}

static void mnodeSendAlterVnodeMsg(SVgObj *pVgroup, SRpcEpSet *epSet) {
  SAlterVnodeMsg *pAlter = mnodeBuildVnodeMsg(pVgroup);
  SRpcMsg rpcMsg;
  rpcMsg.ahandle = NULL;
  rpcMsg.pCont = pAlter;
  rpcMsg.contLen = pAlter ? sizeof(SAlterVnodeMsg) : 0;
  rpcMsg.code = 0;
  rpcMsg.msgType = TSDB_MSG_TYPE_MD_ALTER_VNODE;
  dnodeSendMsgToDnode(epSet, &rpcMsg);
}

void mnodeSendAlterVgroupMsg(SVgObj *pVgroup) {
  mDebug("vgId:%d, send alter all vnodes msg, numOfVnodes:%d db:%s", pVgroup->vgId, pVgroup->numOfVnodes,
         pVgroup->dbName);
  for (int32_t i = 0; i < pVgroup->numOfVnodes; ++i) {
    SRpcEpSet epSet = mnodeGetEpSetFromIp(pVgroup->vnodeGid[i].pDnode->dnodeEp);
    mDebug("vgId:%d, index:%d, send alter vnode msg to dnode %s", pVgroup->vgId, i,
           pVgroup->vnodeGid[i].pDnode->dnodeEp);
    mnodeSendAlterVnodeMsg(pVgroup, &epSet);
  }
}

static void mnodeSendCreateVnodeMsg(SVgObj *pVgroup, SRpcEpSet *epSet, void *ahandle) {
  SCreateVnodeMsg *pCreate = mnodeBuildVnodeMsg(pVgroup);
  SRpcMsg rpcMsg;
  rpcMsg.ahandle = ahandle;
  rpcMsg.pCont = pCreate;
  rpcMsg.contLen = pCreate ? sizeof(SCreateVnodeMsg) : 0;
  rpcMsg.code = 0;
  rpcMsg.msgType = TSDB_MSG_TYPE_MD_CREATE_VNODE;
  dnodeSendMsgToDnode(epSet, &rpcMsg);
}

void mnodeSendCreateVgroupMsg(SVgObj *pVgroup, void *ahandle) {
  mDebug("vgId:%d, send create all vnodes msg, numOfVnodes:%d db:%s", pVgroup->vgId, pVgroup->numOfVnodes,
         pVgroup->dbName);
  for (int32_t i = 0; i < pVgroup->numOfVnodes; ++i) {
    SRpcEpSet epSet = mnodeGetEpSetFromIp(pVgroup->vnodeGid[i].pDnode->dnodeEp);
    mDebug("vgId:%d, index:%d, send create vnode msg to dnode %s, ahandle:%p", pVgroup->vgId,
           i, pVgroup->vnodeGid[i].pDnode->dnodeEp, ahandle);
    mnodeSendCreateVnodeMsg(pVgroup, &epSet, ahandle);
  }
}

void mnodeProcessAlterVnodeRsp(SRpcMsg *rpcMsg) {
  mDebug("alter vnode rsp received");
}

void mnodeProcessCreateVnodeRsp(SRpcMsg *rpcMsg) {
  if (rpcMsg->ahandle == NULL) return;

  SMnodeMsg *mnodeMsg = static_cast<SMnodeMsg *>(rpcMsg->ahandle);
  atomic_add_fetch_8(&mnodeMsg->received, 1);
  if (rpcMsg->code == TSDB_CODE_SUCCESS) {
    atomic_add_fetch_8(&mnodeMsg->successed, 1);
  } else {
    mnodeMsg->code = rpcMsg->code;
  }

  std::shared_ptr<SVgObj> pVgroup(mnodeMsg->pVgroup);
  mDebug("vgId:%d, create vnode rsp received, result:%s received:%d successed:%d expected:%d, thandle:%p ahandle:%p",
         pVgroup->vgId, tstrerror(rpcMsg->code), mnodeMsg->received, mnodeMsg->successed, mnodeMsg->expected,
         mnodeMsg->rpcMsg.handle, rpcMsg->ahandle);

  assert(mnodeMsg->received <= mnodeMsg->expected);

  if (mnodeMsg->received != mnodeMsg->expected) return;

  if (mnodeMsg->received == mnodeMsg->successed) {
    auto row = std::make_shared<SSdbRow>();
    row->type = SDB_OPER_GLOBAL;
    row->pTable = tsVgroupSdb.get();
    row->pObj = pVgroup;
    row->rowSize = sizeof(SVgObj);
    row->pMsg = mnodeMsg;
    row->fpRsp = mnodeCreateVgroupCb;

    int32_t code = sdbInsertRowToQueue(row);
    if (code != TSDB_CODE_SUCCESS && code != TSDB_CODE_MND_ACTION_IN_PROGRESS) {
      mnodeMsg->pVgroup = nullptr;

      if (mnodeMsg->pBatchMasterMsg) {
        ++mnodeMsg->pBatchMasterMsg->received;
        if (mnodeMsg->pBatchMasterMsg->successed + mnodeMsg->pBatchMasterMsg->received
            >= mnodeMsg->pBatchMasterMsg->expected) {
          dnodeSendRpcMWriteRsp(mnodeMsg->pBatchMasterMsg, code);
        }

        mnodeDestroySubMsg(mnodeMsg);

        return;
      }

      dnodeSendRpcMWriteRsp(mnodeMsg, code);
    }
  } else {
    SSdbRow row;
    row.type = SDB_OPER_GLOBAL;
    row.pTable = tsVgroupSdb.get();
    row.pObj = pVgroup;
    row.Delete();

    if (mnodeMsg->pBatchMasterMsg) {
      ++mnodeMsg->pBatchMasterMsg->received;
      if (mnodeMsg->pBatchMasterMsg->successed + mnodeMsg->pBatchMasterMsg->received
          >= mnodeMsg->pBatchMasterMsg->expected) {
        dnodeSendRpcMWriteRsp(mnodeMsg->pBatchMasterMsg, mnodeMsg->code);
      }

      mnodeDestroySubMsg(mnodeMsg);

      return;
    }

    dnodeSendRpcMWriteRsp(mnodeMsg, mnodeMsg->code);
  }
}

static SDropVnodeMsg *mnodeBuildDropVnodeMsg(int32_t vgId) {
  SDropVnodeMsg *pDrop = static_cast<SDropVnodeMsg *>(rpcMallocCont(sizeof(SDropVnodeMsg)));
  if (pDrop == NULL) return NULL;

  pDrop->vgId = htonl(vgId);
  return pDrop;
}

void mnodeSendDropVnodeMsg(int32_t vgId, SRpcEpSet *epSet, void *ahandle) {
  SDropVnodeMsg *pDrop = mnodeBuildDropVnodeMsg(vgId);
  SRpcMsg rpcMsg;
  rpcMsg.ahandle = ahandle;
  rpcMsg.pCont = pDrop;
  rpcMsg.contLen = pDrop ? sizeof(SDropVnodeMsg) : 0;
  rpcMsg.code = 0;
  rpcMsg.msgType = TSDB_MSG_TYPE_MD_DROP_VNODE;
  dnodeSendMsgToDnode(epSet, &rpcMsg);
}

static void mnodeSendDropVgroupMsg(SVgObj *pVgroup, void *ahandle) {
  pVgroup->status = TAOS_VG_STATUS_DROPPING; // deleting
  mDebug("vgId:%d, send drop all vnodes msg, ahandle:%p db:%s", pVgroup->vgId, ahandle, pVgroup->dbName);
  for (int32_t i = 0; i < pVgroup->numOfVnodes; ++i) {
    SRpcEpSet epSet = mnodeGetEpSetFromIp(pVgroup->vnodeGid[i].pDnode->dnodeEp);
    mDebug("vgId:%d, send drop vnode msg to dnode:%d, ahandle:%p", pVgroup->vgId, pVgroup->vnodeGid[i].dnodeId, ahandle);
    mnodeSendDropVnodeMsg(pVgroup->vgId, &epSet, ahandle);
  }
}

void mnodeProcessDropVnodeRsp(SRpcMsg *rpcMsg) {
  mDebug("drop vnode rsp is received, handle:%p", rpcMsg->ahandle);
  if (rpcMsg->ahandle == NULL) return;

  SMnodeMsg *mnodeMsg = static_cast<SMnodeMsg *>(rpcMsg->ahandle);
  mnodeMsg->received++;
  if (rpcMsg->code == TSDB_CODE_SUCCESS) {
    mnodeMsg->code = rpcMsg->code;
    mnodeMsg->successed++;
  }

  std::shared_ptr<SVgObj> pVgroup(mnodeMsg->pVgroup);
  mDebug("vgId:%d, drop vnode rsp received, result:%s received:%d successed:%d expected:%d, thandle:%p ahandle:%p",
         pVgroup->vgId, tstrerror(rpcMsg->code), mnodeMsg->received, mnodeMsg->successed, mnodeMsg->expected,
         mnodeMsg->rpcMsg.handle, rpcMsg->ahandle);

  if (mnodeMsg->received != mnodeMsg->expected) return;

  SSdbRow row;
  row.type = SDB_OPER_GLOBAL;
  row.pTable = tsVgroupSdb.get();
  row.pObj = pVgroup;
  int32_t code = row.Delete();
  if (code != 0) {
    code = TSDB_CODE_MND_SDB_ERROR;
  }

  dnodeReprocessMWriteMsg(mnodeMsg);
}

int32_t mnodeProcessVnodeCfgMsg(SMnodeMsg *pMsg) {
  SConfigVnodeMsg *pCfg = static_cast<SConfigVnodeMsg *>(pMsg->rpcMsg.pCont);
  pCfg->dnodeId = htonl(pCfg->dnodeId);
  pCfg->vgId    = htonl(pCfg->vgId);

  SDnodeObj *pDnode = static_cast<SDnodeObj *>(mnodeGetDnode(pCfg->dnodeId));
  if (pDnode == NULL) {
    mDebug("dnode:%s, vgId:%d, invalid dnode", taosIpStr(pCfg->dnodeId), pCfg->vgId);
    return TSDB_CODE_MND_VGROUP_NOT_EXIST;
  }

  SVgObj *pVgroup = mnodeGetVgroup(pCfg->vgId);
  if (pVgroup == NULL) {
    mDebug("dnode:%s, vgId:%d, no vgroup info", taosIpStr(pCfg->dnodeId), pCfg->vgId);
    return TSDB_CODE_MND_VGROUP_NOT_EXIST;
  }

  mDebug("vgId:%d, send create vnode msg to dnode %s for vnode cfg msg", pVgroup->vgId, pDnode->dnodeEp);
  SRpcEpSet epSet = mnodeGetEpSetFromIp(pDnode->dnodeEp);
  mnodeSendCreateVnodeMsg(pVgroup, &epSet, NULL);

  return TSDB_CODE_SUCCESS;
}

void mnodeDropAllDnodeVgroups(SDnodeObj *pDropDnode) {
  void *  pIter = NULL;
  SVgObj *pVgroup = NULL;
  int32_t numOfVgroups = 0;

  mInfo("dnode:%d, all vgroups will be dropped from sdb", pDropDnode->dnodeId);

  while (1) {
    pIter = mnodeGetNextVgroup(pIter, &pVgroup);
    if (pVgroup == NULL) break;

    if (pVgroup->vnodeGid[0].dnodeId == pDropDnode->dnodeId) {
      mnodeDropAllChildTablesInVgroups(pVgroup);
      SSdbRow row;
      row.type = SDB_OPER_LOCAL;
      row.pTable = tsVgroupSdb.get();
      row.pObj.reset(pVgroup);
      row.Delete();
      numOfVgroups++;
    }
  }

  mInfo("dnode:%d, all vgroups:%d is dropped from sdb", pDropDnode->dnodeId, numOfVgroups);
}

#if 0
void mnodeUpdateAllDbVgroups(SDbObj *pAlterDb) {
  void *  pIter = NULL;
  SVgObj *pVgroup = NULL;

  mInfo("db:%s, all vgroups will be update in sdb", pAlterDb->name);

  while (1) {
    pIter = mnodeGetNextVgroup(pIter, &pVgroup);
    if (pVgroup == NULL) break;

    if (pVgroup->pDb == pAlterDb) {
      mnodeVgroupUpdateIdPool(pVgroup);
    }

  }

  mInfo("db:%s, all vgroups is updated in sdb", pAlterDb->name);
}
#endif

void mnodeDropAllDbVgroups(SDbObj *pDropDb) {
  void *  pIter = NULL;
  int32_t numOfVgroups = 0;
  SVgObj *pVgroup = NULL;

  mInfo("db:%s, all vgroups will be dropped from sdb", pDropDb->name);
  while (1) {
    pIter = mnodeGetNextVgroup(pIter, &pVgroup);
    if (pVgroup == NULL) break;

    if (pVgroup->pDb == pDropDb) {
      SSdbRow row;
      row.type = SDB_OPER_LOCAL;
      row.pTable = tsVgroupSdb.get();
      row.pObj.reset(pVgroup);
      row.Delete();
      numOfVgroups++;
    }

  }

  mInfo("db:%s, all vgroups:%d is dropped from sdb", pDropDb->name, numOfVgroups);
}

void mnodeSendDropAllDbVgroupsMsg(SDbObj *pDropDb) {
  void *  pIter = NULL;
  int32_t numOfVgroups = 0;
  SVgObj *pVgroup = NULL;

  mInfo("db:%s, all vgroups will be dropped in dnode", pDropDb->name);
  while (1) {
    pIter = mnodeGetNextVgroup(pIter, &pVgroup);
    if (pVgroup == NULL) break;

    if (pVgroup->pDb == pDropDb) {
      mnodeSendDropVgroupMsg(pVgroup, NULL);
    }

    numOfVgroups++;
  }

  mInfo("db:%s, all vgroups:%d drop msg is sent to dnode", pDropDb->name, numOfVgroups);
}

int32_t mnodeGetVgidVer(int8_t *cver) {
  int32_t iver = ((int32_t)cver[0]) * 10000 + ((int32_t)cver[1]) * 100 + (int32_t)cver[2];
  return iver;
}

void mnodeSetVgidVer(int8_t *cver, uint64_t iver) {
  cver[0] = (int8_t)((int32_t)(iver % 1000000) / 10000);
  cver[1] = (int8_t)((int32_t)(iver % 100000) / 100);
  cver[2] = (int8_t)(iver % 100);
}
