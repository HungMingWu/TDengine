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
#include "tglobal.h"
#include "trpc.h"
#include "tsync.h"
#include "tbn.h"
#include "tutil.h"
#include "tsocket.h"
#include "tdataformat.h"
#include "dnode.h"
#include "mnode.h"
#include "mnodeDef.h"
#include "mnodeInt.h"
#include "mnodeMnode.h"
#include "mnodeDnode.h"
#include "mnodeSdb.h"
#include "mnodeShow.h"
#include "mnodeUser.h"
#include "mnodeVgroup.h"
#include "SdbMgmt.h"

static std::shared_ptr<SSdbTable> tsMnodeSdb;
static int32_t   tsMnodeUpdateSize = 0;
static SRpcEpSet tsMEpForShell;
static SRpcEpSet tsMEpForPeer;
static SMInfos   tsMInfos;

#if defined(LINUX)
  static pthread_rwlock_t         tsMnodeLock;
  #define mnodeMnodeWrLock()      pthread_rwlock_wrlock(&tsMnodeLock)
  #define mnodeMnodeRdLock()      pthread_rwlock_rdlock(&tsMnodeLock)
  #define mnodeMnodeUnLock()      pthread_rwlock_unlock(&tsMnodeLock)
  #define mnodeMnodeInitLock()    pthread_rwlock_init(&tsMnodeLock, NULL)
  #define mnodeMnodeDestroyLock() pthread_rwlock_destroy(&tsMnodeLock)
#else
  static pthread_mutex_t          tsMnodeLock;
  #define mnodeMnodeWrLock()      pthread_mutex_lock(&tsMnodeLock)
  #define mnodeMnodeRdLock()      pthread_mutex_lock(&tsMnodeLock)
  #define mnodeMnodeUnLock()      pthread_mutex_unlock(&tsMnodeLock)
  #define mnodeMnodeInitLock()    pthread_mutex_init(&tsMnodeLock, NULL)
  #define mnodeMnodeDestroyLock() pthread_mutex_destroy(&tsMnodeLock)
#endif

int32_t SMnodeObj::insert() {
  SDnodeObj *pDnode = static_cast<SDnodeObj *>(mnodeGetDnode(mnodeId));
  if (pDnode == NULL) return TSDB_CODE_MND_DNODE_NOT_EXIST;

  pDnode->isMgmt = true;

  mInfo("mnode:%d, fqdn:%s ep:%s port:%u is created", mnodeId, pDnode->dnodeFqdn, pDnode->dnodeEp,
        pDnode->dnodePort);
  return TSDB_CODE_SUCCESS;
}

int32_t SMnodeObj::remove() {
  SDnodeObj *pDnode = static_cast<SDnodeObj *>(mnodeGetDnode(mnodeId));
  if (pDnode == NULL) return TSDB_CODE_MND_DNODE_NOT_EXIST;
  pDnode->isMgmt = false;

  mDebug("mnode:%d, is dropped from sdb", mnodeId);
  return TSDB_CODE_SUCCESS;
}

int32_t SMnodeObj::update() {
  SMnodeObj *pSaved = static_cast<SMnodeObj *>(mnodeGetMnode(mnodeId));
  if (this != pSaved) {
    memcpy(pSaved, this, sizeof(SMnodeObj));
    delete this;
  }
  return TSDB_CODE_SUCCESS;
}

int32_t SMnodeObj::encode(SSdbRow *pRow) {
  memcpy(pRow->rowData, this, tsMnodeUpdateSize);
  pRow->rowSize = tsMnodeUpdateSize;
  return TSDB_CODE_SUCCESS;
}

class MnodeTable : public SSdbTable {
 public:
  using SSdbTable::SSdbTable;
  int32_t decode(SSdbRow *pRow) override {
    auto pMnode = std::make_shared<SMnodeObj>();
    memcpy(pMnode.get(), pRow->rowData, tsMnodeUpdateSize);
    pRow->pObj = pMnode;
    return TSDB_CODE_SUCCESS;
  }
  int32_t restore() override {
    if (mnodeGetMnodesNum() == 1) {
      SMnodeObj *pMnode = NULL;
      void *     pIter = mnodeGetNextMnode(NULL, &pMnode);
      if (pMnode != NULL) {
        pMnode->role = TAOS_SYNC_ROLE_MASTER;
      }
      mnodeCancelGetNextMnode(pIter);
    }

    mnodeUpdateMnodeEpSet(NULL);

    return TSDB_CODE_SUCCESS;
  }
};

int32_t mnodeInitMnodes() {
  mnodeMnodeInitLock();

  SMnodeObj tObj;
  tsMnodeUpdateSize = (int8_t *)tObj.updateEnd - (int8_t *)&tObj;

  SSdbTableDesc desc;
  desc.id = SDB_TABLE_MNODE;
  desc.name = "mnodes";
  desc.hashSessions = TSDB_DEFAULT_MNODES_HASH_SIZE;
  desc.maxRowSize = tsMnodeUpdateSize;
  desc.keyType = SDB_KEY_INT;

  tsMnodeSdb = SSdbMgmt::instance().openTable<MnodeTable>(desc);
  if (tsMnodeSdb == NULL) {
    mError("failed to init mnodes data");
    return -1;
  }

  mDebug("table:mnodes table is created");
  return TSDB_CODE_SUCCESS;
}

void mnodeCleanupMnodes() {
  tsMnodeSdb.reset();
  mnodeMnodeDestroyLock();
}

int32_t mnodeGetMnodesNum() { 
  return tsMnodeSdb->getNumOfRows();
}

void *mnodeGetMnode(int32_t mnodeId) {
  return tsMnodeSdb->getRow(&mnodeId);
}

void *mnodeGetNextMnode(void *pIter, SMnodeObj **pMnode) { 
  return tsMnodeSdb->fetchRow(pIter, (void **)pMnode); 
}

void mnodeCancelGetNextMnode(void *pIter) {
  tsMnodeSdb->freeIter(pIter);
}

void mnodeUpdateMnodeEpSet(SMInfos *pMinfos) {
  bool    set = false;
  SMInfos mInfos = {0};

  if (pMinfos != NULL) {
    mInfo("vgId:1, update mnodes epSet, numOfMinfos:%d", pMinfos->mnodeNum);
    set = true;
    mInfos = *pMinfos;
  } else {
    mInfo("vgId:1, update mnodes epSet, numOfMnodes:%d", mnodeGetMnodesNum());
    int32_t index = 0;
    void *  pIter = NULL;
    while (1) {
      SMnodeObj *pMnode = NULL;
      pIter = mnodeGetNextMnode(pIter, &pMnode);
      if (pMnode == NULL) break;

      SDnodeObj *pDnode = static_cast<SDnodeObj *>(mnodeGetDnode(pMnode->mnodeId));
      if (pDnode != NULL) {
        set = true;
        mInfos.mnodeInfos[index].mnodeId = pMnode->mnodeId;
        strcpy(mInfos.mnodeInfos[index].mnodeEp, pDnode->dnodeEp);
        if (pMnode->role == TAOS_SYNC_ROLE_MASTER) mInfos.inUse = index;
        index++;
      } else {
        set = false;
      }
    }

    mInfos.mnodeNum = index;
    if (mInfos.mnodeNum < sdbGetReplicaNum()) {
      set = false;
      mDebug("vgId:1, mnodes info not synced, current:%d syncCfgNum:%d", mInfos.mnodeNum, sdbGetReplicaNum());
    }
  }

  mnodeMnodeWrLock();

  if (set) {
    memset(&tsMEpForShell, 0, sizeof(SRpcEpSet));
    memset(&tsMEpForPeer, 0, sizeof(SRpcEpSet));
    memcpy(&tsMInfos, &mInfos, sizeof(SMInfos));
    tsMEpForShell.inUse = tsMInfos.inUse;
    tsMEpForPeer.inUse = tsMInfos.inUse;
    tsMEpForShell.numOfEps = tsMInfos.mnodeNum;
    tsMEpForPeer.numOfEps = tsMInfos.mnodeNum;

    mInfo("vgId:1, mnodes epSet is set, num:%d inUse:%d", tsMInfos.mnodeNum, tsMInfos.inUse);
    for (int index = 0; index < mInfos.mnodeNum; ++index) {
      SMInfo *pInfo = &tsMInfos.mnodeInfos[index];
      taosGetFqdnPortFromEp(pInfo->mnodeEp, tsMEpForShell.fqdn[index], &tsMEpForShell.port[index]);
      taosGetFqdnPortFromEp(pInfo->mnodeEp, tsMEpForPeer.fqdn[index], &tsMEpForPeer.port[index]);
      tsMEpForPeer.port[index] = tsMEpForPeer.port[index] + TSDB_PORT_DNODEDNODE;

      mInfo("vgId:1, mnode:%d, fqdn:%s shell:%u peer:%u", pInfo->mnodeId, tsMEpForShell.fqdn[index],
            tsMEpForShell.port[index], tsMEpForPeer.port[index]);

      tsMEpForShell.port[index] = htons(tsMEpForShell.port[index]);
      tsMEpForPeer.port[index] = htons(tsMEpForPeer.port[index]);
      pInfo->mnodeId = htonl(pInfo->mnodeId);
    }
  } else {
    mInfo("vgId:1, mnodes epSet not set, num:%d inUse:%d", tsMInfos.mnodeNum, tsMInfos.inUse);
    for (int index = 0; index < tsMInfos.mnodeNum; ++index) {
      mInfo("vgId:1, index:%d, ep:%s:%u", index, tsMEpForShell.fqdn[index], htons(tsMEpForShell.port[index]));
    }
  }

  mnodeMnodeUnLock();
}

void mnodeGetMnodeEpSetForPeer(SRpcEpSet *epSet, bool redirect) {
  mnodeMnodeRdLock();
  *epSet = tsMEpForPeer;
  mnodeMnodeUnLock();

  mTrace("vgId:1, mnodes epSet for peer is returned, num:%d inUse:%d", tsMEpForPeer.numOfEps, tsMEpForPeer.inUse);
  for (int32_t i = 0; i < epSet->numOfEps; ++i) {
    if (redirect && strcmp(epSet->fqdn[i], tsLocalFqdn) == 0 && htons(epSet->port[i]) == tsServerPort + TSDB_PORT_DNODEDNODE) {
      epSet->inUse = (i + 1) % epSet->numOfEps;
      mTrace("vgId:1, mnode:%d, for peer ep:%s:%u, set inUse to %d", i, epSet->fqdn[i], htons(epSet->port[i]), epSet->inUse);
    } else {
      mTrace("vgId:1, mpeer:%d, for peer ep:%s:%u", i, epSet->fqdn[i], htons(epSet->port[i]));
    }
  }
}

void mnodeGetMnodeEpSetForShell(SRpcEpSet *epSet, bool redirect) {
  mnodeMnodeRdLock();
  *epSet = tsMEpForShell;
  mnodeMnodeUnLock();

  if (mnodeGetDnodesNum() <= 1) {
    epSet->numOfEps = 0;
    return;
  }

  mTrace("vgId:1, mnodes epSet for shell is returned, num:%d inUse:%d", tsMEpForShell.numOfEps, tsMEpForShell.inUse);
  for (int32_t i = 0; i < epSet->numOfEps; ++i) {
    if (redirect && strcmp(epSet->fqdn[i], tsLocalFqdn) == 0 && htons(epSet->port[i]) == tsServerPort) {
      epSet->inUse = (i + 1) % epSet->numOfEps;
      mTrace("vgId:1, mnode:%d, for shell ep:%s:%u, set inUse to %d", i, epSet->fqdn[i], htons(epSet->port[i]), epSet->inUse);
    } else {
      mTrace("vgId:1, mnode:%d, for shell ep:%s:%u", i, epSet->fqdn[i], htons(epSet->port[i]));
    }
  }
}

char* mnodeGetMnodeMasterEp() {
  return tsMInfos.mnodeInfos[tsMInfos.inUse].mnodeEp;
}

void mnodeGetMnodeInfos(void *pMinfos) {
  mnodeMnodeRdLock();
  *(SMInfos *)pMinfos = tsMInfos;
  mnodeMnodeUnLock();
}

static int32_t mnodeSendCreateMnodeMsg(int32_t dnodeId, char *dnodeEp) {
  SCreateMnodeMsg *pCreate = static_cast<SCreateMnodeMsg *>(rpcMallocCont(sizeof(SCreateMnodeMsg)));
  if (pCreate == NULL) {
    return TSDB_CODE_MND_OUT_OF_MEMORY;
  } else {
    pCreate->dnodeId = htonl(dnodeId);
    tstrncpy(pCreate->dnodeEp, dnodeEp, sizeof(pCreate->dnodeEp));
    mnodeGetMnodeInfos(&pCreate->mnodes);
    bool found = false;
    for (int i = 0; i < pCreate->mnodes.mnodeNum; ++i) {
      if (pCreate->mnodes.mnodeInfos[i].mnodeId == htonl(dnodeId)) {
        found = true;
      }
    }
    if (!found) {
      pCreate->mnodes.mnodeInfos[pCreate->mnodes.mnodeNum].mnodeId = htonl(dnodeId);
      tstrncpy(pCreate->mnodes.mnodeInfos[pCreate->mnodes.mnodeNum].mnodeEp, dnodeEp, sizeof(pCreate->dnodeEp));
      pCreate->mnodes.mnodeNum++;
    }
  }

  mDebug("dnode:%d, send create mnode msg to dnode %s, numOfMnodes:%d", dnodeId, dnodeEp, pCreate->mnodes.mnodeNum);
  for (int32_t i = 0; i < pCreate->mnodes.mnodeNum; ++i) {
    mDebug("index:%d, mnodeId:%d ep:%s", i, pCreate->mnodes.mnodeInfos[i].mnodeId, pCreate->mnodes.mnodeInfos[i].mnodeEp);
  }

  SRpcMsg rpcMsg = {0};
  rpcMsg.pCont = pCreate;
  rpcMsg.contLen = sizeof(SCreateMnodeMsg);
  rpcMsg.msgType = TSDB_MSG_TYPE_MD_CREATE_MNODE;

  SRpcMsg   rpcRsp = {0};
  SRpcEpSet epSet = mnodeGetEpSetFromIp(pCreate->dnodeEp);
  dnodeSendMsgToDnodeRecv(&rpcMsg, &rpcRsp, &epSet);

  if (rpcRsp.code != TSDB_CODE_SUCCESS) {
    mError("dnode:%d, failed to send create mnode msg, ep:%s reason:%s", dnodeId, dnodeEp, tstrerror(rpcRsp.code));
  } else {
    mDebug("dnode:%d, create mnode msg is disposed, mnode is created in dnode", dnodeId);
  }

  rpcFreeCont(rpcRsp.pCont);
  return rpcRsp.code;
}

static int32_t mnodeCreateMnodeCb(SMnodeMsg *pMsg, int32_t code) {
  if (code != TSDB_CODE_SUCCESS) {
    mError("failed to create mnode, reason:%s", tstrerror(code));
  } else {
    mDebug("mnode is created successfully");
    mnodeUpdateMnodeEpSet(NULL);
    sdbUpdateAsync();
  }

  return code;
}

static bool mnodeAllOnline() {
  void *pIter = NULL;
  bool  allOnline = true;

  while (1) {
    SMnodeObj *pMnode = NULL;
    pIter = mnodeGetNextMnode(pIter, &pMnode);
    if (pMnode == NULL) break;
    if (pMnode->role != TAOS_SYNC_ROLE_MASTER && pMnode->role != TAOS_SYNC_ROLE_SLAVE) {
      allOnline = false;
      mDebug("mnode:%d, role:%s, not online", pMnode->mnodeId, syncRole[pMnode->role]);
    }
  }
  mnodeCancelGetNextMnode(pIter);

  return allOnline;
}

void mnodeCreateMnode(int32_t dnodeId, char *dnodeEp, bool needConfirm) {
  auto pMnode = std::make_shared<SMnodeObj>();
  pMnode->mnodeId = dnodeId;
  pMnode->createdTime = taosGetTimestampMs();

  SSdbRow row;
  row.type = SDB_OPER_GLOBAL;
  row.pTable = tsMnodeSdb.get();
  row.pObj = pMnode;
  row.fpRsp = mnodeCreateMnodeCb;

  if (needConfirm && !mnodeAllOnline()) {
    mDebug("wait all mnode online then create new mnode");
    return;
  }

  int32_t code = TSDB_CODE_SUCCESS;
  if (needConfirm) {
    code = mnodeSendCreateMnodeMsg(dnodeId, dnodeEp);
  }

  if (code != TSDB_CODE_SUCCESS) {
    return;
  }

  code = row.Insert();
  if (code != TSDB_CODE_SUCCESS && code != TSDB_CODE_MND_ACTION_IN_PROGRESS) {
    mError("dnode:%d, failed to create mnode, ep:%s reason:%s", dnodeId, dnodeEp, tstrerror(code));
  }
}

void mnodeDropMnodeLocal(int32_t dnodeId) {
  auto pMnode = std::make_shared<SMnodeObj>();
  if (pMnode != NULL) {
    SSdbRow row;
    row.type = SDB_OPER_LOCAL;
    row.pTable = tsMnodeSdb.get();
    row.pObj = pMnode;
    row.Delete();
  }

  mnodeUpdateMnodeEpSet(NULL);
  sdbUpdateAsync();
}

int32_t mnodeDropMnode(int32_t dnodeId) {
  std::shared_ptr<SMnodeObj> pMnode(static_cast<SMnodeObj *>(mnodeGetMnode(dnodeId)));
  if (pMnode == NULL) {
    return TSDB_CODE_MND_DNODE_NOT_EXIST;
  }
  
  SSdbRow row;
  row.type = SDB_OPER_GLOBAL;
  row.pTable = tsMnodeSdb.get();
  row.pObj = pMnode;

  int32_t code = row.Delete();

  mnodeUpdateMnodeEpSet(NULL);
  sdbUpdateAsync();

  return code;
}

int32_t mnodeGetMnodeMeta(STableMetaMsg *pMeta, SShowObj *pShow, void *pConn) {
  sdbUpdateMnodeRoles();
  SUserObj *pUser = mnodeGetUserFromConn(pConn);
  if (pUser == NULL) return 0;

  if (strcmp(pUser->pAcct->user, TSDB_DEFAULT_USER) != 0)  {
    return TSDB_CODE_MND_NO_RIGHTS;
  }

  int32_t  cols = 0;
  SSchema *pSchema = pMeta->schema;

  pShow->bytes[cols] = 2;
  pSchema[cols].type = TSDB_DATA_TYPE_SMALLINT;
  strcpy(pSchema[cols].name, "id");
  pSchema[cols].bytes = htons(pShow->bytes[cols]);
  cols++;

  pShow->bytes[cols] = 40 + VARSTR_HEADER_SIZE;
  pSchema[cols].type = TSDB_DATA_TYPE_BINARY;
  strcpy(pSchema[cols].name, "end_point");
  pSchema[cols].bytes = htons(pShow->bytes[cols]);
  cols++;

  pShow->bytes[cols] = 12 + VARSTR_HEADER_SIZE;
  pSchema[cols].type = TSDB_DATA_TYPE_BINARY;
  strcpy(pSchema[cols].name, "role");
  pSchema[cols].bytes = htons(pShow->bytes[cols]);
  cols++;
  
  pShow->bytes[cols] = 8;
  pSchema[cols].type = TSDB_DATA_TYPE_TIMESTAMP;
  strcpy(pSchema[cols].name, "create_time");
  pSchema[cols].bytes = htons(pShow->bytes[cols]);
  cols++;

  pMeta->numOfColumns = htons(cols);
  pShow->numOfColumns = cols;

  pShow->offset[0] = 0;
  for (int32_t i = 1; i < cols; ++i) {
    pShow->offset[i] = pShow->offset[i - 1] + pShow->bytes[i - 1];
  }

  pShow->numOfRows = mnodeGetMnodesNum();
  pShow->rowSize = pShow->offset[cols - 1] + pShow->bytes[cols - 1];
  pShow->pIter = NULL;

  return 0;
}

int32_t mnodeRetrieveMnodes(SShowObj *pShow, char *data, int32_t rows, void *pConn) {
  int32_t    numOfRows = 0;
  int32_t    cols      = 0;
  SMnodeObj *pMnode   = NULL;
  char      *pWrite;

  while (numOfRows < rows) {
    pShow->pIter = mnodeGetNextMnode(pShow->pIter, &pMnode);
    if (pMnode == NULL) break;

    cols = 0;

    pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
    *(int16_t *)pWrite = pMnode->mnodeId;
    cols++;

    pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
    
    SDnodeObj *pDnode = static_cast<SDnodeObj *>(mnodeGetDnode(pMnode->mnodeId));
    if (pDnode != NULL) {
      STR_WITH_MAXSIZE_TO_VARSTR(pWrite, pDnode->dnodeEp, pShow->bytes[cols]);
    } else {
      STR_WITH_MAXSIZE_TO_VARSTR(pWrite, "invalid ep", pShow->bytes[cols]);
    }

    cols++;

    pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
    const char* roles = syncRole[pMnode->role];
    STR_WITH_MAXSIZE_TO_VARSTR(pWrite, roles, pShow->bytes[cols]);
    cols++;

    pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
    *(int64_t *)pWrite = pMnode->createdTime;
    cols++;
    
    numOfRows++;
  }

  mnodeVacuumResult(data, pShow->numOfColumns, numOfRows, rows, pShow);
  pShow->numOfReads += numOfRows;

  return numOfRows;
}
