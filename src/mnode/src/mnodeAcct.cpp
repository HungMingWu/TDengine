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
#include "dnode.h"
#include "mnodeDef.h"
#include "mnodeInt.h"
#include "mnodeAcct.h"
#include "mnodeDb.h"
#include "mnodeSdb.h"
#include "mnodeUser.h"
#include "mnodeVgroup.h"
#include "SdbMgmt.h"

static int32_t tsAcctUpdateSize;
static int32_t mnodeCreateRootAcct();

int32_t SAcctObj::insert() {
  acctInfo.accessState = TSDB_VN_ALL_ACCCESS;
  return TSDB_CODE_SUCCESS;
}

int32_t SAcctObj::remove() {
  mnodeDropAllUsers(this);
  mnodeDropAllDbs(this);
  return TSDB_CODE_SUCCESS;
}

int32_t SAcctObj::update() {
    #if 0
  SAcctObj *pSaved = static_cast<SAcctObj *>(mnodeGetAcct(user));
  if (this != pSaved) {
    memcpy(pSaved, this, tsAcctUpdateSize);
    delete this;
  }
  #endif
  return TSDB_CODE_SUCCESS;
}

int32_t SAcctObj::encode(binser::memory_output_archive<> &out) {
  out(user, pass, cfg, createdTime, acctId, status, reserved0);
  return TSDB_CODE_SUCCESS;
}

class AcctTable : public SSdbTable {
 public:
  using SSdbTable::SSdbTable;
  int32_t decode(SSdbRow *pRow) override {
    auto pAcct = std::make_shared<SAcctObj>();
    memcpy(pAcct.get(), pRow->rowData, tsAcctUpdateSize);
    pRow->pObj = pAcct;
    return TSDB_CODE_SUCCESS;
  }
  int32_t restore() override {
    int32_t numOfRows = getNumOfRows();
    if (numOfRows <= 0 && dnodeIsFirstDeploy()) {
      mInfo("dnode first deploy, create root acct");
      int32_t code = mnodeCreateRootAcct();
      if (code != TSDB_CODE_SUCCESS && code != TSDB_CODE_MND_ACTION_IN_PROGRESS) {
        mError("failed to create root account, reason:%s", tstrerror(code));
        return code;
      }
    }

    acctInit();
    return TSDB_CODE_SUCCESS;
  }
};

std::shared_ptr<AcctTable> tsAcctSdb;

int32_t mnodeInitAccts() {
  SAcctObj tObj;
  tsAcctUpdateSize = (int8_t *)tObj.updateEnd - (int8_t *)&tObj;

  SSdbTableDesc desc;
  desc.id = SDB_TABLE_ACCOUNT;
  desc.name = "accounts";
  desc.hashSessions = TSDB_DEFAULT_ACCOUNTS_HASH_SIZE;
  desc.maxRowSize = tsAcctUpdateSize;
  desc.keyType = SDB_KEY_STRING;

  tsAcctSdb = SSdbMgmt::instance().openTable<AcctTable>(desc);
  if (tsAcctSdb == NULL) {
    mError("table:%s, failed to create hash", desc.name);
    return -1;
  }

  mDebug("table:%s, hash is created", desc.name);
  return TSDB_CODE_SUCCESS;
}

void mnodeCleanupAccts() {
  acctCleanUp();
  tsAcctSdb.reset();
}

void mnodeGetStatOfAllAcct(SAcctInfo* pAcctInfo) {
  memset(pAcctInfo, 0, sizeof(*pAcctInfo));

  void   *pIter = NULL;
  SAcctObj *pAcct = NULL;
  while (1) {
    pIter = mnodeGetNextAcct(pIter, &pAcct);
    if (pAcct == NULL) {
      break;
    }
    pAcctInfo->numOfDbs += pAcct->acctInfo.numOfDbs;
    pAcctInfo->numOfTimeSeries += pAcct->acctInfo.numOfTimeSeries;
  }

  SVgObj *pVgroup = NULL;
  pIter = NULL;
  while (1) {
    pIter = mnodeGetNextVgroup(pIter, &pVgroup);
    if (pVgroup == NULL) {
       break;
    }
    pAcctInfo->totalStorage += pVgroup->totalStorage;
    pAcctInfo->compStorage += pVgroup->compStorage;
    pAcctInfo->totalPoints += pVgroup->pointsWritten;
  }
}

void *mnodeGetAcct(char *name) {
  return tsAcctSdb->getRow(name);
}

void *mnodeGetNextAcct(void *pIter, SAcctObj **pAcct) {
  return tsAcctSdb->fetchRow(pIter, (void **)pAcct); 
}

void mnodeCancelGetNextAcct(void *pIter) {
  tsAcctSdb->freeIter(pIter);
}

void mnodeAddDbToAcct(AcctObjPtr pAcct, SDbObj *pDb) {
  pAcct->acctInfo.numOfDbs++;
  pDb->pAcct = pAcct;
}

void mnodeDropDbFromAcct(AcctObjPtr pAcct, SDbObj *pDb) {
  pAcct->acctInfo.numOfDbs--;
  pDb->pAcct.reset();
}

void mnodeAddUserToAcct(AcctObjPtr pAcct, SUserObj *pUser) {
  pAcct->acctInfo.numOfUsers++;
  pUser->pAcct = pAcct;
}

void mnodeDropUserFromAcct(AcctObjPtr pAcct, SUserObj *pUser) {
  pAcct->acctInfo.numOfUsers--;
  pUser->pAcct.reset();
}

static int32_t mnodeCreateRootAcct() {
  int32_t numOfAccts = tsAcctSdb->getNumOfRows();
  if (numOfAccts != 0) return TSDB_CODE_SUCCESS;

  auto pAcct = std::make_shared<SAcctObj>();
  strcpy(pAcct->user, TSDB_DEFAULT_USER);
  taosEncryptPass((uint8_t *)TSDB_DEFAULT_PASS, strlen(TSDB_DEFAULT_PASS), pAcct->pass);
  pAcct->cfg.maxUsers = 128;
  pAcct->cfg.maxDbs = 128;
  pAcct->cfg.maxTimeSeries = INT32_MAX;
  pAcct->cfg.maxConnections = 1024;
  pAcct->cfg.maxStreams = 1000;
  pAcct->cfg.maxPointsPerSecond = 10000000;
  pAcct->cfg.maxStorage = INT64_MAX;
  pAcct->cfg.maxQueryTime = INT64_MAX;
  pAcct->cfg.maxInbound = 0;
  pAcct->cfg.maxOutbound = 0;
  pAcct->cfg.accessState = TSDB_VN_ALL_ACCCESS;
  pAcct->acctId = tsAcctSdb->Id();
  pAcct->createdTime = taosGetTimestampMs();

  SSdbRow row;
  row.type = SDB_OPER_GLOBAL;
  row.pTable = tsAcctSdb.get();
  row.pObj = pAcct;

  return row.Insert();
}

#ifndef _ACCT

int32_t acctInit() { return TSDB_CODE_SUCCESS; }
void    acctCleanUp() {}
int32_t acctCheck(void *pAcct, EAcctGrantType type) { return TSDB_CODE_SUCCESS; }

#endif
