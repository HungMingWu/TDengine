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
#include "dnode.h"
#include "mnodeDef.h"
#include "mnodeInt.h"
#include "mnodeCluster.h"
#include "mnodeSdb.h"
#include "mnodeShow.h"
#include "tglobal.h"
#include "SdbMgmt.h"

static std::shared_ptr<SSdbTable> tsClusterSdb;
static int32_t tsClusterUpdateSize;
static char    tsClusterId[TSDB_CLUSTER_ID_LEN];
static int32_t mnodeCreateCluster();

int32_t SClusterObj::insert() {
  return TSDB_CODE_SUCCESS;
}

int32_t SClusterObj::remove() {
  return TSDB_CODE_SUCCESS;
}

int32_t SClusterObj::update() {
  return TSDB_CODE_SUCCESS;
}

int32_t SClusterObj::encode(SSdbRow *pRow) {
  std::vector<unsigned char> data;
  binser::memory_output_archive<> out(data);
  out(uid, createdTime, reserved);
  memcpy(pRow->rowData, this, tsClusterUpdateSize);
  pRow->rowSize = tsClusterUpdateSize;
  return TSDB_CODE_SUCCESS;
}

class ClusterTable : public SSdbTable {
 public:
  using SSdbTable::SSdbTable;
  int32_t decode(SSdbRow *pRow) override {
    auto *pCluster = new SClusterObj;
    if (pCluster == NULL) return TSDB_CODE_MND_OUT_OF_MEMORY;

    memcpy(pCluster, pRow->rowData, tsClusterUpdateSize);
    pRow->pObj = pCluster;
    return TSDB_CODE_SUCCESS;
  }
  int32_t restore() override { 
    int32_t numOfRows = getNumOfRows();
    if (numOfRows <= 0 && dnodeIsFirstDeploy()) {
      mInfo("dnode first deploy, create cluster");
      int32_t code = mnodeCreateCluster();
      if (code != TSDB_CODE_SUCCESS && code != TSDB_CODE_MND_ACTION_IN_PROGRESS) {
        mError("failed to create cluster, reason:%s", tstrerror(code));
        return code;
      }
    }

    mnodeUpdateClusterId();
    return TSDB_CODE_SUCCESS;
  }
};

int32_t mnodeInitCluster() {
  SClusterObj tObj;
  tsClusterUpdateSize = (int8_t *)tObj.updateEnd - (int8_t *)&tObj;

  SSdbTableDesc desc;
  desc.id = SDB_TABLE_CLUSTER;
  desc.name = "cluster";
  desc.hashSessions = TSDB_DEFAULT_CLUSTER_HASH_SIZE;
  desc.maxRowSize = tsClusterUpdateSize;
  desc.keyType = SDB_KEY_STRING;

  tsClusterSdb = SSdbMgmt::instance().openTable<ClusterTable>(desc);
  if (tsClusterSdb == NULL) {
    mError("table:%s failed to create hash", desc.name);
    return -1;
  }

  mDebug("table:%s, hash is created", desc.name);
  return TSDB_CODE_SUCCESS;
}

void mnodeCleanupCluster() {
  tsClusterSdb.reset();
}

void *mnodeGetNextCluster(void *pIter, SClusterObj **pCluster) {
  return tsClusterSdb->fetchRow(pIter, (void **)pCluster); 
}

void mnodeCancelGetNextCluster(void *pIter) {
  tsClusterSdb->freeIter(pIter);
}

void mnodeIncClusterRef(SClusterObj *pCluster) {
  tsClusterSdb->incRef(pCluster);
}

void mnodeDecClusterRef(SClusterObj *pCluster) {
  tsClusterSdb->decRef(pCluster);
}

static int32_t mnodeCreateCluster() {
  int32_t numOfClusters = tsClusterSdb->getNumOfRows();
  if (numOfClusters != 0) return TSDB_CODE_SUCCESS;

  auto pCluster = new SClusterObj;
  pCluster->createdTime = taosGetTimestampMs();
  bool getuid = taosGetSystemUid(pCluster->uid);
  if (!getuid) {
    strcpy(pCluster->uid, "tdengine2.0");
    mError("failed to get uid from system, set to default val %s", pCluster->uid);
  } else {
    mDebug("uid is %s", pCluster->uid);
  }

  SSdbRow row;
  row.type = SDB_OPER_GLOBAL;
  row.pTable = tsClusterSdb.get();
  row.pObj = pCluster;
  binser::memory_output_archive<> output(row.serializeRow);
  output(*pCluster);
  return row.Insert();
}

const char* mnodeGetClusterId() {
  return tsClusterId;
}

void mnodeUpdateClusterId() {
  SClusterObj *pCluster = NULL;
  void *pIter = mnodeGetNextCluster(NULL, &pCluster);
  if (pCluster != NULL) {
    tstrncpy(tsClusterId, pCluster->uid, TSDB_CLUSTER_ID_LEN);
    mDebug("cluster id is set to %s", tsClusterId);
  }

  mnodeDecClusterRef(pCluster);
  mnodeCancelGetNextCluster(pIter);
}

int32_t mnodeGetClusterMeta(STableMetaMsg *pMeta, SShowObj *pShow, void *pConn) {
  int32_t cols = 0;
  SSchema *pSchema = pMeta->schema;

  pShow->bytes[cols] = TSDB_CLUSTER_ID_LEN + VARSTR_HEADER_SIZE;
  pSchema[cols].type = TSDB_DATA_TYPE_BINARY;
  strcpy(pSchema[cols].name, "clusterId");
  pSchema[cols].bytes = htons(pShow->bytes[cols]);
  cols++;

  pShow->bytes[cols] = 8;
  pSchema[cols].type = TSDB_DATA_TYPE_TIMESTAMP;
  strcpy(pSchema[cols].name, "create_time");
  pSchema[cols].bytes = htons(pShow->bytes[cols]);
  cols++;

  pMeta->numOfColumns = htons(cols);
  strcpy(pMeta->tableFname, "show cluster");
  pShow->numOfColumns = cols;

  pShow->offset[0] = 0;
  for (int32_t i = 1; i < cols; ++i) {
    pShow->offset[i] = pShow->offset[i - 1] + pShow->bytes[i - 1];
  }

  pShow->numOfRows = 1;
  pShow->rowSize = pShow->offset[cols - 1] + pShow->bytes[cols - 1];

  return 0;
}

int32_t mnodeRetrieveClusters(SShowObj *pShow, char *data, int32_t rows, void *pConn) {
  int32_t numOfRows = 0;
  int32_t cols = 0;
  char *  pWrite;
  SClusterObj *pCluster = NULL;

  while (numOfRows < rows) {
    pShow->pIter = mnodeGetNextCluster(pShow->pIter, &pCluster);
    if (pCluster == NULL) break;
    
    cols = 0;

    pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
    STR_WITH_MAXSIZE_TO_VARSTR(pWrite, pCluster->uid, TSDB_CLUSTER_ID_LEN);
    cols++;

    pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
    *(int32_t *) pWrite = pCluster->createdTime;
    cols++;

    mnodeDecClusterRef(pCluster);
    numOfRows++;
  }

  mnodeVacuumResult(data, pShow->numOfColumns, numOfRows, rows, pShow);
  pShow->numOfReads += numOfRows;
  return numOfRows;
}
