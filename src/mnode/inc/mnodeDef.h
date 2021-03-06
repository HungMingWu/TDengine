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

#ifndef TDENGINE_MNODE_DEF_H
#define TDENGINE_MNODE_DEF_H

#include <atomic>
#include <memory>
#include <mutex>
#include <vector>
#include "taosdef.h"
#include "taosmsg.h"
#include "tidpool.h"
#include "object.h"
#include "mnodeDef.h"

struct SVgObj;
struct SDbObj;
struct SAcctObj;
struct SUserObj;
struct SMnodeObj;

/*
struct define notes:
1. The first field must be the xxxxId field or name field , e.g. 'int32_t dnodeId', 'int32_t mnodeId', 'char name[]', 'char user[]', ...
2. From the dnodeId field to the updataEnd field, these information will be falled disc;
3. The fields behind the updataEnd field can be changed;
*/

struct SClusterObj : public objectBase {
  char    uid[TSDB_CLUSTER_ID_LEN];
  int64_t createdTime;
  int8_t  reserved[12];
 public:
  ~SClusterObj() override = default;
  int32_t insert() override;
  int32_t remove() override;
  int32_t encode(binser::memory_output_archive<> &) override;
  int32_t update() override;
};
using SClusterObjPtr = std::shared_ptr<SClusterObj>;

struct SDnodeObj : public objectBase, public std::enable_shared_from_this<SDnodeObj> {
  int32_t    dnodeId;
  std::atomic<int32_t>    openVnodes;
  int64_t    createdTime;
  int32_t    resever0;         // from dnode status msg, config information
  int32_t    customScore;      // config by user
  uint32_t   lastAccess;
  uint16_t   numOfCores;       // from dnode status msg
  uint16_t   dnodePort;
  char       dnodeFqdn[TSDB_FQDN_LEN];
  char       dnodeEp[TSDB_EP_LEN];
  int8_t     alternativeRole;  // from dnode status msg, 0-any, 1-mgmt, 2-dnode
  int8_t     status;           // set in balance function
  int8_t     isMgmt;
  int8_t     reserve1[11];  
  // Serialization above
  uint32_t   moduleStatus;
  uint32_t   lastReboot;       // time stamp for last reboot
  float      score;            // calc in balance function
  float      diskAvailable;    // from dnode status msg
  int16_t    diskAvgUsage;     // calc from sys.disk
  int16_t    cpuAvgUsage;      // calc from sys.cpu
  int16_t    memoryAvgUsage;   // calc from sys.mem
  int16_t    bandwidthUsage;   // calc from sys.band
  int8_t     offlineReason;
  int8_t     reserved2[1];

 private:
  int32_t drop(void *pMsg);
 public:
  ~SDnodeObj() override = default;
  bool monitorDropping();
  void update(int status);
  int32_t insert() override;
  int32_t remove() override;
  int32_t encode(binser::memory_output_archive<> &out) override;
  int32_t update() override;
};

struct SMnodeObj : public objectBase {
  int32_t    mnodeId;
  int8_t     reserved0[4];
  int64_t    createdTime;
  int8_t     reserved1[4];
  // Serialization above
  int8_t     role;
  int8_t     reserved2[3];

 public:
  ~SMnodeObj() override = default;
  int32_t insert() override;
  int32_t remove() override;
  int32_t encode(binser::memory_output_archive<> &) override;
  int32_t update() override;
};

struct STableObj : public objectBase {
  struct {
    char * tableId = nullptr;
    int8_t type;
  } info;

 public:
  ~STableObj() { tfree(info.tableId); }
};

struct SSTableObj : public STableObj {
  int8_t     reserved0[9]; // for fill struct STableObj to 4byte align
  int16_t    nextColId;
  int32_t    sversion;
  uint64_t   uid;
  int64_t    createdTime;
  int32_t    tversion;
  int32_t    numOfColumns;
  int32_t    numOfTags;
  // Serialization above
  int32_t    numOfTables;
  std::vector<SSchema>  schema;
  void *     vgHash;

 public:
  ~SSTableObj() override;
  int32_t insert() override;
  int32_t remove() override;
  int32_t encode(binser::memory_output_archive<> &) override;
  int32_t update() override;
};

 struct SCTableObj : public STableObj {
  int8_t     reserved0[9]; // for fill struct STableObj to 4byte align
  int16_t    nextColId;    //used by normal table
  int32_t    sversion;     //used by normal table  
  uint64_t   uid;
  uint64_t   suid;
  int64_t    createdTime;
  int32_t    numOfColumns; //used by normal table
  int32_t    tid;
  int32_t    vgId;
  int32_t    sqlLen;
  // Serialization above
  char*      sql = nullptr;          //used by normal table
  std::vector<SSchema>   schema;       //used by normal table
  SSTableObj*superTable;

 public:
  ~SCTableObj() override;
  int32_t insert() override;
  int32_t remove() override;
  int32_t encode(binser::memory_output_archive<> &out) override;
  int32_t update() override;
 };

struct SVnodeGid {
  int32_t    dnodeId;
  int8_t     role;
  int8_t     vver[3];  // To ensure compatibility, 3 bits are used to represent the remainder of 64 bit version
  SDnodeObj *pDnode;

  template <typename Archive, typename Self>
  static void serialize(Archive &archive, Self &self) {
    archive(self.dnodeId, self.role, self.vver);// bug, need to fix, self.pDnode);
  }
};

struct SVgObj : public objectBase {
  uint32_t       vgId;
  int32_t        numOfVnodes;
  int64_t        createdTime;
  int32_t        lbDnodeId;
  int32_t        lbTime;
  char           dbName[TSDB_ACCT_ID_LEN + TSDB_DB_NAME_LEN];
  int8_t         inUse;
  int8_t         accessState;
  int8_t         status;
  int8_t         reserved0[4];
  SVnodeGid      vnodeGid[TSDB_MAX_REPLICA];
  int32_t        vgCfgVersion;
  int8_t         reserved1[8];
  // Serialization above
  int32_t        numOfTables;
  int64_t        totalStorage;
  int64_t        compStorage;
  int64_t        pointsWritten;
  struct SDbObj *pDb;
  std::unique_ptr<id_pool_t> idPool;

 public:
  ~SVgObj() override = default;
  int32_t insert() override;
  int32_t remove() override;
  int32_t encode(binser::memory_output_archive<>&) override;
  int32_t update() override;
};

struct SDbCfg {
  int32_t cacheBlockSize;
  int32_t totalBlocks;
  int32_t maxTables;
  int32_t daysPerFile;
  int32_t daysToKeep;
  int32_t daysToKeep1;
  int32_t daysToKeep2;
  int32_t minRowsPerFileBlock;
  int32_t maxRowsPerFileBlock;
  int32_t commitTime;
  int32_t fsyncPeriod;
  int8_t  precision;
  int8_t  compression;
  int8_t  walLevel;
  int8_t  replications;
  int8_t  quorum;
  int8_t  update;
  int8_t  cacheLastRow;
  int8_t  reserved[10];

  template <typename Archive, typename Self>
  static void serialize(Archive &archive, Self &self) {
    archive(self.cacheBlockSize, self.totalBlocks, self.maxTables, self.daysPerFile, self.daysToKeep, 
            self.daysToKeep1, self.daysToKeep2, self.minRowsPerFileBlock, self.maxRowsPerFileBlock, self.commitTime,
            self.fsyncPeriod, self.precision, self.compression, self.walLevel, self.replications,
            self.quorum, self.update, self.cacheLastRow, self.reserved);
  }
};

struct SAcctInfo {
  int64_t              totalStorage;  // Total storage wrtten from this account
  int64_t              compStorage;   // Compressed storage on disk
  int64_t              queryTime;
  int64_t              totalPoints;
  int64_t              inblound;
  int64_t              outbound;
  int64_t              sKey;
  std::atomic<int32_t> numOfUsers;
  std::atomic<int32_t> numOfDbs;
  int32_t              numOfTimeSeries;
  int32_t              numOfPointsPerSecond;
  int32_t              numOfConns;
  int32_t              numOfQueries;
  int32_t              numOfStreams;
  int8_t               accessState;  // Checked by mgmt heartbeat message
  int8_t               reserved[3];
};

struct SAcctObj : public objectBase {
  char      user[TSDB_USER_LEN];
  char      pass[TSDB_KEY_LEN];
  SAcctCfg  cfg;
  int64_t   createdTime;
  int32_t   acctId;
  int8_t    status;
  int8_t    reserved0[7];
  // Serialization above
  int8_t    reserved1[4];
  SAcctInfo acctInfo;
  std::mutex  mutex;

 public:
  ~SAcctObj() override = default;
  int32_t insert() override;
  int32_t remove() override;
  int32_t encode(binser::memory_output_archive<> &) override;
  int32_t update() override;
};
using AcctObjPtr = std::shared_ptr<SAcctObj>;

struct SDbObj : public objectBase {
  char    name[TSDB_ACCT_ID_LEN + TSDB_DB_NAME_LEN];
  int8_t  reserved0[4];
  char    acct[TSDB_USER_LEN];
  int64_t createdTime;
  int32_t dbCfgVersion;
  SDbCfg  cfg;
  int8_t  status;
  int8_t  reserved1[11];
  // Serialization above
  int32_t              numOfVgroups;
  std::atomic<int32_t> numOfTables;
  std::atomic<int32_t> numOfSuperTables;
  int32_t              vgListSize;
  int32_t              vgListIndex;
  SVgObj **            vgList = nullptr;
  AcctObjPtr           pAcct;
  std::mutex           mutex;

 public:
  ~SDbObj() override;
  int32_t insert() override;
  int32_t remove() override;
  int32_t encode(binser::memory_output_archive<> &) override;
  int32_t update() override;
};

struct SUserObj : public objectBase {
  char              user[TSDB_USER_LEN];
  char              pass[TSDB_KEY_LEN];
  char              acct[TSDB_USER_LEN];
  int64_t           createdTime;
  int8_t            superAuth;
  int8_t            writeAuth;
  int8_t            reserved[10];
  // Serialization above
  AcctObjPtr        pAcct;

 public:
  ~SUserObj() override = default;
  int32_t insert() override;
  int32_t remove() override;
  int32_t encode(binser::memory_output_archive<> &) override;
  int32_t update() override;
};

typedef struct {
  char     db[TSDB_DB_NAME_LEN];
  int8_t   type;
  int16_t  numOfColumns;
  int32_t  index;
  int32_t  rowSize;
  int32_t  numOfRows;
  void *   pIter;
  void **  ppShow;
  int16_t  offset[TSDB_MAX_COLUMNS];
  int16_t  bytes[TSDB_MAX_COLUMNS];
  int32_t  numOfReads;
  int8_t   maxReplica;
  int8_t   reserved0[1];
  uint16_t payloadLen;
  char     payload[];
} SShowObj;

#endif
