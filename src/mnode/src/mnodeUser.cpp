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
#include "trpc.h"
#include "tutil.h"
#include "tglobal.h"
#include "tgrant.h"
#include "tdataformat.h"
#include "tkey.h"
#include "mnode.h"
#include "dnode.h"
#include "mnodeDef.h"
#include "mnodeInt.h"
#include "mnodeAcct.h"
#include "mnodeMnode.h"
#include "mnodeSdb.h"
#include "mnodeShow.h"
#include "mnodeUser.h"
#include "mnodeWrite.h"
#include "mnodePeer.h"
#include "SdbMgmt.h"

static std::shared_ptr<SSdbTable> tsUserSdb;
static int32_t tsUserUpdateSize = 0;

int32_t SUserObj::insert() {
  std::shared_ptr<SAcctObj> pAcct(static_cast<SAcctObj *>(mnodeGetAcct(acct)));

  if (pAcct != NULL) {
    mnodeAddUserToAcct(pAcct, this);
  } else {
    mError("user:%s, acct:%s info not exist in sdb", user, acct);
    return TSDB_CODE_MND_INVALID_ACCT;
  }

  return TSDB_CODE_SUCCESS;
}

int32_t SUserObj::remove() {
  AcctObjPtr pAcct(static_cast<SAcctObj *>(mnodeGetAcct(acct)));

  if (pAcct != NULL) {
    mnodeDropUserFromAcct(pAcct, this);
  }

  return TSDB_CODE_SUCCESS;
}

int32_t SUserObj::update() {
  SUserObj *pSaved = static_cast<SUserObj *>(mnodeGetUser(user));
  if (this != pSaved) {
    memcpy(pSaved, this, tsUserUpdateSize);
    delete this;
  }
  return TSDB_CODE_SUCCESS;
}

int32_t SUserObj::encode(binser::memory_output_archive<> &out) {
  out(user, pass, acct, createdTime, superAuth, writeAuth, reserved);
  return TSDB_CODE_SUCCESS;
}

static void mnodePrintUserAuth() {
  FILE *fp = fopen("auth.txt", "w");
  if (!fp) {
    mDebug("failed to auth.txt for write");
    return;
  }
  
  void *    pIter = NULL;
  SUserObj *pUser = NULL;

  while (1) {
    pIter = mnodeGetNextUser(pIter, &pUser);
    if (pUser == NULL) break;

    char *base64 = base64_encode((const unsigned char *)pUser->pass, TSDB_KEY_LEN * 2);
    fprintf(fp, "user:%24s auth:%s\n", pUser->user, base64);
    free(base64);
  }

  fflush(fp);
  fclose(fp);
}

class UserTable : public SSdbTable {
 public:
  using SSdbTable::SSdbTable;
  int32_t decode(SSdbRow *pRow) override {
    auto pUser = std::make_shared<SUserObj>();
    memcpy(pUser.get(), pRow->rowData, tsUserUpdateSize);
    pRow->pObj = pUser;
    return TSDB_CODE_SUCCESS;
  }
  int32_t restore() override {
    int32_t numOfRows = getNumOfRows();
    if (numOfRows <= 0 && dnodeIsFirstDeploy()) {
      mInfo("dnode first deploy, create root user");
      SAcctObj *pAcct = static_cast<SAcctObj *>(mnodeGetAcct(TSDB_DEFAULT_USER));
      mnodeCreateUser(pAcct, TSDB_DEFAULT_USER, TSDB_DEFAULT_PASS, NULL);
      mnodeCreateUser(pAcct, "monitor", tsInternalPass, NULL);
      mnodeCreateUser(pAcct, "_" TSDB_DEFAULT_USER, tsInternalPass, NULL);
    }

    if (tsPrintAuth != 0) {
      mInfo("print user auth, for -A parameter is set");
      mnodePrintUserAuth();
    }

    return TSDB_CODE_SUCCESS;
  }
};

int32_t mnodeInitUsers() {
  SUserObj tObj;
  tsUserUpdateSize = (int8_t *)tObj.updateEnd - (int8_t *)&tObj;

  SSdbTableDesc desc;
  desc.id = SDB_TABLE_USER;
  desc.name = "users";
  desc.hashSessions = TSDB_DEFAULT_USERS_HASH_SIZE;
  desc.maxRowSize = tsUserUpdateSize;
  desc.keyType = SDB_KEY_STRING;

  tsUserSdb = SSdbMgmt::instance().openTable<UserTable>(desc);
  if (tsUserSdb == NULL) {
    mError("table:%s, failed to create hash", desc.name);
    return -1;
  }
   
  mDebug("table:%s, hash is created", desc.name);
  return 0;
}

void mnodeCleanupUsers() {
  tsUserSdb.reset();
}

SUserObj *mnodeGetUser(char *name) {
  return (SUserObj *)tsUserSdb->getRow(name);
}

void *mnodeGetNextUser(void *pIter, SUserObj **pUser) { 
  return tsUserSdb->fetchRow(pIter, (void **)pUser); 
}

void mnodeCancelGetNextUser(void *pIter) {
  tsUserSdb->freeIter(pIter);
}

static int32_t mnodeUpdateUser(SUserObj *pUser, void *pMsg) {
  SSdbRow row;
  row.type = SDB_OPER_GLOBAL;
  row.pTable = tsUserSdb.get();
  row.pObj.reset(pUser);
  row.pMsg = static_cast<SMnodeMsg *>(pMsg);

  int32_t code = row.Update();
  if (code != TSDB_CODE_SUCCESS && code != TSDB_CODE_MND_ACTION_IN_PROGRESS) {
    mError("user:%s, failed to alter by %s, reason:%s", pUser->user, mnodeGetUserFromMsg(pMsg), tstrerror(code));
  } else {
    mLInfo("user:%s, is altered by %s", pUser->user, mnodeGetUserFromMsg(pMsg));
  }

  return code;
}

int32_t mnodeCreateUser(SAcctObj *pAcct, char *name, char *pass, void *pMsg) {
  int32_t code = acctCheck(pAcct, ACCT_GRANT_USER);
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }

  if (name[0] == 0) {
    return TSDB_CODE_MND_INVALID_USER_FORMAT;
  }

  if (pass[0] == 0) {
    return TSDB_CODE_MND_INVALID_PASS_FORMAT;
  }

  std::shared_ptr<SUserObj> pUser(mnodeGetUser(name));
  if (pUser != NULL) {
    mDebug("user:%s, is already there", name);
    return TSDB_CODE_MND_USER_ALREADY_EXIST;
  }

  code = grantCheck(TSDB_GRANT_USER);
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }

  pUser.reset(new SUserObj);
  tstrncpy(pUser->user, name, TSDB_USER_LEN);
  taosEncryptPass((uint8_t*) pass, strlen(pass), pUser->pass);
  strcpy(pUser->acct, pAcct->user);
  pUser->createdTime = taosGetTimestampMs();
  pUser->superAuth = 0;
  pUser->writeAuth = 1;
  if (strcmp(pUser->user, TSDB_DEFAULT_USER) == 0 || strcmp(pUser->user, pUser->acct) == 0) {
    pUser->superAuth = 1;
  }

  SSdbRow row;
  row.type = SDB_OPER_GLOBAL;
  row.pTable = tsUserSdb.get();
  row.pObj = pUser;
  row.rowSize = sizeof(SUserObj);
  row.pMsg = static_cast<SMnodeMsg *>(pMsg);

  code = row.Insert();
  if (code != TSDB_CODE_SUCCESS && code != TSDB_CODE_MND_ACTION_IN_PROGRESS) {
    mError("user:%s, failed to create by %s, reason:%s", pUser->user, mnodeGetUserFromMsg(pMsg), tstrerror(code));
  } else {
    mLInfo("user:%s, is created by %s", pUser->user, mnodeGetUserFromMsg(pMsg));
  }

  return code;
}

static int32_t mnodeDropUser(SUserObj *pUser, void *pMsg) {
  SSdbRow row;
  row.type = SDB_OPER_GLOBAL;
  row.pTable = tsUserSdb.get();
  row.pObj.reset(pUser);
  row.pMsg = static_cast<SMnodeMsg *>(pMsg);

  int32_t code = row.Delete();
  if (code != TSDB_CODE_SUCCESS && code != TSDB_CODE_MND_ACTION_IN_PROGRESS) {
    mError("user:%s, failed to drop by %s, reason:%s", pUser->user, mnodeGetUserFromMsg(pMsg), tstrerror(code));
  } else {
    mLInfo("user:%s, is dropped by %s", pUser->user, mnodeGetUserFromMsg(pMsg));
  }

  return code;
}

int32_t mnodeGetUserMeta(STableMetaMsg *pMeta, SShowObj *pShow, void *pConn) {
  SUserObj *pUser = mnodeGetUserFromConn(pConn);
  if (pUser == NULL) {
    return TSDB_CODE_MND_NO_USER_FROM_CONN;
  }

  int32_t cols = 0;
  SSchema *pSchema = pMeta->schema;

  pShow->bytes[cols] = TSDB_USER_LEN + VARSTR_HEADER_SIZE;
  pSchema[cols].type = TSDB_DATA_TYPE_BINARY;
  strcpy(pSchema[cols].name, "name");
  pSchema[cols].bytes = htons(pShow->bytes[cols]);
  cols++;

  pShow->bytes[cols] = 8 + VARSTR_HEADER_SIZE;
  pSchema[cols].type = TSDB_DATA_TYPE_BINARY;
  strcpy(pSchema[cols].name, "privilege");
  pSchema[cols].bytes = htons(pShow->bytes[cols]);
  cols++;

  pShow->bytes[cols] = 8;
  pSchema[cols].type = TSDB_DATA_TYPE_TIMESTAMP;
  strcpy(pSchema[cols].name, "create_time");
  pSchema[cols].bytes = htons(pShow->bytes[cols]);
  cols++;

  pShow->bytes[cols] = TSDB_USER_LEN + VARSTR_HEADER_SIZE;
  pSchema[cols].type = TSDB_DATA_TYPE_BINARY;
  strcpy(pSchema[cols].name, "account");
  pSchema[cols].bytes = htons(pShow->bytes[cols]);
  cols++;

  pMeta->numOfColumns = htons(cols);
  strcpy(pMeta->tableFname, "show users");
  pShow->numOfColumns = cols;

  pShow->offset[0] = 0;
  for (int32_t i = 1; i < cols; ++i) {
    pShow->offset[i] = pShow->offset[i - 1] + pShow->bytes[i - 1];
  }

  pShow->numOfRows = pUser->pAcct->acctInfo.numOfUsers;
  pShow->rowSize = pShow->offset[cols - 1] + pShow->bytes[cols - 1];

  return 0;
}

int32_t mnodeRetrieveUsers(SShowObj *pShow, char *data, int32_t rows, void *pConn) {
  int32_t  numOfRows = 0;
  SUserObj *pUser    = NULL;
  int32_t  cols      = 0;
  char     *pWrite;

  while (numOfRows < rows) {
    pShow->pIter = mnodeGetNextUser(pShow->pIter, &pUser);
    if (pUser == NULL) break;
    
    cols = 0;

    pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
    STR_WITH_MAXSIZE_TO_VARSTR(pWrite, pUser->user, pShow->bytes[cols]);
    cols++;

    pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
    if (pUser->superAuth) {
      const char *src = "super";
      STR_WITH_SIZE_TO_VARSTR(pWrite, src, strlen(src));
    } else if (pUser->writeAuth) {
      const char *src = "writable";
      STR_WITH_SIZE_TO_VARSTR(pWrite, src, strlen(src));
    } else {
      const char *src = "readable";
      STR_WITH_SIZE_TO_VARSTR(pWrite, src, strlen(src));
    }
    cols++;

    pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
    *(int64_t *)pWrite = pUser->createdTime;
    cols++;

    pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
    STR_WITH_MAXSIZE_TO_VARSTR(pWrite, pUser->acct, pShow->bytes[cols]);
    cols++;

    numOfRows++;
  }

  mnodeVacuumResult(data, pShow->numOfColumns, numOfRows, rows, pShow);
  pShow->numOfReads += numOfRows;
  return numOfRows;
}

SUserObj *mnodeGetUserFromConn(void *pConn) {
  SRpcConnInfo connInfo = {0};
  if (rpcGetConnInfo(pConn, &connInfo) == 0) {
    return mnodeGetUser(connInfo.user);
  } else {
    mError("can not get user from conn:%p", pConn);
    return NULL;
  }
}

char *mnodeGetUserFromMsg(void *pMsg) {
  SMnodeMsg *pMnodeMsg = static_cast<SMnodeMsg *>(pMsg);
  if (pMnodeMsg != NULL && pMnodeMsg->pUser != NULL) {
    return pMnodeMsg->pUser->user;
  } else {
    return "system";
  }
}

int32_t mnodeProcessCreateUserMsg(SMnodeMsg *pMsg) {
  SUserObj *pOperUser = pMsg->pUser;
  
  if (pOperUser->superAuth) {
    SCreateUserMsg *pCreate = static_cast<SCreateUserMsg *>(pMsg->rpcMsg.pCont);
    return mnodeCreateUser(pOperUser->pAcct.get(), pCreate->user, pCreate->pass, pMsg);
  } else {
    mError("user:%s, no rights to create user", pOperUser->user);
    return TSDB_CODE_MND_NO_RIGHTS;
  }
}

int32_t mnodeProcessAlterUserMsg(SMnodeMsg *pMsg) {
  int32_t code;
  SUserObj *pOperUser = pMsg->pUser;
  
  SAlterUserMsg *pAlter = static_cast<SAlterUserMsg *>(pMsg->rpcMsg.pCont);
  SUserObj *pUser = mnodeGetUser(pAlter->user);
  if (pUser == NULL) {
    return TSDB_CODE_MND_INVALID_USER;
  }

  if (strcmp(pUser->user, "monitor") == 0 || (strcmp(pUser->user + 1, pUser->acct) == 0 && pUser->user[0] == '_')) {
    return TSDB_CODE_MND_NO_RIGHTS;
  }

  if ((pAlter->flag & TSDB_ALTER_USER_PASSWD) != 0) {
    bool hasRight = false;
    if (strcmp(pOperUser->user, TSDB_DEFAULT_USER) == 0) {
      hasRight = true;
    } else if (strcmp(pUser->user, pOperUser->user) == 0) {
      hasRight = true;
    } else if (pOperUser->superAuth) {
      if (strcmp(pUser->user, TSDB_DEFAULT_USER) == 0) {
        hasRight = false;
      } else if (strcmp(pOperUser->acct, pUser->acct) != 0) {
        hasRight = false;
      } else {
        hasRight = true;
      }
    }

    if (hasRight) {
      memset(pUser->pass, 0, sizeof(pUser->pass));
      taosEncryptPass((uint8_t*)pAlter->pass, strlen(pAlter->pass), pUser->pass);
      code = mnodeUpdateUser(pUser, pMsg);
    } else {
      mError("user:%s, no rights to alter user", pOperUser->user);
      code = TSDB_CODE_MND_NO_RIGHTS;
    }
  } else if ((pAlter->flag & TSDB_ALTER_USER_PRIVILEGES) != 0) {
    bool hasRight = false;

    if (strcmp(pUser->user, TSDB_DEFAULT_USER) == 0) {
      hasRight = false;
    } else if (strcmp(pUser->user, pUser->acct) == 0) {
      hasRight = false;
    } else if (strcmp(pOperUser->user, TSDB_DEFAULT_USER) == 0) {
      hasRight = true;
    } else if (strcmp(pUser->user, pOperUser->user) == 0) {
      hasRight = false;
    } else if (pOperUser->superAuth) {
      if (strcmp(pUser->user, TSDB_DEFAULT_USER) == 0) {
        hasRight = false;
      } else if (strcmp(pOperUser->acct, pUser->acct) != 0) {
        hasRight = false;
      } else {
        hasRight = true;
      }
    }

    if (pAlter->privilege == 1) { // super
      hasRight = false;
    }

    if (hasRight) {
      if (pAlter->privilege == 2) {  // read
        pUser->superAuth = 0;
        pUser->writeAuth = 0;
      }
      if (pAlter->privilege == 3) {  // write
        pUser->superAuth = 0;
        pUser->writeAuth = 1;
      }

      code = mnodeUpdateUser(pUser, pMsg);
    } else {
      mError("user:%s, no rights to alter user", pOperUser->user);
      code = TSDB_CODE_MND_NO_RIGHTS;
    }
  } else {
    mError("user:%s, no rights to alter user", pOperUser->user);
    code = TSDB_CODE_MND_NO_RIGHTS;
  }

  return code;
}

int32_t mnodeProcessDropUserMsg(SMnodeMsg *pMsg) {
  int32_t code;
  SUserObj *pOperUser = pMsg->pUser;

  SDropUserMsg *pDrop = static_cast<SDropUserMsg *>(pMsg->rpcMsg.pCont);
  SUserObj *pUser = mnodeGetUser(pDrop->user);
  if (pUser == NULL) {
    return TSDB_CODE_MND_INVALID_USER;
  }

  if (strcmp(pUser->user, "monitor") == 0 || strcmp(pUser->user, pUser->acct) == 0 ||
    (strcmp(pUser->user + 1, pUser->acct) == 0 && pUser->user[0] == '_')) {
    return TSDB_CODE_MND_NO_RIGHTS;
  }

  bool hasRight = false;
  if (strcmp(pUser->user, TSDB_DEFAULT_USER) == 0) {
    hasRight = false;
  } else if (strcmp(pOperUser->user, TSDB_DEFAULT_USER) == 0) {
    hasRight = true;
  } else if (strcmp(pUser->user, pOperUser->user) == 0) {
    hasRight = false;
  } else if (pOperUser->superAuth) {
    if (strcmp(pOperUser->acct, pUser->acct) != 0) {
      hasRight = false;
    } else {
      hasRight = true;
    }
  }

  if (hasRight) {
    code = mnodeDropUser(pUser, pMsg);
  } else {
    code = TSDB_CODE_MND_NO_RIGHTS;
  }

  return code;
}

void mnodeDropAllUsers(SAcctObj *pAcct)  {
  void *    pIter = NULL;
  int32_t   numOfUsers = 0;
  int32_t   acctNameLen = strlen(pAcct->user);
  SUserObj *pUser = NULL;

  while (1) {
    pIter = mnodeGetNextUser(pIter, &pUser);
    if (pUser == NULL) break;

    if (strncmp(pUser->acct, pAcct->user, acctNameLen) == 0) {
      SSdbRow row;
      row.type = SDB_OPER_LOCAL;
      row.pTable = tsUserSdb.get();
      row.pObj.reset(pUser);
      row.Delete();
      numOfUsers++;
    }

  }

  mDebug("acct:%s, all users:%d is dropped from sdb", pAcct->user, numOfUsers);
}

int32_t mnodeRetriveAuth(char *user, char *spi, char *encrypt, char *secret, char *ckey) {
  if (!sdbIsMaster()) {
    *secret = 0;
    mDebug("user:%s, failed to auth user, mnode is not master", user);
    return TSDB_CODE_APP_NOT_READY;
  }

  SUserObj *pUser = mnodeGetUser(user);
  if (pUser == NULL) {
    *secret = 0;
    mError("user:%s, failed to auth user, reason:%s", user, tstrerror(TSDB_CODE_MND_INVALID_USER));
    return TSDB_CODE_MND_INVALID_USER;
  } else {
    *spi = 1;
    *encrypt = 0;
    *ckey = 0;

    memcpy(secret, pUser->pass, TSDB_KEY_LEN);
    mDebug("user:%s, auth info is returned", user);
    return TSDB_CODE_SUCCESS;
  }
}

int32_t mnodeProcessAuthMsg(SMnodeMsg *pMsg) {
  SAuthMsg *pAuthMsg = static_cast<SAuthMsg *>(pMsg->rpcMsg.pCont);
  SAuthRsp *pAuthRsp = static_cast<SAuthRsp *>(rpcMallocCont(sizeof(SAuthRsp)));
  
  pMsg->rpcRsp.rsp = pAuthRsp;
  pMsg->rpcRsp.len = sizeof(SAuthRsp);
  
  return mnodeRetriveAuth(pAuthMsg->user, &pAuthRsp->spi, &pAuthRsp->encrypt, pAuthRsp->secret, pAuthRsp->ckey);
}
