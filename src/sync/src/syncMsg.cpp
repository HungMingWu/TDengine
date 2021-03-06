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

#define _DEFAULT_SOURCE
#include "os.h"
#include "taoserror.h"
#include "tglobal.h"
#include "tchecksum.h"
#include "syncInt.h"

char *statusType[] = {
  "broadcast",
  "broadcast-rsp",
  "setup-conn",
  "setup-conn-rsp",
  "exchange-data",
  "exchange-data-rsp",
  "check-role",
  "check-role-rsp"
};

uint16_t syncGenTranId() {
  return taosRand() & 0XFFFF;
}

static void syncBuildHead(SSyncHead *pHead) {
  pHead->protocol = SYNC_PROTOCOL_VERSION;
  pHead->signature = SYNC_SIGNATURE;
  pHead->code = 0;
  pHead->cId = 0;
  taosCalcChecksumAppend(0, (uint8_t *)pHead, sizeof(SSyncHead));
}

int32_t SSyncHead::check() {
  if (protocol != SYNC_PROTOCOL_VERSION) return TSDB_CODE_SYN_MISMATCHED_PROTOCOL;
  if (signature != SYNC_SIGNATURE) return TSDB_CODE_SYN_MISMATCHED_SIGNATURE;
  if (cId != 0) return TSDB_CODE_SYN_MISMATCHED_CLUSTERID;
  if (len <= 0 || len > TSDB_MAX_WAL_SIZE) return TSDB_CODE_SYN_INVALID_MSGLEN;
  if (type <= TAOS_SMSG_START || type >= TAOS_SMSG_END) return TSDB_CODE_SYN_INVALID_MSGTYPE;
  if (!taosCheckChecksumWhole((uint8_t *)this, sizeof(SSyncHead))) return TSDB_CODE_SYN_INVALID_CHECKSUM;

  return TSDB_CODE_SUCCESS;
}

void SSyncHead::buildFwdMsg(int32_t vgId, int32_t len) {
  this->type = TAOS_SMSG_SYNC_FWD;
  this->vgId = vgId;
  this->len = len;
  syncBuildHead(this);
}

SFwdRsp::SFwdRsp(int32_t vgId, uint64_t version, int32_t code) {
  head.type = TAOS_SMSG_SYNC_FWD_RSP;
  head.vgId = vgId;
  head.len = sizeof(SFwdRsp) - sizeof(SSyncHead);
  syncBuildHead(&head);

  this->version = version;
  this->code = code;
}

static void syncBuildMsg(SSyncMsg *pMsg, int32_t vgId, ESyncMsgType type) {
  pMsg->head.type = type;
  pMsg->head.vgId = vgId;
  pMsg->head.len = sizeof(SSyncMsg) - sizeof(SSyncHead);
  syncBuildHead(&pMsg->head);

  pMsg->port = tsSyncPort;
  pMsg->tranId = syncGenTranId();
  pMsg->sourceId = vgId;
  tstrncpy(pMsg->fqdn, tsNodeFqdn, TSDB_FQDN_LEN);
}

void syncBuildSyncReqMsg(SSyncMsg *pMsg, int32_t vgId) { syncBuildMsg(pMsg, vgId, TAOS_SMSG_SYNC_REQ); }
void syncBuildSyncDataMsg(SSyncMsg *pMsg, int32_t vgId) { syncBuildMsg(pMsg, vgId, TAOS_SMSG_SYNC_DATA); }
void syncBuildSyncSetupMsg(SSyncMsg *pMsg, int32_t vgId) { syncBuildMsg(pMsg, vgId, TAOS_SMSG_SETUP); }

void syncBuildPeersStatus(SPeersStatus *pMsg, int32_t vgId) {
  pMsg->head.type = TAOS_SMSG_STATUS;
  pMsg->head.vgId = vgId;
  pMsg->head.len = sizeof(SPeersStatus) - sizeof(SSyncHead);
  syncBuildHead(&pMsg->head);
}

void syncBuildFileAck(SFileAck *pMsg, int32_t vgId) {
  pMsg->head.type = TAOS_SMSG_SYNC_FILE_RSP;
  pMsg->head.vgId = vgId;
  pMsg->head.len = sizeof(SFileAck) - sizeof(SSyncHead);
  syncBuildHead(&pMsg->head);
}

void syncBuildFileInfo(SFileInfo *pMsg, int32_t vgId) {
  pMsg->head.type = TAOS_SMSG_SYNC_FILE;
  pMsg->head.vgId = vgId;
  pMsg->head.len = sizeof(SFileInfo) - sizeof(SSyncHead);
  syncBuildHead(&pMsg->head);
}