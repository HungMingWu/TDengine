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

#define TAOS_RANDOM_FILE_FAIL_TEST
#include "os.h"
#include "taoserror.h"
#include "tchecksum.h"
#include "tfile.h"
#include "walInt.h"

int32_t SWal::renew() {
  int32_t code = 0;

  if (stop_) {
    wDebug("vgId:%d, do not create a new wal file", vgId);
    return 0;
  }

  std::lock_guard<std::mutex> _(mutex);

  if (tfd) {
    tfd.reset();
    wDebug("vgId:%d, file:%s, it is closed", vgId, name);
  }

  if (keep == TAOS_WAL_KEEP) {
    fileId = 0;
  } else {
    if (getNewFile(&fileId) != 0) fileId = 0;
    fileId++;
  }

  snprintf(name, sizeof(name), "%s/%s%" PRId64, path, WAL_PREFIX, fileId);
  tfd = tfOpenM(name, O_WRONLY | O_CREAT, S_IRWXU | S_IRWXG | S_IRWXO);

  wDebug("vgId:%d, file:%s, it is created", vgId, name);

  return code;
}

void SWal::removeOneOldFile() {
  if (keep == TAOS_WAL_KEEP) return;
  if (!tfd) return;

  std::lock_guard<std::mutex> _(mutex);

  // remove the oldest wal file
  int64_t oldFileId = -1;
  if (getOldFile(fileId, WAL_FILE_NUM, &oldFileId) == 0) {
    char walName[WAL_FILE_LEN] = {0};
    snprintf(walName, sizeof(walName), "%s/%s%" PRId64, path, WAL_PREFIX, oldFileId);

    if (remove(walName) < 0) {
      wError("vgId:%d, file:%s, failed to remove since %s", vgId, walName, strerror(errno));
    } else {
      wInfo("vgId:%d, file:%s, it is removed", vgId, walName);
    }
  }
}

void SWal::removeAllOldFiles() {
  int64_t fileId = -1;

  std::lock_guard<std::mutex> _(mutex);
  while (getNextFile(&fileId) >= 0) {
    snprintf(name, sizeof(name), "%s/%s%" PRId64, path, WAL_PREFIX, fileId);

    if (remove(name) < 0) {
      wError("vgId:%d, wal:%p file:%s, failed to remove", vgId, this, name);
    } else {
      wInfo("vgId:%d, wal:%p file:%s, it is removed", vgId, this, name);
    }
  }
}

int32_t SWal::write(SWalHead *pHead) {
  int32_t code = 0;

  // no wal
  if (!tfd) return 0;
  if (level == TAOS_WAL_NOLOG) return 0;
  if (pHead->version <= version) return 0;

  pHead->signature = WAL_SIGNATURE;
  taosCalcChecksumAppend(0, (uint8_t *)pHead, sizeof(SWalHead));
  int32_t contLen = pHead->len + sizeof(SWalHead);

  std::lock_guard<std::mutex> _(mutex);

  if (tfd->write(pHead, contLen) != contLen) {
    code = TAOS_SYSTEM_ERROR(errno);
    wError("vgId:%d, file:%s, failed to write since %s", vgId, name, strerror(errno));
  } else {
    wTrace("vgId:%d, write wal, fileId:%" PRId64 " tfd:%" PRId64 " hver:%" PRId64 " wver:%" PRIu64 " len:%d", vgId,
           fileId, tfd.get(), pHead->version, version, pHead->len);
    version = pHead->version;
  }

  ASSERT(contLen == pHead->len + sizeof(SWalHead));

  return code;
}

void SWal::fsync(bool forceFsync) {
  if (!tfd) return;

  if (forceFsync || (level == TAOS_WAL_FSYNC && fsyncPeriod == 0)) {
    wTrace("vgId:%d, fileId:%" PRId64 ", do fsync", vgId, fileId);
    if (tfd->fsync() < 0) {
      wError("vgId:%d, fileId:%" PRId64 ", fsync failed since %s", vgId, fileId, strerror(errno));
    }
  }
}

int32_t SWal::restore(void *pVnode, FWalWrite writeFp) {
  int32_t count = 0;
  int32_t code = 0;
  int64_t fileId = -1;

  while ((code = getNextFile(&fileId)) >= 0) {
    if (fileId == fileId) continue;

    char walName[WAL_FILE_LEN];
    snprintf(walName, sizeof(name), "%s/%s%" PRId64, path, WAL_PREFIX, fileId);

    wInfo("vgId:%d, file:%s, will be restored", vgId, walName);
    int32_t code = restoreWalFile(pVnode, writeFp, walName, fileId);
    if (code != TSDB_CODE_SUCCESS) {
      wError("vgId:%d, file:%s, failed to restore since %s", vgId, walName, tstrerror(code));
      continue;
    }

    wInfo("vgId:%d, file:%s, restore success, wver:%" PRIu64, vgId, walName, version);

    count++;
  }

  if (keep != TAOS_WAL_KEEP) return TSDB_CODE_SUCCESS;

  if (count == 0) {
    wDebug("vgId:%d, wal file not exist, renew it", vgId);
    return renew();
  } else {
    // open the existing WAL file in append mode
    fileId = 0;
    snprintf(name, sizeof(name), "%s/%s%" PRId64, path, WAL_PREFIX, fileId);
    tfd = tfOpenM(name, O_WRONLY | O_CREAT | O_APPEND, S_IRWXU | S_IRWXG | S_IRWXO);
    if (!tfd) {
      wError("vgId:%d, file:%s, failed to open since %s", vgId, name, strerror(errno));
      return TAOS_SYSTEM_ERROR(errno);
    }
    wDebug("vgId:%d, file:%s open success", vgId, name);
  }

  return TSDB_CODE_SUCCESS;
}

int32_t SWal::getWalFile(char *fileName, int64_t *fileId) {
  if (*fileId == 0) *fileId = -1;

  std::lock_guard<std::mutex> _(mutex);

  int32_t code = getNextFile(fileId);
  if (code >= 0) {
    sprintf(fileName, "wal/%s%" PRId64, WAL_PREFIX, *fileId);
    code = (*fileId == this->fileId) ? 0 : 1;
  }

  wDebug("vgId:%d, get wal file, code:%d curId:%" PRId64 " outId:%" PRId64, vgId, code, fileId, *fileId);

  return code;
}

static void walFtruncate(FileOpPtr tfd, int64_t offset) {
  tfd->ftruncate(offset);
  tfd->fsync();
}

int32_t SWal::skipCorruptedRecord(SWalHead *pHead, FileOpPtr tfd, int64_t *offset) {
  int64_t pos = *offset;
  while (1) {
    pos++;

    if (tfd->lseek(pos, SEEK_SET) < 0) {
      wError("vgId:%d, failed to seek from corrupted wal file since %s", vgId, strerror(errno));
      return TSDB_CODE_WAL_FILE_CORRUPTED;
    }

    if (tfd->read(pHead, sizeof(SWalHead)) <= 0) {
      wError("vgId:%d, read to end of corrupted wal file, offset:%" PRId64, vgId, pos);
      return TSDB_CODE_WAL_FILE_CORRUPTED;
    }

    if (pHead->signature != WAL_SIGNATURE) {
      continue;
    }

    if (taosCheckChecksumWhole((uint8_t *)pHead, sizeof(SWalHead))) {
      wInfo("vgId:%d, wal head cksum check passed, offset:%" PRId64, vgId, pos);
      *offset = pos;
      return TSDB_CODE_SUCCESS;
    }
  }

  return TSDB_CODE_WAL_FILE_CORRUPTED;
}

int32_t SWal::restoreWalFile(void *pVnode, FWalWrite writeFp, char *name, int64_t fileId) {
  int32_t size = WAL_MAX_SIZE;
  void *  buffer = tmalloc(size);
  if (buffer == NULL) {
    wError("vgId:%d, file:%s, failed to open for restore since %s", vgId, name, strerror(errno));
    return TAOS_SYSTEM_ERROR(errno);
  }

  FileOpPtr tfd = tfOpen(name, O_RDWR);

  int32_t   code = TSDB_CODE_SUCCESS;
  int64_t   offset = 0;
  SWalHead *pHead = (SWalHead*)buffer;

  while (1) {
    int32_t ret = tfd->read(pHead, sizeof(SWalHead));
    if (ret == 0) break;

    if (ret < 0) {
      wError("vgId:%d, file:%s, failed to read wal head since %s", vgId, name, strerror(errno));
      code = TAOS_SYSTEM_ERROR(errno);
      break;
    }

    if (ret < sizeof(SWalHead)) {
      wError("vgId:%d, file:%s, failed to read wal head, ret is %d", vgId, name, ret);
      walFtruncate(tfd, offset);
      break;
    }

    if (!taosCheckChecksumWhole((uint8_t *)pHead, sizeof(SWalHead))) {
      wError("vgId:%d, file:%s, wal head cksum is messed up, hver:%" PRIu64 " len:%d offset:%" PRId64, vgId, name,
             pHead->version, pHead->len, offset);
      code = skipCorruptedRecord(pHead, tfd, &offset);
      if (code != TSDB_CODE_SUCCESS) {
        walFtruncate(tfd, offset);
        break;
      }
    }

    if (pHead->len < 0 || pHead->len > size - sizeof(SWalHead)) {
      wError("vgId:%d, file:%s, wal head len out of range, hver:%" PRIu64 " len:%d offset:%" PRId64, vgId, name,
             pHead->version, pHead->len, offset);
      code = skipCorruptedRecord(pHead, tfd, &offset);
      if (code != TSDB_CODE_SUCCESS) {
        walFtruncate(tfd, offset);
        break;
      }
    }

    ret = tfd->read(pHead->cont, pHead->len);
    if (ret < 0) {
      wError("vgId:%d, file:%s, failed to read wal body since %s", vgId, name, strerror(errno));
      code = TAOS_SYSTEM_ERROR(errno);
      break;
    }

    if (ret < pHead->len) {
      wError("vgId:%d, file:%s, failed to read wal body, ret:%d len:%d", vgId, name, ret, pHead->len);
      offset += sizeof(SWalHead);
      continue;
    }

    offset = offset + sizeof(SWalHead) + pHead->len;

    wTrace("vgId:%d, restore wal, fileId:%" PRId64 " hver:%" PRIu64 " wver:%" PRIu64 " len:%d", vgId,
           fileId, pHead->version, version, pHead->len);

    version = pHead->version;
    (*writeFp)(pVnode, pHead, TAOS_QTYPE_WAL, NULL);
  }

  tfree(buffer);

  return code;
}

uint64_t SWal::getVersion() {
  return version;
}
