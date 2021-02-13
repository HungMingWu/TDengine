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

#ifndef TDENGINE_WAL_INT_H
#define TDENGINE_WAL_INT_H

#include <mutex>
#include "tlog.h"
#include "tfile.h"

extern int32_t wDebugFlag;

#define wFatal(...) { if (wDebugFlag & DEBUG_FATAL) { taosPrintLog("WAL FATAL ", 255, __VA_ARGS__); }}
#define wError(...) { if (wDebugFlag & DEBUG_ERROR) { taosPrintLog("WAL ERROR ", 255, __VA_ARGS__); }}
#define wWarn(...)  { if (wDebugFlag & DEBUG_WARN)  { taosPrintLog("WAL WARN ", 255, __VA_ARGS__); }}
#define wInfo(...)  { if (wDebugFlag & DEBUG_INFO)  { taosPrintLog("WAL ", 255, __VA_ARGS__); }}
#define wDebug(...) { if (wDebugFlag & DEBUG_DEBUG) { taosPrintLog("WAL ", wDebugFlag, __VA_ARGS__); }}
#define wTrace(...) { if (wDebugFlag & DEBUG_TRACE) { taosPrintLog("WAL ", wDebugFlag, __VA_ARGS__); }}

#define WAL_PREFIX     "wal"
#define WAL_PREFIX_LEN 3
#define WAL_REFRESH_MS 1000
#define WAL_MAX_SIZE   (TSDB_MAX_WAL_SIZE + sizeof(SWalHead) + 16)
#define WAL_SIGNATURE  ((uint32_t)(0xFAFBFDFE))
#define WAL_PATH_LEN   (TSDB_FILENAME_LEN + 12)
#define WAL_FILE_LEN   (WAL_PATH_LEN + 32)
#define WAL_FILE_NUM   1 // 3

typedef enum { TAOS_WAL_NOLOG = 0, TAOS_WAL_WRITE = 1, TAOS_WAL_FSYNC = 2 } EWalType;

typedef enum { TAOS_WAL_NOT_KEEP = 0, TAOS_WAL_KEEP = 1 } EWalKeep;

struct SWalCfg {
  int32_t  vgId;
  int32_t  fsyncPeriod;  // millisecond
  EWalType walLevel;     // wal level
  EWalKeep keep;         // keep the wal file when closed
};

typedef int32_t FWalWrite(void *ahandle, void *pHead, int32_t qtype, void *pMsg);

struct SWalHead {
  int8_t   msgType;
  int8_t   sver;
  int8_t   reserved[2];
  int32_t  len;
  uint64_t version;
  uint32_t signature;
  uint32_t cksum;
  char     cont[];
};

struct SWal {
  uint64_t version;
  int64_t  fileId;
  int64_t  rid;
  FileOpPtr tfd;
  int32_t  vgId;
  int32_t  keep;
  int32_t  level;
  int32_t  fsyncPeriod;
  int32_t  fsyncSeq;
  int8_t   stop_;
  int8_t   reserved[3];
  char     path[WAL_PATH_LEN];
  char     name[WAL_FILE_LEN];
  std::mutex mutex;

 protected:
  int32_t restoreWalFile(void *pVnode, FWalWrite writeFp, char *name, int64_t fileId);
  int32_t skipCorruptedRecord(SWalHead *pHead, FileOpPtr tfd, int64_t *offset);
  int32_t getNextFile(int64_t *nextFileId);
  int32_t getOldFile(int64_t curFileId, int32_t minDiff, int64_t *oldFileId);
  int32_t getNewFile(int64_t *newFileId);
 public:
  int32_t alter(SWalCfg *pCfg);
  void    stop();
  void    close();
  int32_t renew();
  void    removeOneOldFile();
  void    removeAllOldFiles();
  int32_t write(SWalHead *);
  int32_t restore(void *pVnode, FWalWrite writeFp);
  void    fsync(bool forceFsync);
  int32_t getWalFile(char *fileName, int64_t *fileId);
  uint64_t getVersion();
};

SWal*         walOpen(char *path, SWalCfg *pCfg);

#endif
