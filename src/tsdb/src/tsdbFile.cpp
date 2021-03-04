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
#include <regex.h>
#include <fmt/format.h>
#include "os.h"
#include "talgo.h"
#include "tchecksum.h"
#include "tsdbMain.h"
#include "tutil.h"
#include "defer.h"
#include "filesystem.hpp"

const char *tsdbFileSuffix[] = {".head", ".data", ".last", ".stat", ".h", ".d", ".l", ".s"};

static int   tsdbInitFile(SFile *pFile, STsdbRepo *pRepo, int fid, int type);
static void  tsdbDestroyFile(SFile *pFile);
static int   compFGroup(const void *arg1, const void *arg2);
static int   keyFGroupCompFunc(const void *key, const void *fgroup);
static void  tsdbInitFileGroup(SFileGroup *pFGroup, STsdbRepo *pRepo);
static TSKEY tsdbGetCurrMinKey(int8_t precision, int32_t keep);
static int   tsdbGetCurrMinFid(int8_t precision, int32_t keep, int32_t days);

// ---------------- INTERNAL FUNCTIONS ----------------
STsdbFileH *tsdbNewFileH(STsdbCfg *pCfg) {
  STsdbFileH *pFileH = (STsdbFileH *)calloc(1, sizeof(*pFileH));
  if (pFileH == NULL) {
    terrno = TSDB_CODE_TDB_OUT_OF_MEMORY;
    tsdbFreeFileH(pFileH);
    return NULL;
  }

  int code = pthread_rwlock_init(&(pFileH->fhlock), NULL);
  if (code != 0) {
    tsdbError("vgId:%d failed to init file handle lock since %s", pCfg->tsdbId, strerror(code));
    terrno = TAOS_SYSTEM_ERROR(code);
    tsdbFreeFileH(pFileH);
    return NULL;
  }

  pFileH->maxFGroups = TSDB_MAX_FILE(pCfg->keep, pCfg->daysPerFile);

  pFileH->pFGroup = (SFileGroup *)calloc(pFileH->maxFGroups, sizeof(SFileGroup));
  if (pFileH->pFGroup == NULL) {
    terrno = TSDB_CODE_TDB_OUT_OF_MEMORY;
    tsdbFreeFileH(pFileH);
    return NULL;
  }

  return pFileH;
}

void tsdbFreeFileH(STsdbFileH *pFileH) {
  if (pFileH) {
    pthread_rwlock_destroy(&pFileH->fhlock);
    tfree(pFileH->pFGroup);
    free(pFileH);
  }
}

int tsdbOpenFileH(STsdbRepo *pRepo) {
  ASSERT(pRepo != NULL && pRepo->tsdbFileH != NULL);

  DIR *   dir = NULL;
  int     fid = 0;
  int     vid = 0;
  regex_t regex1 = {0}, regex2 = {0};
  SFileGroup fileGroup = {0};
  auto _1 = defer([&] { regfree(&regex1); });
  auto _2 = defer([&] { regfree(&regex2); });
  auto    _4 = defer([&] {
    if (dir != NULL) closedir(dir);
  });
  auto    _5 = defer([&] {
    for (int type = 0; type < TSDB_FILE_TYPE_MAX; type++) tsdbDestroyFile(&fileGroup.files[type]);
  });
  auto _6 = defer([&] { tsdbCloseFileH(pRepo); });
  int     code = 0;
  char    fname[TSDB_FILENAME_LEN] = "\0";

  STsdbFileH *pFileH = pRepo->tsdbFileH;
  STsdbCfg *  pCfg = &(pRepo->config);

  auto tDataDir = tsdbGetDataDirName(pRepo->rootDir.c_str());
  dir = opendir(tDataDir.c_str());

  if (dir == NULL) {
    if (errno == ENOENT) {
      tsdbError("vgId:%d directory %s not exist", REPO_ID(pRepo), tDataDir);
      terrno = TAOS_SYSTEM_ERROR(errno);

      std::error_code ec;
      if (!createDir(tDataDir, ec)) {
        tsdbError("vgId:%d failed to create directory %s since %s", REPO_ID(pRepo), tDataDir, ec.message().c_str());
        terrno = TAOS_SYSTEM_ERROR(errno);
        return -1;
      }

      dir = opendir(tDataDir.c_str());
      if (dir == NULL) {
        tsdbError("vgId:%d failed to open directory %s since %s", REPO_ID(pRepo), tDataDir, strerror(errno));
        terrno = TAOS_SYSTEM_ERROR(errno);
        return -1;
      }
    } else {
      tsdbError("vgId:%d failed to open directory %s since %s", REPO_ID(pRepo), tDataDir, strerror(errno));
      terrno = TAOS_SYSTEM_ERROR(errno);
      return -1;
    }
  }

  code = regcomp(&regex1, "^v[0-9]+f[0-9]+\\.(head|data|last|stat)$", REG_EXTENDED);
  if (code != 0) {
    terrno = TSDB_CODE_TDB_OUT_OF_MEMORY;
    return -1;
  }

  code = regcomp(&regex2, "^v[0-9]+f[0-9]+\\.(h|d|l|s)$", REG_EXTENDED);
  if (code != 0) {
    terrno = TSDB_CODE_TDB_OUT_OF_MEMORY;
    return -1;
  }

  int mfid = tsdbGetCurrMinFid(pCfg->precision, pCfg->keep, pCfg->daysPerFile);

  struct dirent *dp = NULL;
  while ((dp = readdir(dir)) != NULL) {
    if (strcmp(dp->d_name, ".") == 0 || strcmp(dp->d_name, "..") == 0) continue;

    code = regexec(&regex1, dp->d_name, 0, NULL, 0);
    if (code == 0) {
      sscanf(dp->d_name, "v%df%d", &vid, &fid);
      if (vid != REPO_ID(pRepo)) {
        tsdbError("vgId:%d invalid file %s exists, ignore it", REPO_ID(pRepo), dp->d_name);
        continue;
      }

      if (fid < mfid) {
        for (int type = 0; type < TSDB_FILE_TYPE_MAX; type++) {
          tsdbGetDataFileName(pRepo->rootDir.c_str(), pCfg->tsdbId, fid, type, fname);
          (void)remove(fname);
        }
        continue;
      }

      if (tsdbSearchFGroup(pFileH, fid, TD_EQ) != NULL) continue;
      memset((void *)(&fileGroup), 0, sizeof(SFileGroup));
      fileGroup.fileId = fid;

      tsdbInitFileGroup(&fileGroup, pRepo);
    } else if (code == REG_NOMATCH) {
      code = regexec(&regex2, dp->d_name, 0, NULL, 0);
      if (code == 0) {
        auto fname1 = fmt::format("{}/{}", tDataDir, dp->d_name);
        auto fname2 = fmt::format("{}/{}_back_{}", tDataDir, dp->d_name, taosGetTimestamp(TSDB_TIME_PRECISION_MILLI));
        (void)rename(fname1.c_str(), fname2.c_str());
        tsdbDebug("vgId:%d file %s exists, backup it as %s", REPO_ID(pRepo), fname1.c_str(), fname2.c_str());
        continue;
      } else if (code == REG_NOMATCH) {
        tsdbError("vgId:%d invalid file %s exists, ignore it", REPO_ID(pRepo), dp->d_name);
        continue;
      } else {
        return -1;
      }
    } else {
      return -1;
    }

    pFileH->pFGroup[pFileH->nFGroups++] = fileGroup;
    qsort((void *)(pFileH->pFGroup), pFileH->nFGroups, sizeof(SFileGroup), compFGroup);
    tsdbDebug("vgId:%d file group %d is restored, nFGroups %d", REPO_ID(pRepo), fileGroup.fileId, pFileH->nFGroups);
  }
  _5.cancel();
  _6.cancel();
  return 0;
}

void tsdbCloseFileH(STsdbRepo *pRepo) {
  STsdbFileH *pFileH = pRepo->tsdbFileH;

  for (int i = 0; i < pFileH->nFGroups; i++) {
    SFileGroup *pFGroup = pFileH->pFGroup + i;
    for (int type = 0; type < TSDB_FILE_TYPE_MAX; type++) {
      tsdbDestroyFile(&pFGroup->files[type]);
    }
  }
}

SFileGroup *tsdbCreateFGroupIfNeed(STsdbRepo *pRepo, int fid) {
  STsdbFileH *pFileH = pRepo->tsdbFileH;
  STsdbCfg *  pCfg = &(pRepo->config);

  if (pFileH->nFGroups >= pFileH->maxFGroups) {
    int mfid = tsdbGetCurrMinFid(pCfg->precision, pCfg->keep, pCfg->daysPerFile);
    if (pFileH->pFGroup[0].fileId < mfid) {
      pthread_rwlock_wrlock(&pFileH->fhlock);
      tsdbRemoveFileGroup(pRepo, &(pFileH->pFGroup[0]));
      pthread_rwlock_unlock(&pFileH->fhlock);
    }
  }

  ASSERT(pFileH->nFGroups < pFileH->maxFGroups);

  SFileGroup  fGroup;
  SFileGroup *pFGroup = &fGroup;

  SFileGroup *pGroup = tsdbSearchFGroup(pFileH, fid, TD_EQ);
  if (pGroup == NULL) {  // if not exists, create one
    pFGroup->fileId = fid;
    for (int type = 0; type < TSDB_FILE_TYPE_MAX; type++) {
      if (tsdbCreateFile(&pFGroup->files[type], pRepo, fid, type) < 0) {
        for (int i = type; i >= 0; i--) {
          remove(pFGroup->files[i].fname);
        }

        return NULL;
      }
    }

    pthread_rwlock_wrlock(&pFileH->fhlock);
    pFileH->pFGroup[pFileH->nFGroups++] = fGroup;
    qsort((void *)(pFileH->pFGroup), pFileH->nFGroups, sizeof(SFileGroup), compFGroup);
    pthread_rwlock_unlock(&pFileH->fhlock);
    pGroup = tsdbSearchFGroup(pFileH, fid, TD_EQ);
    ASSERT(pGroup != NULL);
  }

  return pGroup;
}

void tsdbInitFileGroupIter(STsdbFileH *pFileH, SFileGroupIter *pIter, int direction) {
  pIter->pFileH = pFileH;
  pIter->direction = direction;

  if (pFileH->nFGroups == 0) {
    pIter->index = -1;
    pIter->fileId = -1;
  } else {
    if (direction == TSDB_FGROUP_ITER_FORWARD) {
      pIter->index = 0;
    } else {
      pIter->index = pFileH->nFGroups - 1;
    }
    pIter->fileId = pFileH->pFGroup[pIter->index].fileId;
  }
}

void tsdbSeekFileGroupIter(SFileGroupIter *pIter, int fid) {
  STsdbFileH *pFileH = pIter->pFileH;

  if (pFileH->nFGroups == 0) {
    pIter->index = -1;
    pIter->fileId = -1;
    return;
  }

  int   flags = (pIter->direction == TSDB_FGROUP_ITER_FORWARD) ? TD_GE : TD_LE;
  void *ptr = taosbsearch(&fid, (void *)pFileH->pFGroup, pFileH->nFGroups, sizeof(SFileGroup), keyFGroupCompFunc, flags);
  if (ptr == NULL) {
    pIter->index = -1;
    pIter->fileId = -1;
  } else {
    pIter->index = (int)(POINTER_DISTANCE(ptr, pFileH->pFGroup) / sizeof(SFileGroup));
    pIter->fileId = ((SFileGroup *)ptr)->fileId;
  }
}

SFileGroup *tsdbGetFileGroupNext(SFileGroupIter *pIter) {
  STsdbFileH *pFileH = pIter->pFileH;
  SFileGroup *pFGroup = NULL;

  if (pIter->index < 0 || pIter->index >= pFileH->nFGroups || pIter->fileId < 0) return NULL;

  pFGroup = &pFileH->pFGroup[pIter->index];
  if (pFGroup->fileId != pIter->fileId) {
    tsdbSeekFileGroupIter(pIter, pIter->fileId);
  }

  if (pIter->index < 0) return NULL;

  pFGroup = &pFileH->pFGroup[pIter->index];
  ASSERT(pFGroup->fileId == pIter->fileId);

  if (pIter->direction == TSDB_FGROUP_ITER_FORWARD) {
    pIter->index++;
  } else {
    pIter->index--;
  }

  if (pIter->index >= 0 && pIter->index < pFileH->nFGroups) {
    pIter->fileId = pFileH->pFGroup[pIter->index].fileId;
  } else {
    pIter->fileId = -1;
  }

  return pFGroup;
}

int tsdbOpenFile(SFile *pFile, int oflag) {
  ASSERT(!TSDB_IS_FILE_OPENED(pFile));

  pFile->fd = open(pFile->fname, oflag, 0755);
  if (pFile->fd < 0) {
    tsdbError("failed to open file %s since %s", pFile->fname, strerror(errno));
    terrno = TAOS_SYSTEM_ERROR(errno);
    return -1;
  }

  tsdbTrace("open file %s, fd %d", pFile->fname, pFile->fd);

  return 0;
}

void tsdbCloseFile(SFile *pFile) {
  if (TSDB_IS_FILE_OPENED(pFile)) {
    tsdbTrace("close file %s, fd %d", pFile->fname, pFile->fd);
    close(pFile->fd);
    pFile->fd = -1;
  }
}

int tsdbCreateFile(SFile *pFile, STsdbRepo *pRepo, int fid, int type) {
  memset((void *)pFile, 0, sizeof(SFile));
  pFile->fd = -1;

  tsdbGetDataFileName(pRepo->rootDir.c_str(), REPO_ID(pRepo), fid, type, pFile->fname);

  if (access(pFile->fname, F_OK) == 0) {
    tsdbError("vgId:%d file %s already exists", REPO_ID(pRepo), pFile->fname);
    terrno = TSDB_CODE_TDB_FILE_ALREADY_EXISTS;
    goto _err;
  }

  if (tsdbOpenFile(pFile, O_RDWR | O_CREAT) < 0) {
    goto _err;
  }

  pFile->info.size = TSDB_FILE_HEAD_SIZE;
  pFile->info.magic = TSDB_FILE_INIT_MAGIC;

  if (tsdbUpdateFileHeader(pFile) < 0) {
    tsdbCloseFile(pFile);
    return -1;
  }

  tsdbCloseFile(pFile);

  return 0;

_err:
  return -1;
}

SFileGroup *tsdbSearchFGroup(STsdbFileH *pFileH, int fid, int flags) {
  void *ptr =
      taosbsearch((void *)(&fid), (void *)(pFileH->pFGroup), pFileH->nFGroups, sizeof(SFileGroup), keyFGroupCompFunc, flags);
  if (ptr == NULL) return NULL;
  return (SFileGroup *)ptr;
}

void tsdbFitRetention(STsdbRepo *pRepo) {
  STsdbCfg *pCfg = &(pRepo->config);
  STsdbFileH *pFileH = pRepo->tsdbFileH;
  SFileGroup *pGroup = pFileH->pFGroup;

  int mfid = tsdbGetCurrMinFid(pCfg->precision, pCfg->keep, pCfg->daysPerFile);

  pthread_rwlock_wrlock(&(pFileH->fhlock));

  while (pFileH->nFGroups > 0 && pGroup[0].fileId < mfid) {
    tsdbRemoveFileGroup(pRepo, pGroup);
  }

  pthread_rwlock_unlock(&(pFileH->fhlock));
}

int tsdbUpdateFileHeader(SFile *pFile) {
  char buf[TSDB_FILE_HEAD_SIZE] = "\0";

  void *pBuf = (void *)buf;
  taosEncodeFixedU32((void **)(&pBuf), TSDB_FILE_VERSION);
  tsdbEncodeSFileInfo((void **)(&pBuf), &(pFile->info));

  taosCalcChecksumAppend(0, (uint8_t *)buf, TSDB_FILE_HEAD_SIZE);

  if (lseek(pFile->fd, 0, SEEK_SET) < 0) {
    tsdbError("failed to lseek file %s since %s", pFile->fname, strerror(errno));
    terrno = TAOS_SYSTEM_ERROR(errno);
    return -1;
  }
  if (taosWrite(pFile->fd, (void *)buf, TSDB_FILE_HEAD_SIZE) < TSDB_FILE_HEAD_SIZE) {
    tsdbError("failed to write %d bytes to file %s since %s", TSDB_FILE_HEAD_SIZE, pFile->fname, strerror(errno));
    terrno = TAOS_SYSTEM_ERROR(errno);
    return -1;
  }

  return 0;
}

int tsdbEncodeSFileInfo(void **buf, const STsdbFileInfo *pInfo) {
  int tlen = 0;
  tlen += taosEncodeFixedU32(buf, pInfo->magic);
  tlen += taosEncodeFixedU32(buf, pInfo->len);
  tlen += taosEncodeFixedU32(buf, pInfo->totalBlocks);
  tlen += taosEncodeFixedU32(buf, pInfo->totalSubBlocks);
  tlen += taosEncodeFixedU32(buf, pInfo->offset);
  tlen += taosEncodeFixedU64(buf, pInfo->size);
  tlen += taosEncodeFixedU64(buf, pInfo->tombSize);

  return tlen;
}

void *tsdbDecodeSFileInfo(void *buf, STsdbFileInfo *pInfo) {
  buf = taosDecodeFixedU32(buf, &(pInfo->magic));
  buf = taosDecodeFixedU32(buf, &(pInfo->len));
  buf = taosDecodeFixedU32(buf, &(pInfo->totalBlocks));
  buf = taosDecodeFixedU32(buf, &(pInfo->totalSubBlocks));
  buf = taosDecodeFixedU32(buf, &(pInfo->offset));
  buf = taosDecodeFixedU64(buf, &(pInfo->size));
  buf = taosDecodeFixedU64(buf, &(pInfo->tombSize));

  return buf;
}

void tsdbRemoveFileGroup(STsdbRepo *pRepo, SFileGroup *pFGroup) {
  ASSERT(pFGroup != NULL);
  STsdbFileH *pFileH = pRepo->tsdbFileH;

  SFileGroup fileGroup = *pFGroup;

  int nFilesLeft = pFileH->nFGroups - (int)(POINTER_DISTANCE(pFGroup, pFileH->pFGroup) / sizeof(SFileGroup) + 1);
  if (nFilesLeft > 0) {
    memmove((void *)pFGroup, POINTER_SHIFT(pFGroup, sizeof(SFileGroup)), sizeof(SFileGroup) * nFilesLeft);
  }

  pFileH->nFGroups--;
  ASSERT(pFileH->nFGroups >= 0);

  for (int type = 0; type < TSDB_FILE_TYPE_MAX; type++) {
    if (remove(fileGroup.files[type].fname) < 0) {
      tsdbError("vgId:%d failed to remove file %s", REPO_ID(pRepo), fileGroup.files[type].fname);
    }
    tsdbDestroyFile(&fileGroup.files[type]);
  }
}

int tsdbLoadFileHeader(SFile *pFile, uint32_t *version) {
  char buf[TSDB_FILE_HEAD_SIZE] = "\0";

  if (lseek(pFile->fd, 0, SEEK_SET) < 0) {
    tsdbError("failed to lseek file %s to start since %s", pFile->fname, strerror(errno));
    terrno = TAOS_SYSTEM_ERROR(errno);
    return -1;
  }

  if (taosRead(pFile->fd, buf, TSDB_FILE_HEAD_SIZE) < TSDB_FILE_HEAD_SIZE) {
    tsdbError("failed to read file %s header part with %d bytes, reason:%s", pFile->fname, TSDB_FILE_HEAD_SIZE,
              strerror(errno));
    terrno = TSDB_CODE_TDB_FILE_CORRUPTED;
    return -1;
  }

  if (!taosCheckChecksumWhole((uint8_t *)buf, TSDB_FILE_HEAD_SIZE)) {
    tsdbError("file %s header part is corrupted with failed checksum", pFile->fname);
    terrno = TSDB_CODE_TDB_FILE_CORRUPTED;
    return -1;
  }

  void *pBuf = (void *)buf;
  pBuf = taosDecodeFixedU32(pBuf, version);
  pBuf = tsdbDecodeSFileInfo(pBuf, &(pFile->info));

  return 0;
}

void tsdbGetFileInfoImpl(const char *fname, uint32_t *magic, int64_t *size) {
  uint32_t      version = 0;
  SFile         file;
  SFile *       pFile = &file;

  strncpy(pFile->fname, fname, TSDB_FILENAME_LEN - 1);
  pFile->fd = -1;

  if (tsdbOpenFile(pFile, O_RDONLY) < 0) {
    *magic = TSDB_FILE_INIT_MAGIC;
    *size = 0;
    return;
  }
  if (tsdbLoadFileHeader(pFile, &version) < 0) {
    *magic = TSDB_FILE_INIT_MAGIC;
    *size = 0;
    return;
  }

  off_t offset = lseek(pFile->fd, 0, SEEK_END);
  tsdbCloseFile(pFile);
  if (offset < 0) {
    *magic = TSDB_FILE_INIT_MAGIC;
    *size = 0;
    return;
  } else {
    *magic = pFile->info.magic;
    *size = offset;
  }
  return;
}

// ---------------- LOCAL FUNCTIONS ----------------
static int tsdbInitFile(SFile *pFile, STsdbRepo *pRepo, int fid, int type) {
  uint32_t version;

  tsdbGetDataFileName(pRepo->rootDir.c_str(), REPO_ID(pRepo), fid, type, pFile->fname);

  pFile->fd = -1;
  if (tsdbOpenFile(pFile, O_RDONLY) < 0) goto _err;

  if (tsdbLoadFileHeader(pFile, &version) < 0) {
    tsdbError("vgId:%d failed to load file %s header part since %s", REPO_ID(pRepo), pFile->fname, tstrerror(terrno));
    goto _err;
  }

  if (pFile->info.size == TSDB_FILE_HEAD_SIZE) {
    pFile->info.size = lseek(pFile->fd, 0, SEEK_END);
  }

  if (version != TSDB_FILE_VERSION) {
    // TODO: deal with error
    tsdbError("vgId:%d file %s version %u is not the same as program version %u which may cause problem",
              REPO_ID(pRepo), pFile->fname, version, TSDB_FILE_VERSION);
  }

  tsdbCloseFile(pFile);

  return 0;
_err:
  tsdbDestroyFile(pFile);
  return -1;
}

static void tsdbDestroyFile(SFile *pFile) { tsdbCloseFile(pFile); }

static int compFGroup(const void *arg1, const void *arg2) {
  int val1 = ((SFileGroup *)arg1)->fileId;
  int val2 = ((SFileGroup *)arg2)->fileId;

  if (val1 < val2) {
    return -1;
  } else if (val1 > val2) {
    return 1;
  } else {
    return 0;
  }
}

static int keyFGroupCompFunc(const void *key, const void *fgroup) {
  int         fid = *(int *)key;
  SFileGroup *pFGroup = (SFileGroup *)fgroup;
  if (fid == pFGroup->fileId) {
    return 0;
  } else {
    return fid > pFGroup->fileId ? 1 : -1;
  }
}

static void tsdbInitFileGroup(SFileGroup *pFGroup, STsdbRepo *pRepo) {
  for (int type = 0; type < TSDB_FILE_TYPE_MAX; type++) {
    if (tsdbInitFile(&pFGroup->files[type], pRepo, pFGroup->fileId, type) < 0) {
      memset(&pFGroup->files[type].info, 0, sizeof(STsdbFileInfo));
      pFGroup->files[type].info.magic = TSDB_FILE_INIT_MAGIC;
      pFGroup->state = 1;
      pRepo->state = TSDB_STATE_BAD_FILE;
      terrno = TSDB_CODE_TDB_FILE_CORRUPTED;
    }
  }
}

static TSKEY tsdbGetCurrMinKey(int8_t precision, int32_t keep) {
  return (TSKEY)(taosGetTimestamp(precision) - keep * tsMsPerDay[precision]);
}

static int tsdbGetCurrMinFid(int8_t precision, int32_t keep, int32_t days) {
  return (int)(TSDB_KEY_FILEID(tsdbGetCurrMinKey(precision, keep), days, precision));
}