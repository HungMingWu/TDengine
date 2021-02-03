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
#include <mutex>
#include <thread>
#include <vector>
#include "os.h"
#include "tulog.h"
#include "tlog.h"
#include "tnote.h"
#include "tutil.h"

#define MAX_LOGLINE_SIZE              (1000)
#define MAX_LOGLINE_BUFFER_SIZE       (MAX_LOGLINE_SIZE + 10)
#define MAX_LOGLINE_CONTENT_SIZE      (MAX_LOGLINE_SIZE - 100)
#define MAX_LOGLINE_DUMP_SIZE         (65 * 1024)
#define MAX_LOGLINE_DUMP_BUFFER_SIZE  (MAX_LOGLINE_DUMP_SIZE + 10)
#define MAX_LOGLINE_DUMP_CONTENT_SIZE (MAX_LOGLINE_DUMP_SIZE - 100)

#define LOG_FILE_NAME_LEN          300
static constexpr size_t TSDB_DEFAULT_LOG_BUF_SIZE = 512 * 1024;  // 512K
static constexpr size_t TSDB_MIN_LOG_BUF_SIZE = 1024;  // 1K
static constexpr size_t TSDB_MAX_LOG_BUF_SIZE = 1024 * 1024; 
static_assert(TSDB_MIN_LOG_BUF_SIZE <= TSDB_DEFAULT_LOG_BUF_SIZE);
static_assert(TSDB_DEFAULT_LOG_BUF_SIZE <= TSDB_MAX_LOG_BUF_SIZE);
#define TSDB_DEFAULT_LOG_BUF_UNIT  1024         // 1K

struct SLogBuff {
  std::vector<char> buffer;
  size_t         buffStart = 0;
  size_t         buffEnd = 0;
  int32_t         fd;
  int32_t         stop = 0;
  std::thread     asyncThread;
  std::mutex      buffMutex;
  tsem_t          buffNotEmpty;

 public:
  SLogBuff(size_t bufSize) : buffer(bufSize) {}
  ~SLogBuff() = default;
  SLogBuff(const SLogBuff &) = delete;
  SLogBuff(SLogBuff &&) = delete;
  SLogBuff& operator=(const SLogBuff &) = delete;
  SLogBuff& operator=(SLogBuff &&) = delete;
  bool push(char *msg, size_t msgLen);
  size_t poll(char *buf, size_t bufSize);
};

struct SLogObj {
  int32_t fileNum = 1;
  int32_t maxLines;
  int32_t lines;
  int32_t flag;
  int32_t openInProgress;
  pid_t   pid;
  char    logName[LOG_FILE_NAME_LEN];
  SLogBuff   logHandle{TSDB_DEFAULT_LOG_BUF_SIZE};
  std::mutex      logMutex;
};

int32_t tsLogKeepDays = 0;
int8_t  tsAsyncLog = 1;
float   tsTotalLogDirGB = 0;
float   tsAvailLogDirGB = 0;
float   tsMinimalLogDirGB = 1.0f;
#ifdef _TD_POWER_
char    tsLogDir[TSDB_FILENAME_LEN] = "/var/log/power";
#else
char    tsLogDir[TSDB_FILENAME_LEN] = "/var/log/taos";
#endif

static SLogObj   tsLogObj;
static void      taosCloseLogByFd(int32_t oldFd);
static int32_t   taosOpenLogFile(char *fn, int32_t maxLines, int32_t maxFileNum);
extern void      taosPrintGlobalCfg();

int32_t taosInitLog(char *logName, int numOfLogLines, int maxFiles) {
  tsem_init(&(tsLogObj.logHandle.buffNotEmpty), 0, 0);
  if (taosOpenLogFile(logName, numOfLogLines, maxFiles) < 0) return -1;
  tsLogObj.logHandle.asyncThread = std::thread(
      [](SLogBuff &tLogBuff) {
        char tempBuffer[TSDB_DEFAULT_LOG_BUF_UNIT];

        while (1) {
          tsem_wait(&(tLogBuff.buffNotEmpty));

          // Polling the buffer
          while (1) {
            size_t log_size = tLogBuff.poll(tempBuffer, TSDB_DEFAULT_LOG_BUF_UNIT);
            if (log_size) {
              taosWrite(tLogBuff.fd, tempBuffer, log_size);
            } else {
              break;
            }
          }

          if (tLogBuff.stop) break;
        }
      },
      std::ref(tsLogObj.logHandle));
  return 0;
}

static void taosStopLog() {
  tsLogObj.logHandle.stop = 1;
}

void taosCloseLog() {
  taosStopLog();
  tsem_post(&(tsLogObj.logHandle.buffNotEmpty));
  tsLogObj.logHandle.asyncThread.join();
  // In case that other threads still use log resources causing invalid write in valgrind
  // we comment two lines below.
  // taosCloseLog();
}

static bool taosLockFile(int32_t fd) {
  if (fd < 0) return false;

  if (tsLogObj.fileNum > 1) {
    int32_t ret = flock(fd, LOCK_EX | LOCK_NB);
    if (ret == 0) {
      return true;
    }
  }

  return false;
}

static void taosUnLockFile(int32_t fd) {
  if (fd < 0) return;

  if (tsLogObj.fileNum > 1) {
    flock(fd, LOCK_UN | LOCK_NB);
  }
}

static void taosKeepOldLog(char *oldName) {
  if (tsLogKeepDays == 0) return;

  int64_t fileSec = taosGetTimestampSec();
  char    fileName[LOG_FILE_NAME_LEN + 20];
  snprintf(fileName, LOG_FILE_NAME_LEN + 20, "%s.%" PRId64, tsLogObj.logName, fileSec);

  taosRename(oldName, fileName);
  if (tsLogKeepDays < 0) {
    char compressFileName[LOG_FILE_NAME_LEN + 20];
    snprintf(compressFileName, LOG_FILE_NAME_LEN + 20, "%s.%" PRId64 ".gz", tsLogObj.logName, fileSec);
    if (taosCompressFile(fileName, compressFileName) == 0) {
      (void)remove(fileName);
    }
  }

  taosRemoveOldLogFiles(tsLogDir, ABS(tsLogKeepDays));
}

static void *taosThreadToOpenNewFile(void *param) {
  char keepName[LOG_FILE_NAME_LEN + 20];
  sprintf(keepName, "%s.%d", tsLogObj.logName, tsLogObj.flag);

  tsLogObj.flag ^= 1;
  tsLogObj.lines = 0;
  char name[LOG_FILE_NAME_LEN + 20];
  sprintf(name, "%s.%d", tsLogObj.logName, tsLogObj.flag);

  umask(0);

  int32_t fd = open(name, O_WRONLY | O_CREAT | O_TRUNC, S_IRWXU | S_IRWXG | S_IRWXO);
  if (fd < 0) {
    uError("open new log file fail! fd:%d reason:%s", fd, strerror(errno));
    return NULL;
  }

  taosLockFile(fd);
  (void)lseek(fd, 0, SEEK_SET);

  int32_t oldFd = tsLogObj.logHandle.fd;
  tsLogObj.logHandle.fd = fd;
  tsLogObj.lines = 0;
  tsLogObj.openInProgress = 0;
  taosCloseLogByFd(oldFd);
  
  uInfo("   new log file:%d is opened", tsLogObj.flag);
  uInfo("==================================");
  taosPrintGlobalCfg();
  taosKeepOldLog(keepName);

  return NULL;
}

static int32_t taosOpenNewLogFile() {
  std::lock_guard<std::mutex> _(tsLogObj.logMutex);

  if (tsLogObj.lines > tsLogObj.maxLines && tsLogObj.openInProgress == 0) {
    tsLogObj.openInProgress = 1;

    uInfo("open new log file ......");
    pthread_t      thread;
    pthread_attr_t attr;
    pthread_attr_init(&attr);
    pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_DETACHED);

    pthread_create(&thread, &attr, taosThreadToOpenNewFile, NULL);
    pthread_attr_destroy(&attr);
  }

  return 0;
}

void taosResetLog() {
  char lastName[LOG_FILE_NAME_LEN + 20];
  sprintf(lastName, "%s.%d", tsLogObj.logName, tsLogObj.flag);

  // force create a new log file
  tsLogObj.lines = tsLogObj.maxLines + 10;

  taosOpenNewLogFile();
  (void)remove(lastName);

  uInfo("==================================");
  uInfo("   reset log file ");
}

static bool taosCheckFileIsOpen(char *logFileName) {
  int32_t fd = open(logFileName, O_WRONLY, S_IRWXU | S_IRWXG | S_IRWXO);
  if (fd < 0) {
    if (errno == ENOENT) {
      return false;
    } else {
      printf("\nfailed to open log file:%s, reason:%s\n", logFileName, strerror(errno));
      return true;
    }
  }

  if (taosLockFile(fd)) {
    taosUnLockFile(fd);
    taosClose(fd);
    return false;
  } else {
    taosClose(fd);
    return true;
  }
}

static void taosGetLogFileName(char *fn) {
  if (tsLogObj.fileNum > 1) {
    for (int32_t i = 0; i < tsLogObj.fileNum; i++) {
      char fileName[LOG_FILE_NAME_LEN];

      sprintf(fileName, "%s%d.0", fn, i);
      bool file1open = taosCheckFileIsOpen(fileName);

      sprintf(fileName, "%s%d.1", fn, i);
      bool file2open = taosCheckFileIsOpen(fileName);

      if (!file1open && !file2open) {
        sprintf(tsLogObj.logName, "%s%d", fn, i);
        return;
      }
    }
  }

  if (strlen(fn) < LOG_FILE_NAME_LEN) {
    strcpy(tsLogObj.logName, fn);
  }
}

static int32_t taosOpenLogFile(char *fn, int32_t maxLines, int32_t maxFileNum) {
#ifdef WINDOWS
  /*
  * always set maxFileNum to 1
  * means client log filename is unique in windows
  */
  maxFileNum = 1;
#endif

  char        name[LOG_FILE_NAME_LEN + 50] = "\0";
  struct stat logstat0, logstat1;
  int32_t     size;

  tsLogObj.maxLines = maxLines;
  tsLogObj.fileNum = maxFileNum;
  taosGetLogFileName(fn);

  if (strlen(fn) < LOG_FILE_NAME_LEN + 50 - 2) {
    strcpy(name, fn);
    strcat(name, ".0");
  }
  bool log0Exist = stat(name, &logstat0) >= 0;

  if (strlen(fn) < LOG_FILE_NAME_LEN + 50 - 2) {
    strcpy(name, fn);
    strcat(name, ".1");
  }
  bool log1Exist = stat(name, &logstat1) >= 0;
  
  // if none of the log files exist, open 0, if both exists, open the old one
  if (!log0Exist && !log1Exist) {
    tsLogObj.flag = 0;
  } else if (!log1Exist) {
    tsLogObj.flag = 0;
  } else if (!log0Exist) {
    tsLogObj.flag = 1;
  } else {
    tsLogObj.flag = (logstat0.st_mtime > logstat1.st_mtime) ? 0 : 1;
  }

  char fileName[LOG_FILE_NAME_LEN + 50] = "\0";
  sprintf(fileName, "%s.%d", tsLogObj.logName, tsLogObj.flag);

  umask(0);
  tsLogObj.logHandle.fd = open(fileName, O_WRONLY | O_CREAT, S_IRWXU | S_IRWXG | S_IRWXO);

  if (tsLogObj.logHandle.fd < 0) {
    printf("\nfailed to open log file:%s, reason:%s\n", fileName, strerror(errno));
    return -1;
  }
  taosLockFile(tsLogObj.logHandle.fd);

  // only an estimate for number of lines
  struct stat filestat;
  if (fstat(tsLogObj.logHandle.fd, &filestat) < 0) {
    printf("\nfailed to fstat log file:%s, reason:%s\n", fileName, strerror(errno));
    return -1;
  }
  size = (int32_t)filestat.st_size;
  tsLogObj.lines = size / 60;

  lseek(tsLogObj.logHandle.fd, 0, SEEK_END);

  sprintf(name, "==================================================\n");
  taosWrite(tsLogObj.logHandle.fd, name, (uint32_t)strlen(name));
  sprintf(name, "                new log file                      \n");
  taosWrite(tsLogObj.logHandle.fd, name, (uint32_t)strlen(name));
  sprintf(name, "==================================================\n");
  taosWrite(tsLogObj.logHandle.fd, name, (uint32_t)strlen(name));

  return 0;
}

void taosPrintLog(const char *flags, int32_t dflag, const char *format, ...) {
  if (tsTotalLogDirGB != 0 && tsAvailLogDirGB < tsMinimalLogDirGB) {
    printf("server disk:%s space remain %.3f GB, total %.1f GB, stop print log.\n", tsLogDir, tsAvailLogDirGB, tsTotalLogDirGB);
    fflush(stdout);
    return;
  }

  va_list        argpointer;
  char           buffer[MAX_LOGLINE_BUFFER_SIZE] = { 0 };
  int32_t        len;
  struct tm      Tm, *ptm;
  struct timeval timeSecs;
  time_t         curTime;

  gettimeofday(&timeSecs, NULL);
  curTime = timeSecs.tv_sec;
  ptm = localtime_r(&curTime, &Tm);

  len = sprintf(buffer, "%02d/%02d %02d:%02d:%02d.%06d 0x%08" PRIx64 " ", ptm->tm_mon + 1, ptm->tm_mday, ptm->tm_hour,
                ptm->tm_min, ptm->tm_sec, (int32_t)timeSecs.tv_usec, taosGetSelfPthreadId());
  len += sprintf(buffer + len, "%s", flags);

  va_start(argpointer, format);
  int32_t writeLen = vsnprintf(buffer + len, MAX_LOGLINE_CONTENT_SIZE, format, argpointer);
  if (writeLen <= 0) {
    char tmp[MAX_LOGLINE_DUMP_BUFFER_SIZE] = {0};
    writeLen = vsnprintf(tmp, MAX_LOGLINE_DUMP_CONTENT_SIZE, format, argpointer);
    strncpy(buffer + len, tmp, MAX_LOGLINE_CONTENT_SIZE);
    len += MAX_LOGLINE_CONTENT_SIZE;
  } else if (writeLen >= MAX_LOGLINE_CONTENT_SIZE) {
    len += MAX_LOGLINE_CONTENT_SIZE;
  } else {
    len += writeLen;
  }
  va_end(argpointer);

  if (len > MAX_LOGLINE_SIZE) len = MAX_LOGLINE_SIZE;

  buffer[len++] = '\n';
  buffer[len] = 0;

  if ((dflag & DEBUG_FILE) && tsLogObj.logHandle.fd >= 0) {
    if (tsAsyncLog) {
      tsLogObj.logHandle.push(buffer, len);
    } else {
      taosWrite(tsLogObj.logHandle.fd, buffer, len);
    }

    if (tsLogObj.maxLines > 0) {
      atomic_add_fetch_32(&tsLogObj.lines, 1);

      if ((tsLogObj.lines > tsLogObj.maxLines) && (tsLogObj.openInProgress == 0)) taosOpenNewLogFile();
    }
  }

  if (dflag & DEBUG_SCREEN) taosWrite(1, buffer, (uint32_t)len);
  if (dflag == 255) nInfo(buffer, len);
}

void taosDumpData(unsigned char *msg, int32_t len) {
  if (tsTotalLogDirGB != 0 && tsAvailLogDirGB < tsMinimalLogDirGB) {
    printf("server disk:%s space remain %.3f GB, total %.1f GB, stop dump log.\n", tsLogDir, tsAvailLogDirGB, tsTotalLogDirGB);
    fflush(stdout);
    return;
  }

  char temp[256];
  int32_t  i, pos = 0, c = 0;

  for (i = 0; i < len; ++i) {
    sprintf(temp + pos, "%02x ", msg[i]);
    c++;
    pos += 3;
    if (c >= 16) {
      temp[pos++] = '\n';
      taosWrite(tsLogObj.logHandle.fd, temp, (uint32_t)pos);
      c = 0;
      pos = 0;
    }
  }

  temp[pos++] = '\n';

  taosWrite(tsLogObj.logHandle.fd, temp, (uint32_t)pos);
}

void taosPrintLongString(const char *flags, int32_t dflag, const char *format, ...) {
  if (tsTotalLogDirGB != 0 && tsAvailLogDirGB < tsMinimalLogDirGB) {
    printf("server disk:%s space remain %.3f GB, total %.1f GB, stop write log.\n", tsLogDir, tsAvailLogDirGB, tsTotalLogDirGB);
    fflush(stdout);
    return;
  }

  va_list        argpointer;
  char           buffer[MAX_LOGLINE_DUMP_BUFFER_SIZE];
  int32_t        len;
  struct tm      Tm, *ptm;
  struct timeval timeSecs;
  time_t         curTime;

  gettimeofday(&timeSecs, NULL);
  curTime = timeSecs.tv_sec;
  ptm = localtime_r(&curTime, &Tm);

  len = sprintf(buffer, "%02d/%02d %02d:%02d:%02d.%06d 0x%08" PRIx64 " ", ptm->tm_mon + 1, ptm->tm_mday, ptm->tm_hour,
                ptm->tm_min, ptm->tm_sec, (int32_t)timeSecs.tv_usec, taosGetSelfPthreadId());
  len += sprintf(buffer + len, "%s", flags);

  va_start(argpointer, format);
  len += vsnprintf(buffer + len, MAX_LOGLINE_DUMP_CONTENT_SIZE, format, argpointer);
  va_end(argpointer);

  if (len > MAX_LOGLINE_DUMP_SIZE) len = MAX_LOGLINE_DUMP_SIZE;

  buffer[len++] = '\n';
  buffer[len] = 0;

  if ((dflag & DEBUG_FILE) && tsLogObj.logHandle.fd >= 0) {
    if (tsAsyncLog) {
      tsLogObj.logHandle.push(buffer, len);
    } else {
      taosWrite(tsLogObj.logHandle.fd, buffer, len);
    }
    
    if (tsLogObj.maxLines > 0) {
      atomic_add_fetch_32(&tsLogObj.lines, 1);

      if ((tsLogObj.lines > tsLogObj.maxLines) && (tsLogObj.openInProgress == 0)) taosOpenNewLogFile();
    }
  }

  if (dflag & DEBUG_SCREEN) taosWrite(1, buffer, (uint32_t)len);
}

#if 0
void taosCloseLog() { 
  taosCloseLogByFd(tsLogObj.logHandle.fd);
}
#endif

static void taosCloseLogByFd(int32_t fd) {
  if (fd >= 0) {
    taosUnLockFile(fd);
    taosClose(fd);
  }
}

bool SLogBuff::push(char *msg, size_t msgLen) {
  if (stop) return false;

  std::lock_guard<std::mutex> _(buffMutex);
  const size_t start = buffStart;
  const size_t end = buffEnd;

  size_t remainSize = (start > end) ? (end - start - 1) : (start + buffer.size() - end - 1);

  if (remainSize <= msgLen) {
    return false;
  }

  if (start > end) {
    memcpy(buffer.data() + end, msg, msgLen);
  } else {
    if (buffer.size() - end < msgLen) {
      memcpy(buffer.data() + end, msg, buffer.size() - end);
      memcpy(buffer.data(), msg + buffer.size() - end, msgLen - buffer.size() + end);
    } else {
      memcpy(buffer.data() + end, msg, msgLen);
    }
  }
  buffEnd = (end + msgLen) % buffer.size();

  // TODO : put string in the buffer

  tsem_post(&buffNotEmpty);

  return true;
}

size_t SLogBuff::poll(char *buf, size_t bufSize) {
  const size_t start = buffStart;
  const size_t end = buffEnd;
  size_t pollSize = 0;

  if (start == end) {
    return 0;
  } else if (start < end) {
    pollSize = MIN(end - start, bufSize);
    memcpy(buf, buffer.data() + start, pollSize);
  } else {
    pollSize = MIN(end + buffer.size() - start, bufSize);
    if (pollSize > buffer.size() - start) {
      size_t tsize = buffer.size() - start;
      memcpy(buf, buffer.data() + start, tsize);
      memcpy(buf + tsize, buffer.data(), pollSize - tsize);
    } else {
      memcpy(buf, buffer.data() + start, pollSize);
    }
  }
  buffStart = (start + pollSize) % buffer.size();
  return pollSize;
}