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
#include "tulog.h"
#include "tutil.h"
#include "tref.h"
#include "tfile.h"

FileOpPtr tfOpen(const char *pathname, int32_t flags) {
  int32_t fd = open(pathname, flags);
  return std::make_shared<FileOp>(fd);
}

FileOpPtr tfOpenM(const char *pathname, int32_t flags, mode_t mode) {
  int32_t fd = open(pathname, flags, mode);
  return std::make_shared<FileOp>(fd);
}

int64_t FileOp::write(void *buf, int64_t count) {
  int64_t ret = taosWrite(fd, buf, count);
  if (ret < 0) terrno = TAOS_SYSTEM_ERROR(errno);
  return ret;
}

int64_t FileOp::read(void *buf, int64_t count) {
  int64_t ret = taosRead(fd, buf, count);
  if (ret < 0) terrno = TAOS_SYSTEM_ERROR(errno);
  return ret;
}

int32_t FileOp::fsync() {
  int32_t code = ::fsync(fd);
  return code;
}

int64_t FileOp::lseek(int64_t offset, int32_t whence) {
  int64_t ret = taosLSeek(fd, offset, whence);
  return ret;
}

int32_t FileOp::ftruncate(int64_t length) {
  int32_t code = taosFtruncate(fd, length);
  return code;
}
