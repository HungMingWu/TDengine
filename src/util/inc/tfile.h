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

#ifndef TDENGINE_TFILE_H
#define TDENGINE_TFILE_H

#include <memory>
class FileOp {
  int fd;

 public:
  FileOp(int64_t fd) : fd(fd) {}
  ~FileOp() { close(fd); }
  int64_t write(void *buf, int64_t count);
  int64_t read(void *buf, int64_t count);
  int32_t fsync();
  int64_t lseek(int64_t offset, int32_t whence);
  int32_t ftruncate(int64_t length);
};

using FileOpPtr = std::shared_ptr<FileOp>;

// the same syntax as UNIX standard open/close/read/write
// but FD is int64_t and will never be reused
FileOpPtr tfOpen(const char *pathname, int32_t flags);
FileOpPtr tfOpenM(const char *pathname, int32_t flags, mode_t mode);

#endif  // TDENGINE_TFILE_H
