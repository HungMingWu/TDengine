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

#ifndef TDENGINE_VNODE_H
#define TDENGINE_VNODE_H

#include <atomic>
#include "trpc.h"
#include "walInt.h"

typedef struct {
  int32_t len;
  void *  rsp;
  void *  qhandle;  // used by query and retrieve msg
} SRspRet;

typedef struct {
  int32_t code;
  int32_t contLen;
  void *  rpcHandle;
  void *  rpcAhandle;
  void *  qhandle;
  void *  pVnode;
  int8_t  qtype;
  int8_t  msgType;
  SRspRet rspRet;
  char    pCont[];
} SVReadMsg;

struct SVWriteMsg {
  int32_t  code;
  std::atomic<int32_t> processedCount;
  int32_t  qtype;
  void *   pVnode;
  SRpcMsg  rpcMsg;
  SRspRet  rspRet;
  char     reserveForSync[24];
  SWalHead pHead;
};

#endif