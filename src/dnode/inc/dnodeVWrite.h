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

#ifndef TDENGINE_DNODE_VWRITE_H
#define TDENGINE_DNODE_VWRITE_H

#include "dnodeInt.h"

int32_t dnodeInitVWrite();
void    dnodeCleanupVWrite();
void    dnodeDispatchToVWriteQueue(SRpcMsg *pMsg);
std::unique_ptr<STaosQueue> dnodeAllocVWriteQueue(void *pVnode);
void    dnodeFreeVWriteQueue(void *pWqueue);
void    dnodeSendRpcVWriteRsp(void *pVnode, void *pWrite, int32_t code);

#endif
