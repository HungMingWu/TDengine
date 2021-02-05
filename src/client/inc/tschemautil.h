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

#ifndef TDENGINE_TSCHEMAUTIL_H
#define TDENGINE_TSCHEMAUTIL_H

#include "taosmsg.h"
#include "tstoken.h"
#include "tsclient.h"

/**
 * create the table meta from the msg
 * @param pTableMetaMsg
 * @param size size of the table meta
 * @return
 */
STableMeta* tscCreateTableMetaFromMsg(STableMetaMsg* pTableMetaMsg);

bool vgroupInfoIdentical(SNewVgroupInfo *pExisted, SVgroupMsg* src);
SNewVgroupInfo createNewVgroupInfo(SVgroupMsg *pVgroupMsg);

#endif  // TDENGINE_TSCHEMAUTIL_H
