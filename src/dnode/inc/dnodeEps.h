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

#ifndef TDENGINE_DNODE_EP_H
#define TDENGINE_DNODE_EP_H

#include "dnodeInt.h"

struct SDnodeEp {
  int32_t  dnodeId;
  uint16_t dnodePort;
  char     dnodeFqdn[TSDB_FQDN_LEN];
};

struct SDnodeEps {
  int32_t  dnodeNum;
  SDnodeEp dnodeEps[];
  void     update();
};

int32_t dnodeInitEps();
void    dnodeCleanupEps();
void    dnodeUpdateEp(int32_t dnodeId, char *epstr, char *fqdn, uint16_t *port);
bool    dnodeCheckEpChanged(int32_t dnodeId, char *epstr);

#endif
