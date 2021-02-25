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

#include "os.h"
#include "workpool.h"
#include "tqueue.h"
#include "tnote.h"
#include "taos.h"
#include "tsclient.h"
#include "httpInt.h"
#include "httpContext.h"
#include "httpSql.h"
#include "httpResp.h"
#include "httpAuth.h"
#include "httpSession.h"
#include "httpQueue.h"

typedef struct {
  void *  param;
  void *  result;
  int32_t code;
  int32_t rows;
  FHttpResultFp fp;
} SHttpResult;

using SHttpItem = std::pair<int, SHttpResult*>;
class HttpPool : public workpool<SHttpItem, HttpPool> {
 public:
  void process(std::vector<SHttpItem> &&items) {
    for (auto &item : items) {
      auto qtype = item.first;
      auto pMsg = item.second;
      httpTrace("context:%p, res:%p will be processed in result queue, code:%d rows:%d", pMsg->param, pMsg->result,
                pMsg->code, pMsg->rows);
      (*pMsg->fp)(pMsg->param, pMsg->result, pMsg->code, pMsg->rows);
    }
  }
  using workpool<SHttpItem, HttpPool>::workpool;
};

static std::shared_ptr<HttpPool> tsHttpPool;

void httpDispatchToResultQueue(void *param, TAOS_RES *result, int32_t code, int32_t rows, FHttpResultFp fp) {
  if (tsHttpPool != NULL) {
    SHttpResult *pMsg = (SHttpResult *)taosAllocateQitem(sizeof(SHttpResult));
    pMsg->param = param;
    pMsg->result = result;
    pMsg->code = code;
    pMsg->rows = rows;
    pMsg->fp = fp;
    tsHttpPool->put(std::make_pair(TAOS_QTYPE_RPC, pMsg));
  } else {
    (*fp)(param, result, code, rows);
  }
}

bool httpInitResultQueue() {
  tsHttpPool = std::make_shared<HttpPool>(tsHttpMaxThreads);
}

void httpCleanupResultQueue() {
  tsHttpPool->stop();
  tsHttpPool.reset();
  httpInfo("http result queue is closed");
}
