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
#include "cJSON.h"
#include "dnodeCfg.h"
#include "defer.h"
#include <mutex>

static SDnodeCfg tsCfg = {0};
static std::mutex tsCfgMutex;

static int32_t dnodeReadCfg();
static int32_t dnodeWriteCfg();
static void    dnodeResetCfg(SDnodeCfg *cfg);
static void    dnodePrintCfg(SDnodeCfg *cfg);

int32_t dnodeInitCfg() {
  dnodeResetCfg(NULL);
  int32_t ret = dnodeReadCfg();
  if (ret == 0) {
    dInfo("dnode cfg is initialized");
  }
  return ret;
}

void dnodeUpdateCfg(SDnodeCfg *cfg) {
  if (tsCfg.dnodeId != 0) return;
  dnodeResetCfg(cfg);
}

int32_t dnodeGetDnodeId() {
  std::lock_guard<std::mutex> lock(tsCfgMutex);
  return tsCfg.dnodeId;
}

void dnodeGetClusterId(char *clusterId) {
  std::lock_guard<std::mutex> lock(tsCfgMutex);
  tstrncpy(clusterId, tsCfg.clusterId, TSDB_CLUSTER_ID_LEN);
}

void dnodeGetCfg(int32_t *dnodeId, char *clusterId) {
  std::lock_guard<std::mutex> lock(tsCfgMutex);
  *dnodeId = tsCfg.dnodeId;
  tstrncpy(clusterId, tsCfg.clusterId, TSDB_CLUSTER_ID_LEN);
}

static void dnodeResetCfg(SDnodeCfg *cfg) {
  if (cfg == NULL) return;
  if (cfg->dnodeId == 0) return;

  std::lock_guard<std::mutex> lock(tsCfgMutex);
  tsCfg.dnodeId = cfg->dnodeId;
  tstrncpy(tsCfg.clusterId, cfg->clusterId, TSDB_CLUSTER_ID_LEN);
  dnodePrintCfg(cfg);
  dnodeWriteCfg();
}

static void dnodePrintCfg(SDnodeCfg *cfg) {
  dInfo("dnodeId is set to %d, clusterId is set to %s", cfg->dnodeId, cfg->clusterId);
}

static int32_t dnodeReadCfg() {
  int32_t   len = 0;
  int32_t   maxLen = 200;
  char *    content = new char[maxLen + 1];
  auto _1 = defer([&] {
    if (content != NULL) delete[] content;
  });
  cJSON *   root = NULL;
  auto _2 = defer([&] {
    if (root != NULL) cJSON_Delete(root);
  });
  FILE *    fp = NULL;
  auto _3 = defer([&] {
    if (fp != NULL) fclose(fp);
  });
  SDnodeCfg cfg = {0};
  auto _4 = defer([&] {
    terrno = 0;

    dnodeResetCfg(&cfg);
  });

  char file[TSDB_FILENAME_LEN + 20] = {0};
  sprintf(file, "%s/dnodeCfg.json", tsDnodeDir);

  fp = fopen(file, "r");
  if (!fp) {
    dDebug("failed to read %s, file not exist", file);
    return 0;
  }

  len = fread(content, 1, maxLen, fp);
  if (len <= 0) {
    dError("failed to read %s, content is null", file);
    return 0;
  }

  content[len] = 0;
  root = cJSON_Parse(content);
  if (root == NULL) {
    dError("failed to read %s, invalid json format", file);
    return 0;
  }

  cJSON *dnodeId = cJSON_GetObjectItem(root, "dnodeId");
  if (!dnodeId || dnodeId->type != cJSON_Number) {
    dError("failed to read %s, dnodeId not found", file);
    return 0;
  }
  cfg.dnodeId = dnodeId->valueint;

  cJSON *clusterId = cJSON_GetObjectItem(root, "clusterId");
  if (!clusterId || clusterId->type != cJSON_String) {
    dError("failed to read %s, clusterId not found", file);
    return 0;
  }
  tstrncpy(cfg.clusterId, clusterId->valuestring, TSDB_CLUSTER_ID_LEN);

  dInfo("read file %s successed", file);

  return 0;
}

static int32_t dnodeWriteCfg() {
  char file[TSDB_FILENAME_LEN + 20] = {0};
  sprintf(file, "%s/dnodeCfg.json", tsDnodeDir);

  FILE *fp = fopen(file, "w");
  if (!fp) {
    dError("failed to write %s, reason:%s", file, strerror(errno));
    return -1;
  }

  int32_t len = 0;
  int32_t maxLen = 200;
  char *  content = new char[maxLen + 1];

  len += snprintf(content + len, maxLen - len, "{\n");
  len += snprintf(content + len, maxLen - len, "  \"dnodeId\": %d,\n", tsCfg.dnodeId);
  len += snprintf(content + len, maxLen - len, "  \"clusterId\": \"%s\"\n", tsCfg.clusterId);
  len += snprintf(content + len, maxLen - len, "}\n");

  fwrite(content, 1, len, fp);
  fflush(fp);
  fclose(fp);
  delete [] content;
  terrno = 0;

  dInfo("successed to write %s", file);
  return 0;
}
