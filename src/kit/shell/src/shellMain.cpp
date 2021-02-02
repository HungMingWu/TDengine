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
#include "shell.h"
#include "tconfig.h"
#include "tnettest.h"

pthread_t pid;
static tsem_t cancelSem;

void shellQueryInterruptHandler(int signum) {
  tsem_post(&cancelSem);
}

void *cancelHandler(void *arg) {
  while(1) {
    if (tsem_wait(&cancelSem) != 0) {
      taosMsleep(10);
      continue;
    }

#ifdef LINUX
    int64_t rid = atomic_val_compare_exchange_64(&result, result, 0);
    SSqlObj* pSql = (SSqlObj*)taosAcquireRef(tscObjRef, rid);
    taos_stop_query(pSql);
    taosReleaseRef(tscObjRef, rid);
#else
    printf("\nReceive ctrl+c or other signal, quit shell.\n");
    exit(0);
#endif
  }
  
  return NULL;
}

// Global configurations
SShellArguments args;
/*
 * Main function.
 */
int main(int argc, char* argv[]) {
  /*setlocale(LC_ALL, "en_US.UTF-8"); */

  shellParseArgument(argc, argv, &args);

  if (args.dump_config) {
    taosInitGlobalCfg();
    taosReadGlobalLogCfg();

    if (!taosReadGlobalCfg()) {
      printf("TDengine read global config failed");
      exit(EXIT_FAILURE);
    }

    taosDumpGlobalCfg();
    exit(0);
  }

  if (args.netTestRole && args.netTestRole[0] != 0) {
    taos_init();
    taosNetTest(args.netTestRole, args.host, args.port, args.pktLen);
    exit(0);
  }

  /* Initialize the shell */
  TAOS* con = shellInit(&args);
  if (con == NULL) {
    exit(EXIT_FAILURE);
  }

  if (tsem_init(&cancelSem, 0, 0) != 0) {
    printf("failed to create cancel semphore\n");
    exit(EXIT_FAILURE);
  }

  pthread_t spid;
  pthread_create(&spid, NULL, cancelHandler, NULL);

  /* Interrupt handler. */
  struct sigaction act;
  memset(&act, 0, sizeof(struct sigaction));
  
  act.sa_handler = shellQueryInterruptHandler;
  sigaction(SIGTERM, &act, NULL);
  sigaction(SIGINT, &act, NULL);

  /* Get grant information */
  shellGetGrantInfo(con);

  /* Loop to query the input. */
  while (1) {
    pthread_create(&pid, NULL, shellLoopQuery, con);
    pthread_join(pid, NULL);
  }
}
