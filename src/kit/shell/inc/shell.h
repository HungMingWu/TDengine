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

#ifndef __SHELL__
#define __SHELL__

#include "taos.h"
#include "taosdef.h"
#include "tsclient.h"

#define MAX_USERNAME_SIZE      64
#define MAX_DBNAME_SIZE        64
#define MAX_IP_SIZE            20
#define MAX_PASSWORD_SIZE      20
#define MAX_HISTORY_SIZE       1000
#define MAX_COMMAND_SIZE       65536
#define HISTORY_FILE           ".taos_history"

typedef struct SShellHistory {
  char* hist[MAX_HISTORY_SIZE];
  int   hstart;
  int   hend;
} SShellHistory;

struct SShellArguments {
  char* host = nullptr;
  char* password = nullptr;
  char* user = nullptr;
  char* auth;
  char* database = nullptr;
  char* timezone = nullptr;
  bool  is_raw_time = false;
  bool  is_use_passwd = false;
  bool  dump_config = false;
  char  file[TSDB_FILENAME_LEN] = {'\0'};
  char  dir[TSDB_FILENAME_LEN] = {'\0'};
  int   threadNum = 5;
  char* commands = nullptr;
  int   abort;
  int   port;
  int   pktLen = 1000;
  char* netTestRole = nullptr;
};

/**************** Function declarations ****************/
extern void shellParseArgument(int argc, char* argv[], SShellArguments* arguments);
extern TAOS* shellInit(SShellArguments* args);
extern void* shellLoopQuery(void* arg);
extern void taos_error(TAOS_RES* tres, int64_t st);
extern int regex_match(const char* s, const char* reg, int cflags);
void shellReadCommand(TAOS* con, char command[]);
int32_t shellRunCommand(TAOS* con, char* command);
void shellRunCommandOnServer(TAOS* con, char command[]);
void read_history();
void write_history();
void source_file(TAOS* con, char* fptr);
void source_dir(TAOS* con, SShellArguments* args);
void get_history_path(char* history);
void cleanup_handler(void* arg);
void exitShell();
int shellDumpResult(TAOS_RES* con, char* fname, int* error_no, bool printMode);
void shellGetGrantInfo(void *con);
int isCommentLine(char *line);

/**************** Global variable declarations ****************/
extern char           PROMPT_HEADER[];
extern char           CONTINUE_PROMPT[];
extern int            prompt_size;
extern SShellHistory  history;
extern struct termios oldtio;
extern void           set_terminal_mode();
extern int get_old_terminal_mode(struct termios* tio);
extern void            reset_terminal_mode();
extern SShellArguments args;
extern int64_t         result;

#endif
