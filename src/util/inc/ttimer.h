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

#ifndef TDENGINE_TTIMER_H
#define TDENGINE_TTIMER_H

#include <functional>

typedef void *tmr_h;
using TAOS_TMR_CALLBACK = std::function<void (void *)>;

extern int      taosTmrThreads;
extern uint32_t tsMaxTmrCtrl;

#define MSECONDS_PER_TICK 5

void *taosTmrInit(int maxTmr, int resoultion, int longest, const char *label);

tmr_h taosTmrStart(TAOS_TMR_CALLBACK fp, int mseconds, void *handle);

bool taosTmrStop(tmr_h tmrId);

bool taosTmrStopA(tmr_h *timerId);

bool taosTmrReset(TAOS_TMR_CALLBACK fp, int mseconds, void *handle, tmr_h *pTmrId);

void taosTmrCleanUp(void *handle);

int32_t taosInitTimer(void (*callback)(int), int32_t ms);

void taosUninitTimer();

#endif  // TDENGINE_TTIMER_H
