/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#ifndef BEAM_CROSSPLATFORM_TIME_H
#define BEAM_CROSSPLATFORM_TIME_H

#include <time.h>

#ifdef _WIN32
#include <windows.h>

/**
 * Alternative to POSIX clock_gettime that may be run on Windows platform. The clk_id parameter is
 * ignored and function act as for CLOCK_MONOTONIC. On Windows XP or later Performance Counter is used.
 * On older platforms, where there's no Performance Counter, SystemTime will be used as a failover.
 */
int clock_gettime(int clk_id, struct timespec *tv) {
    static LARGE_INTEGER counterFrequency = {0};
    static BOOL performanceCounterAvailable = TRUE;
    LARGE_INTEGER counterValue = {0};

    //initialization
    if (0 == counterFrequency.QuadPart && performanceCounterAvailable) {
        if (0 == QueryPerformanceFrequency(&counterFrequency)) {
            performanceCounterAvailable = FALSE;
            counterFrequency.QuadPart = 10000000; // failover SystemTime is provided in 100-nanosecond intervals
        }
    }

    if (performanceCounterAvailable) {
        QueryPerformanceCounter(&counterValue);
    }
    else {
        FILETIME filetime = {0};
        GetSystemTimeAsFileTime(&filetime);
        counterValue.QuadPart = filetime.dwHighDateTime;
        counterValue.QuadPart <<= 32;
        counterValue.QuadPart |= filetime.dwLowDateTime;
    }
    tv->tv_sec = counterValue.QuadPart / counterFrequency.QuadPart;
    #pragma warning( suppress : 4244 ) // nanoseconds may not exceed billion, therefore it's safe to cast
    tv->tv_nsec = ((counterValue.QuadPart % counterFrequency.QuadPart) * 1000000000) / counterFrequency.QuadPart;

    return 0;
}
#endif

#endif //BEAM_CROSSPLATFORM_TIME_H
