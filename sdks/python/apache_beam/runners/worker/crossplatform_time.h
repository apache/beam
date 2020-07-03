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
 * ignored, and function always act as for CLOCK_MONOTONIC. Windows performance counter is used.
 */
int clock_gettime(int clk_id, struct timespec *tv) {
    static LARGE_INTEGER counterFrequency = {0};
    LARGE_INTEGER counterValue;

    if (0 == counterFrequency.QuadPart) {
        if (0 == QueryPerformanceFrequency(&counterFrequency)) {
            /* System doesn't support performance counters. It's guaranteed to not happen
            on systems that run Windows XP or later */
            return -1;
        }
    }
    if (0 == QueryPerformanceCounter(&counterValue)){
        /* Again, it may only fail on systems before Windows XP */
        return -1;
    }

    tv->tv_sec = counterValue.QuadPart / counterFrequency.QuadPart;
    #pragma warning( suppress : 4244 ) // nanoseconds may not exceed billion, therefore it's safe to cast
    tv->tv_nsec = ((counterValue.QuadPart % counterFrequency.QuadPart) * 1000000000) / counterFrequency.QuadPart;

    return 0;
}
#endif

#endif //BEAM_CROSSPLATFORM_TIME_H
