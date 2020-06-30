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
