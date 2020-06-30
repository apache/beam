#ifndef BEAM_CROSSPLATFORM_UNISTD_H
#define BEAM_CROSSPLATFORM_UNISTD_H

#ifdef _WIN32
#include <windows.h>

/**
 * Alternative to POSIX usleep that may be run on Windows platform.
 */
void usleep(__int64 usec) {
    HANDLE timer;
    LARGE_INTEGER timerDueTime;

    // Timer is measuring in 100 of nanoseconds, negative means relative
    timerDueTime.QuadPart = -(10 * usec);

    timer = CreateWaitableTimer(NULL, TRUE, NULL);
    SetWaitableTimer(timer, &timerDueTime, 0, NULL, NULL, 0);
    WaitForSingleObject(timer, INFINITE);
    CloseHandle(timer);
}

#else
#include "unistd.h"
#endif

#endif //BEAM_CROSSPLATFORM_UNISTD_H
