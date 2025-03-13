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
package org.apache.beam.runners.dataflow.worker.windmill.client.throttling;

import org.joda.time.Instant;

/**
 * A stopwatch used to track the amount of time spent throttled due to Resource Exhausted errors.
 * Throttle time is cumulative for all three rpcs types but not for all streams. So if GetWork and
 * CommitWork are both blocked for x, totalTime will be 2x. However, if 2 GetWork streams are both
 * blocked for x totalTime will be x. All methods are thread safe.
 */
public final class ThrottleTimer implements ThrottledTimeTracker {
  // This is -1 if not currently being throttled or the time in
  // milliseconds when throttling for this type started.
  private long startTime = -1;
  // This is the collected total throttle times since the last poll.  Throttle times are
  // reported as a delta so this is cleared whenever it gets reported.
  private long totalTime = 0;

  /**
   * Starts the timer if it has not been started and does nothing if it has already been started.
   */
  public synchronized void start() {
    if (!throttled()) { // This timer is not started yet so start it now.
      startTime = Instant.now().getMillis();
    }
  }

  /** Stops the timer if it has been started and does nothing if it has not been started. */
  public synchronized void stop() {
    if (throttled()) { // This timer has been started already so stop it now.
      totalTime += Instant.now().getMillis() - startTime;
      startTime = -1;
    }
  }

  /** Returns if the specified type is currently being throttled. */
  public synchronized boolean throttled() {
    return startTime != -1;
  }

  /** Returns the combined total of all throttle times and resets those times to 0. */
  @Override
  public synchronized long getAndResetThrottleTime() {
    if (throttled()) {
      stop();
      start();
    }
    long toReturn = totalTime;
    totalTime = 0;
    return toReturn;
  }
}
