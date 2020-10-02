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
package org.apache.beam.runners.dataflow.worker;

import com.google.api.client.util.Clock;
import java.text.MessageFormat;
import org.apache.beam.runners.dataflow.util.TimeUtil;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HotKeyLogger {
  private final Logger LOG = LoggerFactory.getLogger(HotKeyLogger.class);

  /** Clock used to either provide real system time or mocked to virtualize time for testing. */
  private Clock clock = Clock.SYSTEM;

  /**
   * The previous time the HotKeyDetection was logged. This is used to throttle logging to every 5
   * minutes.
   */
  private long prevHotKeyDetectionLogMs = 0;

  /** Throttles logging the detection to every loggingPeriod */
  private final Duration loggingPeriod = Duration.standardMinutes(5);

  HotKeyLogger() {}

  HotKeyLogger(Clock clock) {
    this.clock = clock;
  }

  /** Logs a detection of the hot key every 5 minutes. */
  public void logHotKeyDetection(String userStepName, Duration hotKeyAge) {
    if (isThrottled()) {
      return;
    }
    LOG.warn(getHotKeyMessage(userStepName, TimeUtil.toCloudDuration(hotKeyAge)));
  }

  /**
   * Returns true if the class should log the HotKeyMessage. This method throttles logging to every
   * 5 minutes.
   */
  protected boolean isThrottled() {
    // Throttle logging the HotKeyDetection to every 5 minutes.
    long nowMs = clock.currentTimeMillis();
    if (nowMs - prevHotKeyDetectionLogMs < loggingPeriod.getMillis()) {
      return true;
    }
    prevHotKeyDetectionLogMs = nowMs;
    return false;
  }

  protected String getHotKeyMessage(String userStepName, String hotKeyAge) {
    return MessageFormat.format(
        "A hot key was detected in step ''{0}'' with age of ''{1}''. This is"
            + " a symptom of key distribution being skewed. To fix, please inspect your data and "
            + "pipeline to ensure that elements are evenly distributed across your key space.",
        userStepName, hotKeyAge);
  }
}
