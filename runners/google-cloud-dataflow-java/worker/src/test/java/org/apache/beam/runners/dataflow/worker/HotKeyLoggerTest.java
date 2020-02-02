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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.verify;
import static org.powermock.api.mockito.PowerMockito.mock;
import static org.powermock.api.mockito.PowerMockito.mockStatic;
import static org.powermock.api.mockito.PowerMockito.when;

import com.google.api.client.testing.http.FixedClock;
import com.google.api.client.util.Clock;
import org.joda.time.Duration;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@RunWith(PowerMockRunner.class)
@PrepareForTest({HotKeyLoggerTest.class, LoggerFactory.class})
public class HotKeyLoggerTest {
  private FixedClock clock;

  @Before
  public void SetUp() {
    clock = new FixedClock(Clock.SYSTEM.currentTimeMillis());
  }

  @Test
  public void correctHotKeyMessage() {
    HotKeyLogger hotKeyLogger = new HotKeyLogger(clock);

    assertFalse(hotKeyLogger.isThrottled());
    String m = hotKeyLogger.getHotKeyMessage("TEST_STEP_ID", "1s");
    assertTrue(hotKeyLogger.isThrottled());

    assertEquals(
        "A hot key was detected in step 'TEST_STEP_ID' with age of '1s'. This is a "
            + "symptom of key distribution being skewed. To fix, please inspect your data and "
            + "pipeline to ensure that elements are evenly distributed across your key space.",
        m);
  }

  @Test
  public void throttlesLoggingHotKeyMessage() {
    HotKeyLogger hotKeyLogger = new HotKeyLogger(clock);

    clock.setTime(Clock.SYSTEM.currentTimeMillis());
    assertFalse(hotKeyLogger.isThrottled());
    assertTrue(hotKeyLogger.isThrottled());

    // The class throttles every 5 minutes, so the first time it is called is true. The second time
    // is throttled and returns false.
    clock.setTime(clock.currentTimeMillis() + Duration.standardMinutes(5L).getMillis());
    assertFalse(hotKeyLogger.isThrottled());
    assertTrue(hotKeyLogger.isThrottled());

    // Test that the state variable is set and can log again in 5 minutes.
    clock.setTime(clock.currentTimeMillis() + Duration.standardMinutes(5L).getMillis());
    assertFalse(hotKeyLogger.isThrottled());
    assertTrue(hotKeyLogger.isThrottled());
  }

  @Test
  public void logsHotKeyMessage() {
    mockStatic(LoggerFactory.class);
    Logger logger = mock(Logger.class);
    when(LoggerFactory.getLogger(any(Class.class))).thenReturn(logger);

    HotKeyLogger hotKeyLogger = new HotKeyLogger(clock);

    // Logs because not throttled.
    hotKeyLogger.logHotKeyDetection("TEST_STEP_ID", Duration.standardHours(1));

    // Does not log because throttled.
    hotKeyLogger.logHotKeyDetection("TEST_STEP_ID", Duration.standardHours(1));

    // Only logs once because of throttling.
    verify(logger, Mockito.times(1)).warn(anyString());
  }
}
