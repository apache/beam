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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.google.api.client.testing.http.FixedClock;
import com.google.api.client.util.Clock;
import org.apache.beam.sdk.testing.ExpectedLogs;
import org.joda.time.Duration;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.slf4j.LoggerFactory;

@RunWith(PowerMockRunner.class)
@PrepareForTest({HotKeyLoggerTest.class, LoggerFactory.class})
public class HotKeyLoggerTest {
  @Rule public ExpectedLogs expectedLogs = ExpectedLogs.none(HotKeyLogger.class);

  private FixedClock clock;

  @Before
  public void SetUp() {
    clock = new FixedClock(Clock.SYSTEM.currentTimeMillis());
  }

  @Test
  public void correctHotKeyMessageWithoutKey() {
    HotKeyLogger hotKeyLogger = new HotKeyLogger(clock);

    hotKeyLogger.logHotKeyDetection("TEST_STEP_ID", Duration.standardSeconds(1));
    assertTrue(hotKeyLogger.isThrottled());

    expectedLogs.verifyWarn(
        "A hot key was detected in step 'TEST_STEP_ID' with age of '1s'. This is a "
            + "symptom of key distribution being skewed. To fix, please inspect your data and "
            + "pipeline to ensure that elements are evenly distributed across your key space. If "
            + "you want to log the plain-text key to Cloud Logging please re-run with the "
            + "`hotKeyLoggingEnabled` pipeline option. See "
            + "https://cloud.google.com/dataflow/docs/guides/specifying-exec-params for more "
            + "information.");
  }

  @Test
  public void correctHotKeyMessageWithKey() {
    HotKeyLogger hotKeyLogger = new HotKeyLogger(clock);

    hotKeyLogger.logHotKeyDetection("TEST_STEP_ID", Duration.standardSeconds(1), "my key");
    assertTrue(hotKeyLogger.isThrottled());

    expectedLogs.verifyWarn(
        "A hot key 'my key' was detected in step 'TEST_STEP_ID' with age of '1s'. This is a "
            + "symptom of key distribution being skewed. To fix, please inspect your data and "
            + "pipeline to ensure that elements are evenly distributed across your key space.");
  }

  @Test
  public void correctHotKeyMessageWithNullKey() {
    HotKeyLogger hotKeyLogger = new HotKeyLogger(clock);

    hotKeyLogger.logHotKeyDetection("TEST_STEP_ID", Duration.standardSeconds(1), null);
    assertTrue(hotKeyLogger.isThrottled());

    expectedLogs.verifyWarn(
        "A hot key 'null' was detected in step 'TEST_STEP_ID' with age of '1s'. This is a "
            + "symptom of key distribution being skewed. To fix, please inspect your data and "
            + "pipeline to ensure that elements are evenly distributed across your key space.");
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
}
