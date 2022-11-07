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
package org.apache.beam.sdk.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.concurrent.TimeUnit;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link FastNanoClockAndSleeper}. */
@RunWith(JUnit4.class)
public class FastNanoClockAndSleeperTest {
  @Rule public FastNanoClockAndSleeper fastNanoClockAndSleeper = new FastNanoClockAndSleeper();

  @Test
  public void testClockAndSleeper() throws Exception {
    long sleepTimeMs = TimeUnit.SECONDS.toMillis(30);
    long sleepTimeNano = TimeUnit.MILLISECONDS.toNanos(sleepTimeMs);
    long fakeTimeNano = fastNanoClockAndSleeper.nanoTime();
    long startTimeNano = System.nanoTime();
    fastNanoClockAndSleeper.sleep(sleepTimeMs);
    long maxTimeNano = startTimeNano + TimeUnit.SECONDS.toNanos(1);
    // Verify that actual time didn't progress as much as was requested
    assertTrue(System.nanoTime() < maxTimeNano);
    // Verify that the fake time did go up by the amount requested
    assertEquals(fakeTimeNano + sleepTimeNano, fastNanoClockAndSleeper.nanoTime());
  }
}
