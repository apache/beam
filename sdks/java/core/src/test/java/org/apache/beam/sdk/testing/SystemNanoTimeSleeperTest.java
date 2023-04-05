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
package org.apache.beam.sdk.testing;

import static org.apache.beam.sdk.testing.SystemNanoTimeSleeper.sleepMillis;
import static org.junit.Assert.assertTrue;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link SystemNanoTimeSleeper}. */
@RunWith(JUnit4.class)
public class SystemNanoTimeSleeperTest {
  @Test
  public void testSleep() throws Exception {
    long startTime = System.nanoTime();
    sleepMillis(100);
    long endTime = System.nanoTime();
    assertTrue(endTime - startTime >= 100);
  }

  @Test
  public void testNegativeSleep() throws Exception {
    sleepMillis(-100);
  }

  @Test(expected = InterruptedException.class)
  public void testInterruptionInLoop() throws Exception {
    Thread.currentThread().interrupt();
    sleepMillis(0);
  }

  @Test(expected = InterruptedException.class)
  public void testInterruptionOutsideOfLoop() throws Exception {
    Thread.currentThread().interrupt();
    sleepMillis(-100);
  }
}
