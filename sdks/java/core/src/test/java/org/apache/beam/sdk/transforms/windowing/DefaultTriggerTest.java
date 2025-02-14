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
package org.apache.beam.sdk.transforms.windowing;

import static org.junit.Assert.assertEquals;

import org.joda.time.Instant;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Tests the {@link DefaultTrigger}, which should be equivalent to {@code
 * Repeatedly.forever(AfterWatermark.pastEndOfWindow())}.
 */
@RunWith(JUnit4.class)
public class DefaultTriggerTest {

  @Test
  public void testFireDeadline() throws Exception {
    assertEquals(
        new Instant(9),
        DefaultTrigger.of()
            .getWatermarkThatGuaranteesFiring(new IntervalWindow(new Instant(0), new Instant(10))));
    assertEquals(
        GlobalWindow.INSTANCE.maxTimestamp(),
        DefaultTrigger.of().getWatermarkThatGuaranteesFiring(GlobalWindow.INSTANCE));
  }

  @Test
  public void testContinuation() throws Exception {
    assertEquals(DefaultTrigger.of(), DefaultTrigger.of().getContinuationTrigger());
  }
}
