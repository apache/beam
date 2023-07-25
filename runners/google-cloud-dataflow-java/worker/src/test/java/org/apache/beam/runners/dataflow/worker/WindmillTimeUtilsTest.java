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

import static org.apache.beam.runners.dataflow.worker.WindmillTimeUtils.harnessToWindmillTimestamp;
import static org.apache.beam.runners.dataflow.worker.WindmillTimeUtils.windmillToHarnessTimestamp;
import static org.apache.beam.runners.dataflow.worker.WindmillTimeUtils.windmillToHarnessWatermark;
import static org.junit.Assert.assertEquals;

import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Unit tests for {@link WindmillTimeUtils}. */
@RunWith(JUnit4.class)
public class WindmillTimeUtilsTest {
  @Test
  public void testWindmillToHarnessWatermark() {
    assertEquals(null, windmillToHarnessWatermark(Long.MIN_VALUE));
    assertEquals(BoundedWindow.TIMESTAMP_MAX_VALUE, windmillToHarnessWatermark(Long.MAX_VALUE));
    assertEquals(
        BoundedWindow.TIMESTAMP_MAX_VALUE, windmillToHarnessWatermark(Long.MAX_VALUE - 17));
    assertEquals(new Instant(16), windmillToHarnessWatermark(16999));
    assertEquals(new Instant(17), windmillToHarnessWatermark(17120));
    assertEquals(new Instant(17), windmillToHarnessWatermark(17000));
    assertEquals(new Instant(-17), windmillToHarnessWatermark(-16987));
    assertEquals(new Instant(-17), windmillToHarnessWatermark(-17000));
    assertEquals(new Instant(-18), windmillToHarnessTimestamp(-17001));
  }

  @Test
  public void testWindmillToHarnessTimestamp() {
    assertEquals(BoundedWindow.TIMESTAMP_MAX_VALUE, windmillToHarnessTimestamp(Long.MAX_VALUE));
    assertEquals(
        BoundedWindow.TIMESTAMP_MAX_VALUE, windmillToHarnessTimestamp(Long.MAX_VALUE - 17));
    assertEquals(new Instant(16), windmillToHarnessWatermark(16999));
    assertEquals(new Instant(17), windmillToHarnessTimestamp(17120));
    assertEquals(new Instant(17), windmillToHarnessTimestamp(17000));
    assertEquals(new Instant(-17), windmillToHarnessTimestamp(-16987));
    assertEquals(new Instant(-17), windmillToHarnessTimestamp(-17000));
    assertEquals(new Instant(-18), windmillToHarnessTimestamp(-17001));
    assertEquals(BoundedWindow.TIMESTAMP_MIN_VALUE, windmillToHarnessTimestamp(Long.MIN_VALUE + 1));
    assertEquals(BoundedWindow.TIMESTAMP_MIN_VALUE, windmillToHarnessTimestamp(Long.MIN_VALUE + 2));
    // Long.MIN_VALUE = -9223372036854775808, need to add 1808 microseconds to get to next
    // millisecond returned by Beam.
    assertEquals(
        BoundedWindow.TIMESTAMP_MIN_VALUE.plus(Duration.millis(1)),
        windmillToHarnessTimestamp(Long.MIN_VALUE + 1808));
    assertEquals(
        BoundedWindow.TIMESTAMP_MIN_VALUE, windmillToHarnessTimestamp(Long.MIN_VALUE + 1807));
  }

  @Test
  public void testHarnessToWindmillTimestamp() {
    assertEquals(Long.MAX_VALUE, harnessToWindmillTimestamp(BoundedWindow.TIMESTAMP_MAX_VALUE));
    assertEquals(-1000, harnessToWindmillTimestamp(new Instant(-1)));
    assertEquals(1000, harnessToWindmillTimestamp(new Instant(1)));
    assertEquals(Long.MIN_VALUE + 1, harnessToWindmillTimestamp(new Instant(Long.MIN_VALUE)));
    assertEquals(
        Long.MIN_VALUE + 1, harnessToWindmillTimestamp(new Instant(Long.MIN_VALUE / 1000 - 1)));
  }
}
