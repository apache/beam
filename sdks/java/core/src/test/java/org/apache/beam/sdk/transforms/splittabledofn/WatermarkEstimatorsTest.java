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
package org.apache.beam.sdk.transforms.splittabledofn;

import static org.junit.Assert.assertEquals;

import org.apache.beam.sdk.testing.ResetDateTimeProvider;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.joda.time.DateTimeUtils;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class WatermarkEstimatorsTest {
  public @Rule ResetDateTimeProvider resetDateTimeProvider = new ResetDateTimeProvider();

  @Test
  public void testManualWatermarkEstimator() throws Exception {
    ManualWatermarkEstimator<Instant> watermarkEstimator =
        new WatermarkEstimators.Manual(GlobalWindow.TIMESTAMP_MIN_VALUE);
    assertEquals(GlobalWindow.TIMESTAMP_MIN_VALUE, watermarkEstimator.currentWatermark());
    watermarkEstimator.setWatermark(GlobalWindow.TIMESTAMP_MIN_VALUE);
    watermarkEstimator.setWatermark(
        GlobalWindow.TIMESTAMP_MIN_VALUE.plus(Duration.standardHours(2)));
    assertEquals(
        GlobalWindow.TIMESTAMP_MIN_VALUE.plus(Duration.standardHours(2)),
        watermarkEstimator.currentWatermark());

    // Make sure that even if the watermark goes backwards we report the "greatest" value we have
    // reported so far.
    watermarkEstimator.setWatermark(
        GlobalWindow.TIMESTAMP_MIN_VALUE.plus(Duration.standardHours(1)));
    assertEquals(
        GlobalWindow.TIMESTAMP_MIN_VALUE.plus(Duration.standardHours(2)),
        watermarkEstimator.currentWatermark());

    watermarkEstimator.setWatermark(GlobalWindow.TIMESTAMP_MAX_VALUE);
    assertEquals(GlobalWindow.TIMESTAMP_MAX_VALUE, watermarkEstimator.currentWatermark());
  }

  @Test
  public void testWallTimeWatermarkEstimator() throws Exception {
    DateTimeUtils.setCurrentMillisFixed(GlobalWindow.TIMESTAMP_MIN_VALUE.getMillis());
    WatermarkEstimator<Instant> watermarkEstimator =
        new WatermarkEstimators.WallTime(new Instant());
    DateTimeUtils.setCurrentMillisFixed(GlobalWindow.TIMESTAMP_MIN_VALUE.plus(1).getMillis());
    assertEquals(GlobalWindow.TIMESTAMP_MIN_VALUE.plus(1), watermarkEstimator.currentWatermark());

    DateTimeUtils.setCurrentMillisFixed(GlobalWindow.TIMESTAMP_MIN_VALUE.plus(2).getMillis());
    // Make sure that we don't mutate state even if the clock advanced
    assertEquals(GlobalWindow.TIMESTAMP_MIN_VALUE.plus(1), watermarkEstimator.getState());
    assertEquals(GlobalWindow.TIMESTAMP_MIN_VALUE.plus(2), watermarkEstimator.currentWatermark());
    assertEquals(GlobalWindow.TIMESTAMP_MIN_VALUE.plus(2), watermarkEstimator.getState());

    // Handle the case if the clock ever goes backwards. Could happen if we resumed processing
    // on a machine that had misconfigured clock or due to clock skew.
    DateTimeUtils.setCurrentMillisFixed(GlobalWindow.TIMESTAMP_MIN_VALUE.plus(1).getMillis());
    assertEquals(GlobalWindow.TIMESTAMP_MIN_VALUE.plus(2), watermarkEstimator.currentWatermark());
  }

  @Test
  public void testMonotonicallyIncreasingWatermarkEstimator() throws Exception {
    TimestampObservingWatermarkEstimator<Instant> watermarkEstimator =
        new WatermarkEstimators.MonotonicallyIncreasing(GlobalWindow.TIMESTAMP_MIN_VALUE);
    assertEquals(GlobalWindow.TIMESTAMP_MIN_VALUE, watermarkEstimator.currentWatermark());
    watermarkEstimator.observeTimestamp(GlobalWindow.TIMESTAMP_MIN_VALUE);
    watermarkEstimator.observeTimestamp(
        GlobalWindow.TIMESTAMP_MIN_VALUE.plus(Duration.standardHours(2)));
    assertEquals(
        GlobalWindow.TIMESTAMP_MIN_VALUE.plus(Duration.standardHours(2)),
        watermarkEstimator.currentWatermark());

    // Make sure that even if the watermark goes backwards we report the "greatest" value we have
    // reported so far.
    watermarkEstimator.observeTimestamp(
        GlobalWindow.TIMESTAMP_MIN_VALUE.plus(Duration.standardHours(1)));
    assertEquals(
        GlobalWindow.TIMESTAMP_MIN_VALUE.plus(Duration.standardHours(2)),
        watermarkEstimator.currentWatermark());

    watermarkEstimator.observeTimestamp(GlobalWindow.TIMESTAMP_MAX_VALUE);
    assertEquals(GlobalWindow.TIMESTAMP_MAX_VALUE, watermarkEstimator.currentWatermark());
  }
}
