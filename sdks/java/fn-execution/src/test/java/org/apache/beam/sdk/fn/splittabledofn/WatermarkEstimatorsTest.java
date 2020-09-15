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
package org.apache.beam.sdk.fn.splittabledofn;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import java.util.concurrent.CountDownLatch;
import java.util.function.Consumer;
import org.apache.beam.sdk.transforms.splittabledofn.TimestampObservingWatermarkEstimator;
import org.apache.beam.sdk.transforms.splittabledofn.WatermarkEstimator;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.values.KV;
import org.joda.time.Instant;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link WatermarkEstimators}. */
@RunWith(JUnit4.class)
public class WatermarkEstimatorsTest {
  @Test
  public void testThreadSafeWatermarkEstimator() throws Exception {
    Instant[] reference = new Instant[] {GlobalWindow.TIMESTAMP_MIN_VALUE};
    WatermarkEstimator<Instant> watermarkEstimator =
        new WatermarkEstimator<Instant>() {
          private Instant currentWatermark = GlobalWindow.TIMESTAMP_MIN_VALUE;

          @Override
          public Instant currentWatermark() {
            currentWatermark = reference[0];
            return currentWatermark;
          }

          @Override
          public Instant getState() {
            return currentWatermark;
          }
        };
    testWatermarkEstimatorSnapshotsStateWithCompetingThread(
        WatermarkEstimators.threadSafe(watermarkEstimator), (instant) -> reference[0] = instant);
  }

  @Test
  public void testThreadSafeTimestampObservingWatermarkEstimator() throws Exception {
    WatermarkEstimators.WatermarkAndStateObserver<Instant> threadsafeWatermarkEstimator =
        WatermarkEstimators.threadSafe(
            new org.apache.beam.sdk.transforms.splittabledofn.WatermarkEstimators
                .MonotonicallyIncreasing(GlobalWindow.TIMESTAMP_MIN_VALUE));
    testWatermarkEstimatorSnapshotsStateWithCompetingThread(
        threadsafeWatermarkEstimator,
        ((TimestampObservingWatermarkEstimator) threadsafeWatermarkEstimator)::observeTimestamp);
  }

  public <WatermarkEstimatorT extends WatermarkEstimators.WatermarkAndStateObserver<Instant>>
      void testWatermarkEstimatorSnapshotsStateWithCompetingThread(
          WatermarkEstimatorT watermarkEstimator, Consumer<Instant> watermarkUpdater)
          throws Exception {
    CountDownLatch countDownLatch = new CountDownLatch(1);
    Thread t =
        new Thread(
            () -> {
              countDownLatch.countDown();
              for (int i = 0; i < 1000; ++i) {
                watermarkUpdater.accept(GlobalWindow.TIMESTAMP_MIN_VALUE.plus(i));
              }
            });
    t.start();

    // Ensure the thread has started before we start fetching values.
    countDownLatch.await();
    Instant currentMinimum = GlobalWindow.TIMESTAMP_MIN_VALUE;
    for (int i = 0; i < 100; ++i) {
      KV<Instant, Instant> value = watermarkEstimator.getWatermarkAndState();
      // The watermark estimators that we use ensure that state == current watermark so test that
      // they are equal here
      assertEquals(value.getKey(), value.getValue());
      // Also ensure that the watermark is not declining and somehow we are getting an "old" read
      assertFalse(currentMinimum.isAfter(value.getKey()));
    }

    t.join(10000);
  }
}
