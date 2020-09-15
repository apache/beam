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

import javax.annotation.concurrent.ThreadSafe;
import org.apache.beam.sdk.transforms.splittabledofn.ManualWatermarkEstimator;
import org.apache.beam.sdk.transforms.splittabledofn.TimestampObservingWatermarkEstimator;
import org.apache.beam.sdk.transforms.splittabledofn.WatermarkEstimator;
import org.apache.beam.sdk.values.KV;
import org.joda.time.Instant;

/** Support utilties for interacting with {@link WatermarkEstimator}s. */
public class WatermarkEstimators {
  /** Interface which allows for accessing the current watermark and watermark estimator state. */
  public interface WatermarkAndStateObserver<WatermarkEstimatorStateT>
      extends WatermarkEstimator<WatermarkEstimatorStateT> {
    KV<Instant, WatermarkEstimatorStateT> getWatermarkAndState();
  }

  /**
   * Returns a thread safe {@link WatermarkEstimator} which allows getting a snapshot of the current
   * watermark and watermark estimator state.
   */
  public static <WatermarkEstimatorStateT>
      WatermarkAndStateObserver<WatermarkEstimatorStateT> threadSafe(
          WatermarkEstimator<WatermarkEstimatorStateT> watermarkEstimator) {
    if (watermarkEstimator instanceof TimestampObservingWatermarkEstimator) {
      return new ThreadSafeTimestampObservingWatermarkEstimator<>(watermarkEstimator);
    } else if (watermarkEstimator instanceof ManualWatermarkEstimator) {
      return new ThreadSafeManualWatermarkEstimator<>(watermarkEstimator);
    }
    return new ThreadSafeWatermarkEstimator<>(watermarkEstimator);
  }

  /**
   * Thread safe wrapper for {@link WatermarkEstimator}s that allows one to get a snapshot of the
   * current watermark and the watermark estimator state. \
   */
  @ThreadSafe
  private static class ThreadSafeWatermarkEstimator<WatermarkEstimatorStateT>
      implements WatermarkAndStateObserver<WatermarkEstimatorStateT> {
    protected final WatermarkEstimator<WatermarkEstimatorStateT> watermarkEstimator;

    ThreadSafeWatermarkEstimator(WatermarkEstimator<WatermarkEstimatorStateT> watermarkEstimator) {
      this.watermarkEstimator = watermarkEstimator;
    }

    @Override
    public synchronized Instant currentWatermark() {
      return watermarkEstimator.currentWatermark();
    }

    @Override
    public synchronized WatermarkEstimatorStateT getState() {
      return watermarkEstimator.getState();
    }

    @Override
    public synchronized KV<Instant, WatermarkEstimatorStateT> getWatermarkAndState() {
      // The order of these calls is important. We want to get the watermark and then its
      // associated state representation since state is not allowed to mutate the internal
      // representation.
      return KV.of(watermarkEstimator.currentWatermark(), watermarkEstimator.getState());
    }
  }

  /** Thread safe wrapper for {@link TimestampObservingWatermarkEstimator}s. */
  @ThreadSafe
  private static class ThreadSafeTimestampObservingWatermarkEstimator<WatermarkEstimatorStateT>
      extends ThreadSafeWatermarkEstimator<WatermarkEstimatorStateT>
      implements TimestampObservingWatermarkEstimator<WatermarkEstimatorStateT> {

    ThreadSafeTimestampObservingWatermarkEstimator(
        WatermarkEstimator<WatermarkEstimatorStateT> watermarkEstimator) {
      super(watermarkEstimator);
    }

    @Override
    public synchronized void observeTimestamp(Instant timestamp) {
      ((TimestampObservingWatermarkEstimator) watermarkEstimator).observeTimestamp(timestamp);
    }
  }

  /** Thread safe wrapper for {@link ManualWatermarkEstimator}s. */
  @ThreadSafe
  private static class ThreadSafeManualWatermarkEstimator<WatermarkEstimatorStateT>
      extends ThreadSafeWatermarkEstimator<WatermarkEstimatorStateT>
      implements ManualWatermarkEstimator<WatermarkEstimatorStateT> {

    ThreadSafeManualWatermarkEstimator(
        WatermarkEstimator<WatermarkEstimatorStateT> watermarkEstimator) {
      super(watermarkEstimator);
    }

    @Override
    public synchronized void setWatermark(Instant watermark) {
      ((ManualWatermarkEstimator) watermarkEstimator).setWatermark(watermark);
    }
  }
}
