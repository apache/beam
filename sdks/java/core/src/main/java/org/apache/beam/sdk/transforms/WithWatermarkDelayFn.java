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
package org.apache.beam.sdk.transforms;

import java.util.Arrays;
import java.util.Collections;
import javax.annotation.Nullable;
import org.apache.beam.sdk.transforms.DoFn.BoundedPerElement;
import org.apache.beam.sdk.transforms.splittabledofn.ManualWatermarkEstimator;
import org.apache.beam.sdk.transforms.splittabledofn.RestrictionTracker;
import org.apache.beam.sdk.transforms.splittabledofn.SplitResult;
import org.apache.beam.sdk.transforms.splittabledofn.WatermarkEstimators;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.joda.time.Duration;
import org.joda.time.Instant;

@BoundedPerElement
class WithWatermarkDelayFn<T> extends DoFn<T, T> {

  private final SerializableFunction<T, Instant> fn;
  private final Duration watermarkDelay;

  WithWatermarkDelayFn(SerializableFunction<T, Instant> fn, Duration watermarkDelay) {
    this.fn = fn;
    this.watermarkDelay = watermarkDelay;
  }

  @ProcessElement
  public void processElement(
      @Element T input,
      @Timestamp Instant inputTimestamp,
      RestrictionTracker<Integer, Integer> restrictionTracker,
      OutputReceiver<T> output,
      ManualWatermarkEstimator<Instant> estimator) {
    Instant ts = fn.apply(input);
    output.outputWithTimestamp(input, ts);
    Instant watermark = ts.minus(watermarkDelay);
    // Don't advance the watermark past the input element timestamp.
    watermark = Collections.min(Arrays.asList(watermark, inputTimestamp));
    if (watermark.isAfter(estimator.currentWatermark())) {
      estimator.setWatermark(watermark);
    }
  }

  @GetInitialRestriction
  public Integer getInitialRestriction() {
    return 0;
  }

  @NewTracker
  public RestrictionTracker<Integer, Integer> newTracker() {
    // This is a noop restriction tracker that does nothing.
    return new RestrictionTracker<Integer, Integer>() {

      @Override
      public boolean tryClaim(Integer newPosition) {
        return true;
      }

      @Override
      public Integer currentRestriction() {
        return 0;
      }

      @Override
      public @Nullable SplitResult<Integer> trySplit(double fractionOfRemainder) {
        return null;
      }

      @Override
      public void checkDone() {}

      @Override
      public IsBounded isBounded() {
        return IsBounded.BOUNDED;
      }
    };
  }

  @GetInitialWatermarkEstimatorState
  public Instant getInitialWatermarkEstimatorState() {
    return BoundedWindow.TIMESTAMP_MIN_VALUE;
  }

  @NewWatermarkEstimator
  public ManualWatermarkEstimator<Instant> newWatermarkEstimator(
      @WatermarkEstimatorState Instant state) {
    return new WatermarkEstimators.Manual(state);
  }
}
