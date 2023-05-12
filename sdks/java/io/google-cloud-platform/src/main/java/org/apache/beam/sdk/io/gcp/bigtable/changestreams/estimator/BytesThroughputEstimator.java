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
package org.apache.beam.sdk.io.gcp.bigtable.changestreams.estimator;

import java.math.BigDecimal;
import java.math.RoundingMode;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.annotations.VisibleForTesting;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Duration;
import org.joda.time.Instant;

/**
 * An estimator to provide an estimate on the byte throughput of the outputted elements.
 *
 * <p>This estimator will keep track of the bytes of reported records within a sliding window. The
 * window consists of the configured number of seconds and each record's bytes will fall into
 * exactly one given second bucket. When more than window size seconds have passed from the current
 * time, the bytes reported for the seconds that fall outside of the window will not be considered
 * anymore. The bytes of the records will be estimated using the configured {@link
 * org.apache.beam.sdk.coders.Coder}.
 *
 * <p>The estimator will sample only 2% of records to save the cost of doing sizeEstimation for
 * every element
 */
@Internal
public class BytesThroughputEstimator<T> {

  private static final long serialVersionUID = 6014227751984587954L;
  private static final int DEFAULT_SAMPLE_RATE = 50;
  private final SizeEstimator<T> sizeEstimator;
  private final int sampleRate;
  private long elementCount;
  private BigDecimal currentElementSizeEstimate;
  private final Instant startTimestamp;
  private Instant lastElementTimestamp;
  private BigDecimal totalThroughputEstimate;

  public BytesThroughputEstimator(
      SizeEstimator<T> sizeEstimator, @Nullable Instant lastRunTimestamp) {
    this(
        sizeEstimator,
        DEFAULT_SAMPLE_RATE,
        lastRunTimestamp != null ? lastRunTimestamp : Instant.now());
  }

  @VisibleForTesting
  public BytesThroughputEstimator(
      SizeEstimator<T> sizeEstimator, int sampleRate, Instant startTimestamp) {
    this.sizeEstimator = sizeEstimator;
    this.sampleRate = sampleRate;
    this.startTimestamp = startTimestamp;
    this.elementCount = 0;
    this.currentElementSizeEstimate = BigDecimal.ZERO;
    lastElementTimestamp = this.startTimestamp;
    totalThroughputEstimate = BigDecimal.ZERO;
  }

  /**
   * Updates the estimator with the bytes of records if it is selected to be sampled.
   *
   * @param timeOfRecords the committed timestamp of the records
   * @param element the element to estimate the byte size of
   */
  public void update(Instant timeOfRecords, T element) {
    // Always updates on first element re-estimates size based on sample rate.
    // This is expensive so we avoid doing it too often.
    if (elementCount % sampleRate == 0) {
      currentElementSizeEstimate = BigDecimal.valueOf(sizeEstimator.sizeOf(element));
    }
    lastElementTimestamp = timeOfRecords;
    elementCount += 1;
    totalThroughputEstimate = totalThroughputEstimate.add(currentElementSizeEstimate);
  }

  /** Returns the estimated throughput bytes for this run. */
  public BigDecimal get() {
    if (elementCount == 0) {
      return BigDecimal.ZERO;
    } else {
      BigDecimal processingTimeMillis =
          BigDecimal.valueOf(new Duration(startTimestamp, lastElementTimestamp).getMillis())
              // Avoid divide by zero by rounding up to 1 ms when the difference is less
              // than a full millisecond
              .max(BigDecimal.ONE);

      return totalThroughputEstimate
          .divide(processingTimeMillis, 3, RoundingMode.DOWN)
          .multiply(BigDecimal.valueOf(1000));
    }
  }
}
