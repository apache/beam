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

import java.io.Serializable;
import java.math.BigDecimal;
import java.math.MathContext;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Random;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.sdk.io.gcp.bigtable.changestreams.TimestampConverter;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.annotations.VisibleForTesting;
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
public class BytesThroughputEstimator<T> implements ThroughputEstimator<T> {

  private static final long serialVersionUID = -1147130541208370666L;
  private static final BigDecimal MAX_DOUBLE = BigDecimal.valueOf(Double.MAX_VALUE);
  private static final int DEFAULT_SAMPLE_RATE = 50;

  /** Keeps track of how many bytes of throughput have been seen in a given timestamp. */
  private static class ThroughputEntry implements Serializable {

    private static final long serialVersionUID = 3752325891215855332L;

    private final Instant instant;
    private BigDecimal bytes;

    public ThroughputEntry(Instant instant, long bytes) {
      this.instant = instant;
      this.bytes = BigDecimal.valueOf(bytes);
    }

    public Instant getTimestamp() {
      return instant;
    }

    public long getSeconds() {
      return TimestampConverter.toSeconds(instant);
    }

    public BigDecimal getBytes() {
      return bytes;
    }

    public void addBytes(long bytesToAdd) {
      bytes = bytes.add(BigDecimal.valueOf(bytesToAdd));
    }
  }

  // The deque holds a number of windows in the past in order to calculate
  // a rolling windowing throughput.
  private final Deque<ThroughputEntry> deque;
  // The number of seconds to be accounted for when calculating the throughput
  private final int windowSizeSeconds;
  // Estimates the size in bytes of throughput elements
  private final SizeEstimator<T> sizeEstimator;
  private final int sampleRate;
  private final Random random;

  public BytesThroughputEstimator(int windowSizeSeconds, SizeEstimator<T> sizeEstimator) {
    this(windowSizeSeconds, sizeEstimator, DEFAULT_SAMPLE_RATE);
  }

  @VisibleForTesting
  public BytesThroughputEstimator(
      int windowSizeSeconds, SizeEstimator<T> sizeEstimator, int sampleRate) {
    this.deque = new ArrayDeque<>();
    this.windowSizeSeconds = windowSizeSeconds;
    this.sizeEstimator = sizeEstimator;
    this.sampleRate = sampleRate;
    this.random = new Random();
  }

  /**
   * Updates the estimator with the bytes of records if it is selected to be sampled.
   *
   * @param timeOfRecords the committed timestamp of the records
   * @param element the element to estimate the byte size of
   */
  @SuppressWarnings("nullness") // queue is never null, nor the peeked element
  @Override
  public void update(Instant timeOfRecords, T element) {
    if (random.nextInt(sampleRate) == 0) {
      long bytes = sizeEstimator.sizeOf(element);
      synchronized (deque) {
        if (deque.isEmpty()
            || TimestampConverter.toSeconds(timeOfRecords) > deque.getLast().getSeconds()) {
          deque.addLast(new ThroughputEntry(timeOfRecords, bytes));
        } else {
          deque.getLast().addBytes(bytes);
        }
        cleanQueue(deque.getLast().getTimestamp());
      }
    }
  }

  /** Returns the estimated throughput bytes for now. */
  @Override
  public double get() {
    return getFrom(Instant.now());
  }

  /**
   * Returns the estimated throughput bytes for a specified time.
   *
   * @param time the specified timestamp to check throughput
   */
  @Override
  public double getFrom(Instant time) {
    synchronized (deque) {
      cleanQueue(time);
      if (deque.size() == 0) {
        return 0D;
      }
      BigDecimal throughput = BigDecimal.ZERO;
      for (ThroughputEntry entry : deque) {
        throughput = throughput.add(entry.getBytes());
      }
      return throughput
          // Prevents negative values
          .max(BigDecimal.ZERO)
          .divide(BigDecimal.valueOf(windowSizeSeconds), MathContext.DECIMAL128)
          .multiply(BigDecimal.valueOf(sampleRate))
          // Cap it to Double.MAX_VALUE
          .min(MAX_DOUBLE)
          .doubleValue();
    }
  }

  private void cleanQueue(Instant time) {
    while (deque.size() > 0) {
      final ThroughputEntry entry = deque.getFirst();
      if (entry != null
          && entry.getSeconds() >= TimestampConverter.toSeconds(time) - windowSizeSeconds) {
        break;
      }
      // Remove the element if the timestamp of the first element is beyond
      // the time range to look backward.
      deque.removeFirst();
    }
  }
}
