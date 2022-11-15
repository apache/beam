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
package org.apache.beam.sdk.io.gcp.spanner.changestreams.estimator;

import com.google.cloud.Timestamp;
import java.io.Serializable;
import java.math.BigDecimal;
import java.math.MathContext;
import java.util.ArrayDeque;
import java.util.Deque;

/** An estimator to provide an estimate on the throughput of the outputted elements. */
public class ThroughputEstimator<T> implements Serializable {

  private static final long serialVersionUID = -3597929310338724800L;

  /** Keeps track of how many bytes of throughput have been seen in a given timestamp. */
  private static class ThroughputEntry implements Serializable {

    private static final long serialVersionUID = 3752325891215855332L;

    private final Timestamp timestamp;
    private BigDecimal bytes;

    public ThroughputEntry(Timestamp timestamp, long bytes) {
      this.timestamp = timestamp;
      this.bytes = BigDecimal.valueOf(bytes);
    }

    public Timestamp getTimestamp() {
      return timestamp;
    }

    public long getSeconds() {
      return timestamp.getSeconds();
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

  public ThroughputEstimator(int windowSizeSeconds, SizeEstimator<T> sizeEstimator) {
    this.deque = new ArrayDeque<>();
    this.windowSizeSeconds = windowSizeSeconds;
    this.sizeEstimator = sizeEstimator;
  }

  /**
   * Updates the estimator with the bytes of records.
   *
   * @param timeOfRecords the committed timestamp of the records
   * @param element the element to estimate the byte size of
   */
  @SuppressWarnings("nullness") // queue is never null, nor the peeked element
  public void update(Timestamp timeOfRecords, T element) {
    long bytes = sizeEstimator.sizeOf(element);
    synchronized (deque) {
      if (deque.isEmpty() || timeOfRecords.getSeconds() > deque.getLast().getSeconds()) {
        deque.addLast(new ThroughputEntry(timeOfRecords, bytes));
      } else {
        deque.getLast().addBytes(bytes);
      }
      cleanQueue(deque.getLast().getTimestamp());
    }
  }

  /** Returns the estimated throughput for now. */
  public double get() {
    return getFrom(Timestamp.now());
  }

  /**
   * Returns the estimated throughput for a specified time.
   *
   * @param time the specified timestamp to check throughput
   */
  public double getFrom(Timestamp time) {
    synchronized (deque) {
      cleanQueue(time);
      if (deque.size() == 0) {
        return 0D;
      }
      return deque.stream()
          .reduce(BigDecimal.ZERO, (acc, entry) -> acc.add(entry.getBytes()), BigDecimal::add)
          .max(BigDecimal.ZERO)
          .divide(BigDecimal.valueOf(windowSizeSeconds), MathContext.DECIMAL128)
          .doubleValue();
    }
  }

  private void cleanQueue(Timestamp time) {
    while (deque.size() > 0) {
      final ThroughputEntry entry = deque.getFirst();
      if (entry != null && entry.getSeconds() >= time.getSeconds() - windowSizeSeconds) {
        break;
      }
      // Remove the element if the timestamp of the first element is beyond
      // the time range to look backward.
      deque.removeFirst();
    }
  }
}
