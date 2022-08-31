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
package org.apache.beam.sdk.io.gcp.spanner.changestreams.restriction;

import com.google.cloud.Timestamp;
import java.io.Serializable;
import java.math.BigDecimal;
import java.math.MathContext;
import java.util.ArrayDeque;
import java.util.Queue;
import org.apache.beam.repackaged.core.org.apache.commons.lang3.tuple.ImmutablePair;

/** An estimator to provide an estimate on the throughput of the outputted elements. */
public class ThroughputEstimator implements Serializable {

  private static final long serialVersionUID = -3597929310338724800L;
  // The start time of each per-second window.
  private Timestamp startTimeOfCurrentWindow;
  // The bytes of the current window.
  private BigDecimal bytesInCurrentWindow;
  // The number of seconds to look in the past.
  private final int numOfSeconds = 60;
  // The total bytes of all windows in the queue.
  private BigDecimal bytesInQueue;
  // The queue holds a number of windows in the past in order to calculate
  // a rolling windowing throughput.
  private final Queue<ImmutablePair<Timestamp, BigDecimal>> queue;

  public ThroughputEstimator() {
    queue = new ArrayDeque<>();
    startTimeOfCurrentWindow = Timestamp.MIN_VALUE;
    bytesInCurrentWindow = BigDecimal.valueOf(0L);
    bytesInQueue = BigDecimal.valueOf(0L);
  }

  /**
   * Updates the estimator with the bytes of records.
   *
   * @param timeOfRecords the committed timestamp of the records
   * @param bytes the total bytes of the records
   */
  public void update(Timestamp timeOfRecords, long bytes) {
    synchronized (queue) {
      BigDecimal bytesNum = BigDecimal.valueOf(bytes);
      if (startTimeOfCurrentWindow.equals(Timestamp.MIN_VALUE)) {
        bytesInCurrentWindow = bytesNum;
        startTimeOfCurrentWindow = timeOfRecords;
        return;
      }

      if (timeOfRecords.getSeconds() < startTimeOfCurrentWindow.getSeconds() + 1) {
        bytesInCurrentWindow = bytesInCurrentWindow.add(bytesNum);
      } else {
        queue.add(new ImmutablePair<>(startTimeOfCurrentWindow, bytesInCurrentWindow));
        bytesInQueue = bytesInQueue.add(bytesInCurrentWindow);

        bytesInCurrentWindow = bytesNum;
        startTimeOfCurrentWindow = timeOfRecords;
      }
      cleanQueue(startTimeOfCurrentWindow);
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
    synchronized (queue) {
      cleanQueue(time);
      if (queue.size() == 0) {
        return 0D;
      }
      return bytesInQueue
          .divide(BigDecimal.valueOf(queue.size()), MathContext.DECIMAL128)
          .max(BigDecimal.ZERO)
          .doubleValue();
    }
  }

  private void cleanQueue(Timestamp time) {
    while (queue.size() > 0) {
      ImmutablePair<Timestamp, BigDecimal> peek = queue.peek();
      if (peek != null && peek.getLeft().getSeconds() >= time.getSeconds() - numOfSeconds) {
        break;
      }
      // Remove the element if the timestamp of the first element is beyond
      // the time range to look backward.
      ImmutablePair<Timestamp, BigDecimal> pair = queue.remove();
      bytesInQueue = bytesInQueue.subtract(pair.getRight()).max(BigDecimal.ZERO);
    }
  }
}
