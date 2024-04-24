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

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.beam.runners.core.metrics.CounterCell;
import org.apache.beam.runners.core.metrics.DirtyState;
import org.apache.beam.runners.core.metrics.MetricCell;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.MetricName;

/**
 * Version of {@link CounterCell} supporting multi-thread safe mutations and extraction of delta
 * values. {@link RemoveSafeDeltaCounterCell} allows safe deletions from the underlying {@code
 * countersMap} through the {@code deleteIfZero} method.
 */
public class RemoveSafeDeltaCounterCell implements Counter, MetricCell<Long> {

  private final MetricName metricName;
  /**
   * This class does not own {@code countersMap} and only operates on a single key in the map
   * specified by {@code metricName}. These opeations include the {@link Counter} interface along
   * with the {@code deleteIfZero} method.
   */
  private final ConcurrentHashMap<MetricName, AtomicLong> countersMap;

  /**
   * @param metricName Specifies which metric this counter refers to.
   * @param countersMap The underlying {@code map} used to store this metric.
   */
  public RemoveSafeDeltaCounterCell(
      MetricName metricName, ConcurrentHashMap<MetricName, AtomicLong> countersMap) {
    this.metricName = metricName;
    this.countersMap = countersMap;
  }

  @Override
  public void reset() {
    countersMap.computeIfPresent(
        metricName,
        (unusedName, value) -> {
          value.set(0);
          return value;
        });
  }

  @Override
  public void inc(long n) {
    AtomicLong val = countersMap.get(metricName);
    if (val != null) {
      val.getAndAdd(n);
      return;
    }
    countersMap.computeIfAbsent(metricName, name -> new AtomicLong()).getAndAdd(n);
  }

  @Override
  public void inc() {
    inc(1);
  }

  @Override
  public void dec() {
    inc(-1);
  }

  @Override
  public void dec(long n) {
    inc(-1 * n);
  }

  @Override
  public MetricName getName() {
    return metricName;
  }

  @Override
  public DirtyState getDirty() {
    throw new UnsupportedOperationException(
        String.format("%s doesn't support the getDirty", getClass().getSimpleName()));
  }

  @Override
  public Long getCumulative() {
    throw new UnsupportedOperationException("getCumulative is not supported by Streaming Metrics");
  }

  /** Remove the metric from the {@code countersMap} if the the metric is zero valued. */
  @SuppressWarnings(
      "nullness") // computeIfPresent's remapping function can return null to remove the entry.
  public void deleteIfZero() {
    countersMap.computeIfPresent(
        metricName,
        (name, value) -> {
          return value.get() == 0L ? null : value;
        });
  }
}
