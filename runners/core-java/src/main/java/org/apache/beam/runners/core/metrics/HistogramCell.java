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
package org.apache.beam.runners.core.metrics;

import java.util.Objects;
import org.apache.beam.sdk.metrics.MetricName;
import org.apache.beam.sdk.metrics.MetricsContainer;
import org.apache.beam.sdk.util.HistogramData;
import org.apache.beam.sdk.values.KV;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * Tracks the current value (and delta) for a Histogram metric.
 *
 * <p>This class generally shouldn't be used directly. The only exception is within a runner where a
 * histogram is being reported for a specific step (rather than the histogram in the current
 * context). In that case retrieving the underlying cell and reporting directly to it avoids a step
 * of indirection.
 */
public class HistogramCell
    implements org.apache.beam.sdk.metrics.Histogram, MetricCell<HistogramData> {

  private final DirtyState dirty = new DirtyState();
  private final HistogramData value;
  private final MetricName name;

  /**
   * Generally, runners should construct instances using the methods in {@link
   * MetricsContainerImpl}, unless they need to define their own version of {@link
   * MetricsContainer}. These constructors are *only* public so runners can instantiate.
   */
  public HistogramCell(KV<MetricName, HistogramData.BucketType> kv) {
    this.name = kv.getKey();
    this.value = new HistogramData(kv.getValue());
  }

  @Override
  public void reset() {
    value.clear();
    dirty.reset();
  }

  /** Increment the corresponding histogram bucket count for the value by 1. */
  @Override
  public void update(double value) {
    this.value.record(value);
    dirty.afterModification();
  }

  /**
   * Increment all of the bucket counts in this histogram, by the bucket counts specified in other.
   */
  public void update(HistogramCell other) {
    this.value.update(other.value);
    dirty.afterModification();
  }

  @Override
  public void update(HistogramData data) {
    this.value.update(data);
    dirty.afterModification();
  }

  // TODO(https://github.com/apache/beam/issues/20853): Update this function to allow incrementing
  // the infinite buckets as well.
  // and remove the incTopBucketCount and incBotBucketCount methods.
  // Using 0 and length -1 as the bucketIndex.
  public void incBucketCount(int bucketIndex, long count) {
    this.value.incBucketCount(bucketIndex, count);
    dirty.afterModification();
  }

  public void incTopBucketCount(long count) {
    this.value.incTopBucketCount(count);
    dirty.afterModification();
  }

  public void incBottomBucketCount(long count) {
    this.value.incBottomBucketCount(count);
    dirty.afterModification();
  }

  @Override
  public DirtyState getDirty() {
    return dirty;
  }

  @Override
  public HistogramData getCumulative() {
    return value;
  }

  @Override
  public MetricName getName() {
    return name;
  }

  @Override
  public boolean equals(@Nullable Object object) {
    if (object instanceof HistogramCell) {
      HistogramCell histogramCell = (HistogramCell) object;
      return Objects.equals(dirty, histogramCell.dirty)
          && Objects.equals(value, histogramCell.value)
          && Objects.equals(name, histogramCell.name);
    }

    return false;
  }

  @Override
  public int hashCode() {
    return Objects.hash(dirty, value, name);
  }
}
