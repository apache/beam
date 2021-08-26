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
import java.util.concurrent.atomic.AtomicReference;
import org.apache.beam.sdk.metrics.Gauge;
import org.apache.beam.sdk.metrics.MetricName;
import org.apache.beam.sdk.metrics.MetricsContainer;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * Tracks the current value (and delta) for a {@link Gauge} metric.
 *
 * <p>This class generally shouldn't be used directly. The only exception is within a runner where a
 * gauge is being reported for a specific step (rather than the gauge in the current context). In
 * that case retrieving the underlying cell and reporting directly to it avoids a step of
 * indirection.
 */
public class GaugeCell implements Gauge, MetricCell<GaugeData> {

  private final DirtyState dirty = new DirtyState();
  private final AtomicReference<GaugeData> gaugeValue = new AtomicReference<>(GaugeData.empty());
  private final MetricName name;

  /**
   * Generally, runners should construct instances using the methods in {@link
   * MetricsContainerImpl}, unless they need to define their own version of {@link
   * MetricsContainer}. These constructors are *only* public so runners can instantiate.
   */
  public GaugeCell(MetricName name) {
    this.name = name;
  }

  @Override
  public void reset() {
    dirty.afterModification();
    gaugeValue.set(GaugeData.empty());
  }

  /** Set the gauge to the given value. */
  @Override
  public void set(long value) {
    update(GaugeData.create(value));
  }

  void update(GaugeData data) {
    GaugeData original;
    do {
      original = gaugeValue.get();
    } while (!gaugeValue.compareAndSet(original, original.combine(data)));
    dirty.afterModification();
  }

  @Override
  public DirtyState getDirty() {
    return dirty;
  }

  @Override
  public GaugeData getCumulative() {
    return gaugeValue.get();
  }

  @Override
  public MetricName getName() {
    return name;
  }

  @Override
  public boolean equals(@Nullable Object object) {
    if (object instanceof GaugeCell) {
      GaugeCell gaugeCell = (GaugeCell) object;
      return Objects.equals(dirty, gaugeCell.dirty)
          && Objects.equals(gaugeValue.get(), gaugeCell.gaugeValue.get())
          && Objects.equals(name, gaugeCell.name);
    }

    return false;
  }

  @Override
  public int hashCode() {
    return Objects.hash(dirty, gaugeValue.get(), name);
  }
}
