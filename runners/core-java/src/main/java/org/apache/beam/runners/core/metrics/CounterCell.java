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
import java.util.concurrent.atomic.AtomicLong;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.MetricName;
import org.apache.beam.sdk.metrics.MetricsContainer;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * Tracks the current value (and delta) for a Counter metric for a specific context and bundle.
 *
 * <p>This class generally shouldn't be used directly. The only exception is within a runner where a
 * counter is being reported for a specific step (rather than the counter in the current context).
 * In that case retrieving the underlying cell and reporting directly to it avoids a step of
 * indirection.
 */
public class CounterCell implements Counter, MetricCell<Long> {

  private final DirtyState dirty = new DirtyState();
  private final AtomicLong value = new AtomicLong();
  private final MetricName name;

  /**
   * Generally, runners should construct instances using the methods in {@link
   * MetricsContainerImpl}, unless they need to define their own version of {@link
   * MetricsContainer}. These constructors are *only* public so runners can instantiate.
   */
  public CounterCell(MetricName name) {
    this.name = name;
  }

  @Override
  public void reset() {
    dirty.afterModification();
    value.set(0L);
  }

  /**
   * Increment the counter by the given amount.
   *
   * @param n value to increment by. Can be negative to decrement.
   */
  @Override
  public void inc(long n) {
    value.addAndGet(n);
    dirty.afterModification();
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
  public DirtyState getDirty() {
    return dirty;
  }

  @Override
  public Long getCumulative() {
    return value.get();
  }

  @Override
  public MetricName getName() {
    return name;
  }

  @Override
  public boolean equals(@Nullable Object object) {
    if (object instanceof CounterCell) {
      CounterCell counterCell = (CounterCell) object;
      return Objects.equals(dirty, counterCell.dirty)
          && Objects.equals(value.get(), counterCell.value.get())
          && Objects.equals(name, counterCell.name);
    }

    return false;
  }

  @Override
  public int hashCode() {
    return Objects.hash(dirty, value.get(), name);
  }
}
