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
package org.apache.beam.sdk.metrics;

import java.util.concurrent.atomic.AtomicLong;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.annotations.Experimental.Kind;

/**
 * Tracks the current value (and delta) for a Counter metric for a specific context and bundle.
 *
 * <p>This class generally shouldn't be used directly. The only exception is within a runner where
 * a counter is being reported for a specific step (rather than the counter in the current context).
 * In that case retrieving the underlying cell and reporting directly to it avoids a step of
 * indirection.
 */
@Experimental(Kind.METRICS)
public class CounterCell implements MetricCell<Counter, Long> {

  private final DirtyState dirty = new DirtyState();
  private final AtomicLong value = new AtomicLong();

  /**
   * Package-visibility because all {@link CounterCell CounterCells} should be created by
   * {@link MetricsContainer#getCounter(MetricName)}.
   */
  CounterCell() {}

  /**
   * Increment the counter by the given amount.
   * @param n value to increment by. Can be negative to decrement.
   */
  public void update(long n) {
    value.addAndGet(n);
    dirty.afterModification();
  }

  @Override
  public void update(Long n) {
    throw new UnsupportedOperationException("CounterCell.update(Long n) should not be used"
    + " as it performs unnecessary boxing/unboxing. Use CounterCell.update(long n) instead.");
  }

  @Override public void update(MetricCell<Counter, Long> other) {
    update((long) other.getCumulative());
  }

  @Override
  public DirtyState getDirty() {
    return dirty;
  }

  @Override
  public Long getCumulative() {
    return value.get();
  }
}
