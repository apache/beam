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
import org.apache.beam.sdk.metrics.MetricName;
import org.apache.beam.sdk.metrics.MetricsContainer;
import org.apache.beam.sdk.metrics.StringSet;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableSet;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * Tracks the current value for a {@link StringSet} metric.
 *
 * <p>This class generally shouldn't be used directly. The only exception is within a runner where a
 * counter is being reported for a specific step (rather than the counter in the current context).
 * In that case retrieving the underlying cell and reporting directly to it avoids a step of
 * indirection.
 */
public class StringSetCell implements StringSet, MetricCell<StringSetData> {

  private final DirtyState dirty = new DirtyState();
  private final AtomicReference<StringSetData> setValue =
      new AtomicReference<>(StringSetData.empty());
  private final MetricName name;

  /**
   * Generally, runners should construct instances using the methods in {@link
   * MetricsContainerImpl}, unless they need to define their own version of {@link
   * MetricsContainer}. These constructors are *only* public so runners can instantiate.
   */
  public StringSetCell(MetricName name) {
    this.name = name;
  }

  @Override
  public void reset() {
    setValue.set(StringSetData.empty());
    dirty.reset();
  }

  void update(StringSetData data) {
    StringSetData original;
    do {
      original = setValue.get();
    } while (!setValue.compareAndSet(original, original.combine(data)));
    dirty.afterModification();
  }

  @Override
  public DirtyState getDirty() {
    return dirty;
  }

  @Override
  public StringSetData getCumulative() {
    return setValue.get();
  }

  @Override
  public MetricName getName() {
    return name;
  }

  @Override
  public boolean equals(@Nullable Object object) {
    if (object instanceof StringSetCell) {
      StringSetCell stringSetCell = (StringSetCell) object;
      return Objects.equals(dirty, stringSetCell.dirty)
          && Objects.equals(setValue.get(), stringSetCell.setValue.get())
          && Objects.equals(name, stringSetCell.name);
    }

    return false;
  }

  @Override
  public int hashCode() {
    return Objects.hash(dirty, setValue.get(), name);
  }

  @Override
  public void add(String value) {
    // if the given value is already present in the StringSet then skip this add for efficiency
    if (this.setValue.get().stringSet().contains(value)) {
      return;
    }
    update(StringSetData.create(ImmutableSet.of(value)));
  }

  @Override
  public void add(String... values) {
    update(StringSetData.create(ImmutableSet.copyOf(values)));
  }
}
