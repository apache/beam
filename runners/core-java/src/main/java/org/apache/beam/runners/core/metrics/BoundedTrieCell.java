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

import java.util.Arrays;
import java.util.Objects;
import org.apache.beam.sdk.metrics.BoundedTrie;
import org.apache.beam.sdk.metrics.MetricName;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * Tracks the current value for a {@link BoundedTrie} metric.
 *
 * <p>This class generally shouldn't be used directly. The only exception is within a runner where a
 * counter is being reported for a specific step (rather than the counter in the current context).
 * In that case retrieving the underlying cell and reporting directly to it avoids a step of
 * indirection.
 */
// TODO: Write multi-threaded test in MetricContainerImp for this Cell class too.
public class BoundedTrieCell implements BoundedTrie, MetricCell<BoundedTrieData> {

  private final DirtyState dirty = new DirtyState();
  private BoundedTrieData value;
  private final MetricName name;

  public BoundedTrieCell(MetricName name) {
    this.name = name;
    this.value = new BoundedTrieData();
  }

  public synchronized void update(BoundedTrieData other) {
    this.value = this.value.combine(other);
    dirty.afterModification();
  }

  @Override
  public synchronized void reset() {
    value.clear();
    dirty.reset();
  }

  @Override
  public DirtyState getDirty() {
    return dirty;
  }

  @Override
  public synchronized BoundedTrieData getCumulative() {
    return value.getCumulative();
  }

  @Override
  public MetricName getName() {
    return name;
  }

  @Override
  public boolean equals(@Nullable Object object) {
    if (object instanceof BoundedTrieCell) {
      BoundedTrieCell boundedTrieCell = (BoundedTrieCell) object;
      return Objects.equals(dirty, boundedTrieCell.dirty)
          && Objects.equals(value, boundedTrieCell.value)
          && Objects.equals(name, boundedTrieCell.name);
    }
    return false;
  }

  @Override
  public int hashCode() {
    return Objects.hash(dirty, value, name);
  }

  @Override
  public synchronized void add(Iterable<String> values) {
    this.value.add(values);
    dirty.afterModification();
  }

  @Override
  public synchronized void add(String... values) {
    add(Arrays.asList(values));
  }
}
