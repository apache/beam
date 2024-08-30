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
package org.apache.beam.runners.dataflow.worker.windmill.state;

import java.io.Closeable;
import java.io.IOException;
import java.util.Optional;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill;
import org.apache.beam.sdk.state.ReadableState;
import org.apache.beam.sdk.state.SetState;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Supplier;
import org.checkerframework.checker.initialization.qual.Initialized;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.UnknownKeyFor;

public class WindmillSet<K> extends SimpleWindmillState implements SetState<K> {
  private final AbstractWindmillMap<K, Boolean> windmillMap;

  WindmillSet(AbstractWindmillMap<K, Boolean> windmillMap) {
    this.windmillMap = windmillMap;
  }

  @Override
  protected Windmill.WorkItemCommitRequest persistDirectly(WindmillStateCache.ForKeyAndFamily cache)
      throws IOException {
    return windmillMap.persistDirectly(cache);
  }

  @Override
  public @UnknownKeyFor @NonNull @Initialized ReadableState<
          @UnknownKeyFor @NonNull @Initialized Boolean>
      contains(K k) {
    return windmillMap.getOrDefault(k, false);
  }

  @Override
  public @UnknownKeyFor @NonNull @Initialized ReadableState<
          @UnknownKeyFor @NonNull @Initialized Boolean>
      addIfAbsent(K k) {
    return new WindmillSetAddIfAbsentReadableState(k);
  }

  @Override
  public void remove(K k) {
    windmillMap.remove(k);
  }

  @Override
  public void add(K value) {
    windmillMap.put(value, true);
  }

  @Override
  public @UnknownKeyFor @NonNull @Initialized ReadableState<
          @UnknownKeyFor @NonNull @Initialized Boolean>
      isEmpty() {
    return windmillMap.isEmpty();
  }

  @Override
  public Iterable<K> read() {
    return windmillMap.keys().read();
  }

  @Override
  public @UnknownKeyFor @NonNull @Initialized SetState<K> readLater() {
    windmillMap.keys().readLater();
    return this;
  }

  @Override
  public void clear() {
    windmillMap.clear();
  }

  @Override
  void initializeForWorkItem(
      WindmillStateReader reader, Supplier<Closeable> scopedReadStateSupplier) {
    super.initializeForWorkItem(reader, scopedReadStateSupplier);
    windmillMap.initializeForWorkItem(reader, scopedReadStateSupplier);
  }

  @Override
  void cleanupAfterWorkItem() {
    super.cleanupAfterWorkItem();
    windmillMap.cleanupAfterWorkItem();
  }

  private class WindmillSetAddIfAbsentReadableState implements ReadableState<Boolean> {
    ReadableState<Boolean> putState;

    public WindmillSetAddIfAbsentReadableState(K k) {
      putState = windmillMap.putIfAbsent(k, true);
    }

    @Override
    public Boolean read() {
      return Optional.ofNullable(putState.read()).orElse(false);
    }

    @Override
    public @UnknownKeyFor @NonNull @Initialized ReadableState<Boolean> readLater() {
      putState = putState.readLater();
      return this;
    }
  }
}
