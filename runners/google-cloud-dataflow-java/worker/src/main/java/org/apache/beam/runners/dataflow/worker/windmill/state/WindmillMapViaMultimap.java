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
import java.util.Iterator;
import java.util.Map;
import java.util.function.Function;
import javax.annotation.Nullable;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill;
import org.apache.beam.sdk.state.ReadableState;
import org.apache.beam.sdk.state.ReadableStates;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Supplier;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Iterables;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Iterators;

public class WindmillMapViaMultimap<KeyT, ValueT> extends AbstractWindmillMap<KeyT, ValueT> {
  final WindmillMultimap<KeyT, ValueT> multimap;

  WindmillMapViaMultimap(WindmillMultimap<KeyT, ValueT> multimap) {
    this.multimap = multimap;
  }

  @Override
  protected Windmill.WorkItemCommitRequest persistDirectly(WindmillStateCache.ForKeyAndFamily cache)
      throws IOException {
    return multimap.persistDirectly(cache);
  }

  @Override
  void initializeForWorkItem(
      WindmillStateReader reader, Supplier<Closeable> scopedReadStateSupplier) {
    super.initializeForWorkItem(reader, scopedReadStateSupplier);
    multimap.initializeForWorkItem(reader, scopedReadStateSupplier);
  }

  @Override
  void cleanupAfterWorkItem() {
    super.cleanupAfterWorkItem();
    multimap.cleanupAfterWorkItem();
  }

  @Override
  public void put(KeyT key, ValueT value) {
    multimap.remove(key);
    multimap.put(key, value);
  }

  @Override
  public ReadableState<ValueT> computeIfAbsent(
      KeyT key, Function<? super KeyT, ? extends ValueT> mappingFunction) {
    // Note that computeIfAbsent comments indicate that the read is lazy but this matches the
    // existing eager
    // behavior of WindmillMap.
    Iterable<ValueT> existingValues = multimap.get(key).read();
    if (Iterables.isEmpty(existingValues)) {
      ValueT inserted = mappingFunction.apply(key);
      multimap.put(key, inserted);
      return ReadableStates.immediate(inserted);
    } else {
      return ReadableStates.immediate(Iterables.getOnlyElement(existingValues));
    }
  }

  @Override
  public void remove(KeyT key) {
    multimap.remove(key);
  }

  private static class SingleValueIterableAdaptor<T> implements ReadableState<T> {
    final ReadableState<Iterable<T>> wrapped;
    final @Nullable T defaultValue;

    SingleValueIterableAdaptor(ReadableState<Iterable<T>> wrapped, @Nullable T defaultValue) {
      this.wrapped = wrapped;
      this.defaultValue = defaultValue;
    }

    @Override
    public T read() {
      Iterator<T> iterator = wrapped.read().iterator();
      if (!iterator.hasNext()) {
        return null;
      }
      return Iterators.getOnlyElement(iterator);
    }

    @Override
    public ReadableState<T> readLater() {
      wrapped.readLater();
      return this;
    }
  }

  @Override
  public ReadableState<ValueT> get(KeyT key) {
    return getOrDefault(key, null);
  }

  @Override
  public ReadableState<ValueT> getOrDefault(KeyT key, @Nullable ValueT defaultValue) {
    return new SingleValueIterableAdaptor<>(multimap.get(key), defaultValue);
  }

  @Override
  public ReadableState<Iterable<KeyT>> keys() {
    return multimap.keys();
  }

  private static class RemoveKeyAdaptor<K, V> implements ReadableState<Iterable<V>> {
    final ReadableState<Iterable<Map.Entry<K, V>>> wrapped;

    RemoveKeyAdaptor(ReadableState<Iterable<Map.Entry<K, V>>> wrapped) {
      this.wrapped = wrapped;
    }

    @Override
    public Iterable<V> read() {
      return Iterables.transform(wrapped.read(), Map.Entry::getValue);
    }

    @Override
    public ReadableState<Iterable<V>> readLater() {
      wrapped.readLater();
      return this;
    }
  }

  @Override
  public ReadableState<Iterable<ValueT>> values() {
    return new RemoveKeyAdaptor<>(multimap.entries());
  }

  @Override
  public ReadableState<Iterable<Map.Entry<KeyT, ValueT>>> entries() {
    return multimap.entries();
  }

  @Override
  public ReadableState<Boolean> isEmpty() {
    return multimap.isEmpty();
  }

  @Override
  public void clear() {
    multimap.clear();
  }
}
