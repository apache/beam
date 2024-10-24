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

import static org.apache.beam.runners.dataflow.worker.windmill.state.WindmillStateUtil.encodeKey;

import java.io.Closeable;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.function.Function;
import javax.annotation.Nullable;
import org.apache.beam.runners.core.StateNamespace;
import org.apache.beam.runners.core.StateTag;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.state.MapState;
import org.apache.beam.sdk.state.ReadableState;
import org.apache.beam.sdk.state.ReadableStates;
import org.apache.beam.sdk.util.ByteStringOutputStream;
import org.apache.beam.sdk.util.Weighted;
import org.apache.beam.vendor.grpc.v1p60p1.com.google.protobuf.ByteString;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableSet;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Iterables;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Maps;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Sets;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.util.concurrent.Futures;
import org.checkerframework.checker.initialization.qual.Initialized;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.UnknownKeyFor;

@SuppressWarnings({
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
public class WindmillMap<K, V> extends AbstractWindmillMap<K, V> {
  private final StateNamespace namespace;
  private final StateTag<MapState<K, V>> address;
  private final ByteString stateKeyPrefix;
  private final String stateFamily;
  private final Coder<K> keyCoder;
  private final Coder<V> valueCoder;
  // TODO(reuvenlax): Should we evict items from the cache? We would have to make sure
  // that anything in the cache that is not committed is not evicted. negativeCache could be
  // evicted whenever we want.
  private final Map<K, V> cachedValues = Maps.newHashMap();
  private final Set<K> negativeCache = Sets.newHashSet();
  private final Set<K> localAdditions = Sets.newHashSet();
  private final Set<K> localRemovals = Sets.newHashSet();
  private boolean complete;
  private boolean cleared = false;

  WindmillMap(
      StateNamespace namespace,
      StateTag<MapState<K, V>> address,
      String stateFamily,
      Coder<K> keyCoder,
      Coder<V> valueCoder,
      boolean isNewKey) {
    this.namespace = namespace;
    this.address = address;
    this.stateKeyPrefix = encodeKey(namespace, address);
    this.stateFamily = stateFamily;
    this.keyCoder = keyCoder;
    this.valueCoder = valueCoder;
    this.complete = isNewKey;
  }

  private K userKeyFromProtoKey(ByteString tag) throws IOException {
    Preconditions.checkState(tag.startsWith(stateKeyPrefix));
    ByteString keyBytes = tag.substring(stateKeyPrefix.size());
    return keyCoder.decode(keyBytes.newInput(), Coder.Context.OUTER);
  }

  private ByteString protoKeyFromUserKey(K key) throws IOException {
    ByteStringOutputStream keyStream = new ByteStringOutputStream();
    stateKeyPrefix.writeTo(keyStream);
    keyCoder.encode(key, keyStream, Coder.Context.OUTER);
    return keyStream.toByteString();
  }

  @Override
  protected Windmill.WorkItemCommitRequest persistDirectly(WindmillStateCache.ForKeyAndFamily cache)
      throws IOException {
    if (!cleared && localAdditions.isEmpty() && localRemovals.isEmpty()) {
      // No changes, so return directly.
      return Windmill.WorkItemCommitRequest.newBuilder().buildPartial();
    }

    Windmill.WorkItemCommitRequest.Builder commitBuilder =
        Windmill.WorkItemCommitRequest.newBuilder();

    if (cleared) {
      commitBuilder
          .addTagValuePrefixDeletesBuilder()
          .setStateFamily(stateFamily)
          .setTagPrefix(stateKeyPrefix);
    }
    cleared = false;

    for (K key : localAdditions) {
      ByteString keyBytes = protoKeyFromUserKey(key);
      ByteStringOutputStream valueStream = new ByteStringOutputStream();
      valueCoder.encode(cachedValues.get(key), valueStream, Coder.Context.OUTER);
      ByteString valueBytes = valueStream.toByteString();

      commitBuilder
          .addValueUpdatesBuilder()
          .setTag(keyBytes)
          .setStateFamily(stateFamily)
          .getValueBuilder()
          .setData(valueBytes)
          .setTimestamp(Long.MAX_VALUE);
    }
    localAdditions.clear();

    for (K key : localRemovals) {
      ByteStringOutputStream keyStream = new ByteStringOutputStream();
      stateKeyPrefix.writeTo(keyStream);
      keyCoder.encode(key, keyStream, Coder.Context.OUTER);
      ByteString keyBytes = keyStream.toByteString();
      // Leaving data blank means that we delete the tag.
      commitBuilder.addValueUpdatesBuilder().setTag(keyBytes).setStateFamily(stateFamily);

      V cachedValue = cachedValues.remove(key);
      if (cachedValue != null) {
        ByteStringOutputStream valueStream = new ByteStringOutputStream();
        valueCoder.encode(cachedValues.get(key), valueStream, Coder.Context.OUTER);
      }
    }
    negativeCache.addAll(localRemovals);
    localRemovals.clear();

    // TODO(reuvenlax): We should store in the cache parameter, as that would enable caching the
    // map
    // between work items, reducing fetches to Windmill. To do so, we need keep track of the
    // encoded size
    // of the map, and to do so efficiently (i.e. without iterating over the entire map on every
    // persist)
    // we need to track the sizes of each map entry.
    cache.put(namespace, address, this, 1);
    return commitBuilder.buildPartial();
  }

  @Override
  public @UnknownKeyFor @NonNull @Initialized ReadableState<V> get(K key) {
    return getOrDefault(key, null);
  }

  @Override
  public @UnknownKeyFor @NonNull @Initialized ReadableState<V> getOrDefault(
      K key, @Nullable V defaultValue) {
    return new WindmillMapReadResultReadableState(key, defaultValue);
  }

  @Override
  public @UnknownKeyFor @NonNull @Initialized ReadableState<
          @UnknownKeyFor @NonNull @Initialized Iterable<K>>
      keys() {
    ReadableState<Iterable<Map.Entry<K, V>>> entries = entries();
    return new WindmillMapKeysReadableState(entries);
  }

  @Override
  public @UnknownKeyFor @NonNull @Initialized ReadableState<
          @UnknownKeyFor @NonNull @Initialized Iterable<V>>
      values() {
    ReadableState<Iterable<Map.Entry<K, V>>> entries = entries();
    return new WindmillMapValuesReadableState(entries);
  }

  @Override
  public @UnknownKeyFor @NonNull @Initialized ReadableState<
          @UnknownKeyFor @NonNull @Initialized Iterable<
              Map.@UnknownKeyFor @NonNull @Initialized Entry<K, V>>>
      entries() {
    return new WindmillMapEntriesReadableState();
  }

  @Override
  public ReadableState<Boolean> isEmpty() {
    return new WindmillMapIsEmptyReadableState();
  }

  @Override
  public void put(K key, V value) {
    V oldValue = cachedValues.put(key, value);
    if (valueCoder.consistentWithEquals() && value.equals(oldValue)) {
      return;
    }
    localAdditions.add(key);
    localRemovals.remove(key);
    negativeCache.remove(key);
  }

  @Override
  public @UnknownKeyFor @NonNull @Initialized ReadableState<V> computeIfAbsent(
      K key, Function<? super K, ? extends V> mappingFunction) {
    Future<V> persistedData = getFutureForKey(key);
    try (Closeable scope = scopedReadState()) {
      if (localRemovals.contains(key) || negativeCache.contains(key)) {
        return ReadableStates.immediate(null);
      }
      @Nullable V cachedValue = cachedValues.get(key);
      if (cachedValue != null || complete) {
        return ReadableStates.immediate(cachedValue);
      }

      V persistedValue = persistedData.get();
      if (persistedValue == null) {
        // This is a new value. Add it to the map and return null.
        put(key, mappingFunction.apply(key));
        return ReadableStates.immediate(null);
      }
      // TODO: Don't do this if it was already in cache.
      cachedValues.put(key, persistedValue);
      return ReadableStates.immediate(persistedValue);
    } catch (InterruptedException | ExecutionException | IOException e) {
      if (e instanceof InterruptedException) {
        Thread.currentThread().interrupt();
      }
      throw new RuntimeException("Unable to read state", e);
    }
  }

  @Override
  public void remove(K key) {
    if (localRemovals.add(key)) {
      cachedValues.remove(key);
      localAdditions.remove(key);
    }
  }

  @Override
  public void clear() {
    cachedValues.clear();
    localAdditions.clear();
    localRemovals.clear();
    negativeCache.clear();
    cleared = true;
    complete = true;
  }

  private Future<V> getFutureForKey(K key) {
    try {
      ByteStringOutputStream keyStream = new ByteStringOutputStream();
      stateKeyPrefix.writeTo(keyStream);
      keyCoder.encode(key, keyStream, Coder.Context.OUTER);
      return reader.valueFuture(keyStream.toByteString(), stateFamily, valueCoder);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private Future<Iterable<Map.Entry<ByteString, V>>> getFuture() {
    if (complete) {
      // The caller will merge in local cached values.
      return Futures.immediateFuture(Collections.emptyList());
    } else {
      return reader.valuePrefixFuture(stateKeyPrefix, stateFamily, valueCoder);
    }
  }

  private class WindmillMapKeysReadableState implements ReadableState<Iterable<K>> {
    private final ReadableState<Iterable<Map.Entry<K, V>>> entries;

    public WindmillMapKeysReadableState(ReadableState<Iterable<Map.Entry<K, V>>> entries) {
      this.entries = entries;
    }

    @Override
    public Iterable<K> read() {
      return Iterables.transform(entries.read(), Map.Entry::getKey);
    }

    @Override
    public @UnknownKeyFor @NonNull @Initialized ReadableState<Iterable<K>> readLater() {
      entries.readLater();
      return this;
    }
  }

  private class WindmillMapValuesReadableState implements ReadableState<Iterable<V>> {
    private final ReadableState<Iterable<Map.Entry<K, V>>> entries;

    public WindmillMapValuesReadableState(ReadableState<Iterable<Map.Entry<K, V>>> entries) {
      this.entries = entries;
    }

    @Override
    public @Nullable Iterable<V> read() {
      return Iterables.transform(entries.read(), Map.Entry::getValue);
    }

    @Override
    public @UnknownKeyFor @NonNull @Initialized ReadableState<Iterable<V>> readLater() {
      entries.readLater();
      return this;
    }
  }

  private class WindmillMapEntriesReadableState
      implements ReadableState<Iterable<Map.Entry<K, V>>> {
    @Override
    public Iterable<Map.Entry<K, V>> read() {
      if (complete) {
        return ImmutableMap.copyOf(cachedValues).entrySet();
      }
      Future<Iterable<Map.Entry<ByteString, V>>> persistedData = getFuture();
      try (Closeable scope = scopedReadState()) {
        Iterable<Map.Entry<ByteString, V>> data = persistedData.get();
        Iterable<Map.Entry<K, V>> transformedData =
            Iterables.transform(
                data,
                entry -> {
                  try {
                    return new AbstractMap.SimpleEntry<>(
                        userKeyFromProtoKey(entry.getKey()), entry.getValue());
                  } catch (IOException e) {
                    throw new RuntimeException(e);
                  }
                });

        if (data instanceof Weighted) {
          // This is a known amount of data. Cache it all.
          transformedData.forEach(
              e -> {
                // The cached data overrides what is read from state, so call putIfAbsent.
                cachedValues.putIfAbsent(e.getKey(), e.getValue());
              });
          complete = true;
          return ImmutableMap.copyOf(cachedValues).entrySet();
        } else {
          ImmutableMap<K, V> cachedCopy = ImmutableMap.copyOf(cachedValues);
          ImmutableSet<K> removalCopy = ImmutableSet.copyOf(localRemovals);
          // This means that the result might be too large to cache, so don't add it to the
          // local cache. Instead merge the iterables, giving priority to any local additions
          // (represented in cachedCopy and removalCopy) that may not have been committed
          // yet.
          return Iterables.unmodifiableIterable(
              Iterables.concat(
                  cachedCopy.entrySet(),
                  Iterables.filter(
                      transformedData,
                      e ->
                          !cachedCopy.containsKey(e.getKey())
                              && !removalCopy.contains(e.getKey()))));
        }

      } catch (InterruptedException | ExecutionException | IOException e) {
        if (e instanceof InterruptedException) {
          Thread.currentThread().interrupt();
        }
        throw new RuntimeException("Unable to read state", e);
      }
    }

    @Override
    @SuppressWarnings("FutureReturnValueIgnored")
    public @UnknownKeyFor @NonNull @Initialized ReadableState<Iterable<Map.Entry<K, V>>>
        readLater() {
      WindmillMap.this.getFuture();
      return this;
    }
  }

  private class WindmillMapIsEmptyReadableState implements ReadableState<Boolean> {
    // TODO(reuvenlax): Can we find a more efficient way of implementing isEmpty than reading
    // the entire map?
    final ReadableState<Iterable<K>> keys = WindmillMap.this.keys();

    @Override
    public @Nullable Boolean read() {
      return Iterables.isEmpty(keys.read());
    }

    @Override
    public @UnknownKeyFor @NonNull @Initialized ReadableState<Boolean> readLater() {
      keys.readLater();
      return this;
    }
  }

  private class WindmillMapReadResultReadableState implements ReadableState<V> {
    private final K key;
    private final @Nullable V defaultValue;

    public WindmillMapReadResultReadableState(K key, @Nullable V defaultValue) {
      this.key = key;
      this.defaultValue = defaultValue;
    }

    @Override
    public @Nullable V read() {
      Future<V> persistedData = getFutureForKey(key);
      try (Closeable scope = scopedReadState()) {
        if (localRemovals.contains(key) || negativeCache.contains(key)) {
          return null;
        }
        @Nullable V cachedValue = cachedValues.get(key);
        if (cachedValue != null || complete) {
          return cachedValue;
        }

        V persistedValue = persistedData.get();
        if (persistedValue == null) {
          negativeCache.add(key);
          return defaultValue;
        }
        cachedValues.put(key, persistedValue);
        return persistedValue;
      } catch (InterruptedException | ExecutionException | IOException e) {
        if (e instanceof InterruptedException) {
          Thread.currentThread().interrupt();
        }
        throw new RuntimeException("Unable to read state", e);
      }
    }

    @Override
    @SuppressWarnings("FutureReturnValueIgnored")
    public @UnknownKeyFor @NonNull @Initialized ReadableState<V> readLater() {
      WindmillMap.this.getFutureForKey(key);
      return this;
    }
  }
}
