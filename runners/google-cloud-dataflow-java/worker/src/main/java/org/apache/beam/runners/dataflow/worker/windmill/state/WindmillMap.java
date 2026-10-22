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
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.function.Function;
import javax.annotation.Nullable;
import org.apache.beam.runners.core.StateNamespace;
import org.apache.beam.runners.dataflow.worker.util.common.worker.InternedByteString;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.state.ReadableState;
import org.apache.beam.sdk.state.ReadableStates;
import org.apache.beam.sdk.util.ByteStringOutputStream;
import org.apache.beam.sdk.util.Weighted;
import org.apache.beam.vendor.grpc.v1p69p0.com.google.protobuf.ByteString;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions;
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
  private final InternedByteString stateKeyPrefix;
  private final String stateFamily;
  private final Coder<K> keyCoder;
  private final Coder<V> valueCoder;

  private static class StructuralKey<K> {
    private final K originalKey;
    private final @Nullable Object structuralValue;

    private StructuralKey(K originalKey, @Nullable Object structuralValue) {
      this.originalKey = originalKey;
      this.structuralValue = structuralValue;
    }

    public static <K> StructuralKey<K> of(K key, Coder<K> coder) {
      return new StructuralKey<>(key, key == null ? null : coder.structuralValue(key));
    }

    public K getOriginalKey() {
      return originalKey;
    }

    @Override
    public boolean equals(@Nullable Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof StructuralKey)) {
        return false;
      }
      StructuralKey<?> that = (StructuralKey<?>) o;
      return Objects.equals(structuralValue, that.structuralValue);
    }

    @Override
    public int hashCode() {
      return Objects.hashCode(structuralValue);
    }
  }

  private StructuralKey<K> asStructuralKey(K key) {
    return StructuralKey.of(key, keyCoder);
  }

  // TODO(reuvenlax): Should we evict items from the cache? We would have to make sure
  // that anything in the cache that is not committed is not evicted. negativeCache could be
  // evicted whenever we want.
  private final Map<StructuralKey<K>, V> cachedValues = Maps.newHashMap();
  private final Set<StructuralKey<K>> negativeCache = Sets.newHashSet();
  private final Set<StructuralKey<K>> localAdditions = Sets.newHashSet();
  private final Set<StructuralKey<K>> localRemovals = Sets.newHashSet();
  private boolean complete;
  private boolean cleared = false;

  WindmillMap(
      StateNamespace namespace,
      InternedByteString stateKeyPrefix,
      String stateFamily,
      Coder<K> keyCoder,
      Coder<V> valueCoder,
      boolean isNewKey) {
    this.namespace = namespace;
    this.stateKeyPrefix = stateKeyPrefix;
    this.stateFamily = stateFamily;
    this.keyCoder = keyCoder;
    this.valueCoder = valueCoder;
    this.complete = isNewKey;
  }

  private K userKeyFromProtoKey(ByteString tag) throws IOException {
    Preconditions.checkState(tag.startsWith(stateKeyPrefix.byteString()));
    ByteString keyBytes = tag.substring(stateKeyPrefix.byteString().size());
    return keyCoder.decode(keyBytes.newInput(), Coder.Context.OUTER);
  }

  private ByteString protoKeyFromUserKey(K key) throws IOException {
    ByteStringOutputStream keyStream = new ByteStringOutputStream();
    stateKeyPrefix.byteString().writeTo(keyStream);
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
          .setTagPrefix(stateKeyPrefix.byteString());
    }
    cleared = false;

    for (StructuralKey<K> sk : localAdditions) {
      ByteString keyBytes = protoKeyFromUserKey(sk.getOriginalKey());
      ByteStringOutputStream valueStream = new ByteStringOutputStream();
      valueCoder.encode(cachedValues.get(sk), valueStream, Coder.Context.OUTER);
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

    for (StructuralKey<K> sk : localRemovals) {
      ByteStringOutputStream keyStream = new ByteStringOutputStream();
      stateKeyPrefix.byteString().writeTo(keyStream);
      keyCoder.encode(sk.getOriginalKey(), keyStream, Coder.Context.OUTER);
      ByteString keyBytes = keyStream.toByteString();
      // Leaving data blank means that we delete the tag.
      commitBuilder.addValueUpdatesBuilder().setTag(keyBytes).setStateFamily(stateFamily);
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
    cache.put(namespace, stateKeyPrefix, this, 1);
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
    StructuralKey<K> structKey = asStructuralKey(key);
    boolean wasPresent = cachedValues.containsKey(structKey);
    V oldValue = cachedValues.put(structKey, value);
    if (wasPresent
        && valueCoder.consistentWithEquals()
        && Objects.equals(value, oldValue)
        && !localRemovals.contains(structKey)
        && !negativeCache.contains(structKey)) {
      return;
    }
    localAdditions.remove(structKey);
    localAdditions.add(structKey);
    localRemovals.remove(structKey);
    negativeCache.remove(structKey);
  }

  private ReadableState<V> computeAndPut(K key, Function<? super K, ? extends V> mappingFunction) {
    V computed = mappingFunction.apply(key);
    put(key, computed);
    return ReadableStates.immediate(null);
  }

  @Override
  public @UnknownKeyFor @NonNull @Initialized ReadableState<V> computeIfAbsent(
      K key, Function<? super K, ? extends V> mappingFunction) {
    StructuralKey<K> structKey = asStructuralKey(key);
    if (localRemovals.contains(structKey) || negativeCache.contains(structKey)) {
      return computeAndPut(key, mappingFunction);
    }
    @Nullable V cachedValue = cachedValues.get(structKey);
    if (cachedValue != null) {
      return ReadableStates.immediate(cachedValue);
    }
    if (complete) {
      return computeAndPut(key, mappingFunction);
    }

    Future<V> persistedData = getFutureForKey(key);
    try (Closeable scope = scopedReadState()) {
      V persistedValue = persistedData.get();
      if (persistedValue == null) {
        return computeAndPut(key, mappingFunction);
      }
      cachedValues.put(structKey, persistedValue);
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
    StructuralKey<K> structKey = asStructuralKey(key);
    if (localRemovals.add(structKey)) {
      cachedValues.remove(structKey);
      localAdditions.remove(structKey);
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
      stateKeyPrefix.byteString().writeTo(keyStream);
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
      return reader.valuePrefixFuture(stateKeyPrefix.byteString(), stateFamily, valueCoder);
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

  private Iterable<Map.Entry<K, V>> userEntries() {
    List<Map.Entry<K, V>> result = new ArrayList<>(cachedValues.size());
    for (Map.Entry<StructuralKey<K>, V> entry : cachedValues.entrySet()) {
      result.add(new AbstractMap.SimpleEntry<>(entry.getKey().getOriginalKey(), entry.getValue()));
    }
    return Collections.unmodifiableList(result);
  }

  private class WindmillMapEntriesReadableState
      implements ReadableState<Iterable<Map.Entry<K, V>>> {
    @Override
    public Iterable<Map.Entry<K, V>> read() {
      if (complete) {
        return userEntries();
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
                StructuralKey<K> structKey = asStructuralKey(e.getKey());
                if (!localRemovals.contains(structKey)) {
                  // The cached data overrides what is read from state, so call putIfAbsent.
                  cachedValues.putIfAbsent(structKey, e.getValue());
                }
              });
          complete = true;
          return userEntries();
        } else {
          Map<StructuralKey<K>, V> cachedCopy = Maps.newHashMap(cachedValues);
          Set<StructuralKey<K>> removalCopy = Sets.newHashSet(localRemovals);
          // This means that the result might be too large to cache, so don't add it to the
          // local cache. Instead, merge the iterables, giving priority to any local additions
          // (represented in cachedCopy and removalCopy) that may not have been committed
          // yet.
          return Iterables.unmodifiableIterable(
              Iterables.concat(
                  userEntries(),
                  Iterables.filter(
                      transformedData,
                      e -> {
                        StructuralKey<K> structKey = asStructuralKey(e.getKey());
                        return !cachedCopy.containsKey(structKey)
                            && !removalCopy.contains(structKey);
                      })));
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
      // If there are any cached values, we know the map is not empty.
      if (!cachedValues.isEmpty()) {
        return false;
      }
      if (complete) {
        return true;
      }
      return Iterables.isEmpty(keys.read());
    }

    @Override
    public @UnknownKeyFor @NonNull @Initialized ReadableState<Boolean> readLater() {
      if (cachedValues.isEmpty() && !complete) {
        keys.readLater();
      }
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
      StructuralKey<K> structKey = asStructuralKey(key);
      if (localRemovals.contains(structKey) || negativeCache.contains(structKey)) {
        return defaultValue;
      }
      if (cachedValues.containsKey(structKey)) {
        return cachedValues.get(structKey);
      }
      if (complete) {
        return defaultValue;
      }

      Future<V> persistedData = getFutureForKey(key);
      try (Closeable scope = scopedReadState()) {
        V persistedValue = persistedData.get();
        if (persistedValue == null) {
          negativeCache.add(structKey);
          return defaultValue;
        }
        cachedValues.put(structKey, persistedValue);
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
      StructuralKey<K> structKey = asStructuralKey(key);
      if (!localRemovals.contains(structKey)
          && !negativeCache.contains(structKey)
          && !cachedValues.containsKey(structKey)
          && !complete) {
        WindmillMap.this.getFutureForKey(key);
      }
      return this;
    }
  }
}
