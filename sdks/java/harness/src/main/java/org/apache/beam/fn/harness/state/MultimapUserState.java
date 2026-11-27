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
package org.apache.beam.fn.harness.state;

import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkArgument;
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkState;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.beam.fn.harness.Cache;
import org.apache.beam.fn.harness.Caches;
import org.apache.beam.fn.harness.state.StateFetchingIterators.CachingStateIterable;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.StateAppendRequest;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.StateClearRequest;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.StateKey;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.StateRequest;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.IterableCoder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.fn.stream.PrefetchableIterable;
import org.apache.beam.sdk.fn.stream.PrefetchableIterables;
import org.apache.beam.sdk.fn.stream.PrefetchableIterator;
import org.apache.beam.sdk.util.ByteStringOutputStream;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.vendor.grpc.v1p69p0.com.google.protobuf.ByteString;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Iterables;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Maps;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Sets;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.math.LongMath;

/**
 * An implementation of a multimap user state that utilizes the Beam Fn State API to fetch, clear
 * and persist values.
 *
 * <p>Calling {@link #asyncClose()} schedules any required persistence changes. This object should
 * no longer be used after it is closed.
 */
public class MultimapUserState<K, V> {

  private final Cache<?, ?> cache;
  private final BeamFnStateClient beamFnStateClient;
  private final Coder<K> mapKeyCoder;
  private final Coder<V> valueCoder;
  private final StateRequest keysStateRequest;
  private final StateRequest userStateRequest;
  private final CachingStateIterable<K> persistedKeys;
  private @Nullable CachingStateIterable<KV<K, Iterable<V>>> persistedEntries;

  private boolean isClosed;
  private boolean isCleared;
  // Pending updates to persistent storage
  private final HashMap<Object, K> pendingRemoves = Maps.newHashMap();
  private final HashMap<Object, KV<K, List<V>>> pendingAdds = Maps.newHashMap();
  // Values retrieved from persistent storage
  private final HashMap<Object, KV<K, CachingStateIterable<V>>> persistedValues = Maps.newHashMap();

  public MultimapUserState(
      Cache<?, ?> cache,
      BeamFnStateClient beamFnStateClient,
      String instructionId,
      StateKey stateKey,
      Coder<K> mapKeyCoder,
      Coder<V> valueCoder) {
    checkArgument(
        stateKey.hasMultimapKeysUserState(),
        "Expected MultimapKeysUserState StateKey but received %s.",
        stateKey);
    this.cache = cache;
    this.beamFnStateClient = beamFnStateClient;
    this.mapKeyCoder = mapKeyCoder;
    this.valueCoder = valueCoder;

    this.keysStateRequest =
        StateRequest.newBuilder().setInstructionId(instructionId).setStateKey(stateKey).build();
    this.persistedKeys =
        StateFetchingIterators.readAllAndDecodeStartingFrom(
            cache, beamFnStateClient, keysStateRequest, mapKeyCoder);

    StateRequest.Builder userStateRequestBuilder = StateRequest.newBuilder();
    userStateRequestBuilder
        .setInstructionId(instructionId)
        .getStateKeyBuilder()
        .getMultimapUserStateBuilder()
        .setTransformId(stateKey.getMultimapKeysUserState().getTransformId())
        .setUserStateId(stateKey.getMultimapKeysUserState().getUserStateId())
        .setWindow(stateKey.getMultimapKeysUserState().getWindow())
        .setKey(stateKey.getMultimapKeysUserState().getKey());
    this.userStateRequest = userStateRequestBuilder.build();
  }

  private CachingStateIterable<KV<K, Iterable<V>>> getPersistedEntries() {
    CachingStateIterable<KV<K, Iterable<V>>> entries = this.persistedEntries;
    if (entries == null) {
      StateKey.MultimapKeysUserState keysStateKey =
          keysStateRequest.getStateKey().getMultimapKeysUserState();
      StateRequest.Builder entriesStateRequestBuilder = StateRequest.newBuilder();
      entriesStateRequestBuilder
          .setInstructionId(keysStateRequest.getInstructionId())
          .getStateKeyBuilder()
          .getMultimapEntriesUserStateBuilder()
          .setTransformId(keysStateKey.getTransformId())
          .setUserStateId(keysStateKey.getUserStateId())
          .setWindow(keysStateKey.getWindow())
          .setKey(keysStateKey.getKey());
      entries =
          StateFetchingIterators.readAllAndDecodeStartingFrom(
              Caches.subCache(this.cache, "AllEntries"),
              beamFnStateClient,
              entriesStateRequestBuilder.build(),
              KvCoder.of(mapKeyCoder, IterableCoder.of(valueCoder)));
      this.persistedEntries = entries;
    }
    return entries;
  }

  public void clear() {
    checkState(
        !isClosed,
        "Multimap user state is no longer usable because it is closed for %s",
        keysStateRequest.getStateKey());

    isCleared = true;
    if (!persistedValues.isEmpty()) {
      persistedValues.clear();
    }
    if (!pendingRemoves.isEmpty()) {
      pendingRemoves.clear();
    }
    if (!pendingAdds.isEmpty()) {
      pendingAdds.clear();
    }
  }

  /*
   * Returns an iterable of the values associated with key in this multimap, if any.
   * If there are no values, this returns an empty collection, not null.
   */
  public PrefetchableIterable<V> get(K key) {
    checkState(
        !isClosed,
        "Multimap user state is no longer usable because it is closed for %s",
        keysStateRequest.getStateKey());

    Object structuralKey = mapKeyCoder.structuralValue(key);
    KV<K, List<V>> pendingAddValues = pendingAdds.get(structuralKey);

    PrefetchableIterable<V> pendingValues =
        pendingAddValues == null
            ? PrefetchableIterables.fromArray()
            : PrefetchableIterables.limit(
                pendingAddValues.getValue(), pendingAddValues.getValue().size());
    if (isCleared || pendingRemoves.containsKey(structuralKey)) {
      return pendingValues;
    }

    return PrefetchableIterables.concat(getPersistedValues(structuralKey, key), pendingValues);
  }

  @SuppressWarnings({
    "nullness" // TODO(https://github.com/apache/beam/issues/21068)
  })
  /*
   * Returns an iterables containing all distinct keys in this multimap.
   */
  public PrefetchableIterable<K> keys() {
    checkState(
        !isClosed,
        "Multimap user state is no longer usable because it is closed for %s",
        keysStateRequest.getStateKey());
    if (isCleared) {
      List<K> keys = new ArrayList<>(pendingAdds.size());
      for (Map.Entry<?, KV<K, List<V>>> entry : pendingAdds.entrySet()) {
        keys.add(entry.getValue().getKey());
      }
      return PrefetchableIterables.concat(keys);
    }

    Set<Object> pendingRemovesNow = new HashSet<>(pendingRemoves.keySet());
    Map<Object, K> pendingAddsNow = Maps.newHashMapWithExpectedSize(pendingAdds.size());
    for (Map.Entry<Object, KV<K, List<V>>> entry : pendingAdds.entrySet()) {
      pendingAddsNow.put(entry.getKey(), entry.getValue().getKey());
    }
    return new PrefetchableIterables.Default<K>() {
      @Override
      public PrefetchableIterator<K> createIterator() {
        return new PrefetchableIterator<K>() {
          PrefetchableIterator<K> persistedKeysIterator = persistedKeys.iterator();
          Iterator<K> pendingAddsNowIterator;
          boolean hasNext;
          K nextKey;

          @Override
          public boolean isReady() {
            return persistedKeysIterator.isReady();
          }

          @Override
          public void prefetch() {
            if (!isReady()) {
              persistedKeysIterator.prefetch();
            }
          }

          @Override
          public boolean hasNext() {
            if (hasNext) {
              return true;
            }

            while (persistedKeysIterator.hasNext()) {
              nextKey = persistedKeysIterator.next();
              Object nextKeyStructuralValue = mapKeyCoder.structuralValue(nextKey);
              if (!pendingRemovesNow.contains(nextKeyStructuralValue)) {
                // Remove all keys that we will visit when passing over the persistedKeysIterator,
                // so we do not revisit them when passing over the pendingAddsNowIterator
                pendingAddsNow.remove(nextKeyStructuralValue);
                hasNext = true;
                return true;
              }
            }

            if (pendingAddsNowIterator == null) {
              pendingAddsNowIterator = pendingAddsNow.values().iterator();
            }
            while (pendingAddsNowIterator.hasNext()) {
              nextKey = pendingAddsNowIterator.next();
              hasNext = true;
              return true;
            }

            return false;
          }

          @Override
          public K next() {
            if (!hasNext()) {
              throw new NoSuchElementException();
            }
            hasNext = false;
            return nextKey;
          }
        };
      }
    };
  }

  @SuppressWarnings({
    "nullness" // TODO(https://github.com/apache/beam/issues/21068)
  })
  /*
   * Returns an Iterable containing all <K, V> entries in this multimap.
   */
  public PrefetchableIterable<Map.Entry<K, V>> entries() {
    checkState(
        !isClosed,
        "Multimap user state is no longer usable because it is closed for %s",
        keysStateRequest.getStateKey());
    // Make a deep copy of pendingAdds so this iterator represents a snapshot of state at the time
    // it was created.
    Map<Object, KV<K, List<V>>> pendingAddsNow =
        Maps.newHashMapWithExpectedSize(pendingAdds.size());
    for (Map.Entry<Object, KV<K, List<V>>> entry : pendingAdds.entrySet()) {
      pendingAddsNow.put(
          entry.getKey(),
          KV.of(entry.getValue().getKey(), new ArrayList<>(entry.getValue().getValue())));
    }
    if (isCleared) {
      return PrefetchableIterables.maybePrefetchable(
          Iterables.concat(
              Iterables.transform(
                  pendingAddsNow.entrySet(),
                  entry ->
                      Iterables.transform(
                          entry.getValue().getValue(),
                          value -> Maps.immutableEntry(entry.getValue().getKey(), value)))));
    }

    // Make a deep copy of pendingRemoves so this iterator represents a snapshot of state at the
    // time it was created.
    Set<Object> pendingRemovesNow = new HashSet<>(pendingRemoves.keySet());
    CachingStateIterable<KV<K, Iterable<V>>> entriesIterable = getPersistedEntries();
    return new PrefetchableIterables.Default<Map.Entry<K, V>>() {
      @Override
      public PrefetchableIterator<Map.Entry<K, V>> createIterator() {
        return new PrefetchableIterator<Map.Entry<K, V>>() {
          // We can get the same key multiple times from persistedEntries in the case that its
          // values are paginated across multiple pages. Keep track of which keys we've seen, so we
          // only add in pendingAdds once (with the first page). We'll also use it to return all
          // keys not on the backend at the end of the iterator.
          Set<Object> seenKeys = Sets.newHashSet();
          final PrefetchableIterator<Map.Entry<K, V>> allEntries =
              PrefetchableIterables.concat(
                      Iterables.concat(
                          Iterables.filter(
                              Iterables.transform(
                                  entriesIterable,
                                  entry -> {
                                    final Object structuralKey =
                                        mapKeyCoder.structuralValue(entry.getKey());
                                    if (pendingRemovesNow.contains(structuralKey)) {
                                      return null;
                                    }
                                    // add returns true if we haven't seen this key yet.
                                    if (seenKeys.add(structuralKey)) {
                                      KV<K, List<V>> pendingAdd = pendingAddsNow.get(structuralKey);
                                      if (pendingAdd != null) {
                                        return PrefetchableIterables.concat(
                                            Iterables.transform(
                                                pendingAdd.getValue(),
                                                pendingVal ->
                                                    Maps.immutableEntry(
                                                        entry.getKey(), pendingVal)),
                                            Iterables.transform(
                                                entry.getValue(),
                                                value ->
                                                    Maps.immutableEntry(entry.getKey(), value)));
                                      }
                                    }
                                    return Iterables.transform(
                                        entry.getValue(),
                                        value -> Maps.immutableEntry(entry.getKey(), value));
                                  }),
                              Objects::nonNull)),
                      Iterables.concat(
                          Iterables.filter(
                              Iterables.transform(
                                  pendingAddsNow.entrySet(),
                                  entry -> {
                                    if (seenKeys.contains(entry.getKey())) {
                                      return null;
                                    }
                                    return Iterables.transform(
                                        entry.getValue().getValue(),
                                        value ->
                                            Maps.immutableEntry(entry.getValue().getKey(), value));
                                  }),
                              Objects::nonNull)))
                  .iterator();

          @Override
          public boolean isReady() {
            return allEntries.isReady();
          }

          @Override
          public void prefetch() {
            if (!isReady()) {
              allEntries.prefetch();
            }
          }

          @Override
          public boolean hasNext() {
            return allEntries.hasNext();
          }

          @Override
          public Map.Entry<K, V> next() {
            return allEntries.next();
          }
        };
      }
    };
  }

  /*
   * Store a key-value pair in the multimap.
   * Allows duplicate key-value pairs.
   */
  public void put(K key, V value) {
    checkState(
        !isClosed,
        "Multimap user state is no longer usable because it is closed for %s",
        keysStateRequest.getStateKey());
    Object keyStructuralValue = mapKeyCoder.structuralValue(key);
    KV<K, List<V>> entry = pendingAdds.get(keyStructuralValue);
    if (entry == null) {
      List<V> values = new ArrayList<>();
      values.add(value);
      pendingAdds.put(keyStructuralValue, KV.of(key, values));
    } else {
      entry.getValue().add(value);
    }
  }

  /*
   * Removes all values for this key in the multimap.
   */
  public void remove(K key) {
    checkState(
        !isClosed,
        "Multimap user state is no longer usable because it is closed for %s",
        keysStateRequest.getStateKey());
    Object keyStructuralValue = mapKeyCoder.structuralValue(key);
    pendingAdds.remove(keyStructuralValue);
    if (!isCleared) {
      pendingRemoves.put(keyStructuralValue, key);
    }
  }

  /** Persists pending mutations to the state API client and updates the local state cache. */
  @SuppressWarnings({
    "FutureReturnValueIgnored",
    "nullness" // TODO(https://github.com/apache/beam/issues/21068)
  })
  public void asyncClose() throws Exception {
    checkState(
        !isClosed,
        "Multimap user state is no longer usable because it is closed for %s",
        keysStateRequest.getStateKey());
    isClosed = true;
    // No mutations necessary
    if (!isCleared && pendingRemoves.isEmpty() && pendingAdds.isEmpty()) {
      return;
    }

    ByteStringOutputStream encodeOut =
        (pendingRemoves.isEmpty() && pendingAdds.isEmpty()) ? null : new ByteStringOutputStream();

    // Clear currently persisted key-values, saving encoded requests so each removed key is only
    // encoded once across the clear RPC and cache update.
    Map<Object, StateRequest> encodedRemoves = Collections.emptyMap();
    if (isCleared) {
      beamFnStateClient.handle(
          keysStateRequest.toBuilder().setClear(StateClearRequest.getDefaultInstance()));
    } else if (!pendingRemoves.isEmpty()) {
      encodedRemoves = Maps.newHashMapWithExpectedSize(pendingRemoves.size());
      for (Map.Entry<Object, K> entry : pendingRemoves.entrySet()) {
        StateRequest request = createUserStateRequest(entry.getValue(), encodeOut);
        encodedRemoves.put(entry.getKey(), request);
        beamFnStateClient.handle(
            request.toBuilder().setClear(StateClearRequest.getDefaultInstance()));
      }
    }

    List<K> pendingAddsKeys =
        pendingAdds.isEmpty() ? Collections.emptyList() : new ArrayList<>(pendingAdds.size());
    long pendingAddsKeysBytes = 0;
    List<PendingCacheUpdate<V>> pendingCacheUpdates = null;

    // Persist pending key-values and record encoded byte sizes for cache weighting.
    if (!pendingAdds.isEmpty()) {
      for (Map.Entry<Object, KV<K, List<V>>> entry : pendingAdds.entrySet()) {
        Object structuralKey = entry.getKey();
        K key = entry.getValue().getKey();
        List<V> values = entry.getValue().getValue();
        pendingAddsKeys.add(key);

        StateRequest existingRemoveRequest = encodedRemoves.get(structuralKey);
        ByteString mapKey;
        StateRequest requestForCache;
        if (existingRemoveRequest != null) {
          requestForCache = existingRemoveRequest;
          mapKey = existingRemoveRequest.getStateKey().getMultimapUserState().getMapKey();
        } else {
          mapKey = encodeKey(key, encodeOut);
          requestForCache = null;
        }
        pendingAddsKeysBytes = LongMath.saturatedAdd(pendingAddsKeysBytes, mapKey.size());
        ByteString encodedValues = encodeValues(values, encodeOut);

        if (isCleared) {
          StateRequest request = buildUserStateRequest(mapKey);
          beamFnStateClient.handle(
              request.toBuilder()
                  .setAppend(StateAppendRequest.newBuilder().setData(encodedValues)));
          if (pendingCacheUpdates == null) {
            pendingCacheUpdates = new ArrayList<>(pendingAdds.size());
          }
          KV<K, CachingStateIterable<V>> existing = persistedValues.get(structuralKey);
          CachingStateIterable<V> iterable =
              existing != null ? existing.getValue() : createPersistedValuesIterable(request);
          pendingCacheUpdates.add(
              new PendingCacheUpdate<>(iterable, values, encodedValues.size(), true));
        } else {
          StateRequest.Builder appendBuilder;
          if (requestForCache != null) {
            appendBuilder = requestForCache.toBuilder();
          } else {
            appendBuilder = userStateRequest.toBuilder();
            appendBuilder.getStateKeyBuilder().getMultimapUserStateBuilder().setMapKey(mapKey);
          }
          beamFnStateClient.handle(
              appendBuilder.setAppend(StateAppendRequest.newBuilder().setData(encodedValues)));

          if (requestForCache != null) {
            // Key was removed then re-added in the same bundle: clearAndAppend replaces its cached
            // values directly without emitting a separate empty block first.
            KV<K, CachingStateIterable<V>> existing = persistedValues.get(structuralKey);
            CachingStateIterable<V> iterable =
                existing != null
                    ? existing.getValue()
                    : createPersistedValuesIterable(requestForCache);
            if (pendingCacheUpdates == null) {
              pendingCacheUpdates = new ArrayList<>();
            }
            pendingCacheUpdates.add(
                new PendingCacheUpdate<>(iterable, values, encodedValues.size(), true));
          } else {
            KV<K, CachingStateIterable<V>> existing = persistedValues.get(structuralKey);
            if (existing != null) {
              if (pendingCacheUpdates == null) {
                pendingCacheUpdates = new ArrayList<>();
              }
              pendingCacheUpdates.add(
                  new PendingCacheUpdate<>(
                      existing.getValue(), values, encodedValues.size(), false));
            }
          }
        }
      }
    }

    updateCache(pendingAddsKeys, pendingAddsKeysBytes, encodedRemoves, pendingCacheUpdates);
  }

  private static final class PendingCacheUpdate<V> {
    final CachingStateIterable<V> iterable;
    final List<V> values;
    final long encodedValuesBytes;
    final boolean replaceAll;

    PendingCacheUpdate(
        CachingStateIterable<V> iterable,
        List<V> values,
        long encodedValuesBytes,
        boolean replaceAll) {
      this.iterable = iterable;
      this.values = values;
      this.encodedValuesBytes = encodedValuesBytes;
      this.replaceAll = replaceAll;
    }
  }

  private void updateCache(
      List<K> pendingAddsKeys,
      long pendingAddsKeysBytes,
      Map<Object, StateRequest> encodedRemoves,
      @Nullable List<PendingCacheUpdate<V>> pendingCacheUpdates) {
    // Approximating element weight as encodedBytes + count * REFERENCE_SIZE matches the read path
    // in DataStreams.DataStreamDecoder#decodeFromChunkBoundaryToChunkBoundary. This avoids jamm
    // object-graph traversal on every close (tradeoff: object-heavy/compressed coders may report a
    // lower weight than live Java heap size).
    long keysWeight =
        LongMath.saturatedAdd(
            pendingAddsKeysBytes,
            LongMath.saturatedMultiply(pendingAddsKeys.size(), Caches.REFERENCE_SIZE));

    if (isCleared) {
      // Note this takes ownership of pendingAddsKeys. This object is no longer used after close.
      if (pendingAddsKeys.isEmpty()) {
        persistedKeys.clearAndAppend(Collections.emptyList());
      } else {
        persistedKeys.clearAndAppend(pendingAddsKeys, keysWeight);
      }

      // Since the map was cleared we can cache all pending values since they are complete.
      if (pendingCacheUpdates != null) {
        for (int i = 0; i < pendingCacheUpdates.size(); i++) {
          PendingCacheUpdate<V> update = pendingCacheUpdates.get(i);
          long valuesWeight =
              LongMath.saturatedAdd(
                  update.encodedValuesBytes,
                  LongMath.saturatedMultiply(update.values.size(), Caches.REFERENCE_SIZE));
          update.iterable.clearAndAppend(update.values, valuesWeight);
        }
      }
    } else {
      // The cast to Set<Object> is necessary since the checker framework would like to further
      // limit the type to Set<@KeyFor("this.pendingRemoves") Object> which is incompatible with
      // the API being remove(Set<Object>). We don't want to limit the API for remove either.
      persistedKeys.remove((Set<Object>) pendingRemoves.keySet());
      if (!pendingAddsKeys.isEmpty()) {
        persistedKeys.append(pendingAddsKeys, keysWeight);
      }

      // Clear cached values for keys that were removed and NOT subsequently re-added (re-added
      // keys are replaced directly in the loop below).
      for (Map.Entry<Object, StateRequest> entry : encodedRemoves.entrySet()) {
        if (!pendingAdds.containsKey(entry.getKey())) {
          KV<K, CachingStateIterable<V>> existing = persistedValues.get(entry.getKey());
          CachingStateIterable<V> iterable =
              existing != null
                  ? existing.getValue()
                  : createPersistedValuesIterable(entry.getValue());
          iterable.clearAndAppend(Collections.emptyList());
        }
      }

      // Update cached values for loaded or re-added keys.
      if (pendingCacheUpdates != null) {
        for (int i = 0; i < pendingCacheUpdates.size(); i++) {
          PendingCacheUpdate<V> update = pendingCacheUpdates.get(i);
          long valuesWeight =
              LongMath.saturatedAdd(
                  update.encodedValuesBytes,
                  LongMath.saturatedMultiply(update.values.size(), Caches.REFERENCE_SIZE));
          if (update.replaceAll) {
            update.iterable.clearAndAppend(update.values, valuesWeight);
          } else {
            update.iterable.append(update.values, valuesWeight);
          }
        }
      }
    }
  }

  private ByteString encodeValues(Iterable<V> values, ByteStringOutputStream output) {
    try {
      for (V value : values) {
        valueCoder.encode(value, output);
      }
      return output.toByteStringAndReset();
    } catch (IOException e) {
      throw new IllegalStateException(
          String.format(
              "Failed to encode values for multimap user state id %s.",
              keysStateRequest.getStateKey().getMultimapKeysUserState().getUserStateId()),
          e);
    }
  }

  private ByteString encodeKey(K key, ByteStringOutputStream output) {
    try {
      mapKeyCoder.encode(key, output);
      // Use toByteStringAndReset() so small encoded keys are trimmed to exact-sized
      // LiteralByteStrings instead of retaining the backing ByteStringOutputStream buffer.
      return output.toByteStringAndReset();
    } catch (IOException e) {
      throw new IllegalStateException(
          String.format(
              "Failed to encode key for multimap user state id %s.",
              keysStateRequest.getStateKey().getMultimapKeysUserState().getUserStateId()),
          e);
    }
  }

  private StateRequest buildUserStateRequest(ByteString mapKey) {
    StateRequest.Builder request = userStateRequest.toBuilder();
    request.getStateKeyBuilder().getMultimapUserStateBuilder().setMapKey(mapKey);
    return request.build();
  }

  private StateRequest createUserStateRequest(K key, ByteStringOutputStream output) {
    return buildUserStateRequest(encodeKey(key, output));
  }

  private CachingStateIterable<V> createPersistedValuesIterable(StateRequest request) {
    return StateFetchingIterators.readAllAndDecodeStartingFrom(
        Caches.subCache(
            cache, "ValuesForKey", request.getStateKey().getMultimapUserState().getMapKey()),
        beamFnStateClient,
        request,
        valueCoder);
  }

  private CachingStateIterable<V> getPersistedValues(Object structuralKey, K key) {
    KV<K, CachingStateIterable<V>> existing = persistedValues.get(structuralKey);
    if (existing != null) {
      return existing.getValue();
    }
    StateRequest request = createUserStateRequest(key, new ByteStringOutputStream());
    CachingStateIterable<V> iterable = createPersistedValuesIterable(request);
    persistedValues.put(structuralKey, KV.of(key, iterable));
    return iterable;
  }
}
