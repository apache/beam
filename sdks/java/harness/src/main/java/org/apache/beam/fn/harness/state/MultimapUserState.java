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
import java.util.Set;
import org.apache.beam.fn.harness.Cache;
import org.apache.beam.fn.harness.Caches;
import org.apache.beam.fn.harness.state.StateFetchingIterators.CachingStateIterable;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.StateAppendRequest;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.StateClearRequest;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.StateKey;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.StateRequest;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.fn.stream.PrefetchableIterable;
import org.apache.beam.sdk.fn.stream.PrefetchableIterables;
import org.apache.beam.sdk.fn.stream.PrefetchableIterator;
import org.apache.beam.sdk.util.ByteStringOutputStream;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.vendor.grpc.v1p60p1.com.google.protobuf.ByteString;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Maps;

/**
 * An implementation of a multimap user state that utilizes the Beam Fn State API to fetch, clear
 * and persist values.
 *
 * <p>Calling {@link #asyncClose()} schedules any required persistence changes. This object should
 * no longer be used after it is closed.
 *
 * <p>TODO: Move to an async persist model where persistence is signalled based upon cache memory
 * pressure and its need to flush.
 */
public class MultimapUserState<K, V> {

  private final Cache<?, ?> cache;
  private final BeamFnStateClient beamFnStateClient;
  private final Coder<K> mapKeyCoder;
  private final Coder<V> valueCoder;
  private final StateRequest keysStateRequest;
  private final StateRequest userStateRequest;
  private final CachingStateIterable<K> persistedKeys;

  private boolean isClosed;
  private boolean isCleared;
  // Pending updates to persistent storage
  private HashMap<Object, K> pendingRemoves = Maps.newHashMap();
  private HashMap<Object, KV<K, List<V>>> pendingAdds = Maps.newHashMap();
  // Values retrieved from persistent storage
  private HashMap<Object, KV<K, CachingStateIterable<V>>> persistedValues = Maps.newHashMap();

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

  public void clear() {
    checkState(
        !isClosed,
        "Multimap user state is no longer usable because it is closed for %s",
        keysStateRequest.getStateKey());

    isCleared = true;
    persistedValues = Maps.newHashMap();
    pendingRemoves = Maps.newHashMap();
    pendingAdds = Maps.newHashMap();
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
    Map<Object, K> pendingAddsNow = new HashMap<>();
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
                // Remove all keys that we will visit when passing over the persistedKeysIterator
                // so we do not revisit them when passing over the pendingAddsNowIterator
                if (pendingAddsNow.containsKey(nextKeyStructuralValue)) {
                  pendingAddsNow.remove(nextKeyStructuralValue);
                }
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
    pendingAdds.putIfAbsent(keyStructuralValue, KV.of(key, new ArrayList<>()));
    pendingAdds.get(keyStructuralValue).getValue().add(value);
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

  @SuppressWarnings({
    "FutureReturnValueIgnored",
    "nullness" // TODO(https://github.com/apache/beam/issues/21068)
  })
  // Update data in persistent store
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

    startStateApiWrites();
    updateCache();
  }

  @SuppressWarnings("FutureReturnValueIgnored")
  private void startStateApiWrites() {
    // Clear currently persisted key-values
    if (isCleared) {
      beamFnStateClient.handle(
          keysStateRequest.toBuilder().setClear(StateClearRequest.getDefaultInstance()));
    } else if (!pendingRemoves.isEmpty()) {
      for (K key : pendingRemoves.values()) {
        StateRequest request = createUserStateRequest(key);
        beamFnStateClient.handle(
            request.toBuilder().setClear(StateClearRequest.getDefaultInstance()));
      }
    }

    // Persist pending key-values
    if (!pendingAdds.isEmpty()) {
      for (KV<K, List<V>> entry : pendingAdds.values()) {
        StateRequest request = createUserStateRequest(entry.getKey());
        beamFnStateClient.handle(
            request
                .toBuilder()
                .setAppend(
                    StateAppendRequest.newBuilder().setData(encodeValues(entry.getValue()))));
      }
    }
  }

  private void updateCache() {
    List<K> pendingAddsKeys = new ArrayList<>(pendingAdds.size());
    for (KV<K, List<V>> entry : pendingAdds.values()) {
      pendingAddsKeys.add(entry.getKey());
    }

    if (isCleared) {
      // This will clear all keys and values since values is a sub-cache of keys. Note this
      // takes ownership of pendingAddKeys. This object is no longer used after it has been closed.
      persistedKeys.clearAndAppend(pendingAddsKeys);

      // Since the map was cleared we can add all the values that are pending since we know
      // that they must have been cleared.
      for (Map.Entry<Object, KV<K, List<V>>> entry : pendingAdds.entrySet()) {
        CachingStateIterable<V> iterable =
            getPersistedValues(entry.getKey(), entry.getValue().getKey());
        // Note this takes ownership of the list but this object is no longer used after it has
        // been closed.
        iterable.clearAndAppend(entry.getValue().getValue());
      }
    } else {
      // The cast to Set<Object> is necessary since the checker framework would like to further
      // limit the type to Set<@KeyFor("this.pendingRemoves") Object> which is incompatible with
      // the API being remove(Set<Object>). We don't want to limit the API for remove either.
      persistedKeys.remove((Set<Object>) pendingRemoves.keySet());
      persistedKeys.append(pendingAddsKeys);

      // For each removed key, we want to update the internal cache to clear its set of values
      for (Map.Entry<Object, K> entry : pendingRemoves.entrySet()) {
        CachingStateIterable<V> iterable = getPersistedValues(entry.getKey(), entry.getValue());
        iterable.clearAndAppend(Collections.emptyList());
      }

      // For each added key, try to update the internal cache with the set of values.
      for (Map.Entry<Object, KV<K, List<V>>> entry : pendingAdds.entrySet()) {
        KV<K, CachingStateIterable<V>> value = persistedValues.get(entry.getKey());
        // We don't do anything for keys that haven't been loaded since we have no knowledge whether
        // the key is empty or not.
        if (value != null) {
          value.getValue().append(entry.getValue().getValue());
        }
      }
    }
  }

  private ByteString encodeValues(Iterable<V> values) {
    try {
      ByteStringOutputStream output = new ByteStringOutputStream();
      for (V value : values) {
        valueCoder.encode(value, output);
      }
      return output.toByteString();
    } catch (IOException e) {
      throw new IllegalStateException(
          String.format(
              "Failed to encode values for multimap user state id %s.",
              keysStateRequest.getStateKey().getMultimapKeysUserState().getUserStateId()),
          e);
    }
  }

  private StateRequest createUserStateRequest(K key) {
    try {
      ByteStringOutputStream output = new ByteStringOutputStream();
      mapKeyCoder.encode(key, output);
      StateRequest.Builder request = userStateRequest.toBuilder();
      request.getStateKeyBuilder().getMultimapUserStateBuilder().setMapKey(output.toByteString());
      return request.build();
    } catch (IOException e) {
      throw new IllegalStateException(
          String.format(
              "Failed to encode key for multimap user state id %s.",
              keysStateRequest.getStateKey().getMultimapKeysUserState().getUserStateId()),
          e);
    }
  }

  private CachingStateIterable<V> getPersistedValues(Object structuralKey, K key) {
    return persistedValues
        .computeIfAbsent(
            structuralKey,
            unused -> {
              StateRequest request = createUserStateRequest(key);
              return KV.of(
                  key,
                  StateFetchingIterators.readAllAndDecodeStartingFrom(
                      Caches.subCache(
                          cache,
                          "ValuesForKey",
                          request.getStateKey().getMultimapUserState().getMapKey()),
                      beamFnStateClient,
                      request,
                      valueCoder));
            })
        .getValue();
  }
}
