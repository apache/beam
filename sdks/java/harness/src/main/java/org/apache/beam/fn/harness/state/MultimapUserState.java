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

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkState;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.StateAppendRequest;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.StateClearRequest;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.StateRequest;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.vendor.grpc.v1p36p0.com.google.protobuf.ByteString;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ArrayListMultimap;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Iterables;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Lists;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Maps;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Multimap;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Sets;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * An implementation of a multimap user state that utilizes the Beam Fn State API to fetch, clear
 * and persist values.
 *
 * <p>Calling {@link #asyncClose()} schedules any required persistence changes. This object should
 * no longer be used after it is closed.
 *
 * <p>TODO: Move to an async persist model where persistence is signalled based upon cache memory
 * pressure and its need to flush.
 *
 * <p>TODO: Support block level caching and prefetch.
 */
public class MultimapUserState<K, V> {

  private final BeamFnStateClient beamFnStateClient;
  private final Coder<K> mapKeyCoder;
  private final Coder<V> valueCoder;
  private final String stateId;
  private final StateRequest keysStateRequest;
  private final StateRequest userStateRequest;

  private boolean isClosed;
  private boolean isCleared;
  // Pending updates to persistent storage
  private HashSet<K> pendingRemoves = Sets.newHashSet();
  private HashMap<K, List<V>> pendingAdds = Maps.newHashMap();
  // Map keys with no values in persistent storage
  private HashSet<K> negativeCache = Sets.newHashSet();
  // Values retrieved from persistent storage
  private Multimap<K, V> persistedValues = ArrayListMultimap.create();
  private @Nullable Iterable<K> persistedKeys = null;

  public MultimapUserState(
      BeamFnStateClient beamFnStateClient,
      String instructionId,
      String pTransformId,
      String stateId,
      ByteString encodedWindow,
      ByteString encodedKey,
      Coder<K> mapKeyCoder,
      Coder<V> valueCoder) {
    this.beamFnStateClient = beamFnStateClient;
    this.mapKeyCoder = mapKeyCoder;
    this.valueCoder = valueCoder;
    this.stateId = stateId;

    StateRequest.Builder keysStateRequestBuilder = StateRequest.newBuilder();
    keysStateRequestBuilder
        .setInstructionId(instructionId)
        .getStateKeyBuilder()
        .getMultimapKeysUserStateBuilder()
        .setTransformId(pTransformId)
        .setUserStateId(stateId)
        .setKey(encodedKey)
        .setWindow(encodedWindow);
    keysStateRequest = keysStateRequestBuilder.build();

    StateRequest.Builder userStateRequestBuilder = StateRequest.newBuilder();
    userStateRequestBuilder
        .setInstructionId(instructionId)
        .getStateKeyBuilder()
        .getMultimapUserStateBuilder()
        .setTransformId(pTransformId)
        .setUserStateId(stateId)
        .setWindow(encodedWindow)
        .setKey(encodedKey);
    userStateRequest = userStateRequestBuilder.build();
  }

  public void clear() {
    checkState(
        !isClosed,
        "Multimap user state is no longer usable because it is closed for %s",
        keysStateRequest.getStateKey());

    isCleared = true;
    persistedValues = ArrayListMultimap.create();
    persistedKeys = null;
    pendingRemoves = Sets.newHashSet();
    pendingAdds = Maps.newHashMap();
    negativeCache = Sets.newHashSet();
  }

  /*
   * Returns an iterable of the values associated with key in this multimap, if any.
   * If there are no values, this returns an empty collection, not null.
   */
  public Iterable<V> get(K key) {
    checkState(
        !isClosed,
        "Multimap user state is no longer usable because it is closed for %s",
        keysStateRequest.getStateKey());

    List<V> pendingAddValues = pendingAdds.getOrDefault(key, Collections.emptyList());
    Collection<V> pendingValues =
        Collections.unmodifiableCollection(pendingAddValues.subList(0, pendingAddValues.size()));
    if (isCleared || pendingRemoves.contains(key)) {
      return pendingValues;
    }

    Iterable<V> persistedValues = getPersistedValues(key);
    return Iterables.concat(persistedValues, pendingValues);
  }

  @SuppressWarnings({
    "nullness" // TODO(https://issues.apache.org/jira/browse/BEAM-12687)
  })
  /*
   * Returns an iterables containing all distinct keys in this multimap.
   */
  public Iterable<K> keys() {
    checkState(
        !isClosed,
        "Multimap user state is no longer usable because it is closed for %s",
        keysStateRequest.getStateKey());
    if (isCleared) {
      return Collections.unmodifiableCollection(Lists.newArrayList(pendingAdds.keySet()));
    }

    Set<K> keys = Sets.newHashSet(getPersistedKeys());
    keys.removeAll(pendingRemoves);
    keys.addAll(pendingAdds.keySet());
    return Collections.unmodifiableCollection(keys);
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
    pendingAdds.putIfAbsent(key, new ArrayList<>());
    pendingAdds.get(key).add(value);
  }

  /*
   * Removes all values for this key in the multimap.
   */
  public void remove(K key) {
    checkState(
        !isClosed,
        "Multimap user state is no longer usable because it is closed for %s",
        keysStateRequest.getStateKey());
    pendingAdds.remove(key);
    if (!isCleared) {
      pendingRemoves.add(key);
    }
  }

  @SuppressWarnings({
    "FutureReturnValueIgnored",
    "nullness" // TODO(https://issues.apache.org/jira/browse/BEAM-12687)
  })
  // Update data in persistent store
  public void asyncClose() throws Exception {
    checkState(
        !isClosed,
        "Multimap user state is no longer usable because it is closed for %s",
        keysStateRequest.getStateKey());
    isClosed = true;
    // Nothing to persist
    if (!isCleared && pendingRemoves.isEmpty() && pendingAdds.isEmpty()) {
      return;
    }

    // Clear currently persisted key-values
    if (isCleared) {
      beamFnStateClient
          .handle(keysStateRequest.toBuilder().setClear(StateClearRequest.getDefaultInstance()))
          .get();
    } else if (!pendingRemoves.isEmpty()) {
      for (K key : pendingRemoves) {
        beamFnStateClient
            .handle(
                createUserStateRequest(key)
                    .toBuilder()
                    .setClear(StateClearRequest.getDefaultInstance()))
            .get();
      }
    }

    // Persist pending key-values
    if (!pendingAdds.isEmpty()) {
      for (Map.Entry<K, List<V>> entry : pendingAdds.entrySet()) {
        beamFnStateClient
            .handle(
                createUserStateRequest(entry.getKey())
                    .toBuilder()
                    .setAppend(
                        StateAppendRequest.newBuilder().setData(encodeValues(entry.getValue()))))
            .get();
      }
    }
  }

  private ByteString encodeValues(Iterable<V> values) {
    try {
      ByteString.Output output = ByteString.newOutput();
      for (V value : values) {
        valueCoder.encode(value, output);
      }
      return output.toByteString();
    } catch (IOException e) {
      throw new IllegalStateException(
          String.format("Failed to encode values for multimap user state id %s.", stateId), e);
    }
  }

  private StateRequest createUserStateRequest(K key) {
    try {
      ByteString.Output output = ByteString.newOutput();
      mapKeyCoder.encode(key, output);
      StateRequest.Builder request = userStateRequest.toBuilder();
      request.getStateKeyBuilder().getMultimapUserStateBuilder().setMapKey(output.toByteString());
      return request.build();
    } catch (IOException e) {
      throw new IllegalStateException(
          String.format("Failed to encode key for multimap user state id %s.", stateId), e);
    }
  }

  private Iterable<V> getPersistedValues(K key) {
    if (negativeCache.contains(key)) {
      return Collections.emptyList();
    }

    if (persistedValues.get(key).isEmpty()) {
      Iterable<V> values =
          StateFetchingIterators.readAllAndDecodeStartingFrom(
              beamFnStateClient, createUserStateRequest(key), valueCoder);
      if (Iterables.isEmpty(values)) {
        negativeCache.add(key);
      }
      persistedValues.putAll(key, values);
    }
    return Iterables.unmodifiableIterable(persistedValues.get(key));
  }

  private Iterable<K> getPersistedKeys() {
    checkState(!isCleared);
    if (persistedKeys == null) {
      Iterable<K> keys =
          StateFetchingIterators.readAllAndDecodeStartingFrom(
              beamFnStateClient, keysStateRequest, mapKeyCoder);
      persistedKeys = Iterables.unmodifiableIterable(keys);
    }
    return persistedKeys;
  }
}
