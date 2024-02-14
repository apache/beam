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
import java.util.Collection;
import java.util.Comparator;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.concurrent.CompletableFuture;
import org.apache.beam.fn.harness.Cache;
import org.apache.beam.fn.harness.Caches;
import org.apache.beam.fn.harness.state.StateFetchingIterators.CachingStateIterable;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.OrderedListEntry;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.OrderedListRange;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.OrderedListStateUpdateRequest;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.StateKey;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.StateRequest;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.StateResponse;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.fn.stream.PrefetchableIterable;
import org.apache.beam.sdk.fn.stream.PrefetchableIterables;
import org.apache.beam.sdk.util.ByteStringOutputStream;
import org.apache.beam.sdk.values.TimestampedValue;
import org.apache.beam.sdk.values.TimestampedValue.TimestampedValueCoder;
import org.apache.beam.vendor.grpc.v1p60p1.com.google.protobuf.ByteString;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.BoundType;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Iterables;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Maps;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Range;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.TreeRangeSet;
import org.joda.time.Instant;

/**
 * An implementation of a bag user state that utilizes the Beam Fn State API to fetch, clear and
 * persist values.
 *
 * <p>Calling {@link #asyncClose()} schedules any required persistence changes. This object should
 * no longer be used after it is closed.
 *
 * <p>TODO: Move to an async persist model where persistence is signalled based upon cache memory
 * pressure and its need to flush.
 */
public class OrderedListUserState<T> {
  private final BeamFnStateClient beamFnStateClient;
  private final StateRequest request;
  private final Coder<T> valueCoder;
  private final TimestampedValueCoder<T> timestampedValueCoder;
  // Pending updates to persistent storage
  private NavigableMap<Instant, Collection<T>> pendingAdds = Maps.newTreeMap();
  private TreeRangeSet<Instant> pendingRemoves = TreeRangeSet.create();

  private boolean isCleared = false;
  private boolean isClosed = false;

  public OrderedListUserState(
      Cache<?, ?> cache,
      BeamFnStateClient beamFnStateClient,
      String instructionId,
      StateKey stateKey,
      Coder<T> valueCoder) {
    checkArgument(
        stateKey.hasOrderedListUserState(),
        "Expected OrderedListUserState StateKey but received %s.",
        stateKey);
    this.beamFnStateClient = beamFnStateClient;
    this.valueCoder = valueCoder;
    this.timestampedValueCoder = TimestampedValueCoder.of(this.valueCoder);
    this.request =
        StateRequest.newBuilder().setInstructionId(instructionId).setStateKey(stateKey).build();
  }

  public void add(TimestampedValue<T> value) {
    checkState(
        !isClosed,
        "OrderedList user state is no longer usable because it is closed for %s",
        request.getStateKey());
    Instant timestamp = value.getTimestamp();
    pendingAdds.putIfAbsent(timestamp, new ArrayList<>());
    pendingAdds.get(timestamp).add(value.getValue());
  }

  public Iterable<TimestampedValue<T>> readRange(Instant minTimestamp, Instant limitTimestamp) {
    checkState(
        !isClosed,
        "OrderedList user state is no longer usable because it is closed for %s",
        request.getStateKey());

    // Store pendingAdds whose sort key is in the query range and values are truncated by the
    // current size. The values (collections) of pendingAdds are kept, so that they will still be
    // accessible in pre-existing iterables even after:
    //   (1) a sort key is added to or removed from pendingAdds, or
    //   (2) a new value is added to an existing sort key
    ArrayList<PrefetchableIterable<TimestampedValue<T>>> pendingAddsInRange = new ArrayList<>();
    for (Entry<Instant, Collection<T>> kv :
        pendingAdds.subMap(minTimestamp, limitTimestamp).entrySet()) {
      pendingAddsInRange.add(
          PrefetchableIterables.limit(
              Iterables.transform(kv.getValue(), (v) -> TimestampedValue.of(v, kv.getKey())),
              kv.getValue().size()));
    }
    Iterable<TimestampedValue<T>> valuesInRange = Iterables.concat(pendingAddsInRange);

    if (!isCleared) {
      StateRequest.Builder getRequestBuilder = this.request.toBuilder();
      getRequestBuilder
          .getOrderedListGetBuilder()
          .getRangeBuilder()
          .setStart(minTimestamp.getMillis())
          .setEnd(limitTimestamp.getMillis());

      // TODO: consider use cache here
      CachingStateIterable<TimestampedValue<T>> persistentValues =
          StateFetchingIterators.readAllAndDecodeStartingFrom(
              Caches.noop(),
              this.beamFnStateClient,
              getRequestBuilder.build(),
              this.timestampedValueCoder);

      // Make a snapshot of the current pendingRemoves and use them to filter persistent values.
      // The values of pendingRemoves are kept, so that they will still be accessible in
      // pre-existing iterables even after a sort key is removed.
      TreeRangeSet<Instant> pendingRemovesSnapshot = TreeRangeSet.create(pendingRemoves);
      Iterable<TimestampedValue<T>> persistentValuesAfterRemoval =
          Iterables.filter(
              persistentValues, v -> !pendingRemovesSnapshot.contains(v.getTimestamp()));

      return Iterables.mergeSorted(
          ImmutableList.of(persistentValuesAfterRemoval, valuesInRange),
          Comparator.comparing(TimestampedValue::getTimestamp));
    }

    return valuesInRange;
  }

  public Iterable<TimestampedValue<T>> read() {
    checkState(
        !isClosed,
        "OrderedList user state is no longer usable because it is closed for %s",
        request.getStateKey());

    return readRange(Instant.ofEpochMilli(Long.MIN_VALUE), Instant.ofEpochMilli(Long.MAX_VALUE));
  }

  public void clearRange(Instant minTimestamp, Instant limitTimestamp) {
    checkState(
        !isClosed,
        "OrderedList user state is no longer usable because it is closed for %s",
        request.getStateKey());

    // Remove items (in a collection) in the specific range from pendingAdds.
    // The old values of the removed sub map are kept, so that they will still be accessible in
    // pre-existing iterables even after the sort key is cleared.
    pendingAdds.subMap(minTimestamp, true, limitTimestamp, false).clear();
    if (!isCleared) {
      pendingRemoves.add(
          Range.range(minTimestamp, BoundType.CLOSED, limitTimestamp, BoundType.OPEN));
    }
  }

  public void clear() {
    checkState(
        !isClosed,
        "OrderedList user state is no longer usable because it is closed for %s",
        request.getStateKey());
    isCleared = true;
    // Create a new object for pendingRemoves and clear the mappings in pendingAdds.
    // The entire tree range set of pendingRemoves and the old values in the pendingAdds are kept,
    // so that they will still be accessible in pre-existing iterables even after the state is
    // cleared.
    pendingRemoves = TreeRangeSet.create();
    pendingAdds.clear();
  }

  public void asyncClose() throws Exception {
    isClosed = true;

    OrderedListStateUpdateRequest.Builder updateRequestBuilder =
        OrderedListStateUpdateRequest.newBuilder();
    if (!pendingRemoves.isEmpty()) {
      updateRequestBuilder.addAllDeletes(
          Iterables.transform(
              pendingRemoves.asRanges(),
              (r) ->
                  OrderedListRange.newBuilder()
                      .setStart(r.lowerEndpoint().getMillis())
                      .setEnd(r.upperEndpoint().getMillis())
                      .build()));
      pendingRemoves.clear();
    }

    if (!pendingAdds.isEmpty()) {
      for (Entry<Instant, Collection<T>> entry : pendingAdds.entrySet()) {
        updateRequestBuilder.addAllInserts(
            Iterables.transform(
                entry.getValue(),
                (v) ->
                    OrderedListEntry.newBuilder()
                        .setSortKey(entry.getKey().getMillis())
                        .setData(encodeValue(v))
                        .build()));
      }
      pendingAdds.clear();
    }

    if (updateRequestBuilder.getDeletesCount() > 0 || updateRequestBuilder.getInsertsCount() > 0) {
      StateRequest.Builder stateRequest = this.request.toBuilder();
      stateRequest.setOrderedListUpdate(updateRequestBuilder);

      CompletableFuture<StateResponse> response = beamFnStateClient.handle(stateRequest);
      if (!response.get().getError().isEmpty()) {
        throw new IllegalStateException(response.get().getError());
      }
    }
  }

  private ByteString encodeValue(T value) {
    try {
      ByteStringOutputStream output = new ByteStringOutputStream();
      valueCoder.encode(value, output);
      return output.toByteString();
    } catch (IOException | RuntimeException e) {
      throw new IllegalStateException(
          String.format(
              "Failed to encode values for ordered list user state id %s.",
              request.getStateKey().getOrderedListUserState().getUserStateId()),
          e);
    }
  }
}
