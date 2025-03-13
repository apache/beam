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
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkNotNull;
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkState;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.concurrent.CompletableFuture;
import org.apache.beam.fn.harness.Cache;
import org.apache.beam.fn.harness.Caches;
import org.apache.beam.fn.harness.state.StateFetchingIterators.CachingStateIterable;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.StateClearRequest;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.StateKey;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.StateRequest;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.StateResponse;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.LengthPrefixCoder;
import org.apache.beam.sdk.coders.StructuredCoder;
import org.apache.beam.sdk.coders.VarLongCoder;
import org.apache.beam.sdk.fn.stream.PrefetchableIterable;
import org.apache.beam.sdk.fn.stream.PrefetchableIterables;
import org.apache.beam.sdk.util.ByteStringOutputStream;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TimestampedValue;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.TypeParameter;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.BoundType;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Iterables;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Maps;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Range;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.TreeRangeSet;
import org.joda.time.Instant;

/**
 * An implementation of an ordered list user state that utilizes the Beam Fn State API to fetch,
 * clear and persist values.
 *
 * <p>Calling {@link #asyncClose()} schedules any required persistence changes. This object should
 * no longer be used after it is closed.
 *
 * <p>TODO: Move to an async persist model where persistence is signalled based upon cache memory
 * pressure and its need to flush.
 */
public class OrderedListUserState<T> {
  private final BeamFnStateClient beamFnStateClient;
  private final StateRequest requestTemplate;
  private final TimestampedValueCoder<T> timestampedValueCoder;
  // Pending updates to persistent storage
  // (a) The elements in pendingAdds are the ones that should be added to the persistent storage
  //     during the next async_close(). It doesn't include the ones that are removed by
  //     clear_range() or clear() after the last add.
  // (b) The elements in pendingRemoves are the sort keys that should be removed from the persistent
  //     storage.
  // (c) When syncing local copy with persistent storage, pendingRemoves are performed first and
  //     then pendingAdds. Switching this order may result in wrong results, because a value added
  //     later could be removed from an earlier clear.
  private NavigableMap<Instant, Collection<T>> pendingAdds = Maps.newTreeMap();
  private TreeRangeSet<Instant> pendingRemoves = TreeRangeSet.create();

  private boolean isCleared = false;
  private boolean isClosed = false;

  public static class TimestampedValueCoder<T> extends StructuredCoder<TimestampedValue<T>> {

    private final Coder<T> valueCoder;

    // Internally, a TimestampedValue is encoded with a KvCoder, where the key is encoded with
    // a VarLongCoder and the value is encoded with a LengthPrefixCoder.
    // Refer to the comment in StateAppendRequest
    // (org/apache/beam/model/fn_execution/v1/beam_fn_api.proto) for more detail.
    private final KvCoder<Long, T> internalKvCoder;

    public static <T> OrderedListUserState.TimestampedValueCoder<T> of(Coder<T> valueCoder) {
      return new OrderedListUserState.TimestampedValueCoder<>(valueCoder);
    }

    @Override
    public Object structuralValue(TimestampedValue<T> value) {
      Object structuralValue = valueCoder.structuralValue(value.getValue());
      return TimestampedValue.of(structuralValue, value.getTimestamp());
    }

    @SuppressWarnings("unchecked")
    TimestampedValueCoder(Coder<T> valueCoder) {
      this.valueCoder = checkNotNull(valueCoder);
      this.internalKvCoder = KvCoder.of(VarLongCoder.of(), LengthPrefixCoder.of(valueCoder));
    }

    @Override
    public void encode(TimestampedValue<T> timestampedValue, OutputStream outStream)
        throws IOException {
      internalKvCoder.encode(
          KV.of(timestampedValue.getTimestamp().getMillis(), timestampedValue.getValue()),
          outStream);
    }

    @Override
    public TimestampedValue<T> decode(InputStream inStream) throws IOException {
      KV<Long, T> kv = internalKvCoder.decode(inStream);
      return TimestampedValue.of(kv.getValue(), Instant.ofEpochMilli(kv.getKey()));
    }

    @Override
    public void verifyDeterministic() throws NonDeterministicException {
      verifyDeterministic(
          this, "TimestampedValueCoder requires a deterministic valueCoder", valueCoder);
    }

    @Override
    public List<? extends Coder<?>> getCoderArguments() {
      return Arrays.<Coder<?>>asList(valueCoder);
    }

    public Coder<T> getValueCoder() {
      return valueCoder;
    }

    @Override
    public TypeDescriptor<TimestampedValue<T>> getEncodedTypeDescriptor() {
      return new TypeDescriptor<TimestampedValue<T>>() {}.where(
          new TypeParameter<T>() {}, valueCoder.getEncodedTypeDescriptor());
    }

    @Override
    public List<? extends Coder<?>> getComponents() {
      return Collections.singletonList(valueCoder);
    }
  }

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
    this.timestampedValueCoder = TimestampedValueCoder.of(valueCoder);
    this.requestTemplate =
        StateRequest.newBuilder().setInstructionId(instructionId).setStateKey(stateKey).build();
  }

  public void add(TimestampedValue<T> value) {
    checkState(
        !isClosed,
        "OrderedList user state is no longer usable because it is closed for %s",
        requestTemplate.getStateKey());
    Instant timestamp = value.getTimestamp();
    pendingAdds.putIfAbsent(timestamp, new ArrayList<>());
    pendingAdds.get(timestamp).add(value.getValue());
  }

  public Iterable<TimestampedValue<T>> readRange(Instant minTimestamp, Instant limitTimestamp) {
    checkState(
        !isClosed,
        "OrderedList user state is no longer usable because it is closed for %s",
        requestTemplate.getStateKey());

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
      StateRequest.Builder getRequestBuilder = this.requestTemplate.toBuilder();
      getRequestBuilder
          .getStateKeyBuilder()
          .getOrderedListUserStateBuilder()
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
      // The values of pendingRemoves are copied, so that they will still be accessible in
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
        requestTemplate.getStateKey());

    return readRange(Instant.ofEpochMilli(Long.MIN_VALUE), Instant.ofEpochMilli(Long.MAX_VALUE));
  }

  public void clearRange(Instant minTimestamp, Instant limitTimestamp) {
    checkState(
        !isClosed,
        "OrderedList user state is no longer usable because it is closed for %s",
        requestTemplate.getStateKey());

    // Remove items (in a collection) in the specific range from pendingAdds.
    // The old values of the removed sub map are kept, so that they will still be accessible in
    // pre-existing iterables even after the sort key is cleared.
    pendingAdds.subMap(minTimestamp, limitTimestamp).clear();
    if (!isCleared) {
      pendingRemoves.add(
          Range.range(minTimestamp, BoundType.CLOSED, limitTimestamp, BoundType.OPEN));
    }
  }

  public void clear() {
    checkState(
        !isClosed,
        "OrderedList user state is no longer usable because it is closed for %s",
        requestTemplate.getStateKey());
    isCleared = true;
    // Create a new object for pendingRemoves and clear the mappings in pendingAdds.
    // The entire tree range set of pendingRemoves and the old values in the pendingAdds are kept,
    // so that they will still be accessible in pre-existing iterables even after the state is
    // cleared.
    pendingRemoves = TreeRangeSet.create();
    pendingRemoves.add(
        Range.range(
            Instant.ofEpochMilli(Long.MIN_VALUE),
            BoundType.CLOSED,
            Instant.ofEpochMilli(Long.MAX_VALUE),
            BoundType.OPEN));
    pendingAdds.clear();
  }

  public void asyncClose() throws Exception {
    isClosed = true;

    if (!pendingRemoves.isEmpty()) {
      for (Range<Instant> r : pendingRemoves.asRanges()) {
        StateRequest.Builder stateRequest = this.requestTemplate.toBuilder();
        stateRequest.setClear(StateClearRequest.newBuilder().build());
        stateRequest
            .getStateKeyBuilder()
            .getOrderedListUserStateBuilder()
            .getRangeBuilder()
            .setStart(r.lowerEndpoint().getMillis())
            .setEnd(r.upperEndpoint().getMillis());

        CompletableFuture<StateResponse> response = beamFnStateClient.handle(stateRequest);
        if (!response.get().getError().isEmpty()) {
          throw new IllegalStateException(response.get().getError());
        }
      }
      pendingRemoves.clear();
    }

    if (!pendingAdds.isEmpty()) {
      ByteStringOutputStream outStream = new ByteStringOutputStream();

      for (Entry<Instant, Collection<T>> entry : pendingAdds.entrySet()) {
        for (T v : entry.getValue()) {
          TimestampedValue<T> tv = TimestampedValue.of(v, entry.getKey());
          try {
            timestampedValueCoder.encode(tv, outStream);
          } catch (IOException ex) {
            throw new RuntimeException(ex);
          }
        }
      }
      StateRequest.Builder stateRequest = this.requestTemplate.toBuilder();
      stateRequest.getAppendBuilder().setData(outStream.toByteString());

      CompletableFuture<StateResponse> response = beamFnStateClient.handle(stateRequest);
      if (!response.get().getError().isEmpty()) {
        throw new IllegalStateException(response.get().getError());
      }
      pendingAdds.clear();
    }
  }
}
