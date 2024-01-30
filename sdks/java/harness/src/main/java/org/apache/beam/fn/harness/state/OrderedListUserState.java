package org.apache.beam.fn.harness.state;

import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkArgument;
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkState;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import org.apache.beam.fn.harness.Cache;
import org.apache.beam.fn.harness.Caches;
import org.apache.beam.fn.harness.state.StateFetchingIterators.CachingStateIterable;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.OrderedListStateUpdateRequest;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.StateClearRequest;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.StateGetRequest;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.StateKey;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.StateRequest;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.StateResponse;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.OrderedListStateGetRequest;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.OrderedListStateGetResponse;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.OrderedListRange;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.OrderedListEntry;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.util.ByteStringOutputStream;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TimestampedValue;
import org.apache.beam.sdk.values.TimestampedValue.TimestampedValueCoder;
import org.apache.beam.vendor.grpc.v1p60p1.com.google.protobuf.ByteString;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Throwables;
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
  // private final Cache<?, ?> cache;
  private final BeamFnStateClient beamFnStateClient;
  private final StateRequest request;
  private final Coder<T> valueCoder;
  private final TimestampedValueCoder<T> timestampedValueCoder;

  // Values retrieved from persistent storage
  // private CachingStateIterable<TimestampedValue<T>> oldValues = null;

  // Pending updates to persistent storage
  private NavigableMap<Instant, Collection<T>> pendingAdds = Maps.newTreeMap();
  private TreeRangeSet<Instant> pendingRemoves = TreeRangeSet.create();

  private boolean isCleared = false;
  private boolean isClosed = false;

  // // When keeping states in SDK, We will assign each TimestampedValue with a unique
  // // local id in case we have multiple same TimestampedValue.
  // class TimestampedValueWithId<T> {
  //   private final TimestampedValue<T> value;
  //   private final long localId;
  // }

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
    // this.cache = cache;
    this.beamFnStateClient = beamFnStateClient;
    this.valueCoder = valueCoder;
    this.timestampedValueCoder = TimestampedValueCoder.of(this.valueCoder);
    this.request =
        StateRequest.newBuilder().setInstructionId(instructionId).setStateKey(stateKey).build();
  }

  // We will have an in-memory data structure to keep tracking any new added
  // values. While keeping in-memory, the TimestampedValue will be assigned a
  // unique local id.
  public void add(TimestampedValue<T> value) {
    checkState(
        !isClosed,
        "OrderedList user state is no longer usable because it is closed for %s",
        request.getStateKey());
    Instant timestamp = value.getTimestamp();
    pendingAdds.putIfAbsent(timestamp, new ArrayList<>());
    pendingAdds.get(timestamp).add(value.getValue());
  }

  // An OrderedListStateGetRequest will be issued to runners. Final result should
  // be a combination of OrderedStateGetResponse and in-memory added values.
  public Iterable<TimestampedValue<T>> readRange(Instant minTimestamp, Instant limitTimestamp) {
    checkState(
        !isClosed,
        "OrderedList user state is no longer usable because it is closed for %s",
        request.getStateKey());

    // Convert from {t:[v1, v2],s:[v3,v4]} to [tv1, tv2, sv3, sv4]
    Iterable<TimestampedValue<T>> valuesInRange = Iterables.concat(
        Iterables.transform(pendingAdds.subMap(minTimestamp, limitTimestamp).entrySet(),
            (kv) -> Iterables.transform(kv.getValue(),
                (v) -> TimestampedValue.of(v, kv.getKey()))));

    if (!isCleared) {
      StateRequest.Builder getRequestBuilder = this.request.toBuilder();
      getRequestBuilder
          .getOrderedListGetBuilder()
          .getRangeBuilder()
          .setStart(minTimestamp.getMillis())
          .setEnd(limitTimestamp.getMillis());
      CachingStateIterable<TimestampedValue<T>> oldValues =
          StateFetchingIterators.readAllAndDecodeStartingFrom(
              Caches.noop(), this.beamFnStateClient, getRequestBuilder.build(),
              this.timestampedValueCoder);

      Iterable<TimestampedValue<T>> oldValuesAfterRemoval =
          Iterables.filter(oldValues, v -> !pendingRemoves.contains(v.getTimestamp()));

      return Iterables.mergeSorted(ImmutableList.of(oldValuesAfterRemoval,
          valuesInRange), Comparator.comparing(TimestampedValue::getTimestamp));
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

  // We will have an in-memory data structure to keep tracking all removed ranges.
  // New added values will also be removed if the timestamp is within the range.
  public void clearRange(Instant minTimestamp, Instant limitTimestamp) {
    checkState(
        !isClosed,
        "OrderedList user state is no longer usable because it is closed for %s",
        request.getStateKey());

    // Remove items in the specific range from the list of items to be added
    pendingAdds.subMap(minTimestamp, true, limitTimestamp, false).clear();
    if (!isCleared)
      pendingRemoves.add(Range.range(minTimestamp, BoundType.CLOSED, limitTimestamp, BoundType.OPEN));
  }

  public void clear() {
    checkState(
        !isClosed,
        "OrderedList user state is no longer usable because it is closed for %s",
        request.getStateKey());
    isCleared = true;
    pendingAdds.clear();
    pendingRemoves.clear();
  }

  // We construct and issue OrderedListStateUpdate requests to runners.
  public void asyncClose() throws Exception {
    isClosed = true;

    OrderedListStateUpdateRequest.Builder updateRequestBuilder = OrderedListStateUpdateRequest.newBuilder();
    if (!pendingRemoves.isEmpty()) {
      updateRequestBuilder
          .addAllDeletes(Iterables.transform(pendingRemoves.asRanges(),
              (r) -> OrderedListRange.newBuilder()
                  .setStart(r.lowerEndpoint().getMillis())
                  .setEnd(r.upperEndpoint().getMillis()).build()));
      pendingRemoves.clear();
    }

    if (!pendingAdds.isEmpty()) {
      for (Entry<Instant, Collection<T>> entry : pendingAdds.entrySet()) {
        updateRequestBuilder
            .addAllInserts(Iterables.transform(entry.getValue(),
                (v) -> OrderedListEntry.newBuilder()
                    .setSortKey(entry.getKey().getMillis())
                    .setData(encodeValue(v)).build()));
      }
      pendingAdds.clear();
    }

    if (updateRequestBuilder.getDeletesCount() > 0 || updateRequestBuilder.getInsertsCount() > 0) {
      StateRequest.Builder stateRequest = this.request.toBuilder();
      stateRequest.setOrderedListUpdate(updateRequestBuilder);

      CompletableFuture<StateResponse> response = beamFnStateClient.handle(stateRequest);
      if (!response.get().getError().isEmpty())
        throw new IllegalStateException(response.get().getError());
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
