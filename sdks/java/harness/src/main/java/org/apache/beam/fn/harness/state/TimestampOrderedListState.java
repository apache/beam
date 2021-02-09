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

import com.google.auto.value.AutoValue;
import java.util.Comparator;
import java.util.Iterator;
import java.util.SortedSet;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.StateRequest;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.values.TimestampedValue;
import org.apache.beam.vendor.grpc.v1p26p0.com.google.protobuf.ByteString;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Iterables;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Range;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.RangeSet;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Sets;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.TreeRangeSet;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Instant;

public class TimestampOrderedListState<T> {

  private final BeamFnStateClient beamFnStateClient;
  private final StateRequest request;
  private final Coder<T> valueCoder;
  // Most of implementations are taken from WindmillStateInternals.
  private boolean cleared = false;
  private SortedSet<TimestampedValueWithId<T>> pendingAdds =
      Sets.newTreeSet(TimestampedValueWithId.COMPARATOR);

  private RangeSet<Instant> pendingDeletes = TreeRangeSet.create();

  static final Instant MAX_TIMESTAMP = BoundedWindow.TIMESTAMP_MAX_VALUE;
  static final Instant MIN_TIMESTAMP = BoundedWindow.TIMESTAMP_MIN_VALUE;

  @AutoValue
  abstract static class TimestampedValueWithId<T> {
    private static final Comparator<TimestampedValueWithId<?>> COMPARATOR =
        Comparator.<TimestampedValueWithId<?>, Instant>comparing(v -> v.getValue().getTimestamp())
            .thenComparing(TimestampedValueWithId::getLocalId);

    public abstract TimestampedValue<T> getValue();

    public abstract long getLocalId();

    static <T> TimestampedValueWithId<T> of(TimestampedValue<T> value, long id) {
      return new AutoValue_TimestampOrderedListState_TimestampedValueWithId<>(value, id);
    }

    static <T> TimestampedValueWithId<T> bound(Instant timestamp) {
      return of(TimestampedValue.of(null, timestamp), Long.MIN_VALUE);
    }
  }

  public TimestampOrderedListState(
      BeamFnStateClient beamFnStateClient,
      String instructionId,
      String ptransformId,
      String stateId,
      ByteString encodedWindow,
      ByteString encodedKey,
      Coder<T> valueCoder) {
    this.beamFnStateClient = beamFnStateClient;
    this.valueCoder = valueCoder;
    StateRequest.Builder requestBuilder = StateRequest.newBuilder();
    requestBuilder
        .setInstructionId(instructionId)
        .getStateKeyBuilder()
        .getOrderedListStateBuilder()
        .setTransformId(ptransformId)
        .setUserStateId(stateId)
        .setWindow(encodedKey)
        .setKey(encodedKey);
    request = requestBuilder.build();
  }

  private SortedSet<TimestampedValueWithId<T>> getPendingAddRange(
      @Nullable Instant minTimestamp, @Nullable Instant limitTimestamp) {
    SortedSet<TimestampedValueWithId<T>> pendingInRange = pendingAdds;
    if (minTimestamp != null && limitTimestamp != null) {
      pendingInRange =
          pendingInRange.subSet(
              TimestampedValueWithId.bound(minTimestamp),
              TimestampedValueWithId.bound(limitTimestamp));
    } else if (minTimestamp == null && limitTimestamp != null) {
      pendingInRange = pendingInRange.headSet(TimestampedValueWithId.bound(limitTimestamp));
    } else if (limitTimestamp == null && minTimestamp != null) {
      pendingInRange = pendingInRange.tailSet(TimestampedValueWithId.bound(minTimestamp));
    }
    return pendingInRange;
  }

  public void add(TimestampedValue<T> value) {
    pendingAdds.add(TimestampedValueWithId.of(value, pendingAdds.size()));
  }

  public Iterable<TimestampedValue<T>> read() {
    return readRange(null, null);
  }

  private Iterable<TimestampedValue<T>> fetchDataFromStateRequest(
      Instant minTimestamp, Instant limitTimestamp) {}

  public Iterable<TimestampedValue<T>> readRange(Instant minTimestamp, Instant limitTimestamp) {
    SortedSet<TimestampedValueWithId<T>> pendingInRange =
        getPendingAddRange(minTimestamp, limitTimestamp);
    Iterable<TimestampedValueWithId<T>> data =
        new Iterable<TimestampedValueWithId<T>>() {
          Iterable<TimestampedValue<T>> statesFromResponse =
              fetchDataFromStateRequest(minTimestamp, limitTimestamp);

          @Override
          public Iterator<TimestampedValueWithId<T>> iterator() {
            return new Iterator<TimestampedValueWithId<T>>() {
              private Iterator<TimestampedValue<T>> iter = statesFromResponse.iterator();
              private long currentId = pendingAdds.size();

              @Override
              public boolean hasNext() {
                return iter.hasNext();
              }

              @Override
              public TimestampedValueWithId<T> next() {
                return TimestampedValueWithId.of(iter.next(), currentId++);
              }
            };
          }
        };
    Iterable<TimestampedValueWithId<T>> includingAdds =
        Iterables.mergeSorted(
            ImmutableList.of(data, pendingInRange), TimestampedValueWithId.COMPARATOR);
    Iterable<TimestampedValue<T>> fullIterable =
        Iterables.filter(
            Iterables.transform(includingAdds, TimestampedValueWithId::getValue),
            tv -> !pendingDeletes.contains(tv.getTimestamp()));
    return fullIterable;
  }

  public void clearRange(Instant minTimestamp, Instant limitTimestamp) {
    getPendingAddRange(minTimestamp, limitTimestamp).clear();
    pendingDeletes.add(Range.closedOpen(minTimestamp, limitTimestamp));
  }

  public void clear() {
    cleared = true;
    pendingDeletes.clear();
    pendingAdds.clear();
  }

  public void asyncClose() throws Exception {}
}
