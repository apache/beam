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
import java.util.Collections;
import java.util.Iterator;
import java.util.SortedSet;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import org.apache.beam.runners.core.StateNamespace;
import org.apache.beam.runners.core.StateTable;
import org.apache.beam.runners.core.StateTag;
import org.apache.beam.runners.dataflow.worker.WindmillTimeUtils;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.state.OrderedListState;
import org.apache.beam.sdk.state.ReadableState;
import org.apache.beam.sdk.util.ByteStringOutputStream;
import org.apache.beam.sdk.values.TimestampedValue;
import org.apache.beam.vendor.grpc.v1p60p1.com.google.protobuf.ByteString;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Iterables;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Range;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.RangeSet;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Sets;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.TreeRangeSet;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.util.concurrent.Futures;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Instant;

@SuppressWarnings({
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
public class WindmillOrderedList<T> extends SimpleWindmillState implements OrderedListState<T> {
  // The default proto values for SortedListRange correspond to the minimum and maximum
  // timestamps.
  static final long MIN_TS_MICROS = Windmill.SortedListRange.getDefaultInstance().getStart();
  static final long MAX_TS_MICROS = Windmill.SortedListRange.getDefaultInstance().getLimit();
  private final ByteString stateKey;
  private final String stateFamily;
  private final Coder<T> elemCoder;
  // We need to sort based on timestamp, but we need objects with the same timestamp to be treated
  // as unique. We can't use a MultiSet as we can't construct a comparator that uniquely
  // identifies objects,
  // so we construct a unique in-memory long ids for each element.
  private final SortedSet<TimestampedValueWithId<T>> pendingAdds =
      Sets.newTreeSet(TimestampedValueWithId.COMPARATOR);
  private final RangeSet<Instant> pendingDeletes = TreeRangeSet.create();
  private final IdTracker idTracker;
  private boolean complete;
  private boolean cleared = false;

  WindmillOrderedList(
      StateTable derivedStateTable,
      StateNamespace namespace,
      StateTag<OrderedListState<T>> spec,
      String stateFamily,
      Coder<T> elemCoder,
      boolean isNewKey) {

    this.stateKey = encodeKey(namespace, spec);
    this.stateFamily = stateFamily;
    this.elemCoder = elemCoder;
    this.complete = isNewKey;
    this.idTracker = new IdTracker(derivedStateTable, namespace, spec);
  }

  @Override
  public Iterable<TimestampedValue<T>> read() {
    return readRange(null, null);
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

  @Override
  public Iterable<TimestampedValue<T>> readRange(
      @Nullable Instant minTimestamp, @Nullable Instant limitTimestamp) {
    idTracker.readLater();

    final Future<Iterable<TimestampedValue<T>>> future = getFuture(minTimestamp, limitTimestamp);
    try (Closeable ignored = scopedReadState()) {
      SortedSet<TimestampedValueWithId<T>> pendingInRange =
          getPendingAddRange(minTimestamp, limitTimestamp);

      // Transform the return iterator, so it has the same type as pendingAdds. We need to ensure
      // that the ids don't overlap with any in pendingAdds, so begin with pendingAdds.size().
      Iterable<TimestampedValueWithId<T>> data =
          new Iterable<TimestampedValueWithId<T>>() {
            // Anything returned from windmill that has been deleted should be ignored.
            private final Iterable<TimestampedValue<T>> iterable =
                Iterables.filter(future.get(), tv -> !pendingDeletes.contains(tv.getTimestamp()));

            @Override
            public Iterator<TimestampedValueWithId<T>> iterator() {
              return new Iterator<TimestampedValueWithId<T>>() {
                private final Iterator<TimestampedValue<T>> iter = iterable.iterator();
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

      // TODO(reuvenlax): If we have a known bounded amount of data, cache known ranges.
      return Iterables.transform(includingAdds, TimestampedValueWithId::getValue);
    } catch (InterruptedException | ExecutionException | IOException e) {
      if (e instanceof InterruptedException) {
        Thread.currentThread().interrupt();
      }
      throw new RuntimeException("Unable to read state", e);
    }
  }

  @Override
  public void clear() {
    cleared = true;
    complete = true;
    pendingAdds.clear();
    pendingDeletes.clear();
    try {
      idTracker.clear();
    } catch (ExecutionException | InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void clearRange(Instant minTimestamp, Instant limitTimestamp) {
    getPendingAddRange(minTimestamp, limitTimestamp).clear();
    pendingDeletes.add(Range.closedOpen(minTimestamp, limitTimestamp));
  }

  @Override
  public void add(TimestampedValue<T> value) {
    // We use the current size of the container as the in-memory id. This works because
    // pendingAdds is completely
    // cleared when it is processed (otherwise we could end up with duplicate elements in the same
    // container). These
    // are not the ids that will be sent to windmill.
    pendingAdds.add(TimestampedValueWithId.of(value, pendingAdds.size()));
    // Leave pendingDeletes alone. Since we can have multiple values with the same timestamp, we
    // may still need
    // overlapping deletes to remove previous entries at this timestamp.
  }

  @Override
  public ReadableState<Boolean> isEmpty() {
    return new ReadableState<Boolean>() {
      @Override
      public ReadableState<Boolean> readLater() {
        WindmillOrderedList.this.readLater();
        return this;
      }

      @Override
      public Boolean read() {
        return Iterables.isEmpty(WindmillOrderedList.this.read());
      }
    };
  }

  @Override
  public OrderedListState<T> readLater() {
    return readRangeLater(null, null);
  }

  @Override
  @SuppressWarnings("FutureReturnValueIgnored")
  public OrderedListState<T> readRangeLater(
      @Nullable Instant minTimestamp, @Nullable Instant limitTimestamp) {
    idTracker.readLater();
    getFuture(minTimestamp, limitTimestamp);
    return this;
  }

  @Override
  public Windmill.WorkItemCommitRequest persistDirectly(WindmillStateCache.ForKeyAndFamily cache)
      throws IOException {
    Windmill.WorkItemCommitRequest.Builder commitBuilder =
        Windmill.WorkItemCommitRequest.newBuilder();
    Windmill.TagSortedListUpdateRequest.Builder updatesBuilder =
        commitBuilder
            .addSortedListUpdatesBuilder()
            .setStateFamily(cache.getStateFamily())
            .setTag(stateKey);
    try {
      if (cleared) {
        // Default range.
        updatesBuilder.addDeletesBuilder().build();
        cleared = false;
      }

      if (!pendingAdds.isEmpty()) {
        // TODO(reuvenlax): Once we start caching data, we should remove this line. We have it
        // here now
        // because once we persist
        // added data we forget about it from the cache, so the object is no longer complete.
        complete = false;

        Windmill.TagSortedListInsertRequest.Builder insertBuilder =
            updatesBuilder.addInsertsBuilder();
        idTracker.add(
            pendingAdds,
            (elem, id) -> {
              try {
                ByteStringOutputStream elementStream = new ByteStringOutputStream();
                elemCoder.encode(elem.getValue(), elementStream, Coder.Context.OUTER);
                insertBuilder.addEntries(
                    Windmill.SortedListEntry.newBuilder()
                        .setValue(elementStream.toByteString())
                        .setSortKey(
                            WindmillTimeUtils.harnessToWindmillTimestamp(elem.getTimestamp()))
                        .setId(id));
              } catch (IOException e) {
                throw new RuntimeException(e);
              }
            });
        pendingAdds.clear();
        insertBuilder.build();
      }

      if (!pendingDeletes.isEmpty()) {
        for (Range<Instant> range : pendingDeletes.asRanges()) {
          Windmill.TagSortedListDeleteRequest.Builder deletesBuilder =
              updatesBuilder.addDeletesBuilder();
          deletesBuilder.setRange(
              Windmill.SortedListRange.newBuilder()
                  .setStart(WindmillTimeUtils.harnessToWindmillTimestamp(range.lowerEndpoint()))
                  .setLimit(WindmillTimeUtils.harnessToWindmillTimestamp(range.upperEndpoint())));
          deletesBuilder.build();
          idTracker.remove(range);
        }
        pendingDeletes.clear();
      }
    } catch (ExecutionException | InterruptedException e) {
      throw new RuntimeException(e);
    }
    return commitBuilder.buildPartial();
  }

  private Future<Iterable<TimestampedValue<T>>> getFuture(
      @Nullable Instant minTimestamp, @Nullable Instant limitTimestamp) {
    long startSortKey =
        minTimestamp != null
            ? WindmillTimeUtils.harnessToWindmillTimestamp(minTimestamp)
            : MIN_TS_MICROS;
    long limitSortKey =
        limitTimestamp != null
            ? WindmillTimeUtils.harnessToWindmillTimestamp(limitTimestamp)
            : MAX_TS_MICROS;

    if (complete) {
      // Right now we don't cache any data, so complete means an empty list.
      // TODO(reuvenlax): change this once we start caching data.
      return Futures.immediateFuture(Collections.emptyList());
    }
    return reader.orderedListFuture(
        Range.closedOpen(startSortKey, limitSortKey), stateKey, stateFamily, elemCoder);
  }
}
