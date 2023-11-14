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

import java.util.Comparator;
import java.util.Iterator;
import java.util.Map;
import java.util.SortedSet;
import java.util.concurrent.ExecutionException;
import java.util.function.BiConsumer;
import org.apache.beam.runners.core.StateNamespace;
import org.apache.beam.runners.core.StateTable;
import org.apache.beam.runners.core.StateTag;
import org.apache.beam.runners.core.StateTags;
import org.apache.beam.sdk.coders.InstantCoder;
import org.apache.beam.sdk.coders.MapCoder;
import org.apache.beam.sdk.coders.VarLongCoder;
import org.apache.beam.sdk.state.StateContexts;
import org.apache.beam.sdk.state.ValueState;
import org.apache.beam.sdk.values.TimestampedValue;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Maps;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Range;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.RangeSet;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.TreeRangeSet;
import org.joda.time.Duration;
import org.joda.time.Instant;

/**
 * Tracker for the ids used in an ordered list.
 *
 * <p>Windmill accepts an int64 id for each timestamped-element in the list. Unique elements are
 * identified by the pair of timestamp and id. This means that tow unique elements e1, e2 must have
 * different (ts1, id1), (ts2, id2) pairs. To accomplish this we bucket time into five-minute
 * buckets, and store a free list of ids available for each bucket.
 *
 * <p>When a timestamp range is deleted, we remove id tracking for elements in that range. In order
 * to handle the case where a range is deleted piecemeal, we track sub-range deletions for each
 * range. For example:
 *
 * <p>12:00 - 12:05 ids 12:05 - 12:10 ids
 *
 * <p>delete 12:00-12:06
 *
 * <p>12:00 - 12:05 *removed* 12:05 - 12:10 ids subranges deleted 12:05-12:06
 *
 * <p>delete 12:06 - 12:07
 *
 * <p>12:05 - 12:10 ids subranges deleted 12:05-12:07
 *
 * <p>delete 12:07 - 12:10
 *
 * <p>12:05 - 12:10 *removed*
 */
@SuppressWarnings("nullness" // TODO(https://github.com/apache/beam/issues/20497)
)
final class IdTracker {
  @VisibleForTesting static final String IDS_AVAILABLE_STR = "IdsAvailable";
  @VisibleForTesting static final String DELETIONS_STR = "Deletions";
  // Note that this previously was Long.MIN_VALUE but ids are unsigned when
  // sending to windmill for Streaming Engine. For updated appliance
  // pipelines with existing state, there may be negative ids.
  @VisibleForTesting static final long NEW_RANGE_MIN_ID = 0;

  @VisibleForTesting
  static final MapCoder<Range<Instant>, RangeSet<Long>> IDS_AVAILABLE_CODER =
      MapCoder.of(new RangeCoder<>(InstantCoder.of()), new RangeSetCoder<>(VarLongCoder.of()));

  @VisibleForTesting
  static final MapCoder<Range<Instant>, RangeSet<Instant>> SUBRANGE_DELETIONS_CODER =
      MapCoder.of(new RangeCoder<>(InstantCoder.of()), new RangeSetCoder<>(InstantCoder.of()));

  private static final long NEW_RANGE_MAX_ID = Long.MAX_VALUE;
  // We track ids on five-minute boundaries.
  private static final Duration RESOLUTION = Duration.standardMinutes(5);
  // A map from five-minute ranges to the set of ids available in that interval.
  private final ValueState<Map<Range<Instant>, RangeSet<Long>>> idsAvailableValue;
  // If a timestamp-range in the map has been partially cleared, the cleared intervals are stored
  // here.
  private final ValueState<Map<Range<Instant>, RangeSet<Instant>>> subRangeDeletionsValue;

  IdTracker(StateTable stateTable, StateNamespace namespace, StateTag<?> spec) {
    StateTag<ValueState<Map<Range<Instant>, RangeSet<Long>>>> idsAvailableTag =
        StateTags.makeSystemTagInternal(
            StateTags.value(spec.getId() + IDS_AVAILABLE_STR, IDS_AVAILABLE_CODER));
    StateTag<ValueState<Map<Range<Instant>, RangeSet<Instant>>>> subRangeDeletionsTag =
        StateTags.makeSystemTagInternal(
            StateTags.value(spec.getId() + DELETIONS_STR, SUBRANGE_DELETIONS_CODER));

    this.idsAvailableValue =
        stateTable.get(namespace, idsAvailableTag, StateContexts.nullContext());
    this.subRangeDeletionsValue =
        stateTable.get(namespace, subRangeDeletionsTag, StateContexts.nullContext());
  }

  static <ValueT extends Comparable<? super ValueT>>
      Map<Range<Instant>, RangeSet<ValueT>> newSortedRangeMap() {
    return Maps.newTreeMap(
        Comparator.<Range<Instant>, Instant>comparing(Range::lowerEndpoint)
            .thenComparing(Range::upperEndpoint));
  }

  private Range<Instant> getTrackedRange(Instant ts) {
    Instant snapped =
        new Instant(ts.getMillis() - ts.plus(RESOLUTION).getMillis() % RESOLUTION.getMillis());
    return Range.closedOpen(snapped, snapped.plus(RESOLUTION));
  }

  @SuppressWarnings("FutureReturnValueIgnored")
  void readLater() {
    idsAvailableValue.readLater();
    subRangeDeletionsValue.readLater();
  }

  Map<Range<Instant>, RangeSet<Long>> readIdsAvailable() {
    Map<Range<Instant>, RangeSet<Long>> idsAvailable = idsAvailableValue.read();
    return idsAvailable != null ? idsAvailable : newSortedRangeMap();
  }

  Map<Range<Instant>, RangeSet<Instant>> readSubRangeDeletions() {
    Map<Range<Instant>, RangeSet<Instant>> subRangeDeletions = subRangeDeletionsValue.read();
    return subRangeDeletions != null ? subRangeDeletions : newSortedRangeMap();
  }

  void clear() throws ExecutionException, InterruptedException {
    idsAvailableValue.clear();
    subRangeDeletionsValue.clear();
  }

  <T> void add(
      SortedSet<TimestampedValueWithId<T>> elements, BiConsumer<TimestampedValue<T>, Long> output)
      throws ExecutionException, InterruptedException {
    Range<Long> currentIdRange = null;
    long currentId = 0;

    Range<Instant> currentTsRange = null;
    RangeSet<Instant> currentTsRangeDeletions = null;

    Map<Range<Instant>, RangeSet<Long>> idsAvailable = readIdsAvailable();
    Map<Range<Instant>, RangeSet<Instant>> subRangeDeletions = readSubRangeDeletions();

    RangeSet<Long> availableIdsForTsRange = null;
    Iterator<Range<Long>> idRangeIter = null;
    RangeSet<Long> idsUsed = TreeRangeSet.create();
    for (TimestampedValueWithId<T> pendingAdd : elements) {
      // Since elements are in increasing ts order, often we'll be able to reuse the previous
      // iteration's range.
      if (currentTsRange == null
          || !currentTsRange.contains(pendingAdd.getValue().getTimestamp())) {
        if (availableIdsForTsRange != null) {
          // We're moving onto a new ts range. Remove all used ids
          availableIdsForTsRange.removeAll(idsUsed);
          idsUsed = TreeRangeSet.create();
        }

        // Lookup the range for the current timestamp.
        currentTsRange = getTrackedRange(pendingAdd.getValue().getTimestamp());
        // Lookup available ids for this timestamp range. If nothing there, we default to all ids
        // available.
        availableIdsForTsRange =
            idsAvailable.computeIfAbsent(
                currentTsRange,
                r ->
                    TreeRangeSet.create(
                        ImmutableList.of(Range.closedOpen(NEW_RANGE_MIN_ID, NEW_RANGE_MAX_ID))));
        idRangeIter = availableIdsForTsRange.asRanges().iterator();
        currentIdRange = null;
        currentTsRangeDeletions = subRangeDeletions.get(currentTsRange);
      }

      if (currentIdRange == null || currentId >= currentIdRange.upperEndpoint()) {
        // Move to the next range of free ids, and start assigning ranges from there.
        currentIdRange = idRangeIter.next();
        currentId = currentIdRange.lowerEndpoint();
      }

      if (currentTsRangeDeletions != null) {
        currentTsRangeDeletions.remove(
            Range.closedOpen(
                pendingAdd.getValue().getTimestamp(),
                pendingAdd.getValue().getTimestamp().plus(Duration.millis(1))));
      }
      idsUsed.add(Range.closedOpen(currentId, currentId + 1));
      output.accept(pendingAdd.getValue(), currentId++);
    }
    if (availableIdsForTsRange != null) {
      availableIdsForTsRange.removeAll(idsUsed);
    }
    writeValues(idsAvailable, subRangeDeletions);
  }

  // Remove a timestamp range. Returns ids freed up.
  void remove(Range<Instant> tsRange) throws ExecutionException, InterruptedException {
    Map<Range<Instant>, RangeSet<Long>> idsAvailable = readIdsAvailable();
    Map<Range<Instant>, RangeSet<Instant>> subRangeDeletions = readSubRangeDeletions();

    for (Range<Instant> current = getTrackedRange(tsRange.lowerEndpoint());
        current.lowerEndpoint().isBefore(tsRange.upperEndpoint());
        current = getTrackedRange(current.lowerEndpoint().plus(RESOLUTION))) {
      // TODO(reuvenlax): shouldn't need to iterate over all ranges.
      boolean rangeCleared;
      if (!tsRange.encloses(current)) {
        // This can happen if the beginning or the end of tsRange doesn't fall on a RESOLUTION
        // boundary. Since we are deleting a portion of a tracked range, track what we are deleting.
        RangeSet<Instant> rangeDeletions =
            subRangeDeletions.computeIfAbsent(current, r -> TreeRangeSet.create());
        rangeDeletions.add(tsRange.intersection(current));
        // If we ended up deleting the whole range, then we can simply remove it from the tracking
        // map.
        rangeCleared = rangeDeletions.encloses(current);
      } else {
        rangeCleared = true;
      }
      if (rangeCleared) {
        // Remove the range from both maps.
        idsAvailable.remove(current);
        subRangeDeletions.remove(current);
      }
    }
    writeValues(idsAvailable, subRangeDeletions);
  }

  private void writeValues(
      Map<Range<Instant>, RangeSet<Long>> idsAvailable,
      Map<Range<Instant>, RangeSet<Instant>> subRangeDeletions) {
    if (idsAvailable.isEmpty()) {
      idsAvailable.clear();
    } else {
      idsAvailableValue.write(idsAvailable);
    }
    if (subRangeDeletions.isEmpty()) {
      subRangeDeletionsValue.clear();
    } else {
      subRangeDeletionsValue.write(subRangeDeletions);
    }
  }
}
