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
package org.apache.beam.sdk.extensions.ordered.combiner;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.SortedMap;
import java.util.TreeMap;
import javax.annotation.Nullable;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.CustomCoder;
import org.apache.beam.sdk.coders.NullableCoder;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.coders.VarLongCoder;
import org.apache.beam.sdk.extensions.ordered.ContiguousSequenceRange;
import org.apache.commons.lang3.tuple.Pair;
import org.checkerframework.checker.initialization.qual.Initialized;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.UnknownKeyFor;
import org.joda.time.Instant;

/** Default accumulator used to combine sequence ranges. */
public class SequenceRangeAccumulator {

  private static Instant max(Instant a, Instant b) {
    return a.isAfter(b) ? a : b;
  }

  /**
   * The tree contains a set of non-overlapping contiguous ranges, where the key is the lower
   * inclusive start of the range, left value of the pair is the inclusive end of the range and the
   * right value of the pair is the maximum timestamp in the range.
   *
   * <p>The maximum timestamp is critical for the correctness of the ordered processing. During the
   * merge process the merged range is assigned the maximum timestamp of the two ranges that created
   * this new range.
   */
  private final TreeMap<Long, Pair<Long, Instant>> data = new TreeMap<>();

  private @Nullable Long initialSequence = null;

  public void add(long sequence, Instant timestamp, boolean isInitialSequence) {
    if (isInitialSequence && this.initialSequence != null && sequence != this.initialSequence) {
      throw new IllegalStateException(
          "There are different initial sequences detected: "
              + initialSequence
              + " and "
              + sequence);
    }

    if (sequence == Long.MAX_VALUE) {
      // This is an invalid value and DoFns will not process this element. This will also allow
      // to produce a ContiguousSequenceRange with the exclusive end value.
      return;
    }

    if (isInitialSequence) {
      this.initialSequence = sequence;
      clearRangesBelowInitialSequence(sequence, timestamp);
    } else if (initialSequence != null && sequence <= initialSequence) {
      // No need to add anything lower than the initial sequence to the accumulator.
      return;
    }

    long lowerBound = sequence, upperBound = sequence;

    Entry<Long, Pair<Long, Instant>> lowerRange = data.floorEntry(sequence);
    if (lowerRange != null) {
      long inclusiveUpperBoundary = lowerRange.getValue().getLeft();
      if (sequence <= inclusiveUpperBoundary) {
        // Duplicate. No need to adjust the timestamp.
        return;
      }

      if (inclusiveUpperBoundary + 1 == sequence) {
        // The new element extends the lower range. Remove the range.
        timestamp = max(timestamp, lowerRange.getValue().getValue());
        lowerBound = lowerRange.getKey();
        data.remove(lowerRange.getKey());
      }
    }

    long nextSequenceNumber = sequence + 1;
    Pair<Long, Instant> upperRange = data.get(nextSequenceNumber);
    if (upperRange != null) {
      // The new element will extend the upper range. Remove the range.
      timestamp = max(timestamp, upperRange.getRight());
      upperBound = upperRange.getLeft();
      data.remove(nextSequenceNumber);
    }

    data.put(lowerBound, Pair.of(upperBound, timestamp));
  }

  private void clearRangesBelowInitialSequence(long sequence, Instant timestamp) {
    // First, adjust the current range, if any
    Entry<Long, Pair<Long, Instant>> lowerRange = data.floorEntry(sequence);
    if (lowerRange != null
        && lowerRange.getKey() < sequence
        && lowerRange.getValue().getLeft() > sequence) {
      // The sequence is in the middle of the range. Adjust it.
      data.remove(lowerRange.getKey());
      data.put(
          sequence,
          Pair.of(
              lowerRange.getValue().getKey(), max(timestamp, lowerRange.getValue().getValue())));
    }
    data.subMap(Long.MIN_VALUE, sequence).clear();
  }

  public ContiguousSequenceRange largestContinuousRange() {
    if (initialSequence == null) {
      return ContiguousSequenceRange.EMPTY;
    }

    Entry<Long, Pair<Long, Instant>> firstEntry = data.firstEntry();
    if (firstEntry == null) {
      throw new IllegalStateException("First entry is null when initial sequence is set.");
    }
    Long start = firstEntry.getKey();
    Long end = firstEntry.getValue().getLeft();
    Instant latestTimestamp = firstEntry.getValue().getRight();
    // Upper bound is inclusive, but the ContiguousSequenceRange's end is exclusive.
    // The numeric overflow is prevented by dropping the value of Long.MAX.
    return ContiguousSequenceRange.of(start, end + 1, latestTimestamp);
  }

  public int numberOfRanges() {
    return data.size();
  }

  public void merge(SequenceRangeAccumulator another) {
    if (this.initialSequence != null
        && another.initialSequence != null
        && !this.initialSequence.equals(another.initialSequence)) {
      throw new IllegalStateException(
          "Two accumulators contain different initial sequences: "
              + this.initialSequence
              + " and "
              + another.initialSequence);
    }

    if (another.initialSequence != null) {
      long newInitialSequence = another.initialSequence;
      this.initialSequence = newInitialSequence;
      Entry<Long, Pair<Long, Instant>> firstEntry = another.data.firstEntry();
      if (firstEntry != null) {
        Instant timestampOfTheInitialRange = firstEntry.getValue().getRight();
        clearRangesBelowInitialSequence(newInitialSequence, timestampOfTheInitialRange);
      }
    }

    another
        .data
        .entrySet()
        .forEach(
            entry -> {
              long lowerBound = entry.getKey();
              long upperBound = entry.getValue().getLeft();
              if (this.initialSequence != null) {
                if (upperBound < initialSequence) {
                  // The whole range is below the initial sequence. Ignore it.
                  return;
                }
                if (lowerBound < initialSequence) {
                  // This will cause pruning of the range up to the initial sequence
                  lowerBound = this.initialSequence;
                }
              }

              Entry<Long, Pair<Long, Instant>> lowerRange = this.data.floorEntry(lowerBound);

              if (lowerRange != null) {
                if (lowerRange.getValue().getLeft() < lowerBound - 1) {
                  // Nothing to do. There is a lower non-adjacent range.
                } else {
                  // We found an overlapping range and will replace it with a new one
                  upperBound = Math.max(upperBound, lowerRange.getValue().getLeft());
                  lowerBound = lowerRange.getKey();
                }
              }

              Entry<Long, Pair<Long, Instant>> upperRange = this.data.floorEntry(upperBound + 1);
              if (upperRange == null
                  || (lowerRange != null
                      && Objects.equals(upperRange.getKey(), lowerRange.getKey()))) {
                // Nothing to do - either there is no adjacent upper range or it equals the lower
                // range
              } else {
                upperBound = Math.max(upperBound, upperRange.getValue().getLeft());
              }

              Instant latestTimestamp =
                  removeAllRanges(lowerBound, upperBound, entry.getValue().getRight());

              this.data.put(lowerBound, Pair.of(upperBound, latestTimestamp));
            });
  }

  private Instant removeAllRanges(long lowerBound, long upperBound, Instant currentTimestamp) {
    Instant result = currentTimestamp;
    SortedMap<Long, Pair<Long, Instant>> rangesToRemove = data.subMap(lowerBound, upperBound);
    for (Pair<Long, Instant> value : rangesToRemove.values()) {
      result = result.isAfter(value.getRight()) ? result : value.getRight();
    }
    rangesToRemove.clear();
    return result;
  }

  @Override
  public boolean equals(@Nullable Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof SequenceRangeAccumulator)) {
      return false;
    }
    SequenceRangeAccumulator that = (SequenceRangeAccumulator) o;
    return data.equals(that.data) && Objects.equals(initialSequence, that.initialSequence);
  }

  @Override
  public int hashCode() {
    return Objects.hash(data, initialSequence);
  }

  @Override
  public String toString() {
    return "SequenceRangeAccumulator{initialSequence=" + initialSequence + ", data=" + data + '}';
  }

  public static class SequenceRangeAccumulatorCoder extends CustomCoder<SequenceRangeAccumulator> {

    private static final SequenceRangeAccumulatorCoder INSTANCE =
        new SequenceRangeAccumulatorCoder();

    public static SequenceRangeAccumulatorCoder of() {
      return INSTANCE;
    }

    private SequenceRangeAccumulatorCoder() {}

    private final NullableCoder<Long> initialSequenceCoder = NullableCoder.of(VarLongCoder.of());
    private final VarIntCoder numberOfRangesCoder = VarIntCoder.of();
    private final VarLongCoder dataCoder = VarLongCoder.of();

    @Override
    public void encode(
        SequenceRangeAccumulator value, @UnknownKeyFor @NonNull @Initialized OutputStream outStream)
        throws @UnknownKeyFor @NonNull @Initialized CoderException, @UnknownKeyFor @NonNull
            @Initialized IOException {
      numberOfRangesCoder.encode(value.numberOfRanges(), outStream);
      initialSequenceCoder.encode(value.initialSequence, outStream);
      for (Entry<Long, Pair<Long, Instant>> entry : value.data.entrySet()) {
        dataCoder.encode(entry.getKey(), outStream);
        dataCoder.encode(entry.getValue().getLeft(), outStream);
        dataCoder.encode(entry.getValue().getRight().getMillis(), outStream);
      }
    }

    @Override
    public SequenceRangeAccumulator decode(
        @UnknownKeyFor @NonNull @Initialized InputStream inStream)
        throws @UnknownKeyFor @NonNull @Initialized CoderException, @UnknownKeyFor @NonNull
            @Initialized IOException {
      SequenceRangeAccumulator result = new SequenceRangeAccumulator();
      int numberOfRanges = numberOfRangesCoder.decode(inStream);
      result.initialSequence = initialSequenceCoder.decode(inStream);
      for (int i = 0; i < numberOfRanges; i++) {
        long key = dataCoder.decode(inStream);
        long upperBound = dataCoder.decode(inStream);
        long millis = dataCoder.decode(inStream);
        result.data.put(key, Pair.of(upperBound, Instant.ofEpochMilli(millis)));
      }
      return result;
    }
  }
}
