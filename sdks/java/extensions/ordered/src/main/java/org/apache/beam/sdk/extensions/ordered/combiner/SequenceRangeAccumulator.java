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
import org.apache.beam.sdk.extensions.ordered.CompletedSequenceRange;
import org.apache.commons.lang3.tuple.Pair;
import org.checkerframework.checker.initialization.qual.Initialized;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.UnknownKeyFor;
import org.joda.time.Instant;

public class SequenceRangeAccumulator {

  static Instant max(Instant a, Instant b) {
    return a.isAfter(b) ? a : b;
  }

  private final TreeMap<Long, Pair<Long, Instant>> accumulator = new TreeMap<>();
  private @Nullable Long initialSequence = null;

  public void add(long sequence, Instant timestamp, boolean isInitialSequence) {
    if (isInitialSequence && this.initialSequence != null && sequence != this.initialSequence) {
      throw new IllegalStateException(
          "There are different initial sequences detected: "
              + initialSequence + " and " + sequence);
    }

    if (isInitialSequence) {
      this.initialSequence = sequence;
      clearRangesBelowInitialSequence(sequence, timestamp);
    } else if (initialSequence != null && sequence <= initialSequence) {
      // No need to add anything lower than the initial sequence to the accumulator.
      return;
    }

    long lowerBound = sequence;
    long upperBound = sequence;

    Entry<Long, Pair<Long, Instant>> lowerRange = accumulator.floorEntry(sequence);
    if(lowerRange != null) {
      long inclusiveUpperBoundary = lowerRange.getValue().getLeft();
      if (sequence <= inclusiveUpperBoundary) {
        // Duplicate. No need to adjust the timestamp.
        return;
      }

      if (inclusiveUpperBoundary + 1 == sequence) {
        // The new element extends this range
        timestamp = max(timestamp, lowerRange.getValue().getValue());
        lowerBound = lowerRange.getKey();
        accumulator.remove(lowerRange.getKey());
      }
    }

    long nextSequenceNumber = sequence + 1;
    Pair<Long, Instant> upperRange = accumulator.get(nextSequenceNumber);
    if (upperRange != null) {
      timestamp = max(timestamp, upperRange.getRight());
      upperBound = upperRange.getLeft();
      accumulator.remove(nextSequenceNumber);
    }

    accumulator.put(lowerBound, Pair.of(upperBound, timestamp));
  }

  private void clearRangesBelowInitialSequence(long sequence, Instant timestamp) {
    // First, adjust the current range, if any
    Entry<Long, Pair<Long, Instant>> lowerRange = accumulator.floorEntry(sequence);
    if(lowerRange != null
        && lowerRange.getKey() < sequence
        && lowerRange.getValue().getLeft() > sequence) {
      // The sequence is in the middle of the range. Adjust it.
      accumulator.remove(lowerRange.getKey());
      accumulator.put(sequence,
          Pair.of(lowerRange.getValue().getKey(), max(timestamp, lowerRange.getValue()
          .getValue())));
    }
    accumulator.subMap(Long.MIN_VALUE, sequence).clear();
  }

  public CompletedSequenceRange largestContinuousRange() {
    if (initialSequence == null) {
      return CompletedSequenceRange.EMPTY;
    }

    Entry<Long, Pair<Long, Instant>> firstEntry = accumulator.firstEntry();
    if (firstEntry == null) {
      throw new IllegalStateException("First entry is null");
    }
    Long start = firstEntry.getKey();
    Long end = firstEntry.getValue().getLeft();
    Instant latestTimestamp = firstEntry.getValue().getRight();
    return CompletedSequenceRange.of(start, end, latestTimestamp);
  }

  public int numberOfRanges() {
    return accumulator.size();
  }


  public void merge(SequenceRangeAccumulator another) {
    if (this.initialSequence != null && another.initialSequence != null
        && this.initialSequence.equals(another.initialSequence)) {
      throw new IllegalStateException("Two accumulators contain different initial sequences: "
          + this.initialSequence + " and " + another.initialSequence);
    }

    if (another.initialSequence != null) {
      long newInitialSequence = another.initialSequence;
      this.initialSequence = newInitialSequence;
      Entry<Long, Pair<Long, Instant>> firstEntry = another.accumulator.firstEntry();
      if(firstEntry != null) {
        Instant timestampOfTheInitialRange = firstEntry.getValue().getRight();
        clearRangesBelowInitialSequence(newInitialSequence, timestampOfTheInitialRange);
      }
    }

    another.accumulator.entrySet().stream().forEach(
        entry -> {
          long lowerBound = entry.getKey();
          long upperBound = entry.getValue().getLeft();
          if(this.initialSequence != null) {
            if(upperBound < initialSequence) {
              // The whole range is below the initial sequence. Ignore it.
              return;
            }
            if(lowerBound < initialSequence) {
              // This will cause pruning of the range up to the initial sequence
              lowerBound = this.initialSequence;
            }
          }

          Entry<Long, Pair<Long, Instant>> lowerRange = this.accumulator.floorEntry(lowerBound);

          if (lowerRange != null) {
            if (lowerRange.getValue().getLeft() < lowerBound - 1) {
              // Nothing to do. There is a lower non-adjacent range.
            } else {
              // We found an overlapping range and will replace it with a new one
              upperBound = Math.max(upperBound, lowerRange.getValue().getLeft());
              lowerBound = lowerRange.getKey();
            }
          }

          Entry<Long, Pair<Long, Instant>> upperRange = this.accumulator.floorEntry(upperBound + 1);
          if (upperRange == null ||
              (lowerRange != null && Objects.equals(upperRange.getKey(), lowerRange.getKey()))) {
            // Nothing to do - either there is no adjacent upper range or it equals the lower range
          } else {
            upperBound = Math.max(upperBound, upperRange.getValue().getLeft());
          }

          Instant latestTimestamp = removeAllRanges(lowerBound, upperBound,
              entry.getValue().getRight());

          this.accumulator.put(lowerBound, Pair.of(upperBound, latestTimestamp));
        }
    );
  }

  private Instant removeAllRanges(long lowerBound, long upperBound, Instant currentTimestamp) {
    Instant result = currentTimestamp;
    SortedMap<Long, Pair<Long, Instant>> rangesToRemove = accumulator.subMap(lowerBound,
        upperBound);
    for (Pair<Long, Instant> value : rangesToRemove.values()) {
      result = result.isAfter(value.getRight()) ? result : value.getRight();
    }
    rangesToRemove.clear();
    return result;
  }

  public static class SequenceRangeAccumulatorCoder extends CustomCoder<SequenceRangeAccumulator> {

    // TODO implement
    @Override
    public void encode(SequenceRangeAccumulator value,
        @UnknownKeyFor @NonNull @Initialized OutputStream outStream)
        throws @UnknownKeyFor @NonNull @Initialized CoderException, @UnknownKeyFor @NonNull @Initialized IOException {

    }

    @Override
    public SequenceRangeAccumulator decode(
        @UnknownKeyFor @NonNull @Initialized InputStream inStream)
        throws @UnknownKeyFor @NonNull @Initialized CoderException, @UnknownKeyFor @NonNull @Initialized IOException {
      return new SequenceRangeAccumulator();
    }
  }
}
