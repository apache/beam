package org.apache.beam.sdk.extensions.ordered.combiner;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Map.Entry;
import java.util.TreeMap;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.CustomCoder;
import org.apache.beam.sdk.extensions.ordered.CompletedSequenceRange;
import org.apache.commons.lang3.tuple.Pair;
import org.checkerframework.checker.initialization.qual.Initialized;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.UnknownKeyFor;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SequenceRangeAccumulator {

  private static final Logger LOG = LoggerFactory.getLogger(SequenceRangeAccumulator.class);

  private final TreeMap<Long, Pair<Long, Instant>> accumulator = new TreeMap<>();
  private boolean containsInitialSequence = false;

  public void add(long sequence, Instant timestamp, boolean initialSequence) {
    if (containsInitialSequence && initialSequence) {
      // TODO: more tests
      LOG.error(
          "There are multiple initial sequences detected: "
              + accumulator.keySet().iterator().next()
              + " and " + initialSequence);
    }

    if (initialSequence) {
      this.containsInitialSequence = initialSequence;
    }

    Entry<Long, Pair<Long, Instant>> entry = accumulator.floorEntry(sequence);
    if (entry == null) {
      // Value lower than anything we have seen - this is a new range
      accumulator.put(sequence, Pair.of(sequence, timestamp));
      return;
    }

    Long inclusiveUpperBoundary = entry.getValue().getLeft();

    if (sequence <= inclusiveUpperBoundary) {
      // Duplicate
      return;
    }

    if (inclusiveUpperBoundary + 1 == sequence) {
      // We hit the end of the range

      Pair<Long, Instant> rangeToMergeWith = accumulator.get(inclusiveUpperBoundary + 2);
      if(rangeToMergeWith == null) {
        accumulator.replace(entry.getKey(), Pair.of(sequence, timestamp));
        return;
      } else {
        accumulator.remove(inclusiveUpperBoundary + 2);
        accumulator.replace(entry.getKey(), Pair.of(rangeToMergeWith.getKey(), timestamp));
      }

      return;
    }

    // we are above the range. It's a new range
    accumulator.put(sequence, Pair.of(sequence, timestamp));
  }

  public CompletedSequenceRange largestContinuousRange() {
    if (!containsInitialSequence) {
      return CompletedSequenceRange.EMPTY;
    }

    Entry<Long, Pair<Long, Instant>> firstEntry = accumulator.firstEntry();
    if (firstEntry == null) {
      throw new IllegalStateException("First entry is null");
    }
    Long startingSequence = firstEntry.getKey();

    return CompletedSequenceRange.of(
        startingSequence, firstEntry.getValue().getLeft(), firstEntry.getValue().getRight());
  }

  public int numberOfRanges() {
    return accumulator.size();
  }


  public void merge(SequenceRangeAccumulator another) {
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
