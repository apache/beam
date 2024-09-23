package org.apache.beam.sdk.extensions.ordered.combiner;

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.beam.sdk.extensions.ordered.ContiguousSequenceRange;
import org.joda.time.Instant;
import org.junit.Assert;
import org.junit.Test;

public class SequenceRangeAccumulatorTest {

  // Atomic just in case tests are run in parallel
  private final static AtomicLong currentTicker = new AtomicLong();

  static Instant nextTimestamp() {
    return Instant.ofEpochMilli(currentTicker.getAndIncrement());
  }

  static Instant eventTimestamp(Event[] events, long eventSequence) {
    for (Event e : events) {
      if (e.sequence == eventSequence) {
        return e.timestamp;
      }
    }
    throw new IllegalStateException("Unable to find event with sequence " + eventSequence);
  }

  static class Event {

    long sequence;
    Instant timestamp;
    boolean initialEvent;

    Event(long sequence, Instant ts) {
      this(sequence, ts, false);
    }

    Event(long sequence, Instant ts, boolean initialEvent) {
      this.sequence = sequence;
      this.timestamp = ts;
      this.initialEvent = initialEvent;
    }
  }

  @Test
  public void testSimpleAccumulation() {
    Event[] events = new Event[]{
        new Event(1, nextTimestamp(), true),
        new Event(2, nextTimestamp()),
        new Event(3, nextTimestamp())
    };

    doTestAccumulation(events, ContiguousSequenceRange.of(1, 3, eventTimestamp(events, 3)), 1);
  }

  @Test
  public void testReverseArrivalHandling() {
    Event[] events = new Event[]{
        new Event(3, nextTimestamp()),
        new Event(2, nextTimestamp()),
        new Event(1, nextTimestamp(), true)
    };

    Instant timestampOfEventNumber1 = eventTimestamp(events, 1);
    doTestAccumulation(events, ContiguousSequenceRange.of(1, 3, timestampOfEventNumber1), 1);
  }

  @Test
  public void testPartialRangeAccumulation() {
    Event[] events = new Event[]{
        new Event(1, nextTimestamp(), true),
        new Event(2, nextTimestamp()),
        new Event(3, nextTimestamp()),
        new Event(5, nextTimestamp()),
        new Event(7, nextTimestamp()),
    };

    doTestAccumulation(events, ContiguousSequenceRange.of(1, 3, eventTimestamp(events, 3)), 3);
  }

  @Test
  public void testMergingRangeAccumulation() {
    Event[] events = new Event[]{
        new Event(1, nextTimestamp(), true),
        new Event(2, nextTimestamp()),
        new Event(3, nextTimestamp()),
        new Event(5, nextTimestamp()),
        new Event(7, nextTimestamp()),
        new Event(6, nextTimestamp()),
    };

    doTestAccumulation(events, ContiguousSequenceRange.of(1, 3, eventTimestamp(events, 3)), 2);
  }

  @Test
  public void testNoStartEvent() {
    Event[] events = new Event[]{
        new Event(2, nextTimestamp()),
        new Event(3, nextTimestamp()),
        new Event(1, nextTimestamp()),

        new Event(5, nextTimestamp()),
    };

    doTestAccumulation(events, ContiguousSequenceRange.EMPTY, 2);
  }

  @Test
  public void testNoEventsAccumulation() {
    Event[] events = new Event[]{};

    doTestAccumulation(events, ContiguousSequenceRange.EMPTY, 0);
  }

  @Test
  public void testRemovingRangesBelowInitialSequenceDuringAccumulation() {
    Event[] events = new Event[]{
        // First range
        new Event(2, nextTimestamp()),
        new Event(3, nextTimestamp()),
        new Event(1, nextTimestamp()),

        // Second range
        new Event(5, nextTimestamp()),
        new Event(6, nextTimestamp()),

        // This event should prune everything below
        new Event(7, nextTimestamp(), true),
    };

    doTestAccumulation(events, ContiguousSequenceRange.of(7, 7, eventTimestamp(events, 7)), 1);
  }

  @Test
  public void testRemovingElementsBelowInitialSequenceDuringAccumulation() {

    Event[] events = new Event[]{
        // First range
        new Event(2, nextTimestamp()),
        new Event(3, nextTimestamp()),
        new Event(1, nextTimestamp()),

        // Second range
        new Event(5, nextTimestamp()),
        new Event(6, nextTimestamp()),
        new Event(7, nextTimestamp()),
        new Event(8, nextTimestamp()),

        // This event should reduce the range.
        new Event(7, nextTimestamp(), true),
    };

    Instant timestampOfTheLastEvent = events[events.length - 1].timestamp;
    doTestAccumulation(events, ContiguousSequenceRange.of(7, 8, timestampOfTheLastEvent), 1);
  }

  private static void doTestAccumulation(Event[] events, ContiguousSequenceRange expectedResult,
      int expectedNumberOfRanges) {
    SequenceRangeAccumulator accumulator = new SequenceRangeAccumulator();
    Arrays.stream(events).forEach(e -> accumulator.add(e.sequence, e.timestamp, e.initialEvent));

    Assert.assertEquals("Accumulated results",
        expectedResult,
        accumulator.largestContinuousRange());

    Assert.assertEquals("Number of ranges", expectedNumberOfRanges, accumulator.numberOfRanges());
  }


  @Test
  public void testEmptyMerge() {
    Event[] set1 = new Event[]{};
    Event[] set2 = new Event[]{};

    ContiguousSequenceRange expectedResult = ContiguousSequenceRange.EMPTY;
    int expectedNumberOfRanges = 0;

    doTestMerging(set1, set2, expectedResult, expectedNumberOfRanges);
  }

  @Test
  public void testMergingNonEmptyWithEmpty() {
    Event[] set1 = new Event[]{
        new Event(3, nextTimestamp()),
        new Event(2, nextTimestamp()),
        new Event(1, nextTimestamp(), true)
    };
    Event[] set2 = new Event[]{};

    ContiguousSequenceRange expectedResult = ContiguousSequenceRange.of(1, 3,
        eventTimestamp(set1, 1L));
    int expectedNumberOfRanges = 1;

    doTestMerging(set1, set2, expectedResult, expectedNumberOfRanges);
  }

  @Test
  public void testMergingWithLowerNonAdjacentRange() {
    Event[] set1 = new Event[]{
        new Event(1, nextTimestamp(), true),
        new Event(2, nextTimestamp()),
    };
    Event[] set2 = new Event[]{
        new Event(4, nextTimestamp()),
        new Event(5, nextTimestamp()),
        new Event(6, nextTimestamp())
    };

    ContiguousSequenceRange expectedResult = ContiguousSequenceRange.of(1, 2,
        eventTimestamp(set1, 2L));
    int expectedNumberOfRanges = 2;

    doTestMerging(set1, set2, expectedResult, expectedNumberOfRanges);
  }

  @Test
  public void testMergingWithoutAnyInitialEvents() {
    Event[] set1 = new Event[]{
        new Event(1, nextTimestamp()),
        new Event(2, nextTimestamp()),
    };
    Event[] set2 = new Event[]{
        new Event(4, nextTimestamp()),
        new Event(5, nextTimestamp()),
        new Event(6, nextTimestamp())
    };

    ContiguousSequenceRange expectedResult = ContiguousSequenceRange.EMPTY;
    int expectedNumberOfRanges = 2;

    doTestMerging(set1, set2, expectedResult, expectedNumberOfRanges);
  }

  @Test
  public void testMergingAdjacentRanges() {
    Event[] set1 = new Event[]{
        new Event(1, nextTimestamp(), true),
        new Event(2, nextTimestamp()),
    };
    Event[] set2 = new Event[]{
        new Event(3, nextTimestamp()),
        new Event(4, nextTimestamp()),
        new Event(5, nextTimestamp()),
        new Event(6, nextTimestamp())
    };

    ContiguousSequenceRange expectedResult = ContiguousSequenceRange.of(1, 6,
        eventTimestamp(set2, 6L));
    int expectedNumberOfRanges = 1;

    doTestMerging(set1, set2, expectedResult, expectedNumberOfRanges);
  }

  @Test
  public void testPruningSequencesBelowInitial() {
    Event[] set1 = new Event[]{
        new Event(1, nextTimestamp()),
        new Event(2, nextTimestamp()),
    };
    Event[] set2 = new Event[]{
        new Event(3, nextTimestamp(), true),
        new Event(4, nextTimestamp()),
        new Event(5, nextTimestamp()),
        new Event(6, nextTimestamp())
    };

    ContiguousSequenceRange expectedResult = ContiguousSequenceRange.of(3, 6,
        eventTimestamp(set2, 6L));
    int expectedNumberOfRanges = 1;

    doTestMerging(set1, set2, expectedResult, expectedNumberOfRanges);
  }

  @Test
  public void testDuplicateHandling() {
    Event[] set1 = new Event[]{
        new Event(1, nextTimestamp(), true),
        new Event(2, nextTimestamp()),
        new Event(3, nextTimestamp()),
        new Event(5, nextTimestamp()),
    };
    Event[] set2 = new Event[]{
        new Event(3, nextTimestamp()),
        new Event(4, nextTimestamp()),
        new Event(5, nextTimestamp()),
        new Event(6, nextTimestamp())
    };

    ContiguousSequenceRange expectedResult = ContiguousSequenceRange.of(1, 6,
        eventTimestamp(set2, 6L));
    int expectedNumberOfRanges = 1;

    doTestMerging(set1, set2, expectedResult, expectedNumberOfRanges);
  }

  @Test
  public void testExceptionThrownIfThereAreDifferentInitialSequences() {
    Event[] set1 = new Event[]{
        new Event(1, nextTimestamp(), true),
        new Event(2, nextTimestamp()),
    };
    Event[] set2 = new Event[]{
        new Event(3, nextTimestamp(), true),
        new Event(4, nextTimestamp()),
        new Event(5, nextTimestamp()),
        new Event(6, nextTimestamp())
    };

    try {
      doTestMerging(set1, set2, ContiguousSequenceRange.EMPTY, 0);
      Assert.fail("Expected to throw an exception");
    } catch (IllegalStateException e) {
      Assert.assertEquals("Exception message",
          "Two accumulators contain different initial sequences: 1 and 3", e.getMessage());
    }
  }


  @Test
  public void testSelectingHighestTimestampWhenMerging() {
    Event[] set1 = new Event[]{
        new Event(1, nextTimestamp(), true),
        new Event(2, Instant.ofEpochMilli(currentTicker.get() + 10000)),
    };
    Event[] set2 = new Event[]{
        new Event(3, nextTimestamp()),
        new Event(4, nextTimestamp()),
        new Event(5, nextTimestamp()),
        new Event(6, nextTimestamp())
    };

    ContiguousSequenceRange expectedResult = ContiguousSequenceRange.of(1, 6,
        eventTimestamp(set1, 2L));
    int expectedNumberOfRanges = 1;
    doTestMerging(set1, set2, expectedResult, expectedNumberOfRanges);
  }

  private static void doTestMerging(Event[] set1, Event[] set2,
      ContiguousSequenceRange expectedResult,
      int expectedNumberOfRanges) {
    // Try to merge both set2 to set1 and set1 to set2 - both must return the same results
    mergeAndTest(set1, set2, expectedResult, expectedNumberOfRanges, "set1");
    mergeAndTest(set2, set1, expectedResult, expectedNumberOfRanges, "set2");
  }

  private static void mergeAndTest(Event[] set1, Event[] set2,
      ContiguousSequenceRange expectedResult,
      int expectedNumberOfRanges, String firstSetName) {
    final SequenceRangeAccumulator a1 = new SequenceRangeAccumulator();
    Arrays.stream(set1).forEach(e -> a1.add(e.sequence, e.timestamp, e.initialEvent));

    final SequenceRangeAccumulator a2 = new SequenceRangeAccumulator();
    Arrays.stream(set2).forEach(e -> a2.add(e.sequence, e.timestamp, e.initialEvent));

    a1.merge(a2);

    Assert.assertEquals("Accumulated results - " + firstSetName,
        expectedResult,
        a1.largestContinuousRange());

    Assert.assertEquals("Number of ranges - " + firstSetName, expectedNumberOfRanges,
        a1.numberOfRanges());
  }

}
