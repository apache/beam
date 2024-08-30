package org.apache.beam.sdk.extensions.ordered.combiner;

import java.util.Arrays;
import org.apache.commons.lang3.tuple.Triple;
import org.joda.time.Instant;
import org.junit.Assert;
import org.junit.Test;

public class SequenceRangeAccumulatorTest {

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
    Instant start = Instant.now();
    Event[] events = new Event[]{
        new Event(1, start, true),
        new Event(2, start),
        new Event(3, start)
    };

    SequenceRangeAccumulator accumulator = new SequenceRangeAccumulator();
    Arrays.stream(events).forEach(e -> accumulator.add(e.sequence, e.timestamp, e.initialEvent));

    Assert.assertEquals("Accumulated results",
        Triple.of(1, 3, start),
        accumulator.largestContinuousRange());

  }

}
