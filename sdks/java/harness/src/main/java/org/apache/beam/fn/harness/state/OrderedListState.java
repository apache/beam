package org.apache.beam.fn.harness.state;

import org.apache.beam.sdk.values.TimestampedValue;
import org.joda.time.Instant;

public class OrderedListState<T> {

  class TimestampedValueWithId<T> {
    private final TimestampedValue<T> value;
    private final long localId;
  }

  public void add(TimestampedValue<T> value) {}

  public Iterable<TimestampedValue<T>> readRange(Instant minTimestamp, Instant limitTimestamp) {}

  public void clearRange(Instant minTimestamp, Instant limitTimestamp) {}
}
