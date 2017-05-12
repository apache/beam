package cz.seznam.euphoria.flink.streaming;

import cz.seznam.euphoria.core.client.operator.ExtractEventTime;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;

import java.time.Duration;
import java.util.Objects;

class EventTimeAssigner
        extends BoundedOutOfOrdernessTimestampExtractor<StreamingElement>
{
  private final ExtractEventTime eventTimeFn;

  EventTimeAssigner(Duration allowedLateness, ExtractEventTime eventTimeFn) {
    super(millisTime(allowedLateness.toMillis()));
    this.eventTimeFn = Objects.requireNonNull(eventTimeFn);
  }

  @SuppressWarnings("unchecked")
  @Override
  public long extractTimestamp(StreamingElement element) {
    return eventTimeFn.extractTimestamp(element.getElement());
  }

  private static org.apache.flink.streaming.api.windowing.time.Time
  millisTime(long millis) {
    return org.apache.flink.streaming.api.windowing.time.Time.milliseconds(millis);
  }
}
