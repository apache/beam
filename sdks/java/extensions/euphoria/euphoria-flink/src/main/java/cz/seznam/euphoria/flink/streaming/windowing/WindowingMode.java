package cz.seznam.euphoria.flink.streaming.windowing;

import cz.seznam.euphoria.core.client.dataset.windowing.Time;
import cz.seznam.euphoria.core.client.dataset.windowing.TimeSliding;
import cz.seznam.euphoria.core.client.dataset.windowing.Windowing;
import cz.seznam.euphoria.core.client.functional.UnaryFunction;
import org.apache.flink.streaming.api.TimeCharacteristic;

import java.util.Objects;

public enum WindowingMode {

  /** processing time windowing */
  PROCESSING(TimeCharacteristic.ProcessingTime),

  /** event time windowing */
  EVENT(TimeCharacteristic.EventTime);

  private TimeCharacteristic timeCharacteristic;

  WindowingMode(TimeCharacteristic timeCharacteristic) {
    this.timeCharacteristic = Objects.requireNonNull(timeCharacteristic);
  }

  public TimeCharacteristic timeCharacteristic() {
    return timeCharacteristic;
  }

  public static WindowingMode determine(Windowing w) {
    if (w instanceof Time) {
      return determineFromEventFn(((Time) w).getEventTimeFn());
    }
    if (w instanceof TimeSliding) {
      return determineFromEventFn(((TimeSliding) w).getEventTimeFn());
    }
    // TODO add other types of windowing

    // TODO session windowing?

    // default is processing time
    return PROCESSING;
  }

  static WindowingMode determineFromEventFn(UnaryFunction fn) {
    return isProcessingTime(fn)
        ? PROCESSING
        : EVENT;
  }

  static boolean isProcessingTime(UnaryFunction fn) {
    return (fn instanceof Time.ProcessingTime);
  }
}
