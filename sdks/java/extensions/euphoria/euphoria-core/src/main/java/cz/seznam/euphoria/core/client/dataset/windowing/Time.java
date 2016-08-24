
package cz.seznam.euphoria.core.client.dataset.windowing;

import cz.seznam.euphoria.core.client.functional.UnaryFunction;
import cz.seznam.euphoria.core.client.triggers.TimeTrigger;
import cz.seznam.euphoria.core.client.triggers.Trigger;
import cz.seznam.euphoria.core.executor.TriggerScheduler;
import java.io.Serializable;
import java.time.Duration;
import java.util.ArrayList;
import static java.util.Collections.singleton;
import java.util.List;
import java.util.Objects;
import static java.util.Objects.requireNonNull;
import java.util.Optional;
import java.util.Set;

/**
 * Time based tumbling windowing.
 */
public class Time<T> implements AlignedWindowing<T, Time.TimeInterval, Time.TimeWindowContext> {

  public static final class ProcessingTime<T> implements UnaryFunction<T, Long> {
    private static final ProcessingTime INSTANCE = new ProcessingTime();

    // singleton
    private ProcessingTime() {}

    // ~ suppressing the warning is safe due to the returned
    // object not relying on the generic information in any way
    @SuppressWarnings("unchecked")
    public static <T> UnaryFunction<T, Long> get() {
      return INSTANCE;
    }

    @Override
    public Long apply(T what) {
      return System.currentTimeMillis();
    }
  } // ~ end of ProcessingTime

  public static final class TimeInterval implements Serializable {
    private final long startMillis;
    private final long intervalMillis;

    public TimeInterval(long startMillis, long intervalMillis) {
      this.startMillis = startMillis;
      this.intervalMillis = intervalMillis;
    }

    public long getStartMillis() {
      return startMillis;
    }

    public long getIntervalMillis() {
      return intervalMillis;
    }

    @Override
    public boolean equals(Object o) {
      if (o instanceof TimeInterval) {
        TimeInterval that = (TimeInterval) o;
        return this.startMillis == that.startMillis
            && this.intervalMillis == that.intervalMillis;
      }
      return false;
    }

    @Override
    public int hashCode() {
      return Objects.hash(startMillis, intervalMillis);
    }

    @Override
    public String toString() {
      return "TimeInterval{" +
          "startMillis=" + startMillis +
          ", intervalMillis=" + intervalMillis +
          '}';
    }
  } // ~ end of TimeInterval

  public static class TimeWindowContext
      extends EarlyTriggeredWindowContext<Void, TimeInterval> {

    private final long fireStamp;

    TimeWindowContext(long startMillis, long intervalMillis, Duration earlyTriggering) {
      super(
          WindowID.aligned(new TimeInterval(startMillis, intervalMillis)),
          earlyTriggering, startMillis + intervalMillis);
      this.fireStamp = startMillis + intervalMillis;
    }

    @Override
    public List<Trigger> createTriggers() {
      List<Trigger> triggers = new ArrayList<>(1);
      if (isEarlyTriggered()) {
        triggers.add(getEarlyTrigger());
      }

      triggers.add(new TimeTrigger(fireStamp));

      return triggers;
    }
  } // ~ end of TimeWindowContext

  // ~  an untyped variant of the Time windowing; does not dependent
  // on the input elements type
  public static class UTime<T> extends Time<T> {
    UTime(long durationMillis, Duration earlyTriggeringPeriod) {
      super(durationMillis, earlyTriggeringPeriod, ProcessingTime.get());
    }

    /**
     * Early results will be triggered periodically until the window is finally closed.
     */
    @SuppressWarnings("unchecked")
    public <T> UTime<T> earlyTriggering(Duration timeout) {
      // ~ the unchecked cast is ok: eventTimeFn is
      // ProcessingTime which is independent of <T>
      return new UTime<>(super.durationMillis, timeout);
    }

    /**
     * Function that will extract timestamp from data
     */
    public <T> TTime<T> using(UnaryFunction<T, Long> fn) {
      return new TTime<>(this.durationMillis, earlyTriggeringPeriod, requireNonNull(fn));
    }
  }

  // ~ a typed variant of the Time windowing; depends on the type of
  // input elements
  public static class TTime<T> extends Time<T> {
    TTime(long durationMillis,
          Duration earlyTriggeringPeriod,
          UnaryFunction<T, Long> eventTimeFn)
    {
      super(durationMillis, earlyTriggeringPeriod, eventTimeFn);
    }
  } // ~ end of EventTimeBased

  final long durationMillis;
  final Duration earlyTriggeringPeriod;
  final UnaryFunction<T, Long> eventTimeFn;

  public static <T> UTime<T> of(Duration duration) {
    return new UTime<>(duration.toMillis(), null);
  }

  Time(long durationMillis, Duration earlyTriggeringPeriod, UnaryFunction<T, Long> eventTimeFn) {
    this.durationMillis = durationMillis;
    this.earlyTriggeringPeriod = earlyTriggeringPeriod;
    this.eventTimeFn = eventTimeFn;
  }

  @Override
  public Set<WindowID<Void, TimeInterval>> assignWindows(T input) {
    long ts = eventTimeFn.apply(input);
    return singleton(WindowID.aligned(new TimeInterval(
        ts - (ts + durationMillis) % durationMillis, durationMillis)));
  }

  
  @Override
  public TimeWindowContext createWindowContext(WindowID<Void, TimeInterval> id) {
    return new TimeWindowContext(
        id.getLabel().getStartMillis(),
        id.getLabel().getIntervalMillis(),
        earlyTriggeringPeriod);
  }


  public long getDuration() {
    return durationMillis;
  }

  public UnaryFunction<T, Long> getEventTimeFn() {
    return eventTimeFn;
  }

  @Override
  public Type getType() {
    return eventTimeFn.getClass() == Time.ProcessingTime.class
        ? Type.PROCESSING : Type.EVENT;
  }

  @Override
  public Optional<UnaryFunction<T, Long>> getTimeAssigner() {
    if (getType() == Type.EVENT)
      return Optional.of(eventTimeFn);
    return Optional.empty();
  }

  
}

