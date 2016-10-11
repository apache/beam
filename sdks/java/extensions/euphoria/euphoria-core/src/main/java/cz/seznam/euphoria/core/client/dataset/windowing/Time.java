package cz.seznam.euphoria.core.client.dataset.windowing;

import cz.seznam.euphoria.core.client.functional.UnaryFunction;
import cz.seznam.euphoria.core.client.triggers.AfterFirstCompositeTrigger;
import cz.seznam.euphoria.core.client.triggers.PeriodicTimeTrigger;
import cz.seznam.euphoria.core.client.triggers.TimeTrigger;
import cz.seznam.euphoria.core.client.triggers.Trigger;

import java.time.Duration;
import static java.util.Collections.singleton;

import java.util.Arrays;

import static java.util.Objects.requireNonNull;
import java.util.Optional;
import java.util.Set;

/**
 * Time based tumbling windowing.
 */
public class Time<T> implements Windowing<T, TimeInterval> {

  public static final class ProcessingTime<T> implements UnaryFunction<T, Long> {

    private static final ProcessingTime INSTANCE = new ProcessingTime();

    // singleton
    private ProcessingTime() { }

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
      return new TTime<>(this.durationMillis,
          earlyTriggeringPeriod, requireNonNull(fn));
    }
  } // ~ end of UTime

  // ~ a typed variant of the Time windowing; depends on the type of
  // input elements
  public static class TTime<T> extends Time<T> {
    TTime(long durationMillis,
          Duration earlyTriggeringPeriod,
          UnaryFunction<T, Long> eventTimeFn) {
      
      super(durationMillis, earlyTriggeringPeriod, eventTimeFn);
    }
  } // ~ end of TTime

  final long durationMillis;
  final Duration earlyTriggeringPeriod;
  final UnaryFunction<T, Long> eventTimeFn;

  public static <T> UTime<T> of(Duration duration) {
    return new UTime<>(duration.toMillis(), null);
  }

  Time(long durationMillis,
      Duration earlyTriggeringPeriod,
      UnaryFunction<T, Long> eventTimeFn) {
    
    this.durationMillis = durationMillis;
    this.earlyTriggeringPeriod = earlyTriggeringPeriod;
    this.eventTimeFn = eventTimeFn;
  }

  @Override
  public Set<TimeInterval> assignWindowsToElement(
      WindowedElement<?, T> input) {
    long ts = eventTimeFn.apply(input.get());

    long start = ts - (ts + durationMillis) % durationMillis;
    long end = start + durationMillis;
    return singleton(new TimeInterval(start, end));
  }

  @Override
  public Trigger<T, TimeInterval> getTrigger() {
    if (earlyTriggeringPeriod != null) {
      return new AfterFirstCompositeTrigger<>(Arrays.asList(
              new TimeTrigger<>(),
              new PeriodicTimeTrigger<>(earlyTriggeringPeriod.toMillis())));
    }
    return new TimeTrigger<>();
  }

  public Duration getEarlyTriggeringPeriod() {
    return earlyTriggeringPeriod;
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
  public Optional<UnaryFunction<T, Long>> getTimestampAssigner() {
    if (getType() == Type.EVENT)
      return Optional.of(eventTimeFn);
    return Optional.empty();
  }

}

