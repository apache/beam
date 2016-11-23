package cz.seznam.euphoria.core.client.dataset.windowing;

import cz.seznam.euphoria.core.client.triggers.AfterFirstCompositeTrigger;
import cz.seznam.euphoria.core.client.triggers.PeriodicTimeTrigger;
import cz.seznam.euphoria.core.client.triggers.TimeTrigger;
import cz.seznam.euphoria.core.client.triggers.Trigger;

import java.time.Duration;
import java.util.Arrays;
import java.util.Objects;
import java.util.Set;

import static java.util.Collections.singleton;

/**
 * Time based tumbling windowing. Windows can't overlap.
 */
public class Time<T> implements Windowing<T, TimeInterval> {

  private final long durationMillis;
  private Duration earlyTriggeringPeriod;

  public static <T> Time<T> of(Duration duration) {
    return new Time<>(duration.toMillis());
  }

  Time(long durationMillis) {
    this.durationMillis = durationMillis;
  }

  /**
   * Early results will be triggered periodically until the window is finally closed.
   */
  public <T> Time<T> earlyTriggering(Duration timeout) {
    this.earlyTriggeringPeriod = Objects.requireNonNull(timeout);
    return (Time) this;
  }

  @Override
  public Set<TimeInterval> assignWindowsToElement(WindowedElement<?, T> el) {
    long start = el.timestamp - (el.timestamp + durationMillis) % durationMillis;
    long end = start + durationMillis;
    return singleton(new TimeInterval(start, end));
  }

  @Override
  public Trigger<TimeInterval> getTrigger() {
    if (earlyTriggeringPeriod != null) {
      return new AfterFirstCompositeTrigger<>(Arrays.asList(
              new TimeTrigger(),
              new PeriodicTimeTrigger(earlyTriggeringPeriod.toMillis())));
    }
    return new TimeTrigger();
  }

  public Duration getEarlyTriggeringPeriod() {
    return earlyTriggeringPeriod;
  }

  public long getDuration() {
    return durationMillis;
  }
}

