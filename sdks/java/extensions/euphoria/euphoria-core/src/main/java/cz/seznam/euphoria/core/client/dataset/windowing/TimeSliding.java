
package cz.seznam.euphoria.core.client.dataset.windowing;

import cz.seznam.euphoria.core.client.io.Context;
import cz.seznam.euphoria.core.client.triggers.TimeTrigger;
import cz.seznam.euphoria.core.client.triggers.Trigger;
import cz.seznam.euphoria.guava.shaded.com.google.common.base.Preconditions;
import cz.seznam.euphoria.guava.shaded.com.google.common.collect.Sets;

import java.time.Duration;
import java.util.Set;

/**
 * Time sliding windowing.
 */
public final class TimeSliding<T>
    implements Windowing<T, TimeInterval> {

  public static <T> TimeSliding<T> of(Duration duration, Duration step) {
    return new TimeSliding<>(duration.toMillis(), step.toMillis());
  }

  /** Helper method to extract window label from context. */
  public static TimeInterval getLabel(Context<?> context) {
    return (TimeInterval) context.getWindow();
  }

  private final long duration;
  private final long slide;

  private TimeSliding(
      long duration,
      long slide) {

    this.duration = duration;
    this.slide = slide;

    Preconditions.checkArgument(duration > 0, "Windowing with zero duration");

    if (duration % slide != 0) {
      throw new IllegalArgumentException(
          "This time sliding window can manage only aligned sliding windows");
    }
  }

  @Override
  public Set<TimeInterval> assignWindowsToElement(WindowedElement<?, T> el) {
    Set<TimeInterval> ret =
            Sets.newHashSetWithExpectedSize((int) (this.duration / this.slide));
    for (long start = el.timestamp - el.timestamp % this.slide;
         start > el.timestamp - this.duration;
         start -= this.slide) {
      ret.add(new TimeInterval(start, start + this.duration));
    }
    return ret;
  }

  @Override
  public Trigger<TimeInterval> getTrigger() {
    return new TimeTrigger();
  }

  @Override
  public String toString() {
    return "TimeSliding{" +
        "duration=" + duration +
        ", slide=" + slide +
        '}';
  }

  public long getDuration() {
    return duration;
  }

  public long getSlide() {
    return slide;
  }
}

