
package cz.seznam.euphoria.core.client.dataset.windowing;

import cz.seznam.euphoria.core.client.functional.UnaryFunction;
import cz.seznam.euphoria.core.client.io.Context;
import cz.seznam.euphoria.core.client.triggers.TimeTrigger;
import cz.seznam.euphoria.core.client.triggers.Trigger;
import java.time.Duration;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import static java.util.Objects.requireNonNull;
import java.util.Optional;
import java.util.Set;

/**
 * Time sliding windowing.
 */
public final class TimeSliding<T>
    implements Windowing<T, TimeInterval, TimeSliding.SlidingWindowContext> {

  public static class SlidingWindowContext extends WindowContext<TimeInterval> {

    private SlidingWindowContext(WindowID<TimeInterval> interval) {
      super(interval);
    }

    @Override
    public List<Trigger> createTriggers() {
      TimeInterval it = getWindowID().getLabel();
      return Collections.singletonList(
          new TimeTrigger(it.getStartMillis() + it.getIntervalMillis()));
    }

    @Override
    public String toString() {
      return "SlidingWindow{interval=" + getWindowID().getLabel() + '}';
    }
  }

  public static <T> TimeSliding<T> of(Duration duration, Duration step) {
    return new TimeSliding<>(duration.toMillis(), step.toMillis(),
        Time.ProcessingTime.get());
  }

  /** Helper method to extract window label from context. */
  public static TimeInterval getLabel(Context<?> context) {
    return (TimeInterval) context.getWindow();
  }

  private final long duration;
  private final long slide;
  private final UnaryFunction<T, Long> eventTimeFn;

  private TimeSliding(
      long duration,
      long slide,
      UnaryFunction<T, Long> eventTimeFn) {

    this.duration = duration;
    this.slide = slide;
    this.eventTimeFn = requireNonNull(eventTimeFn);

    if (duration % slide != 0) {
      throw new IllegalArgumentException(
          "This time sliding window can manage only aligned sliding windows");
    }
  }

  /**
   * Specify the event time extraction function.
   */
  public <T> TimeSliding<T> using(UnaryFunction<T, Long> eventTimeFn) {
    return new TimeSliding<>(this.duration, this.slide, eventTimeFn);
  }

  @Override
  public Set<WindowID<TimeInterval>> assignWindowsToElement(
      WindowedElement<?, T> input) {
    long evtTime = eventTimeFn.apply(input.get());
    Set<WindowID<TimeInterval>> ret = new HashSet<>();
    for (long start = evtTime - evtTime % this.slide;
         start > evtTime - this.duration;
         start -= this.slide) {
      ret.add(new WindowID<>(new TimeInterval(start, this.duration)));
    }
    return ret;
  }

  @Override
  public SlidingWindowContext createWindowContext(WindowID<TimeInterval> id) {
    return new SlidingWindowContext(id);
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

