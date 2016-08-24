
package cz.seznam.euphoria.core.client.dataset.windowing;

import cz.seznam.euphoria.core.client.functional.UnaryFunction;
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
    implements AlignedWindowing<T, Long, TimeSliding.SlidingWindowContext> {

  public static class SlidingWindowContext extends WindowContext<Void, Long> {

    private final long startTime;
    private final long duration;

    private SlidingWindowContext(long startTime, long duration) {
      super(WindowID.aligned(startTime));
      this.startTime = startTime;
      this.duration = duration;
    }

    @Override
    public List<Trigger> createTriggers() {
      return Collections.singletonList(new TimeTrigger(startTime + duration));
    }

    @Override
    public String toString() {
      return "SlidingWindow{" +
          "startTime=" + startTime +
          ", duration=" + duration +
          '}';
    }
  }

  public static <T> TimeSliding<T> of(Duration duration, Duration step) {
    return new TimeSliding<>(duration.toMillis(), step.toMillis(),
        Time.ProcessingTime.get());
  }

  private final long duration;
  private final long slide;
  private final int stepsPerWindow;
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
    stepsPerWindow = (int) (duration / slide);
  }

  /**
   * Specify the event time extraction function.
   */
  public <T> TimeSliding<T> using(UnaryFunction<T, Long> eventTimeFn) {
    return new TimeSliding<>(this.duration, this.slide, eventTimeFn);
  }

  @Override
  public Set<WindowID<Void, Long>> assignWindows(T input) {
    long now = eventTimeFn.apply(input) - duration + slide;
    long boundary = now / slide * slide;
    Set<WindowID<Void, Long>> ret = new HashSet<>();
    for (int i = 0; i < stepsPerWindow; i++) {
      ret.add(WindowID.aligned(boundary));
      boundary += slide;
    }
    return ret;
  }

  @Override
  public SlidingWindowContext createWindowContext(WindowID<Void, Long> id) {
    return new SlidingWindowContext(id.getLabel(), duration);
  }

  @Override
  public String toString() {
    return "TimeSliding{" +
        "duration=" + duration +
        ", step=" + slide +
        ", stepsPerWindow=" + stepsPerWindow +
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
  public Optional<UnaryFunction<T, Long>> getTimeAssigner() {
    if (getType() == Type.EVENT)
      return Optional.of(eventTimeFn);
    return Optional.empty();
  }


}

