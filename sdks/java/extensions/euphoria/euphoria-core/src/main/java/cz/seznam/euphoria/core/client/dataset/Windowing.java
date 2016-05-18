
package cz.seznam.euphoria.core.client.dataset;

import cz.seznam.euphoria.core.client.functional.UnaryFunction;
import cz.seznam.euphoria.core.client.util.Pair;

import java.io.Serializable;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.stream.Collectors;

import static java.util.Collections.singleton;
import static java.util.Collections.singletonList;
import static java.util.Objects.requireNonNull;


/**
 * A windowing policy of a dataset.
 */
public interface Windowing<T, GROUP, LABEL, W extends Window<GROUP, LABEL>>
    extends Serializable
{
  /** Time windows. */
  class Time<T>
      implements AlignedWindowing<T, Windowing.Time.TimeInterval, Windowing.Time.TimeWindow>
  {
    public static final class ProcessingTime<T> implements UnaryFunction<T, Long> {
      private static final ProcessingTime INSTANCE = new ProcessingTime();

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
        int result = (int) (startMillis ^ (startMillis >>> 32));
        result = 31 * result + (int) (intervalMillis ^ (intervalMillis >>> 32));
        return result;
      }
    } // ~ end of TimeInterval

    public static class TimeWindow
        implements AlignedWindow<TimeInterval> {

      private final TimeInterval label;
      private final long fireStamp;

      TimeWindow(long startMillis, long intervalMillis) {
        this.label = new TimeInterval(startMillis, intervalMillis);
        this.fireStamp = startMillis + intervalMillis;
      }

      @Override
      public TimeInterval getLabel() {
        return label;
      }

      @Override
      public TriggerState registerTrigger(
          Triggering triggering, UnaryFunction<Window<?, ?>, Void> evict) {
        
        if (triggering.scheduleAt(this.fireStamp, () -> evict.apply(this))) {
          return TriggerState.ACTIVATED;
        }
        return TriggerState.PASSED;
      }
    } // ~ end of TimeWindow

    // ~  an untyped variant of the Time windowing; does not dependent
    // on the input elements type
    public static class UTime<T> extends Time<T> {
      UTime(long durationMillis, boolean aggregating) {
        super(durationMillis, aggregating, ProcessingTime.get());
      }

      @SuppressWarnings("unchecked")
      public <T> Time<T> aggregating() {
        // ~ the unchecked cast is ok: eventTimeFn is
        // ProcessingTime which is independent of <T>
        return new Time<>(super.durationMillis, true, (UnaryFunction) super.eventTimeFn);
      }
    }

    // ~ a typed variant of the Time windowing; depends on the type of
    // input elements
    public static class TTime<T> extends Time<T> {
      TTime(long durationMillis,
            boolean aggregating,
            UnaryFunction<T, Long> eventTimeFn)
      {
        super(durationMillis, aggregating, eventTimeFn);
      }

      public TTime<T> aggregating() {
        return new TTime<>(super.durationMillis, true, super.eventTimeFn);
      }
    } // ~ end of EventTimeBased

    final long durationMillis;
    final boolean aggregating;
    final UnaryFunction<T, Long> eventTimeFn;

    public static <T> UTime<T> seconds(long seconds) {
      return new UTime<>(seconds * 1000, false);
    }

    public static <T> UTime<T> minutes(long minutes) {
      return new UTime<>(minutes * 1000 * 60, false);
    }

    public static <T> UTime<T> hours(long hours) {
      return new UTime<>(hours * 1000 * 60 * 60, false);
    }

    public <T> TTime<T> using(UnaryFunction<T, Long> fn) {
      return new TTime<>(this.durationMillis, this.aggregating, requireNonNull(fn));
    }

    Time(long durationMillis, boolean aggregating, UnaryFunction<T, Long> eventTimeFn) {
      this.durationMillis = durationMillis;
      this.aggregating = aggregating;
      this.eventTimeFn = eventTimeFn;
    }

    @Override
    public Set<TimeWindow> assignWindows(T input) {
      long ts = eventTimeFn.apply(input);
      return singleton(
          new TimeWindow(ts - (ts + durationMillis) % durationMillis, durationMillis));
    }

    @Override
    public void updateTriggering(Triggering triggering, T input) {
      triggering.updateProcessed(eventTimeFn.apply(input));
    }

    @Override
    public boolean isAggregating() {
      return aggregating;
    }
  } // ~ end of Time

  final class Count<T>
      implements AlignedWindowing<T, Windowing.Count.Counted, Windowing.Count.CountWindow>,
                 MergingWindowing<T, Void, Windowing.Count.Counted, Windowing.Count.CountWindow>
  {
    private final int size;
    private boolean aggregating = false;

    private Count(int size) {
      this.size = size;
    }

    public static final class Counted implements Serializable {
      // ~ no equals/hashCode ... every instance is unique
    } // ~ end of Counted

    public static class CountWindow
        implements AlignedWindow<Counted>
    {
      private final Counted label = new Counted();
      int currentCount;

      CountWindow(int currentCount) {
        this.currentCount = currentCount;
      }

      @Override
      public Counted getLabel() {
        return label;
      }

      @Override
      public TriggerState registerTrigger(
          Triggering triggering, UnaryFunction<Window<?, ?>, Void> evict) {
        return TriggerState.INACTIVE;
      }

      @Override
      public String toString() {
        return "CountWindow { currentCount = " + currentCount + ", label = " +
            System.identityHashCode(label) + " }";
      }
    } // ~ end of CountWindow

    @Override
    public Set<CountWindow> assignWindows(T input) {
      return singleton(new CountWindow(1));
    }

    @Override
    public Collection<Pair<Collection<CountWindow>, CountWindow>>
    mergeWindows(Collection<CountWindow> actives)
    {
      Iterator<CountWindow> iter = actives.iterator();
      CountWindow r = null;
      while (r == null && iter.hasNext()) {
        CountWindow w = iter.next();
        if (w.currentCount < size) {
          r = w;
        }
      }
      if (r == null) {
        return actives.stream()
            .map(a -> Pair.of((Collection<CountWindow>) singleton(a), a))
            .collect(Collectors.toList());
      }

      Set<CountWindow> merged = null;
      iter = actives.iterator();
      while (iter.hasNext()) {
        CountWindow w = iter.next();
        if (r != w && r.currentCount + w.currentCount <= size) {
          r.currentCount += w.currentCount;
          if (merged == null) {
            merged = new HashSet<>();
          }
          merged.add(w);
        }
      }
      if (merged != null && !merged.isEmpty()) {
        merged.add(r);
        return singletonList(Pair.of(merged, r));
      }
      return null;
    }

    @Override
    public boolean isComplete(CountWindow window) {
      return window.currentCount >= size;
    }

    @Override
    public boolean isAggregating() {
      return aggregating;
    }

    public static <T> Count<T> of(int count) {
      return new Count<>(count);
    }

    @SuppressWarnings("unchecked")
    public <T> Count<T> aggregating() {
      this.aggregating = true;
      return (Count<T>) this;
    }
  } // ~ end of Count


  public static final class TimeSliding<T>
      implements AlignedWindowing<T, Long, TimeSliding.SlidingWindow> {

    public static class SlidingWindow implements AlignedWindow<Long> {

      private final long startTime;
      private final long duration;

      private SlidingWindow(long startTime, long duration) {
        this.startTime = startTime;
        this.duration = duration;
      }

      @Override
      public Long getLabel() {
        return startTime;
      }

      @Override
      public TriggerState registerTrigger(
          Triggering triggering, UnaryFunction<Window<?, ?>, Void> evict) {
        long scheduleAt = startTime + duration;
        if (triggering.scheduleAt(scheduleAt, () -> evict.apply(this))) {
          return TriggerState.ACTIVATED;
        }
        return TriggerState.PASSED;
      }

      @Override
      public boolean equals(Object obj) {
        if (obj == this) return true;
        if (obj instanceof SlidingWindow) {
          SlidingWindow other = (SlidingWindow) obj;
          return other.startTime == startTime;
        }
        return false;
      }

      @Override
      public int hashCode() {
        return (int) (startTime ^ (startTime >>> Integer.SIZE));
      }


    }

    public static <T> TimeSliding<T> of(long duration, long step) {
      return new TimeSliding<>(duration, step);
    }

    private final long duration;
    private final long step;
    private final int stepsPerWindow;

    private TimeSliding(long duration, long step) {
      this.duration = duration;
      this.step = step;

      if (duration % step != 0) {
        throw new IllegalArgumentException(
            "This time sliding window can manage only aligned sliding windows");
      }
      stepsPerWindow = (int) (duration / step);
    }


    @Override
    public Set<SlidingWindow> assignWindows(T input) {
      long now = System.currentTimeMillis() - duration + step;
      long boundary = now / step * step;
      Set<SlidingWindow> ret = new HashSet<>();
      for (int i = 0; i < stepsPerWindow; i++) {
        ret.add(new SlidingWindow(boundary, duration));
        boundary += step;
      }
      return ret;
    }


  }

  Set<W> assignWindows(T input);

  /**
   * Update triggering by given input. This is needed to enable the windowing
   * to move triggering in watermarking processing schemes based on event time.
   */
  default void updateTriggering(Triggering triggering, T input) {
    triggering.updateProcessed(System.currentTimeMillis());
  }

  default boolean isAggregating() {
    return false;
  }
  
}
