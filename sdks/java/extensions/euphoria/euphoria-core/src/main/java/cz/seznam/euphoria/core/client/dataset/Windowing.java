
package cz.seznam.euphoria.core.client.dataset;

import cz.seznam.euphoria.core.client.functional.UnaryFunction;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.Collections;
import java.util.Set;


/**
 * A windowing policy of a dataset.
 */
public interface Windowing<T, KEY, W extends Window<KEY>> extends Serializable {

  /** Time windows. */
  final class Time<T>
      extends AbstractWindowing<T, Void, Windowing.Time.TimeWindow>
      implements AlignedWindowing<T, Windowing.Time.TimeWindow> {

    private boolean aggregating;

    public static class TimeWindow
        extends AbstractWindow<Void>
        implements AlignedWindow
    {
      private final long startMillis;
      private final long intervalMillis;

      TimeWindow(long startMillis, long intervalMillis) {
        this.startMillis = startMillis;
        this.intervalMillis = intervalMillis;
      }

      @Override
      public boolean equals(Object other) {
        if (other instanceof TimeWindow) {
          TimeWindow that = (TimeWindow) other;
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

      @Override
      public void registerTrigger(Triggering triggering, UnaryFunction<Window<?>, Void> evict) {
        LoggerFactory.getLogger(Windowing.class)
            .info("registering trigger for window: {}", this);
        triggering.scheduleOnce(this.intervalMillis, () -> evict.apply(this));
      }

      @Override
      public String toString() {
        return "TimeWindow{" +
            "startMillis=" + startMillis +
            ", intervalMillis=" + intervalMillis +
            '}';
      }
    } // ~ end of IntervalWindow

    public static <T> Time<T> seconds(long seconds) {
      return new Time<>(seconds * 1000);
    }

    public static <T> Time<T> minutes(long minutes) {
      return new Time<>(minutes * 1000 * 60);
    }

    public static <T> Time<T> hours(long hours) {
      return new Time<>(hours * 1000 * 60 * 60);
    }

    @SuppressWarnings("unchecked")
    public <T> Time<T> aggregating() {
      this.aggregating = true;
      return (Time<T>) this;
    }

    final long duration;

    Time(long duration) {
      this.duration = duration;
    }

    @Override
    public Set<TimeWindow> assignWindows(T input) {
      long ts = timestampMillis(input);
      return Collections.singleton(
          new TimeWindow(ts - (ts + duration) % duration, duration));
    }

    protected long timestampMillis(T input) {
      return System.currentTimeMillis();
    }

    @Override
    public boolean isAggregating() {
      return aggregating;
    }
  }

//  final class Count<T>
//      extends AbstractWindowing<T, Void, Windowing.Count.CountWindow>
//      implements AlignedWindowing<T, Windowing.Count.CountWindow> {
//
//    private final int count;
//    private boolean aggregating = false;
//
//    private Count(int count) {
//      this.count = count;
//    }
//
//    public static class CountWindow extends AbstractWindow<Void>
//        implements AlignedWindow {
//
//      final int maxCount;
//      int currentCount = 0;
//      UnaryFunction<Window<?>, Void> evict = null;
//
//      public CountWindow(int maxCount) {
//        this.maxCount = maxCount;
//        this.currentCount = 1;
//      }
//
//
//      @Override
//      public void registerTrigger(Triggering triggering,
//          UnaryFunction<Window<?>, Void> evict) {
//        this.evict = evict;
//      }
//    }
//
//    @Override
//    public Set<CountWindow> allocateWindows(
//        T input, Triggering triggering,
//        UnaryFunction<Window<?>, Void> evict) {
//      Set<CountWindow> ret = new HashSet<>();
//      for (CountWindow w : getActive(null)) {
//        if (w.currentCount < count) {
//          w.currentCount++;
//          ret.add(w);
//        }
//      }
//      if (ret.isEmpty()) {
//        CountWindow w = new CountWindow(count);
//        this.addNewWindow(w, triggering, evict);
//        ret.add(w);
//      }
//      return ret;
//    }
//
//
//    @Override
//    public boolean isAggregating() {
//      return aggregating;
//    }
//
//
//    public static <T> Count<T> of(int count) {
//      return new Count<>(count);
//    }
//
//    @SuppressWarnings("unchecked")
//    public <T> Count<T> aggregating() {
//      this.aggregating = true;
//      return (Count<T>) this;
//    }
//
//
//  }

  Set<W> assignWindows(T input);

// XXX to be replaced with assignWindows and mergeWindows
//  /**
//   * Allocate and return windows needed for given element.
//   * Register trigger for all newly created windows.
//   * @return the set of windows suitable for given element
//   **/
//  Set<W> allocateWindows(T input, Triggering triggering,
//      UnaryFunction<Window<?>, Void> evict);

// XXX to be dropped entirely
//  /** Retrieve currently active set of windows for given key. */
//  Set<W> getActive(KEY key);

  /** Evict given window. */
  void close(W window);

  default boolean isAggregating() {
    return false;
  }
  
}
