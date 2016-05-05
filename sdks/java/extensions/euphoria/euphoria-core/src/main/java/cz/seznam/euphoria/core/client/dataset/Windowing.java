
package cz.seznam.euphoria.core.client.dataset;

import cz.seznam.euphoria.core.client.functional.UnaryFunction;

import java.io.Serializable;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
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
  } // ~ end of Time

  final class Count<T>
      extends AbstractWindowing<T, Void, Windowing.Count.CountWindow>
      implements AlignedWindowing<T, Windowing.Count.CountWindow>,
                 MergingWindowing<T, Void, Windowing.Count.CountWindow>
  {
    private final int size;
    private boolean aggregating = false;

    private Count(int size) {
      this.size = size;
    }

    public static class CountWindow
        implements AlignedWindow
    {
      int currentCount;
      UnaryFunction<Window<?>, Void> evict = null;

      public CountWindow(int currentCount) {
        this.currentCount = currentCount;
      }

      @Override
      public void registerTrigger(Triggering triggering,
          UnaryFunction<Window<?>, Void> evict) {
        this.evict = evict;
      }

      // ~ no equals/hashCode all instances are considered unique

      @Override
      public String toString() {
        return "CountWindow { currentCount = " + currentCount + ", identity = " +
            System.identityHashCode(this) + " }";
      }
    } // ~ end of CountWindow

    @Override
    public Set<CountWindow> assignWindows(T input) {
      return Collections.singleton(new CountWindow(1));
    }

    @Override
    public void mergeWindows(Set<CountWindow> actives,
                             Merging<Void, CountWindow> merging)
    {
      // XXX this will need a rewrite for better efficiency

      Iterator<CountWindow> iter = actives.iterator();
      CountWindow r = null;
      while (r == null && iter.hasNext()) {
        CountWindow w = iter.next();
        if (w.currentCount < size) {
          r = w;
        }
      }
      if (r == null) {
        return;
      }

      Set<CountWindow> merged = null;
      iter = actives.iterator();
      while (iter.hasNext()) {
        CountWindow w = iter.next();
        if (r.equals(w)) {
          continue;
        }
        if (r.currentCount + w.currentCount <= size) {
          r.currentCount += w.currentCount;
          if (merged == null) {
            merged = new HashSet<>();
          }
          merged.add(w);
        }
      }
      if (merged != null && !merged.isEmpty()) {
        merged.add(r);
        merging.onMerge(merged, r);
        if (r.currentCount >= size) {
          r.evict.apply(r);
        }
      }
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

  Set<W> assignWindows(T input);

  default boolean isAggregating() {
    return false;
  }
  
}
