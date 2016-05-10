
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


/**
 * A windowing policy of a dataset.
 */
public interface Windowing<T, GROUP, LABEL, W extends Window<GROUP, LABEL>>
    extends Serializable
{
  /** Time windows. */
  final class Time<T>
      implements AlignedWindowing<T, Windowing.Time.TimeInterval, Windowing.Time.TimeWindow>
  {
    private boolean aggregating;

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
        implements AlignedWindow<TimeInterval>
    {
      private final TimeInterval label;

      TimeWindow(long startMillis, long intervalMillis) {
        this.label = new TimeInterval(startMillis, intervalMillis);
      }

      @Override
      public TimeInterval getLabel() {
        return label;
      }

      @Override
      public void registerTrigger(
          Triggering triggering, UnaryFunction<Window<?, ?>, Void> evict)
      {
        triggering.scheduleOnce(this.label.getIntervalMillis(), () -> evict.apply(this));
      }
    } // ~ end of TimeWindow

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

    final long durationMillis;

    Time(long durationMillis) {
      this.durationMillis = durationMillis;
    }

    @Override
    public Set<TimeWindow> assignWindows(T input) {
      long ts = timestampMillis(input);
      return singleton(
          new TimeWindow(ts - (ts + durationMillis) % durationMillis, durationMillis));
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
      public void registerTrigger(
          Triggering triggering, UnaryFunction<Window<?, ?>, Void> evict)
      {
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

  Set<W> assignWindows(T input);

  default boolean isAggregating() {
    return false;
  }
  
}
