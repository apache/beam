
package cz.seznam.euphoria.core.client.dataset;

import com.google.common.collect.Sets;
import cz.seznam.euphoria.core.client.functional.UnaryFunction;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Set;


/**
 * A windowing policy of a dataset.
 */
public interface Windowing<T, KEY, W extends Window<KEY, W>> extends Serializable {

  /** Time windows. */
  final class Time<T>
      extends AbstractWindowing<T, Void, Windowing.Time.TimeWindow>
      implements AlignedWindowing<T, Windowing.Time.TimeWindow> {

    private boolean aggregating;

    public static class TimeWindow extends AbstractWindow<Void, TimeWindow>
        implements AlignedWindow<TimeWindow> {

      private final Time<?> windowing;
      private final long startStamp;
      private final long duration;
      
      TimeWindow(Time<?> windowing, long startStamp, long duration) {
        this.windowing = windowing;
        this.startStamp = startStamp;
        this.duration = duration;
      }


      @Override
      public void registerTrigger(final Triggering triggering,
          final UnaryFunction<TimeWindow, Void> evict) {
        triggering.scheduleOnce(duration, () -> {
          TimeWindow newWindow = windowing.createNew(triggering, evict);
          if (windowing.aggregating) {
            TimeWindow.this.getStates().stream().forEach(newWindow::addState);
          }
          evict.apply(TimeWindow.this);
        });
      }

    }

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
    public synchronized Set<TimeWindow> allocateWindows(
        T what, Triggering triggering, UnaryFunction<TimeWindow, Void> evict) {
      Set<TimeWindow> ret = new HashSet<>();
      for (TimeWindow w : getActive(null)) {
        // FIXME: need a way to extract stamp from value
        if (System.currentTimeMillis() - w.startStamp < duration) {
          // accept this window
          ret.add(w);
        }
      }
      if (ret.isEmpty()) {
        return Sets.newHashSet(createNew(triggering, evict));
      }
      return ret;
    }

    @Override
    public boolean isAggregating() {
      return aggregating;
    }

    // create new window
    synchronized TimeWindow createNew(Triggering triggering, UnaryFunction<TimeWindow, Void> evict) {
      return this.addNewWindow(new TimeWindow(
          this, System.currentTimeMillis(), duration), triggering, evict);
    }

  }


  final class Count<T>
      extends AbstractWindowing<T, Void, Windowing.Count.CountWindow>
      implements AlignedWindowing<T, Windowing.Count.CountWindow> {

    private final int count;
    private boolean aggregating = false;

    private Count(int count) {
      this.count = count;
    }

    public static class CountWindow extends AbstractWindow<Void, CountWindow>
        implements AlignedWindow<CountWindow> {

      final int maxCount;
      int currentCount = 0;
      UnaryFunction<CountWindow, Void> evict = null;

      public CountWindow(int maxCount) {
        this.maxCount = maxCount;
        this.currentCount = 1;
      }


      @Override
      public void registerTrigger(Triggering triggering,
          UnaryFunction<CountWindow, Void> evict) {
        this.evict = evict;
      }
    }

    @Override
    public Set<CountWindow> allocateWindows(
        T input, Triggering triggering, UnaryFunction<CountWindow, Void> evict) {
      Set<CountWindow> ret = new HashSet<>();
      for (CountWindow w : getActive(null)) {
        if (w.currentCount < count) {
          w.currentCount++;
          ret.add(w);
        }
      }
      if (ret.isEmpty()) {
        CountWindow w = new CountWindow(count);
        this.addNewWindow(w, triggering, evict);
        ret.add(w);
      }
      return ret;
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


  }

  /**
   * Allocate and return windows needed for given element.
   * Register trigger for all newly created windows.
   * @return the set of windows suitable for given element
   **/
  Set<W> allocateWindows(T input, Triggering triggering, UnaryFunction<W, Void> evict);

  /** Retrieve currently active set of windows for given key. */
  Set<W> getActive(KEY key);

  /** Evict given window. */
  void close(W window);

  default boolean isAggregating() {
    return false;
  }
  
}
