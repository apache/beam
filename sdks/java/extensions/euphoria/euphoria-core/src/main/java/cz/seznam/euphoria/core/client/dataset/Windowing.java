
package cz.seznam.euphoria.core.client.dataset;

import cz.seznam.euphoria.core.client.functional.UnaryFunction;
import cz.seznam.euphoria.core.client.triggers.TimeTrigger;
import cz.seznam.euphoria.core.client.triggers.Trigger;
import cz.seznam.euphoria.core.client.util.Pair;
import cz.seznam.euphoria.core.executor.TriggerScheduler;

import java.io.Serializable;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
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

      // singleton
      private ProcessingTime() {}

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

      @Override
      public String toString() {
        return "TimeInterval{" +
            "startMillis=" + startMillis +
            ", intervalMillis=" + intervalMillis +
            '}';
      }
    } // ~ end of TimeInterval

    public static class TimeWindow
        extends EarlyTriggeredWindow
        implements AlignedWindow<TimeInterval>
    {
      private final TimeInterval label;
      private final long fireStamp;

      TimeWindow(long startMillis, long intervalMillis, Duration earlyTriggering) {
        super(earlyTriggering, startMillis + intervalMillis);
        this.label = new TimeInterval(startMillis, intervalMillis);
        this.fireStamp = startMillis + intervalMillis;
      }

      @Override
      public TimeInterval getLabel() {
        return label;
      }

      @Override
      public List<Trigger> createTriggers() {
        List<Trigger> triggers = new ArrayList<>(1);
        if (isEarlyTriggered()) {
          triggers.add(getEarlyTrigger());
        }

        triggers.add(new TimeTrigger(fireStamp));

        return triggers;
      }
    } // ~ end of TimeWindow

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
      public <T> UTime<T> earlyTriggering(After time) {
        // ~ the unchecked cast is ok: eventTimeFn is
        // ProcessingTime which is independent of <T>
        return new UTime<>(super.durationMillis, time.period);
      }

      /**
       * Function that will extract timestamp from data
       */
      public <T> TTime<T> using(UnaryFunction<T, Long> fn) {
        return new TTime<>(this.durationMillis, earlyTriggeringPeriod, requireNonNull(fn));
      }
    }

    // ~ a typed variant of the Time windowing; depends on the type of
    // input elements
    public static class TTime<T> extends Time<T> {
      TTime(long durationMillis,
            Duration earlyTriggeringPeriod,
            UnaryFunction<T, Long> eventTimeFn)
      {
        super(durationMillis, earlyTriggeringPeriod, eventTimeFn);
      }
    } // ~ end of EventTimeBased

    final long durationMillis;
    final Duration earlyTriggeringPeriod;
    final UnaryFunction<T, Long> eventTimeFn;

    public static <T> UTime<T> seconds(long seconds) {
      return new UTime<>(seconds * 1000, null);
    }

    public static <T> UTime<T> minutes(long minutes) {
      return new UTime<>(minutes * 1000 * 60, null);
    }

    public static <T> UTime<T> hours(long hours) {
      return new UTime<>(hours * 1000 * 60 * 60, null);
    }

    public Time(long durationMillis, Duration earlyTriggeringPeriod, UnaryFunction<T, Long> eventTimeFn) {
      this.durationMillis = durationMillis;
      this.earlyTriggeringPeriod = earlyTriggeringPeriod;
      this.eventTimeFn = eventTimeFn;
    }

    @Override
    public Set<TimeWindow> assignWindows(T input) {
      long ts = eventTimeFn.apply(input);
      return singleton(
          new TimeWindow(ts - (ts + durationMillis) % durationMillis, durationMillis, earlyTriggeringPeriod));
    }

    @Override
    public void updateTriggering(TriggerScheduler triggering, T input) {
      triggering.updateProcessed(eventTimeFn.apply(input));
    }
  } // ~ end of Time

  final class Count<T>
      implements AlignedWindowing<T, Windowing.Count.Counted, Windowing.Count.CountWindow>,
                 MergingWindowing<T, Void, Windowing.Count.Counted, Windowing.Count.CountWindow>
  {
    private final int size;

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
      public List<Trigger> createTriggers() {
        return Collections.emptyList();
      }

      @Override
      public String toString() {
        return "CountWindow { currentCount = " + currentCount
            + ", label = " + label + " }";
      }
    } // ~ end of CountWindow

    @Override
    public Set<CountWindow> assignWindows(T input) {
      return singleton(new CountWindow(1));
    }

    @Override
    public void updateTriggering(TriggerScheduler triggering, T input) {
      // ~ no-op; count windows is not registering any triggers
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

    public static <T> Count<T> of(int count) {
      return new Count<>(count);
    }
  } // ~ end of Count


  final class TimeSliding<T>
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
      public List<Trigger> createTriggers() {
        return Collections.singletonList(new TimeTrigger(startTime + duration));
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

      @Override
      public String toString() {
        return "SlidingWindow{" +
            "startTime=" + startTime +
            ", duration=" + duration +
            '}';
      }
    }

    public static <T> TimeSliding<T> of(long duration, long step) {
      return new TimeSliding<>(duration, step, Time.ProcessingTime.get());
    }

    private final long duration;
    private final long step;
    private final int stepsPerWindow;
    private final UnaryFunction<T, Long> eventTimeFn;

    private TimeSliding(long duration, long step, UnaryFunction<T, Long> eventTimeFn) {
      this.duration = duration;
      this.step = step;
      this.eventTimeFn = requireNonNull(eventTimeFn);

      if (duration % step != 0) {
        throw new IllegalArgumentException(
            "This time sliding window can manage only aligned sliding windows");
      }
      stepsPerWindow = (int) (duration / step);
    }

    /**
     * Specify the event time extraction function.
     */
    public <T> TimeSliding<T> using(UnaryFunction<T, Long> eventTimeFn) {
      return new TimeSliding<>(this.duration, this.step, eventTimeFn);
    }

    @Override
    public Set<SlidingWindow> assignWindows(T input) {
      long now = eventTimeFn.apply(input) - duration + step;
      long boundary = now / step * step;
      Set<SlidingWindow> ret = new HashSet<>();
      for (int i = 0; i < stepsPerWindow; i++) {
        ret.add(new SlidingWindow(boundary, duration));
        boundary += step;
      }
      return ret;
    }

    @Override
    public void updateTriggering(TriggerScheduler triggering, T input) {
      triggering.updateProcessed(eventTimeFn.apply(input));
    }

    @Override
    public String toString() {
      return "TimeSliding{" +
          "duration=" + duration +
          ", step=" + step +
          ", stepsPerWindow=" + stepsPerWindow +
          '}';
    }
  } // ~ end of TimeSliding

  /** Session windows. */
  final class Session<T, G>
      implements MergingWindowing<T, G, Session.SessionInterval, Session.SessionWindow<G>>
  {
    public static final class SessionInterval
        implements Serializable, Comparable<SessionInterval>
    {
      private final long startMillis;
      private final long endMillis;

      public SessionInterval(long startMillis, long endMillis) {
        this.startMillis = startMillis;
        this.endMillis = endMillis;
      }

      public long getStartMillis() {
        return startMillis;
      }

      public long getEndMillis() {
        return endMillis;
      }

      @Override
      public boolean equals(Object o) {
        if (o instanceof SessionInterval) {
          SessionInterval that = (SessionInterval) o;
          return this.startMillis == that.startMillis
              && this.endMillis == that.endMillis;
        }
        return false;
      }

      @Override
      public int hashCode() {
        int result = (int) (startMillis ^ (startMillis >>> 32));
        result = 31 * result + (int) (endMillis ^ (endMillis >>> 32));
        return result;
      }

      boolean intersects(SessionInterval that) {
        return this.startMillis < that.endMillis
            && this.endMillis > that.startMillis;
      }

      SessionInterval createSpanned(SessionInterval that) {
        return new SessionInterval(
            Long.min(this.startMillis, that.startMillis),
            Long.max(this.endMillis, that.endMillis));
      }

      @Override
      public int compareTo(SessionInterval that) {
        if (this.startMillis == that.startMillis) {
          return (int) (this.endMillis - that.endMillis);
        }
        // this.startMillis == that.startMillis captured above already
        return (int) (this.startMillis - that.startMillis);
      }

      @Override
      public String toString() {
        return "SessionInterval{" +
            "startMillis=" + startMillis +
            ", endMillis=" + endMillis +
            '}';
      }
    } // ~ end of SessionInterval

    public static final class SessionWindow<G>
        extends EarlyTriggeredWindow
        implements Window<G, SessionInterval>
    {
      private final G group;
      private final SessionInterval label;

      SessionWindow(G group, SessionInterval label, Duration earlyFiringDuration) {
        super(earlyFiringDuration, label.getEndMillis());
        this.group = group;
        this.label = label;
      }

      @Override
      public G getGroup() {
        return group;
      }

      @Override
      public SessionInterval getLabel() {
        return label;
      }

      @Override
      public List<Trigger> createTriggers() {
        if (isEarlyTriggered()) {
          return Arrays.asList(
              getEarlyTrigger(),
              new TimeTrigger(label.getEndMillis()));
        } else {
          return Collections.singletonList(new TimeTrigger(label.getEndMillis()));
        }
      }

      @Override
      public String toString() {
        return "SessionWindow{" +
            "group=" + group +
            ", label=" + label +
            '}';
      }
    } // ~ end of SessionWindow

    public static final class OfChain {
      private final long gapMillis;

      private OfChain(Duration gap) {
        gapMillis = gap.toMillis();
      }

      public EarlyTriggeringChain earlyTriggering(After time) {
        return new EarlyTriggeringChain(this, requireNonNull(time));
      }

      public <T, G> Session<T, G> using(UnaryFunction<T, G> groupFn) {
        return new EarlyTriggeringChain(this, null).using(groupFn);
      }

      public <T, G> Session<T, G> using(
          UnaryFunction<T, G> groupFn, UnaryFunction<T, Long> eventFn)
      {
        return new EarlyTriggeringChain(this, null).using(groupFn, eventFn);
      }
    } // ~ end of OfChain

    public static class EarlyTriggeringChain {
      private final OfChain ofChain;
      private final After earlyTriggering;

      private EarlyTriggeringChain(
          OfChain ofChain,
          After earlyTriggering /* optional */ )
      {
        this.ofChain = requireNonNull(ofChain);
        this.earlyTriggering = earlyTriggering;
      }

      public <T, G> Session<T, G> using(UnaryFunction<T, G> groupFn) {
        return new Session<>(groupFn, Time.ProcessingTime.get(),
            this.ofChain.gapMillis, null);
      }

      public <T, G> Session<T, G> using(
          UnaryFunction<T, G> groupFn, UnaryFunction<T, Long> eventFn)
      {
        return new Session<>(groupFn, eventFn,
            this.ofChain.gapMillis,
            this.earlyTriggering == null ? null : this.earlyTriggering.period);
      }
    } // ~ end of EarlyTriggeringChain

    public static OfChain of(Duration gapDuration) {
      return new OfChain(gapDuration);
    }

    final UnaryFunction<T, G> groupFn;
    final UnaryFunction<T, Long> eventTimeFn;
    final long gapDurationMillis;
    final Duration earlyTriggeringPeriod;

    private Session(
        UnaryFunction<T, G> groupFn,
        UnaryFunction<T, Long> eventTimeFn,
        long gapDurationMillis,
        Duration earlyTriggeringPeriod /* optional */)
    {
      this.groupFn = requireNonNull(groupFn);
      this.eventTimeFn = requireNonNull(eventTimeFn);
      this.gapDurationMillis = gapDurationMillis;
      this.earlyTriggeringPeriod = earlyTriggeringPeriod;
    }

    @Override
    public Set<SessionWindow<G>> assignWindows(T input) {
      long evtMillis = this.eventTimeFn.apply(input);
      SessionWindow<G> w = new SessionWindow<>(
          this.groupFn.apply(input),
          new SessionInterval(evtMillis, evtMillis + gapDurationMillis),
          earlyTriggeringPeriod);
      return Collections.singleton(w);
    }

    @Override
    public Collection<Pair<Collection<SessionWindow<G>>, SessionWindow<G>>>
    mergeWindows(Collection<SessionWindow<G>> actives)
    {
      if (actives.size() < 2) {
        return Collections.emptyList();
      }

      ArrayList<SessionWindow<G>> sorted = new ArrayList<>(actives);
      sorted.sort(Comparator.comparing(SessionWindow<G>::getLabel));

      Iterator<SessionWindow<G>> windows = sorted.iterator();

      // ~ the final collection of merges to be performed by the framework
      List<Pair<Collection<SessionWindow<G>>, SessionWindow<G>>> merges = null;

      // ~ holds the list of existing session windows to be merged
      List<SessionWindow<G>> toMerge = null;
      // ~ the current merge candidate
      SessionWindow<G> mergeCandidate = windows.next();
      // ~ true if `mergeCandidate` is a newly created window
      boolean transientCandidate = false;
      while (windows.hasNext()) {
        SessionWindow<G> w = windows.next();
        if (mergeCandidate.getLabel().intersects(w.getLabel())) {
          if (toMerge == null) {
            toMerge = new ArrayList<>();
          }
          if (!transientCandidate) {
            toMerge.add(mergeCandidate);
          }
          toMerge.add(w);
          mergeCandidate = new SessionWindow<>(
              mergeCandidate.getGroup(), mergeCandidate.getLabel().createSpanned(w.getLabel()),
              earlyTriggeringPeriod);
          transientCandidate = true;
        } else {
          if (toMerge != null && !toMerge.isEmpty()) {
            if (merges == null) {
              merges = new ArrayList<>();
            }
            merges.add(Pair.of(toMerge, mergeCandidate));
            toMerge = null;
          }
          mergeCandidate = w;
          transientCandidate = false;
        }
      }
      // ~ flush pending state
      if (toMerge != null) {
        if (merges == null) {
          merges = new ArrayList<>();
        }
        merges.add(Pair.of(toMerge, mergeCandidate));
      }
      // ~ deliver results (be sure not to return null)
      return merges == null ? Collections.emptyList() : merges;
    }

    @Override
    public void updateTriggering(TriggerScheduler triggering, T input) {
      triggering.updateProcessed(eventTimeFn.apply(input));
    }
  } // ~ end of Session

  Set<W> assignWindows(T input);

  /**
   * Update triggering by given input. This is needed to enable the windowing
   * to move triggering in watermarking processing schemes based on event time.
   */
  default void updateTriggering(TriggerScheduler triggering, T input) {
    triggering.updateProcessed(System.currentTimeMillis());
  }
}
