
package cz.seznam.euphoria.core.client.dataset.windowing;

import cz.seznam.euphoria.core.client.functional.UnaryFunction;
import cz.seznam.euphoria.core.client.triggers.TimeTrigger;
import cz.seznam.euphoria.core.client.triggers.Trigger;
import cz.seznam.euphoria.core.client.util.Pair;
import java.io.Serializable;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import static java.util.Objects.requireNonNull;
import java.util.Optional;
import java.util.Set;

/**
 * Session windowing.
 */
public final class Session<T> implements
    MergingWindowing<T, Session.SessionInterval, Session.SessionWindowContext> {

  public static final class SessionInterval
      implements Serializable, Comparable<SessionInterval> {
    
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

  public static final class SessionWindowContext
      extends EarlyTriggeredWindowContext<SessionInterval> {

    SessionWindowContext(
        SessionInterval label,
        Duration earlyFiringDuration) {
      super(new WindowID<>(label),
          earlyFiringDuration, label.getStartMillis(), label.getEndMillis());
   }

    @Override
    public List<Trigger> createTriggers() {
      if (isEarlyTriggered()) {
        return Arrays.asList(
            getEarlyTrigger(),
            new TimeTrigger(getWindowID().getLabel().getEndMillis()));
      } else {
        return Collections.singletonList(
            new TimeTrigger(getWindowID().getLabel().getEndMillis()));
      }
    }

    @Override
    public String toString() {
      return "SessionWindowContext{" +
          "id=" + getWindowID() +
          '}';
    }


  } // ~ end of SessionWindowContext

  public static final class OfChain {
    private final long gapMillis;

    private OfChain(Duration gap) {
      gapMillis = gap.toMillis();
    }

    public EarlyTriggeringChain earlyTriggering(Duration timeout) {
      return new EarlyTriggeringChain(this, requireNonNull(timeout));
    }

    public <T> Session<T> using(UnaryFunction<T, Long> eventFn) {
      return new EarlyTriggeringChain(this, null).using(eventFn);
    }
  } // ~ end of OfChain

  public static class EarlyTriggeringChain {
    private final OfChain ofChain;
    private final Duration earlyTriggering;

    private EarlyTriggeringChain(
        OfChain ofChain,
        Duration earlyTriggering /* optional */ ) {
      
      this.ofChain = requireNonNull(ofChain);
      this.earlyTriggering = earlyTriggering;
    }

    public <T> Session<T> using(UnaryFunction<T, Long> eventFn) {
      return new Session<>(eventFn, this.ofChain.gapMillis, this.earlyTriggering);
    }
    
  }

  public static OfChain of(Duration gapDuration) {
    return new OfChain(gapDuration);
  }

  final UnaryFunction<T, Long> eventTimeFn;
  final long gapDurationMillis;
  final Duration earlyTriggeringPeriod;

  private Session(
      UnaryFunction<T, Long> eventTimeFn,
      long gapDurationMillis,
      Duration earlyTriggeringPeriod /* optional */)
  {
    this.eventTimeFn = requireNonNull(eventTimeFn);
    this.gapDurationMillis = gapDurationMillis;
    this.earlyTriggeringPeriod = earlyTriggeringPeriod;
  }

  @Override
  public Set<WindowID<SessionInterval>> assignWindowsToElement(
      WindowedElement<?, T> input) {
    long evtMillis = this.eventTimeFn.apply(input.get());
    WindowID<SessionInterval> ret = new WindowID<>(
        new SessionInterval(evtMillis, evtMillis + gapDurationMillis));
    return Collections.singleton(ret);
  }

  @Override
  public Collection<Pair<Collection<SessionWindowContext>, SessionWindowContext>>
  mergeWindows(Collection<SessionWindowContext> actives) {
    if (actives.size() < 2) {
      return Collections.emptyList();
    }

    ArrayList<SessionWindowContext> sorted = new ArrayList<>(actives);
    sorted.sort(Comparator.comparing(w -> w.getWindowID().getLabel()));

    Iterator<SessionWindowContext> windows = sorted.iterator();

    // ~ the final collection of merges to be performed by the framework
    List<Pair<Collection<SessionWindowContext>, SessionWindowContext>> merges = null;

    // ~ holds the list of existing session windows to be merged
    List<SessionWindowContext> toMerge = null;
    // ~ the current merge candidate
    SessionWindowContext mergeCandidate = windows.next();
    // ~ true if `mergeCandidate` is a newly created window
    boolean transientCandidate = false;
    while (windows.hasNext()) {
      SessionWindowContext w = windows.next();
      if (mergeCandidate.getWindowID().getLabel().intersects(w.getWindowID().getLabel())) {
        if (toMerge == null) {
          toMerge = new ArrayList<>();
        }
        if (!transientCandidate) {
          toMerge.add(mergeCandidate);
        }
        toMerge.add(w);
        mergeCandidate = new SessionWindowContext(
            mergeCandidate.getWindowID().getLabel()
                .createSpanned(w.getWindowID().getLabel()),
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
  public SessionWindowContext createWindowContext(WindowID<SessionInterval> id) {
    return new SessionWindowContext(
        id.getLabel(),
        earlyTriggeringPeriod);
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
