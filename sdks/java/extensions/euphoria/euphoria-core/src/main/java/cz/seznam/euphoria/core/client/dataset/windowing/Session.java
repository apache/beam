
package cz.seznam.euphoria.core.client.dataset.windowing;

import cz.seznam.euphoria.core.client.functional.UnaryFunction;
import cz.seznam.euphoria.core.client.triggers.AfterFirstCompositeTrigger;
import cz.seznam.euphoria.core.client.triggers.PeriodicTimeTrigger;
import cz.seznam.euphoria.core.client.triggers.TimeTrigger;
import cz.seznam.euphoria.core.client.triggers.Trigger;
import cz.seznam.euphoria.core.client.util.Pair;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import static java.util.Objects.requireNonNull;

import java.util.Optional;
import java.util.Set;

/**
 * Session windowing.
 */
public final class Session<T> implements
        MergingWindowing<T, TimeInterval> {

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
            Duration earlyTriggering /* optional */) {

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
          Duration earlyTriggeringPeriod /* optional */) {
    this.eventTimeFn = requireNonNull(eventTimeFn);
    this.gapDurationMillis = gapDurationMillis;
    this.earlyTriggeringPeriod = earlyTriggeringPeriod;
  }

  @Override
  public Set<TimeInterval> assignWindowsToElement(
          WindowedElement<?, T> input) {
    long evtMillis = this.eventTimeFn.apply(input.get());
    TimeInterval ret =
            new TimeInterval(evtMillis, evtMillis + gapDurationMillis);
    return Collections.singleton(ret);
  }

  @Override
  public Trigger<T, TimeInterval> getTrigger() {
    if (earlyTriggeringPeriod != null) {
      return new AfterFirstCompositeTrigger<>(Arrays.asList(
              new TimeTrigger<>(),
              new PeriodicTimeTrigger<>(earlyTriggeringPeriod.toMillis())));
    }

    return new TimeTrigger<>();
  }

  @Override
  public Collection<Pair<Collection<TimeInterval>, TimeInterval>>
  mergeWindows(Collection<TimeInterval> actives) {
    if (actives.size() < 2) {
      return Collections.emptyList();
    }

    List<TimeInterval> sorted = new ArrayList<>(actives);
    Collections.sort(sorted);

    Iterator<TimeInterval> windows = sorted.iterator();

    // ~ the final collection of merges to be performed by the framework
    List<Pair<Collection<TimeInterval>, TimeInterval>> merges = null;

    // ~ holds the list of existing session windows to be merged
    List<TimeInterval> toMerge = null;
    // ~ the current merge candidate
    TimeInterval mergeCandidate = windows.next();
    // ~ true if `mergeCandidate` is a newly created window
    boolean transientCandidate = false;
    while (windows.hasNext()) {
      TimeInterval w = windows.next();
      if (mergeCandidate.intersects(w)) {
        if (toMerge == null) {
          toMerge = new ArrayList<>();
        }
        if (!transientCandidate) {
          toMerge.add(mergeCandidate);
        }
        toMerge.add(w);
        mergeCandidate = mergeCandidate.cover(w);

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
