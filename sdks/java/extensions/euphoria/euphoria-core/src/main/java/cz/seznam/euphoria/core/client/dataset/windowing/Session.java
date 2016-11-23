
package cz.seznam.euphoria.core.client.dataset.windowing;

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
import java.util.Objects;
import java.util.Set;

/**
 * Session windowing.
 */
public final class Session<T> implements MergingWindowing<T, TimeInterval> {

  private final long gapDurationMillis;
  private Duration earlyTriggeringPeriod;

  public static <T> Session<T> of(Duration gapDuration) {
    return new Session<>(gapDuration.toMillis());
  }

  private Session(long gapDurationMillis) {
    this.gapDurationMillis = gapDurationMillis;
  }

  /**
   * Early results will be triggered periodically until the window is finally closed.
   */
  public <T> Session<T> earlyTriggering(Duration timeout) {
    this.earlyTriggeringPeriod = Objects.requireNonNull(timeout);
    return (Session) this;
  }

  @Override
  public Set<TimeInterval> assignWindowsToElement(WindowedElement<?, T> el) {
    TimeInterval ret =
            new TimeInterval(el.timestamp, el.timestamp + gapDurationMillis);
    return Collections.singleton(ret);
  }

  @Override
  public Trigger<TimeInterval> getTrigger() {
    if (earlyTriggeringPeriod != null) {
      return new AfterFirstCompositeTrigger<>(Arrays.asList(
              new TimeTrigger(),
              new PeriodicTimeTrigger(earlyTriggeringPeriod.toMillis())));
    }

    return new TimeTrigger();
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
}
