/*
 * Copyright 2016-2018 Seznam.cz, a.s.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package cz.seznam.euphoria.core.client.dataset.windowing;

import static com.google.common.base.Preconditions.checkArgument;

import cz.seznam.euphoria.core.annotation.audience.Audience;
import cz.seznam.euphoria.core.annotation.stability.Experimental;
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
import javax.annotation.Nullable;

/** Session windowing. */
@Audience(Audience.Type.CLIENT)
public final class Session<T> implements MergingWindowing<T, TimeInterval> {

  private final long gapDurationMillis;
  @Nullable private Duration earlyTriggeringPeriod;

  private Session(long gapDurationMillis) {
    checkArgument(gapDurationMillis > 0, "Windowing with zero duration");
    this.gapDurationMillis = gapDurationMillis;
  }

  public static <T> Session<T> of(Duration gapDuration) {
    return new Session<>(gapDuration.toMillis());
  }

  /**
   * Early results will be triggered periodically until the window is finally closed.
   *
   * @param <T> the type of elements dealt with
   * @param timeout the period after which to periodically trigger windows
   * @return this instance (for method chaining purposes)
   */
  @Experimental("https://github.com/seznam/euphoria/issues/43")
  @SuppressWarnings("unchecked")
  public <T> Session<T> earlyTriggering(Duration timeout) {
    this.earlyTriggeringPeriod = Objects.requireNonNull(timeout);
    // ~ the cast is safe, this windowing implementation is self contained,
    // i.e. cannot be subclasses, and is not dependent the actual <T> at all
    return (Session) this;
  }

  @Override
  public Iterable<TimeInterval> assignWindowsToElement(WindowedElement<?, T> el) {
    long stamp = el.getTimestamp();
    TimeInterval ret = new TimeInterval(stamp, stamp + gapDurationMillis);
    return Collections.singleton(ret);
  }

  @Override
  public Trigger<TimeInterval> getTrigger() {
    if (earlyTriggeringPeriod != null) {
      return new AfterFirstCompositeTrigger<>(
          Arrays.asList(
              new TimeTrigger(), new PeriodicTimeTrigger(earlyTriggeringPeriod.toMillis())));
    }

    return new TimeTrigger();
  }

  @Override
  public Collection<Pair<Collection<TimeInterval>, TimeInterval>> mergeWindows(
      Collection<TimeInterval> actives) {
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
  public boolean equals(Object obj) {
    if (obj instanceof Session) {
      Session other = (Session) obj;
      return other.earlyTriggeringPeriod == earlyTriggeringPeriod
          && other.gapDurationMillis == gapDurationMillis;
    }
    return false;
  }

  @Override
  public int hashCode() {
    return Objects.hash(earlyTriggeringPeriod, gapDurationMillis);
  }
}
