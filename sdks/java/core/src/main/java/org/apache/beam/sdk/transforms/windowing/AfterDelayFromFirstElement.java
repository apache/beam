/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.beam.sdk.transforms.windowing;

import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.coders.InstantCoder;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.Min;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.windowing.Trigger.OnceTrigger;
import org.apache.beam.sdk.util.TimeDomain;
import org.apache.beam.sdk.util.state.AccumulatorCombiningState;
import org.apache.beam.sdk.util.state.CombiningState;
import org.apache.beam.sdk.util.state.MergingStateAccessor;
import org.apache.beam.sdk.util.state.StateAccessor;
import org.apache.beam.sdk.util.state.StateMerging;
import org.apache.beam.sdk.util.state.StateTag;
import org.apache.beam.sdk.util.state.StateTags;

import com.google.common.collect.ImmutableList;

import org.joda.time.Duration;
import org.joda.time.Instant;
import org.joda.time.format.PeriodFormat;
import org.joda.time.format.PeriodFormatter;

import java.util.List;
import java.util.Locale;
import java.util.Objects;
import javax.annotation.Nullable;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * A base class for triggers that happen after a processing time delay from the arrival
 * of the first element in a pane.
 *
 * <p>This class is for internal use only and may change at any time.
 */
@Experimental(Experimental.Kind.TRIGGER)
public abstract class AfterDelayFromFirstElement extends OnceTrigger {

  protected static final List<SerializableFunction<Instant, Instant>> IDENTITY =
      ImmutableList.<SerializableFunction<Instant, Instant>>of();

  protected static final StateTag<Object, AccumulatorCombiningState<Instant,
                                              Combine.Holder<Instant>, Instant>> DELAYED_UNTIL_TAG =
      StateTags.makeSystemTagInternal(StateTags.combiningValueFromInputInternal(
          "delayed", InstantCoder.of(), Min.MinFn.<Instant>naturalOrder()));

  private static final PeriodFormatter PERIOD_FORMATTER = PeriodFormat.wordBased(Locale.ENGLISH);

  /**
   * To complete an implementation, return the desired time from the TriggerContext.
   */
  @Nullable
  public abstract Instant getCurrentTime(Trigger.TriggerContext context);

  /**
   * To complete an implementation, return a new instance like this one, but incorporating
   * the provided timestamp mapping functions. Generally should be used by calling the
   * constructor of this class from the constructor of the subclass.
   */
  protected abstract AfterDelayFromFirstElement newWith(
      List<SerializableFunction<Instant, Instant>> transform);

  /**
   * A list of timestampMappers m1, m2, m3, ... m_n considered to be composed in sequence. The
   * overall mapping for an instance `instance` is `m_n(... m3(m2(m1(instant))`,
   * implemented via #computeTargetTimestamp
   */
  protected final List<SerializableFunction<Instant, Instant>> timestampMappers;

  private final TimeDomain timeDomain;

  public AfterDelayFromFirstElement(
      TimeDomain timeDomain,
      List<SerializableFunction<Instant, Instant>> timestampMappers) {
    super(null);
    this.timestampMappers = timestampMappers;
    this.timeDomain = timeDomain;
  }

  private Instant getTargetTimestamp(OnElementContext c) {
    return computeTargetTimestamp(c.currentProcessingTime());
  }

  /**
   * Aligns timestamps to the smallest multiple of {@code size} since the {@code offset} greater
   * than the timestamp.
   *
   * <p>TODO: Consider sharing this with FixedWindows, and bring over the equivalent of
   * CalendarWindows.
   */
  public AfterDelayFromFirstElement alignedTo(final Duration size, final Instant offset) {
    return newWith(new AlignFn(size, offset));
  }

  /**
   * Aligns the time to be the smallest multiple of {@code size} greater than the timestamp
   * since the epoch.
   */
  public AfterDelayFromFirstElement alignedTo(final Duration size) {
    return alignedTo(size, new Instant(0));
  }

  /**
   * Adds some delay to the original target time.
   *
   * @param delay the delay to add
   * @return An updated time trigger that will wait the additional time before firing.
   */
  public AfterDelayFromFirstElement plusDelayOf(final Duration delay) {
    return newWith(new DelayFn(delay));
  }

  /**
   * @deprecated This will be removed in the next major version. Please use only
   *             {@link #plusDelayOf} and {@link #alignedTo}.
   */
  @Deprecated
  public OnceTrigger mappedTo(SerializableFunction<Instant, Instant> timestampMapper) {
    return newWith(timestampMapper);
  }

  @Override
  public boolean isCompatible(Trigger other) {
    if (!getClass().equals(other.getClass())) {
      return false;
    }

    AfterDelayFromFirstElement that = (AfterDelayFromFirstElement) other;
    return this.timestampMappers.equals(that.timestampMappers);
  }


  private AfterDelayFromFirstElement newWith(
      SerializableFunction<Instant, Instant> timestampMapper) {
    return newWith(
        ImmutableList.<SerializableFunction<Instant, Instant>>builder()
            .addAll(timestampMappers)
            .add(timestampMapper)
            .build());
  }

  @Override
  @SuppressFBWarnings(value = "RV_RETURN_VALUE_IGNORED_NO_SIDE_EFFECT", justification =
      "prefetch side effect")
  public void prefetchOnElement(StateAccessor<?> state) {
    state.access(DELAYED_UNTIL_TAG).readLater();
  }

  @Override
  public void onElement(OnElementContext c) throws Exception {
    CombiningState<Instant, Instant> delayUntilState = c.state().access(DELAYED_UNTIL_TAG);
    Instant oldDelayUntil = delayUntilState.read();

    // Since processing time can only advance, resulting in target wake-up times we would
    // ignore anyhow, we don't bother with it if it is already set.
    if (oldDelayUntil != null) {
      return;
    }

    Instant targetTimestamp = getTargetTimestamp(c);
    delayUntilState.add(targetTimestamp);
    c.setTimer(targetTimestamp, timeDomain);
  }

  @Override
  public void prefetchOnMerge(MergingStateAccessor<?, ?> state) {
    super.prefetchOnMerge(state);
    StateMerging.prefetchCombiningValues(state, DELAYED_UNTIL_TAG);
  }

  @Override
  public void onMerge(OnMergeContext c) throws Exception {
    // NOTE: We could try to delete all timers which are still active, but we would
    // need access to a timer context for each merging window.
    // for (CombiningValueStateInternal<Instant, Combine.Holder<Instant>, Instant> state :
    //    c.state().accessInEachMergingWindow(DELAYED_UNTIL_TAG).values()) {
    //   Instant timestamp = state.get().read();
    //   if (timestamp != null) {
    //     <context for merging window>.deleteTimer(timestamp, timeDomain);
    //   }
    // }
    // Instead let them fire and be ignored.

    // If the trigger is already finished, there is no way it will become re-activated
    if (c.trigger().isFinished()) {
      StateMerging.clear(c.state(), DELAYED_UNTIL_TAG);
      // NOTE: We do not attempt to delete  the timers.
      return;
    }

    // Determine the earliest point across all the windows, and delay to that.
    StateMerging.mergeCombiningValues(c.state(), DELAYED_UNTIL_TAG);

    Instant earliestTargetTime = c.state().access(DELAYED_UNTIL_TAG).read();
    if (earliestTargetTime != null) {
      c.setTimer(earliestTargetTime, timeDomain);
    }
  }

  @Override
  @SuppressFBWarnings(value = "RV_RETURN_VALUE_IGNORED_NO_SIDE_EFFECT", justification =
      "prefetch side effect")
  public void prefetchShouldFire(StateAccessor<?> state) {
    state.access(DELAYED_UNTIL_TAG).readLater();
  }

  @Override
  public void clear(TriggerContext c) throws Exception {
    c.state().access(DELAYED_UNTIL_TAG).clear();
  }

  @Override
  public Instant getWatermarkThatGuaranteesFiring(BoundedWindow window) {
    return BoundedWindow.TIMESTAMP_MAX_VALUE;
  }

  @Override
  public boolean shouldFire(Trigger.TriggerContext context) throws Exception {
    Instant delayedUntil = context.state().access(DELAYED_UNTIL_TAG).read();
    return delayedUntil != null
        && getCurrentTime(context) != null
        && getCurrentTime(context).isAfter(delayedUntil);
  }

  @Override
  protected void onOnlyFiring(Trigger.TriggerContext context) throws Exception {
    clear(context);
  }

  protected Instant computeTargetTimestamp(Instant time) {
    Instant result = time;
    for (SerializableFunction<Instant, Instant> timestampMapper : timestampMappers) {
      result = timestampMapper.apply(result);
    }
    return result;
  }

  /**
   * A {@link SerializableFunction} to delay the timestamp at which this triggers fires.
   */
  private static final class DelayFn implements SerializableFunction<Instant, Instant> {
    private final Duration delay;

    public DelayFn(Duration delay) {
      this.delay = delay;
    }

    @Override
    public Instant apply(Instant input) {
      return input.plus(delay);
    }

    @Override
    public boolean equals(Object object) {
      if (object == this) {
        return true;
      }

      if (!(object instanceof DelayFn)) {
        return false;
      }

      return this.delay.equals(((DelayFn) object).delay);
    }

    @Override
    public int hashCode() {
      return Objects.hash(delay);
    }

    @Override
    public String toString() {
      return PERIOD_FORMATTER.print(delay.toPeriod());
    }
  }

  /**
   * A {@link SerializableFunction} to align an instant to the nearest interval boundary.
   */
  static final class AlignFn implements SerializableFunction<Instant, Instant> {
    private final Duration size;
    private final Instant offset;


    /**
     * Aligns timestamps to the smallest multiple of {@code size} since the {@code offset} greater
     * than the timestamp.
     */
    public AlignFn(Duration size, Instant offset) {
      this.size = size;
      this.offset = offset;
    }

    @Override
    public Instant apply(Instant point) {
      long millisSinceStart = new Duration(offset, point).getMillis() % size.getMillis();
      return millisSinceStart == 0 ? point : point.plus(size).minus(millisSinceStart);
    }

    @Override
    public boolean equals(Object object) {
      if (object == this) {
        return true;
      }

      if (!(object instanceof AlignFn)) {
        return false;
      }

      AlignFn other = (AlignFn) object;
      return other.size.equals(this.size)
          && other.offset.equals(this.offset);
    }

    @Override
    public int hashCode() {
      return Objects.hash(size, offset);
    }
  }
}
