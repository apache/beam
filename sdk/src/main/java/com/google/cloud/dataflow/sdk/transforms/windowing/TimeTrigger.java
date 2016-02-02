/*
 * Copyright (C) 2015 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.google.cloud.dataflow.sdk.transforms.windowing;

import com.google.cloud.dataflow.sdk.annotations.Experimental;
import com.google.cloud.dataflow.sdk.coders.InstantCoder;
import com.google.cloud.dataflow.sdk.transforms.Min;
import com.google.cloud.dataflow.sdk.transforms.SerializableFunction;
import com.google.cloud.dataflow.sdk.transforms.windowing.Trigger.OnceTrigger;
import com.google.cloud.dataflow.sdk.util.state.CombiningValueState;
import com.google.cloud.dataflow.sdk.util.state.StateTag;
import com.google.cloud.dataflow.sdk.util.state.StateTags;
import com.google.common.collect.ImmutableList;

import org.joda.time.Duration;
import org.joda.time.Instant;

import java.util.List;
import java.util.Objects;

/**
 * Support for manipulating the time at which time-based {@link Trigger}s fire.
 *
 * @param <W> {@link BoundedWindow} subclass used to represent the windows used.
 */
@Experimental(Experimental.Kind.TRIGGER)
public abstract class TimeTrigger<W extends BoundedWindow> extends OnceTrigger<W> {

  protected static final StateTag<CombiningValueState<Instant, Instant>> DELAYED_UNTIL_TAG =
      StateTags.makeSystemTagInternal(StateTags.combiningValueFromInputInternal(
          "delayed", InstantCoder.of(), Min.MinFn.<Instant>naturalOrder()));

  protected static final List<SerializableFunction<Instant, Instant>> IDENTITY;
  static {
    IDENTITY = ImmutableList.<SerializableFunction<Instant, Instant>>of();
  }

  /**
   * A list of timestampMappers m1, m2, m3, ... m_n considered to be composed in sequence. The
   * overall mapping for an instance `instance` is `m_n(... m3(m2(m1(instant))`,
   * implemented via #computeTargetTimestamp
   */
  protected final List<SerializableFunction<Instant, Instant>> timestampMappers;

  protected TimeTrigger(
      List<SerializableFunction<Instant, Instant>> timestampMappers) {
    super(null);
    this.timestampMappers = timestampMappers;
  }

  protected Instant computeTargetTimestamp(Instant time) {
    Instant result = time;
    for (SerializableFunction<Instant, Instant> timestampMapper : timestampMappers) {
      result = timestampMapper.apply(result);
    }
    return result;
  }

  /**
   * Adds some delay to the original target time.
   *
   * @param delay the delay to add
   * @return An updated time trigger that will wait the additional time before firing.
   */
  public TimeTrigger<W> plusDelayOf(final Duration delay) {
    return newWith(new DelayFn(delay));
  }

  /**
   * Aligns timestamps to the smallest multiple of {@code size} since the {@code offset} greater
   * than the timestamp.
   *
   * <p>TODO: Consider sharing this with FixedWindows, and bring over the equivalent of
   * CalendarWindows.
   */
  public TimeTrigger<W> alignedTo(final Duration size, final Instant offset) {
    return newWith(new AlignFn(size, offset));
  }

  /**
   * Aligns the time to be the smallest multiple of {@code size} greater than the timestamp
   * since the epoch.
   */
  public TimeTrigger<W> alignedTo(final Duration size) {
    return alignedTo(size, new Instant(0));
  }

  /**
   * Adjust the time at which the trigger will fire.
   *
   * <p>The {@code timestampMapper} function must have the following properties for all values
   * {@code a} and {@code b}:
   *
   * <ul>
   *   <li> Deterministic: If {@code a = b} then {@code timestampMapper(a) = timestampMapper(b)}
   *   <li> Monotonicity: If {@code a < b} then {@code timestampMapper(a) <= timestampMapper(b)}
   * </ul>
   *
   * @param timestampMapper Function that will be invoked on the proposed trigger time to determine
   *        the time at which the trigger should actually fire.
   */
  public TimeTrigger<W> mappedTo(SerializableFunction<Instant, Instant> timestampMapper) {
    return newWith(timestampMapper);
  }

  @Override
  public boolean isCompatible(Trigger<?> other) {
    if (!getClass().equals(other.getClass())) {
      return false;
    }

    TimeTrigger<?> that = (TimeTrigger<?>) other;
    return this.timestampMappers.equals(that.timestampMappers);
  }

  private TimeTrigger<W> newWith(SerializableFunction<Instant, Instant> timestampMapper) {
    return newWith(ImmutableList.<SerializableFunction<Instant, Instant>>builder()
        .addAll(timestampMappers)
        .add(timestampMapper)
        .build());
  }

  /**
   * Method to create an updated version of this {@code TimeTrigger} modified to use the specified
   * {@code transform}.
   *
   * @param transform The new transform to apply to target times.
   * @return a new {@code TimeTrigger}.
   */
  protected abstract TimeTrigger<W> newWith(List<SerializableFunction<Instant, Instant>> transform);

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
  }

  /**
   * A {@link SerializableFunction} to align an instant to the nearest interval boundary.
   */
  private static final class AlignFn implements SerializableFunction<Instant, Instant> {
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
