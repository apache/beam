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
import com.google.cloud.dataflow.sdk.transforms.SerializableFunction;
import com.google.cloud.dataflow.sdk.transforms.windowing.Trigger.OnceTrigger;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;

import org.joda.time.Duration;
import org.joda.time.Instant;

import java.util.List;

/**
 * Support for manipulating the time at which time-based {@link Trigger}s fire.
 *
 * @param <W> {@link BoundedWindow} subclass used to represent the windows used.
 * @param <T> {@code TimeTrigger} subclass produced by modifying the current {@code TimeTrigger}.
 */
@Experimental(Experimental.Kind.TRIGGER)
public abstract class TimeTrigger<W extends BoundedWindow, T extends TimeTrigger<W, T>>
    extends OnceTrigger<W> {

  private static final long serialVersionUID = 0L;

  protected static final List<SerializableFunction<Instant, Instant>> IDENTITY =
      ImmutableList.<SerializableFunction<Instant, Instant>>of();

  private final List<SerializableFunction<Instant, Instant>> timestampMappers;

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
  public T plusDelayOf(final Duration delay) {
    return newWith(new SerializableFunction<Instant, Instant>() {
      private static final long serialVersionUID = 0L;

      @Override
      public Instant apply(Instant input) {
        return input.plus(delay);
      }
    });
  }

  /**
   * Aligns timestamps to the smallest multiple of {@code size} since the {@code offset} greater
   * than the timestamp.
   *
   * <p> TODO: Consider sharing this with FixedWindows, and bring over the equivalent of
   * CalendarWindows.
   */
  public T alignedTo(final Duration size, final Instant offset) {
    return newWith(new SerializableFunction<Instant, Instant>() {
      private static final long serialVersionUID = 0L;

      @Override
      public Instant apply(Instant point) {
        return alignedTo(point, size, offset);
      }
    });
  }

  /**
   * Aligns the time to be the smallest multiple of {@code size} greater than the timestamp
   * since the epoch.
   */
  public T alignedTo(final Duration size) {
    return alignedTo(size, new Instant(0));
  }

  @VisibleForTesting static Instant alignedTo(Instant point, Duration size, Instant offset) {
    long millisSinceStart = new Duration(offset, point).getMillis() % size.getMillis();
    return millisSinceStart == 0 ? point : point.plus(size).minus(millisSinceStart);
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
  public T mappedTo(SerializableFunction<Instant, Instant> timestampMapper) {
    return newWith(timestampMapper);
  }

  @Override
  public boolean isCompatible(Trigger<?> other) {
    if (!getClass().equals(other.getClass())) {
      return false;
    }

    TimeTrigger<?, ?> that = (TimeTrigger<?, ?>) other;
    return this.timestampMappers.equals(that.timestampMappers);
  }

  private T newWith(SerializableFunction<Instant, Instant> timestampMapper) {
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
  protected abstract T newWith(List<SerializableFunction<Instant, Instant>> transform);
}
