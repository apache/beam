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

import com.google.cloud.dataflow.sdk.transforms.SerializableFunction;

import org.joda.time.Duration;
import org.joda.time.Instant;

/**
 * Support for manipulating the time at which time-based triggers fire.
 *
 * @param <W> {@link BoundedWindow} subclass used to represent the windows used.
 * @param <T> {@code TimeTrigger} subclass produced by modifying the current {@code TimeTrigger}.
 */
public abstract class TimeTrigger<W extends BoundedWindow, T extends TimeTrigger<W, T>>
    implements Trigger<W> {

  protected static final SerializableFunction<Instant, Instant> IDENTITY =
      new SerializableFunction<Instant, Instant>() {

    private static final long serialVersionUID = 0L;

    @Override
    public Instant apply(Instant input) {
      return input;
    }
  };

  private SerializableFunction<Instant, Instant> composedTimestampMapper;

  protected TimeTrigger(SerializableFunction<Instant, Instant> composedTimestampMapper) {
    this.composedTimestampMapper = composedTimestampMapper;
  }

  protected Instant computeTargetTimestamp(Instant time) {
    return composedTimestampMapper.apply(time);
  }

  /**
   * Adds some delay to the original target time.
   *
   * @param delay the delay to add
   * @return An updated time trigger which will wait the additional time before firing.
   */
  public T plusDelay(final Duration delay) {
    return mappedTo(new SerializableFunction<Instant, Instant>() {
      private static final long serialVersionUID = 0L;

      @Override
      public Instant apply(Instant input) {
        return input.plus(delay);
      }
    });
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
  public T mappedTo(final SerializableFunction<Instant, Instant> timestampMapper) {
    return newWith(new SerializableFunction<Instant, Instant>() {

      private static final long serialVersionUID = 0L;

      @Override
      public Instant apply(Instant input) {
        return timestampMapper.apply(composedTimestampMapper.apply(input));
      }
    });
  }

  /**
   * Method to create an updated version of this {@code TimeTrigger} modified to use the specified
   * {@code transform}.
   *
   * @param transform The new transform to apply to target times.
   * @return a new {@code TimeTrigger}.
   */
  protected abstract T newWith(SerializableFunction<Instant, Instant> transform);
}
