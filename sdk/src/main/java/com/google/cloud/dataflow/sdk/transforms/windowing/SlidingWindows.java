/*
 * Copyright (C) 2014 Google Inc.
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

import com.google.cloud.dataflow.sdk.coders.Coder;

import org.joda.time.Duration;
import org.joda.time.Instant;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * A WindowFn that windows values into possibly overlapping fixed-size
 * timestamp-based windows.
 *
 * <p> For example, in order to window data into 10 minute windows that
 * update every minute:
 * <pre> {@code
 * PCollection<Integer> items = ...;
 * PCollection<Integer> windowedItems = items.apply(
 *   Window.<Integer>by(SlidingWindows.of(Duration.standardMinutes(10))));
 * } </pre>
 */
@SuppressWarnings("serial")
public class SlidingWindows extends NonMergingWindowFn<Object, IntervalWindow> {

  /**
   * Amount of time between generated windows.
   */
  private final Duration period;

  /**
   * Size of the generated windows.
   */
  private final Duration size;

  /**
   * Offset of the generated windows.
   * Windows start at time N * start + offset, where 0 is the epoch.
   */
  private final Duration offset;

  /**
   * Assigns timestamps into half-open intervals of the form
   * [N * period, N * period + size), where 0 is the epoch.
   *
   * <p> If {@link SlidingWindows#every} is not called, the period defaults
   * to one millisecond.
   */
  public static SlidingWindows of(Duration size) {
    return new SlidingWindows(new Duration(1), size, Duration.ZERO);
  }

  /**
   * Returns a new {@code SlidingWindows} with the original size, that assigns
   * timestamps into half-open intervals of the form
   * [N * period, N * period + size), where 0 is the epoch.
   */
  public SlidingWindows every(Duration period) {
    return new SlidingWindows(period, size, offset);
  }

  /**
   * Assigns timestamps into half-open intervals of the form
   * [N * period + offset, N * period + offset + size).
   *
   * @throws IllegalArgumentException if offset is not in [0, period)
   */
  public SlidingWindows withOffset(Duration offset) {
    return new SlidingWindows(period, size, offset);
  }

  private SlidingWindows(Duration period, Duration size, Duration offset) {
    if (offset.isShorterThan(Duration.ZERO)
        || !offset.isShorterThan(period)
        || !size.isLongerThan(Duration.ZERO)) {
      throw new IllegalArgumentException(
          "SlidingWindows WindowingStrategies must have 0 <= offset < period and 0 < size");
    }
    this.period = period;
    this.size = size;
    this.offset = offset;
  }

  @Override
  public Coder<IntervalWindow> windowCoder() {
    return IntervalWindow.getFixedSizeCoder(size);
  }

  @Override
  public Collection<IntervalWindow> assignWindows(AssignContext c) {
    List<IntervalWindow> windows =
        new ArrayList<>((int) (size.getMillis() / period.getMillis()));
    Instant timestamp = c.timestamp();
    long lastStart = timestamp.getMillis()
        - timestamp.plus(period).minus(offset).getMillis() % period.getMillis();
    for (long start = lastStart;
         start > timestamp.minus(size).getMillis();
         start -= period.getMillis()) {
      windows.add(new IntervalWindow(new Instant(start), size));
    }
    return windows;
  }

  @Override
  public boolean isCompatible(WindowFn<?, ?> other) {
    if (other instanceof SlidingWindows) {
      SlidingWindows that = (SlidingWindows) other;
      return period.equals(that.period)
        && size.equals(that.size)
        && offset.equals(that.offset);
    } else {
      return false;
    }
  }
}
