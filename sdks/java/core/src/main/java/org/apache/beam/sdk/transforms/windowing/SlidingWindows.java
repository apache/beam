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

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.annotations.Experimental.Kind;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Duration;
import org.joda.time.Instant;

/**
 * A {@link WindowFn} that windows values into possibly overlapping fixed-size timestamp-based
 * windows.
 *
 * <p>For example, in order to window data into 10 minute windows that update every minute:
 *
 * <pre>{@code
 * PCollection<Integer> items = ...;
 * PCollection<Integer> windowedItems = items.apply(
 *   Window.<Integer>into(SlidingWindows.of(Duration.standardMinutes(10))));
 * }</pre>
 */
public class SlidingWindows extends NonMergingWindowFn<Object, IntervalWindow> {

  /** Amount of time between generated windows. */
  private final Duration period;

  /** Size of the generated windows. */
  private final Duration size;

  /**
   * Offset of the generated windows. Windows start at time N * start + offset, where 0 is the
   * epoch.
   */
  private final Duration offset;

  /**
   * Assigns timestamps into half-open intervals of the form [N * period, N * period + size), where
   * 0 is the epoch.
   *
   * <p>If {@link SlidingWindows#every} is not called, the period defaults to the largest time unit
   * smaller than the given duration. For example, specifying a size of 5 seconds will result in a
   * default period of 1 second.
   */
  public static SlidingWindows of(Duration size) {
    return new SlidingWindows(getDefaultPeriod(size), size, Duration.ZERO);
  }

  /**
   * Returns a new {@code SlidingWindows} with the original size, that assigns timestamps into
   * half-open intervals of the form [N * period, N * period + size), where 0 is the epoch.
   */
  public SlidingWindows every(Duration period) {
    return new SlidingWindows(period, size, offset);
  }

  /**
   * Assigns timestamps into half-open intervals of the form [N * period + offset, N * period +
   * offset + size).
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
    return IntervalWindow.getCoder();
  }

  @Override
  public Collection<IntervalWindow> assignWindows(AssignContext c) {
    return assignWindows(c.timestamp());
  }

  public Collection<IntervalWindow> assignWindows(Instant timestamp) {
    List<IntervalWindow> windows = new ArrayList<>((int) (size.getMillis() / period.getMillis()));
    long lastStart = lastStartFor(timestamp);
    for (long start = lastStart;
        start > timestamp.minus(size).getMillis();
        start -= period.getMillis()) {
      windows.add(new IntervalWindow(new Instant(start), size));
    }
    return windows;
  }

  /**
   * Return a {@link WindowMappingFn} that returns the earliest window that contains the end of the
   * main-input window.
   */
  @Override
  public WindowMappingFn<IntervalWindow> getDefaultWindowMappingFn() {
    return new WindowMappingFn<IntervalWindow>() {
      @Override
      public IntervalWindow getSideInputWindow(BoundedWindow mainWindow) {
        if (mainWindow instanceof GlobalWindow) {
          throw new IllegalArgumentException(
              "Attempted to get side input window for GlobalWindow from non-global WindowFn");
        }
        long lastStart = lastStartFor(mainWindow.maxTimestamp().minus(size));
        return new IntervalWindow(new Instant(lastStart + period.getMillis()), size);
      }
    };
  }

  @Override
  public boolean isCompatible(WindowFn<?, ?> other) {
    return equals(other);
  }

  @Override
  public boolean assignsToOneWindow() {
    return !this.period.isShorterThan(this.size);
  }

  @Override
  public void verifyCompatibility(WindowFn<?, ?> other) throws IncompatibleWindowException {
    if (!this.isCompatible(other)) {
      throw new IncompatibleWindowException(
          other,
          String.format(
              "Only %s objects with the same size, period and offset are compatible.",
              SlidingWindows.class.getSimpleName()));
    }
  }

  @Override
  public void populateDisplayData(DisplayData.Builder builder) {
    super.populateDisplayData(builder);
    builder
        .add(DisplayData.item("size", size).withLabel("Window Size"))
        .add(DisplayData.item("period", period).withLabel("Window Period"))
        .add(DisplayData.item("offset", offset).withLabel("Window Start Offset"));
  }

  /** Return the last start of a sliding window that contains the timestamp. */
  private long lastStartFor(Instant timestamp) {
    return timestamp.getMillis()
        - timestamp.plus(period).minus(offset).getMillis() % period.getMillis();
  }

  static Duration getDefaultPeriod(Duration size) {
    if (size.isLongerThan(Duration.standardHours(1))) {
      return Duration.standardHours(1);
    }
    if (size.isLongerThan(Duration.standardMinutes(1))) {
      return Duration.standardMinutes(1);
    }
    if (size.isLongerThan(Duration.standardSeconds(1))) {
      return Duration.standardSeconds(1);
    }
    return Duration.millis(1);
  }

  public Duration getPeriod() {
    return period;
  }

  public Duration getSize() {
    return size;
  }

  public Duration getOffset() {
    return offset;
  }

  /**
   * Ensures that later sliding windows have an output time that is past the end of earlier windows.
   *
   * <p>If this is the earliest sliding window containing {@code inputTimestamp}, that's fine.
   * Otherwise, we pick the earliest time that doesn't overlap with earlier windows.
   */
  @Experimental(Kind.OUTPUT_TIME)
  @Override
  public Instant getOutputTime(Instant inputTimestamp, IntervalWindow window) {
    Instant startOfLastSegment = window.maxTimestamp().minus(period);
    return startOfLastSegment.isBefore(inputTimestamp)
        ? inputTimestamp
        : startOfLastSegment.plus(1);
  }

  @Override
  public boolean equals(@Nullable Object object) {
    if (!(object instanceof SlidingWindows)) {
      return false;
    }
    SlidingWindows other = (SlidingWindows) object;
    return getOffset().equals(other.getOffset())
        && getSize().equals(other.getSize())
        && getPeriod().equals(other.getPeriod());
  }

  @Override
  public int hashCode() {
    return Objects.hash(size, offset, period);
  }
}
