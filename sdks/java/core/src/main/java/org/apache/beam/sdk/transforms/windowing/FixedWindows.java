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

import java.util.Objects;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Duration;
import org.joda.time.Instant;

/**
 * A {@link WindowFn} that windows values into fixed-size timestamp-based windows.
 *
 * <p>For example, in order to partition the data into 10 minute windows:
 *
 * <pre>{@code
 * PCollection<Integer> items = ...;
 * PCollection<Integer> windowedItems = items.apply(
 *   Window.<Integer>into(FixedWindows.of(Duration.standardMinutes(10))));
 * }</pre>
 */
public class FixedWindows extends PartitioningWindowFn<Object, IntervalWindow> {

  /** Size of this window. */
  private final Duration size;

  /** Offset of this window. Windows start at time N * size + offset, where 0 is the epoch. */
  private final Duration offset;

  /**
   * Partitions the timestamp space into half-open intervals of the form [N * size, (N + 1) * size),
   * where 0 is the epoch.
   */
  public static FixedWindows of(Duration size) {
    return new FixedWindows(size, Duration.ZERO);
  }

  /**
   * Partitions the timestamp space into half-open intervals of the form [N * size + offset, (N + 1)
   * * size + offset), where 0 is the epoch.
   *
   * @throws IllegalArgumentException if offset is not in [0, size)
   */
  public FixedWindows withOffset(Duration offset) {
    return new FixedWindows(size, offset);
  }

  private FixedWindows(Duration size, Duration offset) {
    if (offset.isShorterThan(Duration.ZERO) || !offset.isShorterThan(size)) {
      throw new IllegalArgumentException(
          "FixedWindows WindowingStrategies must have 0 <= offset < size");
    }
    this.size = size;
    this.offset = offset;
  }

  @Override
  public IntervalWindow assignWindow(Instant timestamp) {
    Instant start =
        new Instant(
            timestamp.getMillis()
                - timestamp.plus(size).minus(offset).getMillis() % size.getMillis());

    // The global window is inclusive of max timestamp, while interval window excludes its
    // upper bound
    Instant endOfGlobalWindow = GlobalWindow.INSTANCE.maxTimestamp().plus(1);

    // The end of the window is either start + size if that is within the allowable range, otherwise
    // the end of the global window. Truncating the window drives many other
    // areas of this system in the appropriate way automatically.
    //
    // Though it is curious that the very last representable fixed window is shorter than the rest,
    // when we are processing data in the year 294247, we'll probably have technology that can
    // account for this.
    Instant end =
        start.isAfter(endOfGlobalWindow.minus(size)) ? endOfGlobalWindow : start.plus(size);

    return new IntervalWindow(start, end);
  }

  @Override
  public void populateDisplayData(DisplayData.Builder builder) {
    super.populateDisplayData(builder);
    builder
        .add(DisplayData.item("size", size).withLabel("Window Duration"))
        .addIfNotDefault(
            DisplayData.item("offset", offset).withLabel("Window Start Offset"), Duration.ZERO);
  }

  @Override
  public Coder<IntervalWindow> windowCoder() {
    return IntervalWindow.getCoder();
  }

  @Override
  public boolean isCompatible(WindowFn<?, ?> other) {
    return this.equals(other);
  }

  @Override
  public void verifyCompatibility(WindowFn<?, ?> other) throws IncompatibleWindowException {
    if (!this.isCompatible(other)) {
      throw new IncompatibleWindowException(
          other,
          String.format(
              "Only %s objects with the same size and offset are compatible.",
              FixedWindows.class.getSimpleName()));
    }
  }

  public Duration getSize() {
    return size;
  }

  public Duration getOffset() {
    return offset;
  }

  @Override
  public boolean equals(@Nullable Object object) {
    if (!(object instanceof FixedWindows)) {
      return false;
    }
    FixedWindows other = (FixedWindows) object;
    return getOffset().equals(other.getOffset()) && getSize().equals(other.getSize());
  }

  @Override
  public int hashCode() {
    return Objects.hash(size, offset);
  }
}
