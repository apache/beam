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

import com.google.cloud.dataflow.sdk.coders.Coder;

import org.joda.time.Duration;
import org.joda.time.Instant;

import java.util.Objects;

/**
 * A {@link WindowFn} that windows values into fixed-size timestamp-based windows.
 *
 * <p>For example, in order to partition the data into 10 minute windows:
 * <pre> {@code
 * PCollection<Integer> items = ...;
 * PCollection<Integer> windowedItems = items.apply(
 *   Window.<Integer>into(FixedWindows.of(Duration.standardMinutes(10))));
 * } </pre>
 */
public class FixedWindows extends PartitioningWindowFn<Object, IntervalWindow> {

  /**
   * Size of this window.
   */
  private final Duration size;

  /**
   * Offset of this window.  Windows start at time
   * N * size + offset, where 0 is the epoch.
   */
  private final Duration offset;

  /**
   * Partitions the timestamp space into half-open intervals of the form
   * [N * size, (N + 1) * size), where 0 is the epoch.
   */
  public static FixedWindows of(Duration size) {
    return new FixedWindows(size, Duration.ZERO);
  }

  /**
   * Partitions the timestamp space into half-open intervals of the form
   * [N * size + offset, (N + 1) * size + offset),
   * where 0 is the epoch.
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
    long start = timestamp.getMillis()
        - timestamp.plus(size).minus(offset).getMillis() % size.getMillis();
    return new IntervalWindow(new Instant(start), size);
  }

  @Override
  public Coder<IntervalWindow> windowCoder() {
    return IntervalWindow.getCoder();
  }

  @Override
  public boolean isCompatible(WindowFn<?, ?> other) {
    return this.equals(other);
  }

  public Duration getSize() {
    return size;
  }

  public Duration getOffset() {
    return offset;
  }

  @Override
  public boolean equals(Object object) {
    if (!(object instanceof FixedWindows)) {
      return false;
    }
    FixedWindows other = (FixedWindows) object;
    return getOffset().equals(other.getOffset())
        && getSize().equals(other.getSize());
  }

  @Override
  public int hashCode() {
    return Objects.hash(size, offset);
  }
}
