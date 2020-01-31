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

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkArgument;

import java.util.Arrays;
import java.util.Collections;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.annotations.Experimental.Kind;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Iterables;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Ordering;
import org.joda.time.Instant;

/**
 * Policies for combining timestamps that occur within a window.
 *
 * <p>In particular, these govern the timestamp on the output of a grouping transform such as {@link
 * GroupByKey} or {@link Combine}.
 */
@Experimental(Kind.OUTPUT_TIME)
public enum TimestampCombiner {
  /**
   * The policy of taking at the earliest of a set of timestamps.
   *
   * <p>When used in windowed aggregations, the timestamps of non-late inputs will be combined after
   * they are shifted by the {@link WindowFn} (to allow downstream watermark progress).
   *
   * <p>If data arrives late, it has no effect on the output timestamp.
   */
  EARLIEST {
    @Override
    public Instant combine(Iterable<? extends Instant> timestamps) {
      return Ordering.natural().min(timestamps);
    }

    @Override
    public Instant merge(BoundedWindow intoWindow, Iterable<? extends Instant> mergingTimestamps) {
      return combine(mergingTimestamps);
    }

    @Override
    public boolean dependsOnlyOnEarliestTimestamp() {
      return true;
    }

    @Override
    public boolean dependsOnlyOnWindow() {
      return false;
    }
  },

  /**
   * The policy of taking the latest of a set of timestamps.
   *
   * <p>When used in windowed aggregations, the timestamps of non-late inputs will be combined after
   * they are shifted by the {@link WindowFn} (to allow downstream watermark progress).
   *
   * <p>If data arrives late, it has no effect on the output timestamp.
   */
  LATEST {
    @Override
    public Instant combine(Iterable<? extends Instant> timestamps) {
      return Ordering.natural().max(timestamps);
    }

    @Override
    public Instant merge(BoundedWindow intoWindow, Iterable<? extends Instant> mergingTimestamps) {
      return combine(mergingTimestamps);
    }

    @Override
    public boolean dependsOnlyOnEarliestTimestamp() {
      return false;
    }

    @Override
    public boolean dependsOnlyOnWindow() {
      return false;
    }
  },

  /**
   * The policy of using the end of the window, regardless of input timestamps.
   *
   * <p>When used in windowed aggregations, the timestamps of non-late inputs will be combined after
   * they are shifted by the {@link WindowFn} (to allow downstream watermark progress).
   *
   * <p>If data arrives late, it has no effect on the output timestamp.
   */
  END_OF_WINDOW {
    @Override
    public Instant combine(Iterable<? extends Instant> timestamps) {
      checkArgument(Iterables.size(timestamps) > 0);
      return Iterables.get(timestamps, 0);
    }

    @Override
    public Instant merge(BoundedWindow intoWindow, Iterable<? extends Instant> mergingTimestamps) {
      return intoWindow.maxTimestamp();
    }

    @Override
    public boolean dependsOnlyOnEarliestTimestamp() {
      return false;
    }

    @Override
    public boolean dependsOnlyOnWindow() {
      return true;
    }
  };

  /**
   * Combines the given times, which must be from the same window and must have been passed through
   * {@link #merge}.
   *
   * <ul>
   *   <li>{@code combine} must be commutative: {@code combine(a, b).equals(combine(b, a))}.
   *   <li>{@code combine} must be associative: {@code combine(a, combine(b,
   *       c)).equals(combine(combine(a, b), c))}.
   * </ul>
   */
  public abstract Instant combine(Iterable<? extends Instant> timestamps);

  /**
   * Merges the given timestamps, which may have originated in separate windows, into the context of
   * the result window.
   */
  public abstract Instant merge(
      BoundedWindow intoWindow, Iterable<? extends Instant> mergingTimestamps);

  /**
   * Shorthand for {@link #merge} with just one element, to place it into the context of a window.
   *
   * <p>For example, the {@link #END_OF_WINDOW} policy moves the timestamp to the end of the window.
   */
  public final Instant assign(BoundedWindow intoWindow, Instant timestamp) {
    return merge(intoWindow, Collections.singleton(timestamp));
  }

  /** Varargs variant of {@link #combine}. */
  public final Instant combine(Instant... timestamps) {
    return combine(Arrays.asList(timestamps));
  }

  /** Varargs variant of {@link #merge}. */
  public final Instant merge(BoundedWindow intoWindow, Instant... timestamps) {
    return merge(intoWindow, Arrays.asList(timestamps));
  }

  /**
   * Returns {@code true} if the result of combination of many output timestamps actually depends
   * only on the earliest.
   *
   * <p>This may allow optimizations when it is very efficient to retrieve the earliest timestamp to
   * be combined.
   */
  public abstract boolean dependsOnlyOnEarliestTimestamp();

  /**
   * Returns {@code true} if the result does not depend on what outputs were combined but only the
   * window they are in. The canonical example is if all timestamps are sure to be the end of the
   * window.
   *
   * <p>This may allow optimizations, since it is typically very efficient to retrieve the window
   * and combining output timestamps is not necessary.
   */
  public abstract boolean dependsOnlyOnWindow();
}
