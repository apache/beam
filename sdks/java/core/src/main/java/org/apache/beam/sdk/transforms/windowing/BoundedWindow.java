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

import java.util.concurrent.TimeUnit;
import org.joda.time.Instant;

/**
 * A {@link BoundedWindow} represents window information assigned to data elements.
 *
 * <p>It has one method {@link #maxTimestamp()} to define an upper bound (inclusive) for element
 * timestamps. A {@link WindowFn} must assign an element only to windows where {@link
 * #maxTimestamp()} is greater than or equal to the element timestamp. When the watermark passes the
 * maximum timestamp, all data for a window is estimated to be received.
 *
 * <p>A window does not need to have a lower bound. Only the upper bound is mandatory because it
 * governs management of triggering and discarding of the window.
 *
 * <p>Windows must also implement {@link Object#equals} and {@link Object#hashCode} such that
 * windows that are logically equal will be treated as equal by {@code equals()} and {@code
 * hashCode()}.
 */
public abstract class BoundedWindow {
  // The min and max timestamps that won't overflow when they are converted to
  // usec.

  /**
   * The minimum value for any Beam timestamp. Often referred to as "-infinity".
   *
   * <p>This value and {@link #TIMESTAMP_MAX_VALUE} are chosen so that their
   * microseconds-since-epoch can be safely represented with a {@code long}.
   */
  public static final Instant TIMESTAMP_MIN_VALUE =
      new Instant(TimeUnit.MICROSECONDS.toMillis(Long.MIN_VALUE));

  /**
   * The maximum value for any Beam timestamp. Often referred to as "+infinity".
   *
   * <p>This value and {@link #TIMESTAMP_MIN_VALUE} are chosen so that their
   * microseconds-since-epoch can be safely represented with a {@code long}.
   */
  public static final Instant TIMESTAMP_MAX_VALUE =
      new Instant(TimeUnit.MICROSECONDS.toMillis(Long.MAX_VALUE));

  /**
   * Formats a {@link Instant} timestamp with additional Beam-specific metadata, such as indicating
   * whether the timestamp is the end of the global window or one of the distinguished values {@link
   * #TIMESTAMP_MIN_VALUE} or {@link #TIMESTAMP_MIN_VALUE}.
   */
  public static String formatTimestamp(Instant timestamp) {
    if (timestamp.equals(TIMESTAMP_MIN_VALUE)) {
      return timestamp.toString() + " (TIMESTAMP_MIN_VALUE)";
    } else if (timestamp.equals(TIMESTAMP_MAX_VALUE)) {
      return timestamp.toString() + " (TIMESTAMP_MAX_VALUE)";
    } else if (timestamp.equals(GlobalWindow.INSTANCE.maxTimestamp())) {
      return timestamp.toString() + " (end of global window)";
    } else {
      return timestamp.toString();
    }
  }

  /**
   * Returns the inclusive upper bound of timestamps for values in this window.
   */
  public abstract Instant maxTimestamp();
}
