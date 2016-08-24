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
 * A {@code BoundedWindow} represents a finite grouping of elements, with an
 * upper bound (larger timestamps represent more recent data) on the timestamps
 * of elements that can be placed in the window. This finiteness means that for
 * every window, at some point in time, all data for that window will have
 * arrived and can be processed together.
 *
 * <p>Windows must also implement {@link Object#equals} and
 * {@link Object#hashCode} such that windows that are logically equal will
 * be treated as equal by {@code equals()} and {@code hashCode()}.
 */
public abstract class BoundedWindow {
  // The min and max timestamps that won't overflow when they are converted to
  // usec.
  public static final Instant TIMESTAMP_MIN_VALUE =
      new Instant(TimeUnit.MICROSECONDS.toMillis(Long.MIN_VALUE));
  public static final Instant TIMESTAMP_MAX_VALUE =
      new Instant(TimeUnit.MICROSECONDS.toMillis(Long.MAX_VALUE));

  /**
   * Returns the inclusive upper bound of timestamps for values in this window.
   */
  public abstract Instant maxTimestamp();
}
