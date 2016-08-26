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
import org.apache.beam.sdk.values.PCollection;
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
  public static final Instant NEGATIVE_INFINITY =
      new Instant(TimeUnit.MICROSECONDS.toMillis(Long.MIN_VALUE));
  /**
   * The minimum timestamp.
   *
   * @deprecated instead use {@link #NEGATIVE_INFINITY}
   */
  @Deprecated
  public static final Instant TIMESTAMP_MIN_VALUE = NEGATIVE_INFINITY;
  /**
   * The timestamp that represents positive infinity.
   *
   * <p>Elements must have timestamps that are before this value. {@link BoundedWindow Windows} must
   * {@link #maxTimestamp() end} before this value. A window which does not end before
   * POSITIVE_INFINITY cannot be produced using any watermark-based {@link Trigger}, such as {@link
   * AfterWatermark} or the {@link DefaultTrigger}.
   *
   * <p>After a watermark reaches this value, all future elements that arrive are droppably late. As
   * such, the runner is permitted to shut down sources after their watermark reaches this value,
   * and the pipeline is complete and permitted to shut down after the watermark for all
   * {@link PCollection PCollections} has reached this value.
   */
  public static final Instant POSITIVE_INFINITY =
      new Instant(TimeUnit.MICROSECONDS.toMillis(Long.MAX_VALUE));
  /**
   * The maximum timestamp.
   *
   * @deprecated instead use {@link #POSITIVE_INFINITY}
   */
  @Deprecated
  public static final Instant TIMESTAMP_MAX_VALUE = POSITIVE_INFINITY;

  /**
   * Returns the inclusive upper bound of timestamps for values in this window.
   */
  public abstract Instant maxTimestamp();
}
