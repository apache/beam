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

import com.google.auto.value.AutoValue;
import java.io.Serializable;
import org.apache.beam.sdk.annotations.Internal;
import org.joda.time.Duration;
import org.joda.time.Instant;

/**
 * <b><i>For internal use only; no backwards-compatibility guarantees.</i></b>
 *
 * <p>An abstract description of a standardized transformation on timestamps.
 */
@Internal
public abstract class TimestampTransform implements Serializable {

  TimestampTransform() {}

  /**
   * <b><i>For internal use only; no backwards-compatibility guarantees.</i></b>
   *
   * <p>Returns a transform that shifts a timestamp later by {@code delay}.
   */
  @Internal
  public static TimestampTransform delay(Duration delay) {
    return new AutoValue_TimestampTransform_Delay(delay);
  }

  /**
   * <b><i>For internal use only; no backwards-compatibility guarantees.</i></b>
   *
   * <p>Returns a transform that aligns a timestamp to the next boundary of {@code period}, starting
   * from {@code offset}.
   */
  @Internal
  public static TimestampTransform alignTo(Duration period, Instant offset) {
    return new AutoValue_TimestampTransform_AlignTo(period, offset);
  }

  /**
   * <b><i>For internal use only; no backwards-compatibility guarantees.</i></b>
   *
   * <p>Returns a transform that aligns a timestamp to the next boundary of {@code period}, starting
   * from the start of the epoch.
   */
  @Internal
  public static TimestampTransform alignTo(Duration period) {
    return alignTo(period, new Instant(0));
  }

  /**
   * <b><i>For internal use only; no backwards-compatibility guarantees.</i></b>
   *
   * <p>Represents the transform that aligns a timestamp to the next boundary of {@link
   * #getPeriod()} start at {@link #getOffset()}.
   */
  @Internal
  @AutoValue
  public abstract static class AlignTo extends TimestampTransform {
    public abstract Duration getPeriod();

    public abstract Instant getOffset();
  }

  /**
   * <b><i>For internal use only; no backwards-compatibility guarantees.</i></b>
   *
   * <p>Represents the transform that delays a timestamp by {@link #getDelay()}.
   */
  @Internal
  @AutoValue
  public abstract static class Delay extends TimestampTransform {
    public abstract Duration getDelay();
  }
}
