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

import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.annotations.Experimental.Kind;
import org.apache.beam.sdk.transforms.windowing.Trigger.OnceTrigger;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.joda.time.format.PeriodFormat;
import org.joda.time.format.PeriodFormatter;

/**
 * A {@link Trigger} trigger that fires at a specified point in processing time, relative to when
 * input first arrives.
 */
@Experimental(Kind.TRIGGER)
public class AfterProcessingTime extends OnceTrigger {

  private static final PeriodFormatter DURATION_FORMATTER = PeriodFormat.wordBased(Locale.ENGLISH);

  private final List<TimestampTransform> timestampTransforms;

  private AfterProcessingTime(List<TimestampTransform> timestampTransforms) {
    super(null);
    this.timestampTransforms = timestampTransforms;
  }

  /**
   * Creates a trigger that fires when the current processing time passes the processing time at
   * which this trigger saw the first element in a pane.
   */
  public static AfterProcessingTime pastFirstElementInPane() {
    return new AfterProcessingTime(Collections.emptyList());
  }

  /**
   * The transforms applied to the arrival time of an element to determine when this trigger allows
   * output.
   */
  public List<TimestampTransform> getTimestampTransforms() {
    return timestampTransforms;
  }

  /**
   * Adds some delay to the original target time.
   *
   * @param delay the delay to add
   * @return An updated time trigger that will wait the additional time before firing.
   */
  public AfterProcessingTime plusDelayOf(final Duration delay) {
    return new AfterProcessingTime(
        ImmutableList.<TimestampTransform>builder()
            .addAll(timestampTransforms)
            .add(TimestampTransform.delay(delay))
            .build());
  }

  /**
   * Aligns timestamps to the smallest multiple of {@code period} since the {@code offset} greater
   * than the timestamp.
   */
  public AfterProcessingTime alignedTo(final Duration period, final Instant offset) {
    return new AfterProcessingTime(
        ImmutableList.<TimestampTransform>builder()
            .addAll(timestampTransforms)
            .add(TimestampTransform.alignTo(period, offset))
            .build());
  }

  /**
   * Aligns the time to be the smallest multiple of {@code period} greater than the epoch boundary
   * (aka {@code new Instant(0)}).
   */
  public AfterProcessingTime alignedTo(final Duration period) {
    return alignedTo(period, new Instant(0));
  }

  @Override
  public boolean isCompatible(Trigger other) {
    return this.equals(other);
  }

  @Override
  public Instant getWatermarkThatGuaranteesFiring(BoundedWindow window) {
    return BoundedWindow.TIMESTAMP_MAX_VALUE;
  }

  @Override
  protected Trigger getContinuationTrigger(List<Trigger> continuationTriggers) {
    return AfterSynchronizedProcessingTime.ofFirstElement();
  }

  @Override
  public String toString() {
    StringBuilder builder = new StringBuilder("AfterProcessingTime.pastFirstElementInPane()");
    for (TimestampTransform transform : getTimestampTransforms()) {
      if (transform instanceof TimestampTransform.Delay) {
        TimestampTransform.Delay delay = (TimestampTransform.Delay) transform;
        builder
            .append(".plusDelayOf(")
            .append(DURATION_FORMATTER.print(delay.getDelay().toPeriod()))
            .append(")");
      } else if (transform instanceof TimestampTransform.AlignTo) {
        TimestampTransform.AlignTo alignTo = (TimestampTransform.AlignTo) transform;
        builder
            .append(".alignedTo(")
            .append(DURATION_FORMATTER.print(alignTo.getPeriod().toPeriod()))
            .append(", ")
            .append(alignTo.getOffset())
            .append(")");
      }
    }

    return builder.toString();
  }

  @Override
  public boolean equals(@Nullable Object obj) {
    if (this == obj) {
      return true;
    }
    if (!(obj instanceof AfterProcessingTime)) {
      return false;
    }

    AfterProcessingTime that = (AfterProcessingTime) obj;
    return getTimestampTransforms().equals(that.getTimestampTransforms());
  }

  @Override
  public int hashCode() {
    return Objects.hash(getTimestampTransforms());
  }
}
