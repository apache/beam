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

import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.util.TimeDomain;

import org.joda.time.Instant;

import java.util.List;
import java.util.Objects;

import javax.annotation.Nullable;

/**
 * {@code AfterProcessingTime} triggers fire based on the current processing time. They operate in
 * the real-time domain.
 *
 * <p>The time at which to fire the timer can be adjusted via the methods in
 * {@link AfterDelayFromFirstElement}, such as {@link AfterDelayFromFirstElement#plusDelayOf} or
 * {@link AfterDelayFromFirstElement#alignedTo}.
 */
@Experimental(Experimental.Kind.TRIGGER)
public class AfterProcessingTime extends AfterDelayFromFirstElement {

  @Override
  @Nullable
  public Instant getCurrentTime(Trigger.TriggerContext context) {
    return context.currentProcessingTime();
  }

  private AfterProcessingTime(List<SerializableFunction<Instant, Instant>> transforms) {
    super(TimeDomain.PROCESSING_TIME, transforms);
  }

  /**
   * Creates a trigger that fires when the current processing time passes the processing time
   * at which this trigger saw the first element in a pane.
   */
  public static AfterProcessingTime pastFirstElementInPane() {
    return new AfterProcessingTime(IDENTITY);
  }

  @Override
  protected AfterProcessingTime newWith(
      List<SerializableFunction<Instant, Instant>> transforms) {
    return new AfterProcessingTime(transforms);
  }

  @Override
  public Instant getWatermarkThatGuaranteesFiring(BoundedWindow window) {
    return BoundedWindow.TIMESTAMP_MAX_VALUE;
  }

  @Override
  protected Trigger getContinuationTrigger(List<Trigger> continuationTriggers) {
    return new AfterSynchronizedProcessingTime();
  }

  @Override
  public String toString() {
    StringBuilder builder = new StringBuilder("AfterProcessingTime.pastFirstElementInPane()");
    for (SerializableFunction<Instant, Instant> delayFn : timestampMappers) {
      builder
          .append(".plusDelayOf(")
          .append(delayFn)
          .append(")");
    }

    return builder.toString();
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (!(obj instanceof AfterProcessingTime)) {
      return false;
    }
    AfterProcessingTime that = (AfterProcessingTime) obj;
    return Objects.equals(this.timestampMappers, that.timestampMappers);
  }

  @Override
  public int hashCode() {
    return Objects.hash(getClass(), this.timestampMappers);
  }
}
