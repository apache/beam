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

import com.google.cloud.dataflow.sdk.annotations.Experimental;
import com.google.cloud.dataflow.sdk.transforms.SerializableFunction;
import com.google.cloud.dataflow.sdk.util.TimeDomain;

import org.joda.time.Instant;

import java.util.List;
import java.util.Objects;

import javax.annotation.Nullable;

/**
 * {@code AfterProcessingTime} triggers fire based on the current processing time. They operate in
 * the real-time domain.
 *
 * <p>The time at which to fire the timer can be adjusted via the methods in {@link TimeTrigger},
 * such as {@link TimeTrigger#plusDelayOf} or {@link TimeTrigger#alignedTo}.
 *
 * @param <W> {@link BoundedWindow} subclass used to represent the windows used
 */
@Experimental(Experimental.Kind.TRIGGER)
public class AfterProcessingTime<W extends BoundedWindow> extends AfterDelayFromFirstElement<W> {

  @Override
  @Nullable
  public Instant getCurrentTime(Trigger<W>.TriggerContext context) {
    return context.currentProcessingTime();
  }

  private AfterProcessingTime(List<SerializableFunction<Instant, Instant>> transforms) {
    super(TimeDomain.PROCESSING_TIME, transforms);
  }

  /**
   * Creates a trigger that fires when the current processing time passes the processing time
   * at which this trigger saw the first element in a pane.
   */
  public static <W extends BoundedWindow> AfterProcessingTime<W> pastFirstElementInPane() {
    return new AfterProcessingTime<W>(IDENTITY);
  }

  @Override
  protected AfterProcessingTime<W> newWith(
      List<SerializableFunction<Instant, Instant>> transforms) {
    return new AfterProcessingTime<W>(transforms);
  }

  @Override
  public Instant getWatermarkThatGuaranteesFiring(W window) {
    return BoundedWindow.TIMESTAMP_MAX_VALUE;
  }

  @Override
  protected Trigger<W> getContinuationTrigger(List<Trigger<W>> continuationTriggers) {
    return new AfterSynchronizedProcessingTime<W>();
  }

  @Override
  public String toString() {
    return "AfterProcessingTime.pastFirstElementInPane(" + timestampMappers + ")";
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (!(obj instanceof AfterProcessingTime)) {
      return false;
    }
    AfterProcessingTime<?> that = (AfterProcessingTime<?>) obj;
    return Objects.equals(this.timestampMappers, that.timestampMappers);
  }

  @Override
  public int hashCode() {
    return Objects.hash(getClass(), this.timestampMappers);
  }
}
