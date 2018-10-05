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
package org.apache.beam.sdk.extensions.euphoria.core.client.dataset.windowing;

import com.google.common.collect.AbstractIterator;
import org.apache.beam.sdk.extensions.euphoria.core.annotation.audience.Audience;
import org.apache.beam.sdk.extensions.euphoria.core.client.io.Collector;
import org.apache.beam.sdk.extensions.euphoria.core.client.triggers.TimeTrigger;
import org.apache.beam.sdk.extensions.euphoria.core.client.triggers.Trigger;

import java.time.Duration;
import java.util.Iterator;
import java.util.Objects;

import static com.google.common.base.Preconditions.checkArgument;

/** Time sliding windowing. */
@Audience(Audience.Type.CLIENT)
public final class TimeSliding<T> implements Windowing<T, TimeInterval> {

  private final long duration;
  private final long slide;

  private TimeSliding(long duration, long slide) {
    this.duration = duration;
    this.slide = slide;

    checkArgument(duration > 0, "Windowing with zero duration");

    if (duration % slide != 0) {
      throw new IllegalArgumentException(
          "This time sliding window can manage only aligned sliding windows");
    }
  }

  public static <T> TimeSliding<T> of(Duration duration, Duration step) {
    return new TimeSliding<>(duration.toMillis(), step.toMillis());
  }

  /**
   * Helper method to extract window label from context.
   *
   * @param context the execution context
   * @return the {@link TimeInterval} window of this execution
   * @throws ClassCastException if the context is not part of a time-sliding execution
   */
  public static TimeInterval getLabel(Collector<?> context) {
    return (TimeInterval) context.getWindow();
  }

  @Override
  public Iterable<TimeInterval> assignWindowsToElement(WindowedElement<?, T> el) {
    return new SlidingWindowSet(el.getTimestamp(), duration, slide);
  }

  @Override
  public Trigger<TimeInterval> getTrigger() {
    return new TimeTrigger();
  }

  @Override
  public String toString() {
    return "TimeSliding{" + "duration=" + duration + ", slide=" + slide + '}';
  }

  public long getDuration() {
    return duration;
  }

  public long getSlide() {
    return slide;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof TimeSliding) {
      TimeSliding other = (TimeSliding) obj;
      return other.duration == duration && other.slide == slide;
    }
    return false;
  }

  @Override
  public int hashCode() {
    return Objects.hash(duration, slide);
  }

  /** Calculates window boundaries lazily during the iteration. */
  public static class SlidingWindowSet implements Iterable<TimeInterval> {

    private final long elementStamp;
    private final long duration;
    private final long slide;

    public SlidingWindowSet(long elementStamp, long duration, long slide) {
      this.elementStamp = elementStamp;
      this.duration = duration;
      this.slide = slide;
    }

    @Override
    public Iterator<TimeInterval> iterator() {
      return new AbstractIterator<TimeInterval>() {

        private long start = elementStamp - elementStamp % slide;

        @Override
        protected TimeInterval computeNext() {
          TimeInterval window = null;
          if (start > elementStamp - duration) {
            window = new TimeInterval(start, start + duration);
            start -= slide;
          } else {
            endOfData();
          }

          return window;
        }
      };
    }
  }
}
