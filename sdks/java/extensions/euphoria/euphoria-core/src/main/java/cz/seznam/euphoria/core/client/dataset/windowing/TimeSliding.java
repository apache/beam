/**
 * Copyright 2016-2017 Seznam.cz, a.s.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package cz.seznam.euphoria.core.client.dataset.windowing;

import cz.seznam.euphoria.core.annotation.audience.Audience;
import cz.seznam.euphoria.core.client.io.Collector;
import cz.seznam.euphoria.core.client.triggers.TimeTrigger;
import cz.seznam.euphoria.core.client.triggers.Trigger;
import com.google.common.base.Preconditions;
import com.google.common.collect.AbstractIterator;

import java.time.Duration;
import java.util.Iterator;

/**
 * Time sliding windowing.
 */
@Audience(Audience.Type.CLIENT)
public final class TimeSliding<T>
    implements Windowing<T, TimeInterval> {

  public static <T> TimeSliding<T> of(Duration duration, Duration step) {
    return new TimeSliding<>(duration.toMillis(), step.toMillis());
  }

  /**
   * Helper method to extract window label from context.
   *
   * @param context the execution context
   *
   * @return the {@link TimeInterval} window of this execution
   *
   * @throws ClassCastException if the context is not part of a
   *          time-sliding execution
   */
  public static TimeInterval getLabel(Collector<?> context) {
    return (TimeInterval) context.getWindow();
  }

  private final long duration;
  private final long slide;

  private TimeSliding(long duration, long slide) {
    this.duration = duration;
    this.slide = slide;

    Preconditions.checkArgument(duration > 0, "Windowing with zero duration");

    if (duration % slide != 0) {
      throw new IllegalArgumentException(
          "This time sliding window can manage only aligned sliding windows");
    }
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
    return "TimeSliding{" +
        "duration=" + duration +
        ", slide=" + slide +
        '}';
  }

  public long getDuration() {
    return duration;
  }

  public long getSlide() {
    return slide;
  }


  /**
   * Calculates window boundaries lazily during the iteration.
   */
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

