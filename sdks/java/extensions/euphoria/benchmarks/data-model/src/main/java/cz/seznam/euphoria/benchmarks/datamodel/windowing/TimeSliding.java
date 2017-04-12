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
package cz.seznam.euphoria.benchmarks.datamodel.windowing;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

/**
 * Time sliding windowing.
 */
public final class TimeSliding implements Windowing {

  public static TimeSliding of(Duration duration, Duration step) {
    return new TimeSliding(duration.toMillis(), step.toMillis());
  }

  private final long duration;
  private final long slide;

  private TimeSliding(long duration, long slide) {
    this.duration = duration;
    this.slide = slide;
    if (duration % slide != 0) {
      throw new IllegalArgumentException("This time sliding window can manage only aligned sliding windows");
    }
  }

  @Override
  public List<Long> generate(long timestamp) {
    List<Long> ret = new ArrayList<>((int) (this.duration / this.slide));
    for (long start = timestamp - timestamp % this.slide; start > timestamp
        - this.duration; start -= this.slide) {
      ret.add(start + this.duration);
    }
    return ret;
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
}
