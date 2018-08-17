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

public final class TimeInterval
    extends Window<TimeInterval>
    implements TimedWindow {

  private final long startMillis;
  private final long endMillis;

  public TimeInterval(long startMillis, long endMillis) {
    this.startMillis = startMillis;
    this.endMillis = endMillis;
  }

  public long getStartMillis() {
    return startMillis;
  }

  public long getEndMillis() {
    return endMillis;
  }

  public long getDurationMillis() {
    return endMillis - startMillis;
  }

  /**
   * Returns {@code true} if this window intersects the given window.
   */
  boolean intersects(TimeInterval that) {
    return this.startMillis < that.endMillis
            && this.endMillis > that.startMillis;
  }

  /**
   * Returns the minimal window covers both this window and the given window.
   */
  TimeInterval cover(TimeInterval that) {
    return new TimeInterval(
            Math.min(this.startMillis, that.startMillis),
            Math.max(this.endMillis, that.endMillis));
  }

  @Override
  public long maxTimestamp() {
    // ~ TimeInterval is practically right-open interval.
    // End time should be excluded because it would overlap with
    // the start time of the following window.
    return this.endMillis - 1;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof TimeInterval)) return false;

    TimeInterval that = (TimeInterval) o;

    return startMillis == that.startMillis && endMillis == that.endMillis;
  }

  @Override
  public int hashCode() {
    int result = (int) (startMillis ^ (startMillis >>> 32));
    result = 31 * result + (int) (endMillis ^ (endMillis >>> 32));
    return result;
  }

  @Override
  public String toString() {
    return "TimeInterval{" +
        "startMillis=" + startMillis +
        ", endMillis=" + endMillis +
        '}';
  }

  @Override
  public int compareTo(TimeInterval o) {
    long cmp = startMillis - o.startMillis;
    if (cmp != 0) {
      return cmp < 0 ? -1 : 1;
    }
    cmp = endMillis - o.endMillis;
    if (cmp == 0) {
      return 0;
    }
    return cmp < 0 ? -1 : 1;
  }
}
