
package cz.seznam.euphoria.core.client.dataset.windowing;

import java.io.Serializable;
import java.util.Objects;

public final class TimeInterval implements Serializable, Comparable<TimeInterval> {
  private final long startMillis;
  private final long intervalMillis;

  public TimeInterval(long startMillis, long intervalMillis) {
    this.startMillis = startMillis;
    this.intervalMillis = intervalMillis;
  }

  public long getStartMillis() {
    return startMillis;
  }

  public long getEndMillis() {
    return startMillis + intervalMillis;
  }

  public long getIntervalMillis() {
    return intervalMillis;
  }

  @Override
  public boolean equals(Object o) {
    if (o instanceof TimeInterval) {
      TimeInterval that = (TimeInterval) o;
      return this.startMillis == that.startMillis
          && this.intervalMillis == that.intervalMillis;
    }
    return false;
  }

  @Override
  public int hashCode() {
    return Objects.hash(startMillis, intervalMillis);
  }

  @Override
  public String toString() {
    return "TimeInterval{" +
        "startMillis=" + startMillis +
        ", intervalMillis=" + intervalMillis +
        '}';
  }

  @Override
  public int compareTo(TimeInterval o) {
    long cmp = startMillis - o.startMillis;
    if (cmp != 0) {
      return cmp < 0 ? -1 : 1;
    }
    return intervalMillis - o.intervalMillis < 0 ? -1 : 1;
  }
}