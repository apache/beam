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

import com.google.cloud.dataflow.sdk.coders.AtomicCoder;
import com.google.cloud.dataflow.sdk.coders.Coder;
import com.google.cloud.dataflow.sdk.coders.CoderException;
import com.google.cloud.dataflow.sdk.coders.DurationCoder;
import com.google.cloud.dataflow.sdk.coders.InstantCoder;

import com.fasterxml.jackson.annotation.JsonCreator;

import org.joda.time.Duration;
import org.joda.time.Instant;
import org.joda.time.ReadableDuration;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * An implementation of {@link BoundedWindow} that represents an interval from
 * {@link #start} (inclusive) to {@link #end} (exclusive).
 */
public class IntervalWindow extends BoundedWindow
    implements Comparable<IntervalWindow> {
  /**
   * Start of the interval, inclusive.
   */
  private final Instant start;

  /**
   * End of the interval, exclusive.
   */
  private final Instant end;

  /**
   * Creates a new IntervalWindow that represents the half-open time
   * interval [start, end).
   */
  public IntervalWindow(Instant start, Instant end) {
    this.start = start;
    this.end = end;
  }

  public IntervalWindow(Instant start, ReadableDuration size) {
    this.start = start;
    this.end = start.plus(size);
  }

  /**
   * Returns the start of this window, inclusive.
   */
  public Instant start() {
    return start;
  }

  /**
   * Returns the end of this window, exclusive.
   */
  public Instant end() {
    return end;
  }

  /**
   * Returns the largest timestamp that can be included in this window.
   */
  @Override
  public Instant maxTimestamp() {
    // end not inclusive
    return end.minus(1);
  }

  /**
   * Returns whether this window contains the given window.
   */
  public boolean contains(IntervalWindow other) {
    return !this.start.isAfter(other.start) && !this.end.isBefore(other.end);
  }

  /**
   * Returns whether this window is disjoint from the given window.
   */
  public boolean isDisjoint(IntervalWindow other) {
    return !this.end.isAfter(other.start) || !other.end.isAfter(this.start);
  }

  /**
   * Returns whether this window intersects the given window.
   */
  public boolean intersects(IntervalWindow other) {
    return !isDisjoint(other);
  }

  /**
   * Returns the minimal window that includes both this window and
   * the given window.
   */
  public IntervalWindow span(IntervalWindow other) {
    return new IntervalWindow(
        new Instant(Math.min(start.getMillis(), other.start.getMillis())),
        new Instant(Math.max(end.getMillis(), other.end.getMillis())));
  }

  @Override
  public boolean equals(Object o) {
    return (o instanceof IntervalWindow)
        && ((IntervalWindow) o).end.isEqual(end)
        && ((IntervalWindow) o).start.isEqual(start);
  }

  @Override
  public int hashCode() {
    // The end values are themselves likely to be arithmetic sequence, which
    // is a poor distribution to use for a hashtable, so we
    // add a highly non-linear transformation.
    return (int)
        (start.getMillis() + modInverse((int) (end.getMillis() << 1) + 1));
  }

  /**
   * Compute the inverse of (odd) x mod 2^32.
   */
  private int modInverse(int x) {
    // Cube gives inverse mod 2^4, as x^4 == 1 (mod 2^4) for all odd x.
    int inverse = x * x * x;
    // Newton iteration doubles correct bits at each step.
    inverse *= 2 - x * inverse;
    inverse *= 2 - x * inverse;
    inverse *= 2 - x * inverse;
    return inverse;
  }

  @Override
  public String toString() {
    return "[" + start + ".." + end + ")";
  }

  @Override
  public int compareTo(IntervalWindow o) {
    if (start.isEqual(o.start)) {
      return end.compareTo(o.end);
    }
    return start.compareTo(o.start);
  }

  /**
   * Returns a {@link Coder} suitable for {@link IntervalWindow}.
   */
  public static Coder<IntervalWindow> getCoder() {
    return IntervalWindowCoder.of();
  }

  /**
   * Encodes an {@link IntervalWindow} as a pair of its upper bound and duration.
   */
  private static class IntervalWindowCoder extends AtomicCoder<IntervalWindow> {

    private static final IntervalWindowCoder INSTANCE =
        new IntervalWindowCoder();

    private static final Coder<Instant> instantCoder = InstantCoder.of();
    private static final Coder<ReadableDuration> durationCoder = DurationCoder.of();

    @JsonCreator
    public static IntervalWindowCoder of() {
      return INSTANCE;
    }

    @Override
    public void encode(IntervalWindow window,
                       OutputStream outStream,
                       Context context)
        throws IOException, CoderException {
      instantCoder.encode(window.end, outStream, context.nested());
      durationCoder.encode(new Duration(window.start, window.end), outStream, context.nested());
    }

    @Override
    public IntervalWindow decode(InputStream inStream, Context context)
        throws IOException, CoderException {
      Instant end = instantCoder.decode(inStream, context.nested());
      ReadableDuration duration = durationCoder.decode(inStream, context.nested());
      return new IntervalWindow(end.minus(duration), end);
    }
  }
}
