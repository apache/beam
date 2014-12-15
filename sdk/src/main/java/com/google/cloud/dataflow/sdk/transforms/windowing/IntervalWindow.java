/*
 * Copyright (C) 2014 Google Inc.
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

import static com.google.cloud.dataflow.sdk.util.Structs.addString;
import static com.google.cloud.dataflow.sdk.util.TimeUtil.fromCloudDuration;
import static com.google.cloud.dataflow.sdk.util.TimeUtil.toCloudDuration;

import com.google.cloud.dataflow.sdk.coders.AtomicCoder;
import com.google.cloud.dataflow.sdk.coders.Coder;
import com.google.cloud.dataflow.sdk.coders.CoderException;
import com.google.cloud.dataflow.sdk.coders.InstantCoder;
import com.google.cloud.dataflow.sdk.util.CloudObject;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import org.joda.time.Duration;
import org.joda.time.Instant;

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

  public IntervalWindow(Instant start, Duration size) {
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
    // The end values are themselves likely to be arithmetic sequence,
    // which is a poor distribution to use for a hashtable, so we
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
   * Returns a Coder suitable for encoding IntervalWindows.
   */
  public static Coder<IntervalWindow> getCoder() {
    return IntervalWindowCoder.of();
  }

  /**
   * Returns a Coder for encoding interval windows of fixed size (which
   * is more efficient than {@link #getCoder()} as it only needs to encode
   * one endpoint).
   */
  public static Coder<IntervalWindow> getFixedSizeCoder(final Duration size) {
    return FixedSizeIntervalWindowCoder.of(size);
  }

  @SuppressWarnings("serial")
  private static class IntervalWindowCoder extends AtomicCoder<IntervalWindow> {
    private static final IntervalWindowCoder INSTANCE =
        new IntervalWindowCoder();
    private static final Coder<Instant> instantCoder = InstantCoder.of();

    @JsonCreator
    public static IntervalWindowCoder of() {
      return INSTANCE;
    }

    @Override
    public void encode(IntervalWindow window,
                       OutputStream outStream,
                       Context context)
        throws IOException, CoderException {
      instantCoder.encode(window.start, outStream, context.nested());
      instantCoder.encode(window.end, outStream, context.nested());
    }

    @Override
    public IntervalWindow decode(InputStream inStream, Context context)
        throws IOException, CoderException {
      Instant start = instantCoder.decode(inStream, context.nested());
      Instant end = instantCoder.decode(inStream, context.nested());
      return new IntervalWindow(start, end);
    }

    @Override
    public boolean isDeterministic() { return true; }
  }

  @SuppressWarnings("serial")
  private static class FixedSizeIntervalWindowCoder
      extends AtomicCoder<IntervalWindow> {
    private static final Coder<Instant> instantCoder = InstantCoder.of();

    private final Duration size;

    @JsonCreator
    public static FixedSizeIntervalWindowCoder of(
        @JsonProperty("duration") String duration) {
      return of(fromCloudDuration(duration));
    }

    public static FixedSizeIntervalWindowCoder of(Duration size) {
      return new FixedSizeIntervalWindowCoder(size);
    }

    private FixedSizeIntervalWindowCoder(Duration size) {
      this.size = size;
    }

    @Override
    public void encode(IntervalWindow window,
                       OutputStream outStream,
                       Context context)
        throws IOException, CoderException {
      instantCoder.encode(window.start, outStream, context);
    }

    @Override
    public IntervalWindow decode(InputStream inStream, Context context)
        throws IOException, CoderException {
      Instant start = instantCoder.decode(inStream, context);
      return new IntervalWindow(start, size);
    }

    @Override
    public boolean isDeterministic() { return true; }

    @Override
    public CloudObject asCloudObject() {
      CloudObject result = super.asCloudObject();
      addString(result, "duration", toCloudDuration(Duration.millis(size.getMillis())));
      return result;
    }
  }
}
