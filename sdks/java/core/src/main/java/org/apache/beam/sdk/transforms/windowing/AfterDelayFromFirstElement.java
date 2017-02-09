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

import com.google.common.collect.ImmutableList;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.windowing.Trigger.OnceTrigger;
import org.apache.beam.sdk.util.TimeDomain;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.joda.time.format.PeriodFormat;
import org.joda.time.format.PeriodFormatter;

/**
 * A base class for triggers that happen after a processing time delay from the arrival
 * of the first element in a pane.
 *
 * <p>This class is for internal use only and may change at any time.
 */
@Experimental(Experimental.Kind.TRIGGER)
public abstract class AfterDelayFromFirstElement extends OnceTrigger {

  protected static final List<SerializableFunction<Instant, Instant>> IDENTITY =
      ImmutableList.<SerializableFunction<Instant, Instant>>of();

  private static final PeriodFormatter PERIOD_FORMATTER = PeriodFormat.wordBased(Locale.ENGLISH);

  /**
   * To complete an implementation, return a new instance like this one, but incorporating
   * the provided timestamp mapping functions. Generally should be used by calling the
   * constructor of this class from the constructor of the subclass.
   */
  protected abstract AfterDelayFromFirstElement newWith(
      List<SerializableFunction<Instant, Instant>> transform);

  /**
   * A list of timestampMappers m1, m2, m3, ... m_n considered to be composed in sequence. The
   * overall mapping for an instance `instance` is `m_n(... m3(m2(m1(instant))`,
   * implemented via #computeTargetTimestamp
   */
  protected final List<SerializableFunction<Instant, Instant>> timestampMappers;

  private final TimeDomain timeDomain;

  public AfterDelayFromFirstElement(
      TimeDomain timeDomain,
      List<SerializableFunction<Instant, Instant>> timestampMappers) {
    super(null);
    this.timestampMappers = timestampMappers;
    this.timeDomain = timeDomain;
  }

  /**
   * The time domain according for which this trigger sets timers.
   */
  public TimeDomain getTimeDomain() {
    return timeDomain;
  }

  /**
   * The mapping functions applied to the arrival time of an element to determine when to
   * set a wake-up timer for triggering.
   */
  public List<SerializableFunction<Instant, Instant>> getTimestampMappers() {
    return timestampMappers;
  }

  /**
   * Aligns timestamps to the smallest multiple of {@code size} since the {@code offset} greater
   * than the timestamp.
   *
   * <p>TODO: Consider sharing this with FixedWindows, and bring over the equivalent of
   * CalendarWindows.
   */
  public AfterDelayFromFirstElement alignedTo(final Duration size, final Instant offset) {
    return newWith(new AlignFn(size, offset));
  }

  /**
   * Aligns the time to be the smallest multiple of {@code size} greater than the timestamp
   * since the epoch.
   */
  public AfterDelayFromFirstElement alignedTo(final Duration size) {
    return alignedTo(size, new Instant(0));
  }

  /**
   * Adds some delay to the original target time.
   *
   * @param delay the delay to add
   * @return An updated time trigger that will wait the additional time before firing.
   */
  public AfterDelayFromFirstElement plusDelayOf(final Duration delay) {
    return newWith(new DelayFn(delay));
  }

  /**
   * @deprecated This will be removed in the next major version. Please use only
   *             {@link #plusDelayOf} and {@link #alignedTo}.
   */
  @Deprecated
  public OnceTrigger mappedTo(SerializableFunction<Instant, Instant> timestampMapper) {
    return newWith(timestampMapper);
  }

  @Override
  public boolean isCompatible(Trigger other) {
    if (!getClass().equals(other.getClass())) {
      return false;
    }

    AfterDelayFromFirstElement that = (AfterDelayFromFirstElement) other;
    return this.timestampMappers.equals(that.timestampMappers);
  }


  private AfterDelayFromFirstElement newWith(
      SerializableFunction<Instant, Instant> timestampMapper) {
    return newWith(
        ImmutableList.<SerializableFunction<Instant, Instant>>builder()
            .addAll(timestampMappers)
            .add(timestampMapper)
            .build());
  }

  @Override
  public Instant getWatermarkThatGuaranteesFiring(BoundedWindow window) {
    return BoundedWindow.TIMESTAMP_MAX_VALUE;
  }

  protected Instant computeTargetTimestamp(Instant time) {
    Instant result = time;
    for (SerializableFunction<Instant, Instant> timestampMapper : timestampMappers) {
      result = timestampMapper.apply(result);
    }
    return result;
  }

  /**
   * A {@link SerializableFunction} to delay the timestamp at which this triggers fires.
   */
  private static final class DelayFn implements SerializableFunction<Instant, Instant> {
    private final Duration delay;

    public DelayFn(Duration delay) {
      this.delay = delay;
    }

    @Override
    public Instant apply(Instant input) {
      return input.plus(delay);
    }

    @Override
    public boolean equals(Object object) {
      if (object == this) {
        return true;
      }

      if (!(object instanceof DelayFn)) {
        return false;
      }

      return this.delay.equals(((DelayFn) object).delay);
    }

    @Override
    public int hashCode() {
      return Objects.hash(delay);
    }

    @Override
    public String toString() {
      return PERIOD_FORMATTER.print(delay.toPeriod());
    }
  }

  /**
   * A {@link SerializableFunction} to align an instant to the nearest interval boundary.
   */
  static final class AlignFn implements SerializableFunction<Instant, Instant> {
    private final Duration size;
    private final Instant offset;


    /**
     * Aligns timestamps to the smallest multiple of {@code size} since the {@code offset} greater
     * than the timestamp.
     */
    public AlignFn(Duration size, Instant offset) {
      this.size = size;
      this.offset = offset;
    }

    @Override
    public Instant apply(Instant point) {
      long millisSinceStart = new Duration(offset, point).getMillis() % size.getMillis();
      return millisSinceStart == 0 ? point : point.plus(size).minus(millisSinceStart);
    }

    @Override
    public boolean equals(Object object) {
      if (object == this) {
        return true;
      }

      if (!(object instanceof AlignFn)) {
        return false;
      }

      AlignFn other = (AlignFn) object;
      return other.size.equals(this.size)
          && other.offset.equals(this.offset);
    }

    @Override
    public int hashCode() {
      return Objects.hash(size, offset);
    }
  }
}
