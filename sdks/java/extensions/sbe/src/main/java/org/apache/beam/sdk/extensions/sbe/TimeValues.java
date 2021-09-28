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
package org.apache.beam.sdk.extensions.sbe;

import static org.apache.beam.sdk.extensions.sbe.Schemas.TZ_TIME_SCHEMA;
import static org.apache.beam.sdk.extensions.sbe.Schemas.TimeFieldNames.TIME_FIELD_NAME;
import static org.apache.beam.sdk.extensions.sbe.Schemas.TimeFieldNames.TIME_ZONE_HOUR_FIELD_NAME;
import static org.apache.beam.sdk.extensions.sbe.Schemas.TimeFieldNames.TIME_ZONE_MINUTE_FIELD_NAME;
import static org.apache.beam.sdk.extensions.sbe.Schemas.TimeFieldNames.UNIT_FIELD_NAME;
import static org.apache.beam.sdk.extensions.sbe.Schemas.UTC_TIME_SCHEMA;
import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkArgument;

import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import org.apache.beam.sdk.values.Row;
import org.checkerframework.checker.initialization.qual.Initialized;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * Representations of SBE value types.
 *
 * <p>These are convertible to/from a {@link Row} that is a direct mapping of an SBE composite type.
 */
public final class TimeValues {
  private TimeValues() {}

  /** Represents an SBE time unit with conversions to/from the SBE-specified values. */
  public enum SbeTimeUnit {
    SECONDS((byte) 0),
    MILLISECONDS((byte) 3),
    MICROSECONDS((byte) 6),
    NANOSECONDS((byte) 9);

    private final byte value;

    SbeTimeUnit(byte value) {
      this.value = value;
    }

    /** Returns the {@link SbeTimeUnit} associated with {@code value}. */
    public static SbeTimeUnit fromByte(byte value) {
      switch (value) {
        case 0:
          return SECONDS;
        case 3:
          return MILLISECONDS;
        case 6:
          return MICROSECONDS;
        case 9:
          return NANOSECONDS;
        default:
          throw new IllegalArgumentException(
              String.format("Value (%s) not a valid time unit", value));
      }
    }

    /** Returns the SBE-specified uint8 value. */
    public byte asByte() {
      return value;
    }

    /** Returns as Java's {@link TimeUnit}. */
    public TimeUnit asJavaTimeUnit() {
      switch (value) {
        case 0:
          return TimeUnit.SECONDS;
        case 3:
          return TimeUnit.MILLISECONDS;
        case 6:
          return TimeUnit.MICROSECONDS;
        case 9:
          return TimeUnit.NANOSECONDS;
        default:
          throw new IllegalStateException(String.format("Somehow value is invalid: %s", value));
      }
    }

    /** Returns as {@link ChronoUnit}. */
    public ChronoUnit asChronoUnit() {
      // TODO(zhoufek): Switch to converting from Java TimeUnit once fully on Java 11
      switch (value) {
        case 0:
          return ChronoUnit.SECONDS;
        case 3:
          return ChronoUnit.MILLIS;
        case 6:
          return ChronoUnit.MICROS;
        case 9:
          return ChronoUnit.NANOS;
        default:
          throw new IllegalStateException(String.format("Somehow value is invalid: %s", value));
      }
    }
  }

  /**
   * Abstract class for SBE's time types.
   *
   * <p>Implementations mostly just add additional semantics and possibly Java type conversions.
   */
  private abstract static class TimeValue {
    private final long time;
    private final byte unit;

    /**
     * Constructor.
     *
     * <p>This will handle the necessary input validation.
     */
    private TimeValue(long time, byte unit) {
      SbeTimeUnit.fromByte(unit); // Validates unit

      this.time = time;
      this.unit = unit;
    }

    /** Returns the SBE time value. */
    public long time() {
      return time;
    }

    /** Returns the SBE unit value. */
    public byte unit() {
      return unit;
    }

    /** Returns the SBE unit as an enum. */
    public SbeTimeUnit timeUnit() {
      return SbeTimeUnit.fromByte(unit);
    }

    /** Helper for resolving the {@link ZoneOffset} in setting Java offset types. */
    protected abstract ZoneOffset zoneOffset();

    /** Returns this value as a {@link Row}. */
    public abstract Row asRow();

    @Override
    public int hashCode() {
      return Objects.hash(time, unit);
    }

    @Override
    public boolean equals(@Initialized @Nullable Object obj) {
      if (this == obj) {
        return true;
      }
      // Since different implementations may be representing different SBE semantic types, we
      // want an exact match on the class objects.
      if (obj == null || !getClass().equals(obj.getClass())) {
        return false;
      }

      TimeValue asTimeValue = (TimeValue) obj;
      return time == asTimeValue.time && unit == asTimeValue.unit;
    }

    @Override
    public abstract String toString();
  }

  /** Representation of SBE's UTCTimestamp or TZTimestamp types. */
  private abstract static class TimestampValue extends TimeValue {
    private TimestampValue(long time, byte unit) {
      super(time, unit);
    }

    /** Returns as a {@link OffsetDateTime}. */
    public OffsetDateTime asOffsetDateTime() {
      return Instant.EPOCH.plus(time(), timeUnit().asChronoUnit()).atOffset(zoneOffset());
    }

    @Override
    public String toString() {
      return asOffsetDateTime().toString();
    }
  }

  /** Representation of SBE's UTCTimeOnly and TZTimeOnly types. */
  private abstract static class TimeOnlyValue extends TimeValue {
    private TimeOnlyValue(long time, byte unit) {
      super(time, unit);
    }

    /** Returns as a {@link OffsetTime}. */
    public OffsetTime asOffsetTime() {
      OffsetTime offsetTime = OffsetTime.of(0, 0, 0, 0, zoneOffset());
      return offsetTime.plus(time(), timeUnit().asChronoUnit());
    }

    @Override
    public String toString() {
      return asOffsetTime().toString();
    }
  }

  /** Represents SBE's UTCTimestamp type. */
  public static class UTCTimestampValue extends TimestampValue {
    private UTCTimestampValue(long time, byte unit) {
      super(time, unit);
    }

    /** Creates from SBE primitive values. */
    public static UTCTimestampValue fromPrimitives(long time, byte unit) {
      return new UTCTimestampValue(time, unit);
    }

    /** Creates from Beam {@link Row}. */
    public static UTCTimestampValue fromRow(@NonNull Row row) {
      checkArgument(row.getSchema().equals(UTC_TIME_SCHEMA));
      return new UTCTimestampValue(getTime(row), getUnit(row));
    }

    @Override
    protected ZoneOffset zoneOffset() {
      return ZoneOffset.UTC;
    }

    @Override
    public Row asRow() {
      return createUtcRow(time(), unit());
    }
  }

  /** Represents SBE's UTCTimeOnly type. */
  public static class UTCTimeOnlyValue extends TimeOnlyValue {
    private UTCTimeOnlyValue(long time, byte unit) {
      super(time, unit);
    }

    /** Creates from SBE primitive values. */
    public static UTCTimeOnlyValue fromPrimitives(long time, byte unit) {
      return new UTCTimeOnlyValue(time, unit);
    }

    /** Creates from Beam {@link Row}. */
    public static UTCTimeOnlyValue fromRow(@NonNull Row row) {
      checkArgument(row.getSchema().equals(UTC_TIME_SCHEMA));
      return new UTCTimeOnlyValue(getTime(row), getUnit(row));
    }

    @Override
    protected ZoneOffset zoneOffset() {
      return ZoneOffset.UTC;
    }

    @Override
    public Row asRow() {
      return createUtcRow(time(), unit());
    }
  }

  /** Represents SBE's TZTimestamp type. */
  public static class TZTimestampValue extends TimestampValue {
    private final byte timezoneHour;
    private final byte timezoneMinute;

    private TZTimestampValue(long time, byte unit, byte timezoneHour, byte timezoneMinute) {
      super(time, unit);
      this.timezoneHour = timezoneHour;
      this.timezoneMinute = timezoneMinute;
    }

    /** Creates from SBE primitive values. */
    public static TZTimestampValue fromPrimitives(
        long time, byte unit, byte timezoneHour, byte timezoneMinute) {
      return new TZTimestampValue(time, unit, timezoneHour, timezoneMinute);
    }

    /** Creates from Beam {@link Row}. */
    public static TZTimestampValue fromRow(@NonNull Row row) {
      checkArgument(row.getSchema().equals(TZ_TIME_SCHEMA));
      return new TZTimestampValue(
          getTime(row), getUnit(row), getTimezoneHour(row), getTimezoneMinute(row));
    }

    /** Returns the SBE timezoneHour value. */
    public byte timezoneHour() {
      return timezoneHour;
    }

    /** Returns the SBE timezoneMinute value. */
    public byte timezoneMinute() {
      return timezoneMinute;
    }

    @Override
    protected ZoneOffset zoneOffset() {
      return createZoneOffset(timezoneHour, timezoneMinute);
    }

    @Override
    public Row asRow() {
      return createTzRow(time(), unit(), timezoneHour, timezoneMinute);
    }

    @Override
    public int hashCode() {
      return Objects.hash(time(), unit(), timezoneHour, timezoneMinute);
    }

    @Override
    @SuppressWarnings("nullness") // Null check done in super call
    public boolean equals(@Initialized @Nullable Object obj) {
      boolean fromSuper = super.equals(obj);
      if (!fromSuper) {
        return false;
      }

      TZTimestampValue asValue = (TZTimestampValue) obj;
      return timezoneHour == asValue.timezoneHour && timezoneMinute == asValue.timezoneMinute;
    }
  }

  /** Represents SBE's TZTimeOnly type. */
  public static final class TZTimeOnlyValue extends TimeOnlyValue {
    private final byte timezoneHour;
    private final byte timezoneMinute;

    private TZTimeOnlyValue(long time, byte unit, byte timezoneHour, byte timezoneMinute) {
      super(time, unit);
      this.timezoneHour = timezoneHour;
      this.timezoneMinute = timezoneMinute;
    }

    /** Creates from SBE primitive values. */
    public static TZTimeOnlyValue fromPrimitives(
        long time, byte unit, byte timezoneHour, byte timezoneMinute) {
      return new TZTimeOnlyValue(time, unit, timezoneHour, timezoneMinute);
    }

    /** Creates from Beam {@link Row}. */
    public static TZTimeOnlyValue fromRow(@NonNull Row row) {
      checkArgument(row.getSchema().equals(TZ_TIME_SCHEMA));
      return new TZTimeOnlyValue(
          getTime(row), getUnit(row), getTimezoneHour(row), getTimezoneMinute(row));
    }

    /** Returns the SBE timezoneHour value. */
    public byte timezoneHour() {
      return timezoneHour;
    }

    /** Returns the SBE timezoneMinute value. */
    public byte timezoneMinute() {
      return timezoneMinute;
    }

    @Override
    protected ZoneOffset zoneOffset() {
      return createZoneOffset(timezoneHour, timezoneMinute);
    }

    @Override
    public Row asRow() {
      return createTzRow(time(), unit(), timezoneHour, timezoneMinute);
    }

    @Override
    public int hashCode() {
      return Objects.hash(time(), unit(), timezoneHour, timezoneMinute);
    }

    @Override
    @SuppressWarnings("nullness") // Null check done in super call
    public boolean equals(@Initialized @Nullable Object obj) {
      boolean fromSuper = super.equals(obj);
      if (!fromSuper) {
        return false;
      }

      TZTimeOnlyValue asValue = (TZTimeOnlyValue) obj;
      return timezoneHour == asValue.timezoneHour && timezoneMinute == asValue.timezoneMinute;
    }
  }

  // Utility methods that may be shared among multiple types.

  private static final String ROW_NULL_VALUE_MESSAGE = "Row has correct schema but null values.";

  // All the getField methods are suppressing null checks to avoid false positives from the checker
  // framework not recognizing `checkArgument(value != null, ...)` as a null check.

  /** Gets the SBE time value from {@code row}. */
  @SuppressWarnings("nullness")
  private static long getTime(Row row) {
    Long time = row.getInt64(TIME_FIELD_NAME);
    checkArgument(time != null, ROW_NULL_VALUE_MESSAGE);
    return time;
  }

  /** Gets the SBE unit value from {@code row}. */
  @SuppressWarnings("nullness")
  private static byte getUnit(Row row) {
    Byte unit = row.getByte(UNIT_FIELD_NAME);
    checkArgument(unit != null, ROW_NULL_VALUE_MESSAGE);
    return unit;
  }

  /** Gets the SBE timezoneHour value from {@code row}. */
  @SuppressWarnings("nullness")
  private static byte getTimezoneHour(Row row) {
    Byte timezoneHour = row.getByte(TIME_ZONE_HOUR_FIELD_NAME);
    checkArgument(timezoneHour != null, ROW_NULL_VALUE_MESSAGE);
    return timezoneHour;
  }

  /** Gets the SBE timezoneMinute value from {@code row}. */
  @SuppressWarnings("nullness")
  private static byte getTimezoneMinute(Row row) {
    Byte timezoneMinute = row.getByte(TIME_ZONE_MINUTE_FIELD_NAME);
    checkArgument(timezoneMinute != null, ROW_NULL_VALUE_MESSAGE);
    return timezoneMinute;
  }

  /** Creates the {@link ZoneOffset} representation of the SBE offset. */
  private static ZoneOffset createZoneOffset(byte timezoneHour, byte timezoneMinute) {
    byte actualMinute = timezoneHour >= 0 ? timezoneMinute : (byte) -timezoneMinute;
    return ZoneOffset.ofHoursMinutes(timezoneHour, actualMinute);
  }

  /** Returns a {@link Row} that could represent a UTCTimestamp or UTCTimeOnly. */
  private static Row createUtcRow(long time, byte unit) {
    return Row.withSchema(UTC_TIME_SCHEMA)
        .withFieldValue(TIME_FIELD_NAME, time)
        .withFieldValue(UNIT_FIELD_NAME, unit)
        .build();
  }

  /** Returns a {@link Row} that could represent a TZTimestamp or TZTimeOnly. */
  private static Row createTzRow(long time, byte unit, byte timezoneHour, byte timezoneMinute) {
    return Row.withSchema(TZ_TIME_SCHEMA)
        .withFieldValue(TIME_FIELD_NAME, time)
        .withFieldValue(UNIT_FIELD_NAME, unit)
        .withFieldValue(TIME_ZONE_HOUR_FIELD_NAME, timezoneHour)
        .withFieldValue(TIME_ZONE_MINUTE_FIELD_NAME, timezoneMinute)
        .build();
  }
}
