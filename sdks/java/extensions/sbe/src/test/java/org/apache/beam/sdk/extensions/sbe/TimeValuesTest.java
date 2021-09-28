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
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.concurrent.TimeUnit;
import org.apache.beam.sdk.extensions.sbe.TimeValues.SbeTimeUnit;
import org.apache.beam.sdk.extensions.sbe.TimeValues.TZTimeOnlyValue;
import org.apache.beam.sdk.extensions.sbe.TimeValues.TZTimestampValue;
import org.apache.beam.sdk.extensions.sbe.TimeValues.UTCTimeOnlyValue;
import org.apache.beam.sdk.extensions.sbe.TimeValues.UTCTimestampValue;
import org.apache.beam.sdk.values.Row;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Unit tests for {@link TimeValues}. */
@RunWith(JUnit4.class)
public final class TimeValuesTest {
  private static final byte SECOND_UNIT_VALUE = 0;
  private static final byte MILLISECOND_UNIT_VALUE = 3;
  private static final byte MICROSECOND_UNIT_VALUE = 6;
  private static final byte NANOSECOND_UNIT_VALUE = 9;

  private static final Instant TIME = Instant.now();
  private static final long MILLISECONDS = TIME.toEpochMilli();
  private static final long SECONDS = TimeUnit.SECONDS.convert(MILLISECONDS, TimeUnit.MILLISECONDS);
  private static final long MICROSECONDS =
      TimeUnit.MICROSECONDS.convert(MILLISECONDS, TimeUnit.MILLISECONDS);
  private static final long NANOSECONDS =
      TimeUnit.NANOSECONDS.convert(MILLISECONDS, TimeUnit.MILLISECONDS);

  private static final byte HOUR_OFFSET = -2;
  private static final byte MINUTE_OFFSET = 30;
  private static final ZoneOffset ZONE_OFFSET =
      ZoneOffset.ofHoursMinutes(HOUR_OFFSET, -MINUTE_OFFSET);

  private static final Row UTC_ROW =
      Row.withSchema(UTC_TIME_SCHEMA)
          .withFieldValue(TIME_FIELD_NAME, MILLISECONDS)
          .withFieldValue(UNIT_FIELD_NAME, MILLISECOND_UNIT_VALUE)
          .build();

  private static final Row TZ_ROW =
      Row.withSchema(TZ_TIME_SCHEMA)
          .withFieldValue(TIME_FIELD_NAME, MILLISECONDS)
          .withFieldValue(UNIT_FIELD_NAME, MILLISECOND_UNIT_VALUE)
          .withFieldValue(TIME_ZONE_HOUR_FIELD_NAME, HOUR_OFFSET)
          .withFieldValue(TIME_ZONE_MINUTE_FIELD_NAME, MINUTE_OFFSET)
          .build();

  @Test
  public void testSbeTimeUnitByteValues() {
    assertEquals(SECOND_UNIT_VALUE, SbeTimeUnit.SECONDS.asByte());
    assertEquals(MILLISECOND_UNIT_VALUE, SbeTimeUnit.MILLISECONDS.asByte());
    assertEquals(MICROSECOND_UNIT_VALUE, SbeTimeUnit.MICROSECONDS.asByte());
    assertEquals(NANOSECOND_UNIT_VALUE, SbeTimeUnit.NANOSECONDS.asByte());
    assertEquals(SbeTimeUnit.SECONDS, SbeTimeUnit.fromByte(SECOND_UNIT_VALUE));
    assertEquals(SbeTimeUnit.MILLISECONDS, SbeTimeUnit.fromByte(MILLISECOND_UNIT_VALUE));
    assertEquals(SbeTimeUnit.MICROSECONDS, SbeTimeUnit.fromByte(MICROSECOND_UNIT_VALUE));
    assertEquals(SbeTimeUnit.NANOSECONDS, SbeTimeUnit.fromByte(NANOSECOND_UNIT_VALUE));
  }

  @Test
  public void testSbeTimeUnitFromInvalidByte() {
    byte badValue = 1;
    IllegalArgumentException exception =
        assertThrows(IllegalArgumentException.class, () -> SbeTimeUnit.fromByte(badValue));
    assertTrue(exception.getMessage().contains(Byte.toString(badValue)));
  }

  @Test
  public void testSbeTimeUnitJavaValue() {
    assertEquals(TimeUnit.SECONDS, SbeTimeUnit.SECONDS.asJavaTimeUnit());
    assertEquals(TimeUnit.MILLISECONDS, SbeTimeUnit.MILLISECONDS.asJavaTimeUnit());
    assertEquals(TimeUnit.MICROSECONDS, SbeTimeUnit.MICROSECONDS.asJavaTimeUnit());
    assertEquals(TimeUnit.NANOSECONDS, SbeTimeUnit.NANOSECONDS.asJavaTimeUnit());
  }

  @Test
  public void testCreateTimeValuesFromPrimitives() {
    UTCTimestampValue utcTimestampValue =
        UTCTimestampValue.fromPrimitives(MILLISECONDS, MILLISECOND_UNIT_VALUE);
    UTCTimeOnlyValue utcTimeOnlyValue =
        UTCTimeOnlyValue.fromPrimitives(MILLISECONDS, MILLISECOND_UNIT_VALUE);
    TZTimestampValue tzTimestampValue =
        TZTimestampValue.fromPrimitives(
            MILLISECONDS, MILLISECOND_UNIT_VALUE, HOUR_OFFSET, MINUTE_OFFSET);
    TZTimeOnlyValue tzTimeOnlyValue =
        TZTimeOnlyValue.fromPrimitives(
            MILLISECONDS, MILLISECOND_UNIT_VALUE, HOUR_OFFSET, MINUTE_OFFSET);

    assertEquals(MILLISECONDS, utcTimestampValue.time());
    assertEquals(MILLISECOND_UNIT_VALUE, utcTimestampValue.unit());
    assertEquals(MILLISECONDS, utcTimeOnlyValue.time());
    assertEquals(MILLISECOND_UNIT_VALUE, utcTimeOnlyValue.unit());
    assertEquals(MILLISECONDS, tzTimestampValue.time());
    assertEquals(MILLISECOND_UNIT_VALUE, tzTimestampValue.unit());
    assertEquals(HOUR_OFFSET, tzTimestampValue.timezoneHour());
    assertEquals(MINUTE_OFFSET, tzTimestampValue.timezoneMinute());
    assertEquals(MILLISECONDS, tzTimeOnlyValue.time());
    assertEquals(MILLISECOND_UNIT_VALUE, tzTimeOnlyValue.unit());
    assertEquals(HOUR_OFFSET, tzTimestampValue.timezoneHour());
    assertEquals(MINUTE_OFFSET, tzTimestampValue.timezoneMinute());
  }

  @Test
  public void testCreateTimeValuesWithBadUnit() {
    byte badUnit = 1;

    assertThrows(
        IllegalArgumentException.class,
        () -> UTCTimestampValue.fromPrimitives(MILLISECONDS, badUnit));
    assertThrows(
        IllegalArgumentException.class,
        () -> UTCTimeOnlyValue.fromPrimitives(MILLISECONDS, badUnit));
    assertThrows(
        IllegalArgumentException.class,
        () -> TZTimestampValue.fromPrimitives(MILLISECONDS, badUnit, HOUR_OFFSET, MINUTE_OFFSET));
    assertThrows(
        IllegalArgumentException.class,
        () -> TZTimeOnlyValue.fromPrimitives(MILLISECONDS, badUnit, HOUR_OFFSET, MINUTE_OFFSET));
  }

  @Test
  public void testUTCTimestampAsOffsetDateTime() {
    UTCTimestampValue utcSeconds = UTCTimestampValue.fromPrimitives(SECONDS, SECOND_UNIT_VALUE);
    UTCTimestampValue utcMilliseconds =
        UTCTimestampValue.fromPrimitives(MILLISECONDS, MILLISECOND_UNIT_VALUE);
    UTCTimestampValue utcMicroseconds =
        UTCTimestampValue.fromPrimitives(MICROSECONDS, MICROSECOND_UNIT_VALUE);
    UTCTimestampValue utcNanoseconds =
        UTCTimestampValue.fromPrimitives(NANOSECONDS, NANOSECOND_UNIT_VALUE);

    assertEquals(
        createOffsetDateTime(SECONDS, ChronoUnit.SECONDS, ZoneOffset.UTC),
        utcSeconds.asOffsetDateTime());
    assertEquals(
        createOffsetDateTime(MILLISECONDS, ChronoUnit.MILLIS, ZoneOffset.UTC),
        utcMilliseconds.asOffsetDateTime());
    assertEquals(
        createOffsetDateTime(MICROSECONDS, ChronoUnit.MICROS, ZoneOffset.UTC),
        utcMicroseconds.asOffsetDateTime());
    assertEquals(
        createOffsetDateTime(NANOSECONDS, ChronoUnit.NANOS, ZoneOffset.UTC),
        utcNanoseconds.asOffsetDateTime());
  }

  @Test
  public void testTZTimestampValueAsOffsetDateTimePositiveOffset() {
    byte hourOffset = 2;
    byte minuteOffset = 30;
    ZoneOffset offset = ZoneOffset.ofHoursMinutes(2, 30);

    TZTimestampValue tzSeconds =
        TZTimestampValue.fromPrimitives(SECONDS, SECOND_UNIT_VALUE, hourOffset, minuteOffset);
    TZTimestampValue tzMilliseconds =
        TZTimestampValue.fromPrimitives(
            MILLISECONDS, MILLISECOND_UNIT_VALUE, hourOffset, minuteOffset);
    TZTimestampValue tzMicroseconds =
        TZTimestampValue.fromPrimitives(
            MICROSECONDS, MICROSECOND_UNIT_VALUE, hourOffset, minuteOffset);
    TZTimestampValue tzNanoseconds =
        TZTimestampValue.fromPrimitives(
            NANOSECONDS, NANOSECOND_UNIT_VALUE, hourOffset, minuteOffset);

    assertEquals(
        createOffsetDateTime(SECONDS, ChronoUnit.SECONDS, offset), tzSeconds.asOffsetDateTime());
    assertEquals(
        createOffsetDateTime(MILLISECONDS, ChronoUnit.MILLIS, offset),
        tzMilliseconds.asOffsetDateTime());
    assertEquals(
        createOffsetDateTime(MICROSECONDS, ChronoUnit.MICROS, offset),
        tzMicroseconds.asOffsetDateTime());
    assertEquals(
        createOffsetDateTime(NANOSECONDS, ChronoUnit.NANOS, offset),
        tzNanoseconds.asOffsetDateTime());
  }

  @Test
  public void testTZTimestampValueAsOffsetDateTimeNegativeOffset() {
    byte hourOffset = -2;
    byte minuteOffset = 30;
    ZoneOffset offset = ZoneOffset.ofHoursMinutes(-2, -30);

    TZTimestampValue tzSeconds =
        TZTimestampValue.fromPrimitives(SECONDS, SECOND_UNIT_VALUE, hourOffset, minuteOffset);
    TZTimestampValue tzMilliseconds =
        TZTimestampValue.fromPrimitives(
            MILLISECONDS, MILLISECOND_UNIT_VALUE, hourOffset, minuteOffset);
    TZTimestampValue tzMicroseconds =
        TZTimestampValue.fromPrimitives(
            MICROSECONDS, MICROSECOND_UNIT_VALUE, hourOffset, minuteOffset);
    TZTimestampValue tzNanoseconds =
        TZTimestampValue.fromPrimitives(
            NANOSECONDS, NANOSECOND_UNIT_VALUE, hourOffset, minuteOffset);

    assertEquals(
        createOffsetDateTime(SECONDS, ChronoUnit.SECONDS, offset), tzSeconds.asOffsetDateTime());
    assertEquals(
        createOffsetDateTime(MILLISECONDS, ChronoUnit.MILLIS, offset),
        tzMilliseconds.asOffsetDateTime());
    assertEquals(
        createOffsetDateTime(MICROSECONDS, ChronoUnit.MICROS, offset),
        tzMicroseconds.asOffsetDateTime());
    assertEquals(
        createOffsetDateTime(NANOSECONDS, ChronoUnit.NANOS, offset),
        tzNanoseconds.asOffsetDateTime());
  }

  @Test
  public void testUTCTimeOnlyValueAsOffsetTime() {
    UTCTimeOnlyValue utcSeconds = UTCTimeOnlyValue.fromPrimitives(SECONDS, SECOND_UNIT_VALUE);
    UTCTimeOnlyValue utcMilliseconds =
        UTCTimeOnlyValue.fromPrimitives(MILLISECONDS, MILLISECOND_UNIT_VALUE);
    UTCTimeOnlyValue utcMicroseconds =
        UTCTimeOnlyValue.fromPrimitives(MICROSECONDS, MICROSECOND_UNIT_VALUE);
    UTCTimeOnlyValue utcNanoseconds =
        UTCTimeOnlyValue.fromPrimitives(NANOSECONDS, NANOSECOND_UNIT_VALUE);

    assertEquals(
        createOffsetTime(SECONDS, ChronoUnit.SECONDS, ZoneOffset.UTC), utcSeconds.asOffsetTime());
    assertEquals(
        createOffsetTime(MILLISECONDS, ChronoUnit.MILLIS, ZoneOffset.UTC),
        utcMilliseconds.asOffsetTime());
    assertEquals(
        createOffsetTime(MICROSECONDS, ChronoUnit.MICROS, ZoneOffset.UTC),
        utcMicroseconds.asOffsetTime());
    assertEquals(
        createOffsetTime(NANOSECONDS, ChronoUnit.NANOS, ZoneOffset.UTC),
        utcNanoseconds.asOffsetTime());
  }

  @Test
  public void testTZTimeOnlyValueAsOffsetTimePositiveOffset() {
    byte hourOffset = 2;
    byte minuteOffset = 30;
    ZoneOffset offset = ZoneOffset.ofHoursMinutes(2, 30);

    TZTimeOnlyValue tzSeconds =
        TZTimeOnlyValue.fromPrimitives(SECONDS, SECOND_UNIT_VALUE, hourOffset, minuteOffset);
    TZTimeOnlyValue tzMilliseconds =
        TZTimeOnlyValue.fromPrimitives(
            MILLISECONDS, MILLISECOND_UNIT_VALUE, hourOffset, minuteOffset);
    TZTimeOnlyValue tzMicroseconds =
        TZTimeOnlyValue.fromPrimitives(
            MICROSECONDS, MICROSECOND_UNIT_VALUE, hourOffset, minuteOffset);
    TZTimeOnlyValue tzNanoseconds =
        TZTimeOnlyValue.fromPrimitives(
            NANOSECONDS, NANOSECOND_UNIT_VALUE, hourOffset, minuteOffset);

    assertEquals(createOffsetTime(SECONDS, ChronoUnit.SECONDS, offset), tzSeconds.asOffsetTime());
    assertEquals(
        createOffsetTime(MILLISECONDS, ChronoUnit.MILLIS, offset), tzMilliseconds.asOffsetTime());
    assertEquals(
        createOffsetTime(MICROSECONDS, ChronoUnit.MICROS, offset), tzMicroseconds.asOffsetTime());
    assertEquals(
        createOffsetTime(NANOSECONDS, ChronoUnit.NANOS, offset), tzNanoseconds.asOffsetTime());
  }

  @Test
  public void testTZTimeOnlyValueAsOffsetTimeNegativeOffset() {
    byte hourOffset = -2;
    byte minuteOffset = 30;
    ZoneOffset offset = ZoneOffset.ofHoursMinutes(-2, -30);

    TZTimeOnlyValue tzSeconds =
        TZTimeOnlyValue.fromPrimitives(SECONDS, SECOND_UNIT_VALUE, hourOffset, minuteOffset);
    TZTimeOnlyValue tzMilliseconds =
        TZTimeOnlyValue.fromPrimitives(
            MILLISECONDS, MILLISECOND_UNIT_VALUE, hourOffset, minuteOffset);
    TZTimeOnlyValue tzMicroseconds =
        TZTimeOnlyValue.fromPrimitives(
            MICROSECONDS, MICROSECOND_UNIT_VALUE, hourOffset, minuteOffset);
    TZTimeOnlyValue tzNanoseconds =
        TZTimeOnlyValue.fromPrimitives(
            NANOSECONDS, NANOSECOND_UNIT_VALUE, hourOffset, minuteOffset);

    assertEquals(createOffsetTime(SECONDS, ChronoUnit.SECONDS, offset), tzSeconds.asOffsetTime());
    assertEquals(
        createOffsetTime(MILLISECONDS, ChronoUnit.MILLIS, offset), tzMilliseconds.asOffsetTime());
    assertEquals(
        createOffsetTime(MICROSECONDS, ChronoUnit.MICROS, offset), tzMicroseconds.asOffsetTime());
    assertEquals(
        createOffsetTime(NANOSECONDS, ChronoUnit.NANOS, offset), tzNanoseconds.asOffsetTime());
  }

  @Test
  public void testTimeValueRowConversions() {
    UTCTimestampValue utcTimestampValue = UTCTimestampValue.fromRow(UTC_ROW);
    UTCTimeOnlyValue utcTimeOnlyValue = UTCTimeOnlyValue.fromRow(UTC_ROW);
    TZTimestampValue tzTimestampValue = TZTimestampValue.fromRow(TZ_ROW);
    TZTimeOnlyValue tzTimeOnlyValue = TZTimeOnlyValue.fromRow(TZ_ROW);

    assertEquals(UTC_ROW, utcTimestampValue.asRow());
    assertEquals(UTC_ROW, utcTimeOnlyValue.asRow());
    assertEquals(TZ_ROW, tzTimestampValue.asRow());
    assertEquals(TZ_ROW, tzTimeOnlyValue.asRow());
    assertEquals(MILLISECONDS, utcTimestampValue.time());
    assertEquals(MILLISECOND_UNIT_VALUE, utcTimestampValue.unit());
    assertEquals(MILLISECONDS, utcTimeOnlyValue.time());
    assertEquals(MILLISECOND_UNIT_VALUE, utcTimeOnlyValue.unit());
    assertEquals(MILLISECONDS, tzTimestampValue.time());
    assertEquals(MILLISECOND_UNIT_VALUE, tzTimestampValue.unit());
    assertEquals(HOUR_OFFSET, tzTimestampValue.timezoneHour());
    assertEquals(MINUTE_OFFSET, tzTimestampValue.timezoneMinute());
    assertEquals(MILLISECONDS, tzTimeOnlyValue.time());
    assertEquals(MILLISECOND_UNIT_VALUE, tzTimeOnlyValue.unit());
    assertEquals(HOUR_OFFSET, tzTimeOnlyValue.timezoneHour());
    assertEquals(MINUTE_OFFSET, tzTimeOnlyValue.timezoneMinute());
  }

  @Test
  public void testTimestampCreateRowWithInvalidRow() {
    // Note that swapping UTC/TZ is invalid.
    assertThrows(IllegalArgumentException.class, () -> UTCTimestampValue.fromRow(TZ_ROW));
    assertThrows(IllegalArgumentException.class, () -> UTCTimeOnlyValue.fromRow(TZ_ROW));
    assertThrows(IllegalArgumentException.class, () -> TZTimestampValue.fromRow(UTC_ROW));
    assertThrows(IllegalArgumentException.class, () -> TZTimeOnlyValue.fromRow(UTC_ROW));
  }

  @Test
  @SuppressWarnings("AssertBetweenInconvertibleTypes") // Part of what we're testing
  public void testTimeValueEquals() {
    UTCTimestampValue utcTimestampValue =
        UTCTimestampValue.fromPrimitives(MILLISECONDS, MILLISECOND_UNIT_VALUE);
    UTCTimeOnlyValue utcTimeOnlyValue =
        UTCTimeOnlyValue.fromPrimitives(MILLISECONDS, MILLISECOND_UNIT_VALUE);
    TZTimestampValue tzTimestampValue =
        TZTimestampValue.fromPrimitives(
            MILLISECONDS, MILLISECOND_UNIT_VALUE, HOUR_OFFSET, MINUTE_OFFSET);
    TZTimeOnlyValue tzTimeOnlyValue =
        TZTimeOnlyValue.fromPrimitives(
            MILLISECONDS, MILLISECOND_UNIT_VALUE, HOUR_OFFSET, MINUTE_OFFSET);

    // Identity
    assertEquals(utcTimestampValue, utcTimestampValue);
    assertEquals(utcTimeOnlyValue, utcTimeOnlyValue);
    assertEquals(tzTimestampValue, tzTimestampValue);
    assertEquals(tzTimeOnlyValue, tzTimeOnlyValue);
    // Same field values
    assertEquals(UTCTimestampValue.fromRow(UTC_ROW), utcTimestampValue);
    assertEquals(UTCTimeOnlyValue.fromRow(UTC_ROW), utcTimeOnlyValue);
    assertEquals(TZTimestampValue.fromRow(TZ_ROW), tzTimestampValue);
    assertEquals(TZTimeOnlyValue.fromRow(TZ_ROW), tzTimeOnlyValue);
    // Null
    assertNotEquals(null, utcTimestampValue);
    assertNotEquals(null, utcTimeOnlyValue);
    assertNotEquals(null, tzTimestampValue);
    assertNotEquals(null, tzTimeOnlyValue);
    // Different types
    assertNotEquals(utcTimestampValue, utcTimeOnlyValue);
    assertNotEquals(utcTimeOnlyValue, utcTimestampValue);
    assertNotEquals(tzTimestampValue, tzTimeOnlyValue);
    assertNotEquals(tzTimeOnlyValue, tzTimestampValue);
    // Different field value
    assertNotEquals(
        UTCTimestampValue.fromPrimitives(MILLISECONDS, SECOND_UNIT_VALUE), utcTimestampValue);
    assertNotEquals(
        UTCTimeOnlyValue.fromPrimitives(MILLISECONDS, SECOND_UNIT_VALUE), utcTimeOnlyValue);
    assertNotEquals(
        TZTimestampValue.fromPrimitives(
            MILLISECONDS, SECOND_UNIT_VALUE, HOUR_OFFSET, MINUTE_OFFSET),
        tzTimestampValue);
    assertNotEquals(
        TZTimeOnlyValue.fromPrimitives(MILLISECONDS, SECOND_UNIT_VALUE, HOUR_OFFSET, MINUTE_OFFSET),
        tzTimeOnlyValue);
  }

  @Test
  public void testTimeValueToString() {
    UTCTimestampValue utcTimestampValue = UTCTimestampValue.fromRow(UTC_ROW);
    UTCTimeOnlyValue utcTimeOnlyValue = UTCTimeOnlyValue.fromRow(UTC_ROW);
    TZTimestampValue tzTimestampValue = TZTimestampValue.fromRow(TZ_ROW);
    TZTimeOnlyValue tzTimeOnlyValue = TZTimeOnlyValue.fromRow(TZ_ROW);

    String expectedUtcTimestamp =
        createOffsetDateTime(MILLISECONDS, ChronoUnit.MILLIS, ZoneOffset.UTC).toString();
    String expectedUtcTimeOnly =
        createOffsetTime(MILLISECONDS, ChronoUnit.MILLIS, ZoneOffset.UTC).toString();
    String expectedTzTimestamp =
        createOffsetDateTime(MILLISECONDS, ChronoUnit.MILLIS, ZONE_OFFSET).toString();
    String expectedTzTimeOnly =
        createOffsetTime(MILLISECONDS, ChronoUnit.MILLIS, ZONE_OFFSET).toString();

    assertEquals(expectedUtcTimestamp, utcTimestampValue.toString());
    assertEquals(expectedUtcTimeOnly, utcTimeOnlyValue.toString());
    assertEquals(expectedTzTimestamp, tzTimestampValue.toString());
    assertEquals(expectedTzTimeOnly, tzTimeOnlyValue.toString());
  }

  /** Creates an expected {@link OffsetDateTime}. */
  private static OffsetDateTime createOffsetDateTime(
      long time, ChronoUnit unit, ZoneOffset offset) {
    return Instant.EPOCH.plus(time, unit).atOffset(offset);
  }

  /** Creates an expected {@link OffsetTime}. */
  private static OffsetTime createOffsetTime(long time, ChronoUnit unit, ZoneOffset offset) {
    return OffsetTime.of(0, 0, 0, 0, offset).plus(time, unit);
  }
}
