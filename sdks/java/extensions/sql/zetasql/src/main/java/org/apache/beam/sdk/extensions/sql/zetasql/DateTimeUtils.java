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
package org.apache.beam.sdk.extensions.sql.zetasql;

import com.google.zetasql.Value;
import io.grpc.Status;
import java.time.Instant;
import java.time.LocalTime;
import java.util.List;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.avatica.util.TimeUnit;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Splitter;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Lists;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

/** DateTimeUtils. */
public class DateTimeUtils {

  public static final Long MILLIS_PER_DAY = 86400000L;
  private static final int MICROS_PER_SECOND = 1000000;
  private static final int NANOS_PER_MICRO = 1000;

  @SuppressWarnings("unchecked")
  private enum TimestampPatterns {
    TIMESTAMP_PATTERN,
    TIMESTAMP_PATTERN_SUBSECOND,
    TIMESTAMP_PATTERN_T,
    TIMESTAMP_PATTERN_SUBSECOND_T,
  }

  @SuppressWarnings("unchecked")
  private static final ImmutableMap<Enum, DateTimeFormatter> TIMESTAMP_PATTERN_WITHOUT_TZ =
      ImmutableMap.of(
          TimestampPatterns.TIMESTAMP_PATTERN, DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss"),
          TimestampPatterns.TIMESTAMP_PATTERN_SUBSECOND,
              DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss.SSS"),
          TimestampPatterns.TIMESTAMP_PATTERN_T, DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss"),
          TimestampPatterns.TIMESTAMP_PATTERN_SUBSECOND_T,
              DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss.SSS"));

  @SuppressWarnings("unchecked")
  private static final ImmutableMap<Enum, DateTimeFormatter> TIMESTAMP_PATTERN_WITH_TZ =
      ImmutableMap.of(
          TimestampPatterns.TIMESTAMP_PATTERN, DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ssZZ"),
          TimestampPatterns.TIMESTAMP_PATTERN_SUBSECOND,
              DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss.SSSZZ"),
          TimestampPatterns.TIMESTAMP_PATTERN_T,
              DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ssZZ"),
          TimestampPatterns.TIMESTAMP_PATTERN_SUBSECOND_T,
              DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss.SSSZZ"));

  public static DateTimeFormatter findDateTimePattern(String str) {
    if (str.indexOf('+') == -1) {
      return findDateTimePattern(str, TIMESTAMP_PATTERN_WITHOUT_TZ);
    } else {
      return findDateTimePattern(str, TIMESTAMP_PATTERN_WITH_TZ);
    }
  }

  @SuppressWarnings("unchecked")
  public static DateTimeFormatter findDateTimePattern(
      String str, ImmutableMap<Enum, DateTimeFormatter> patternMap) {
    if (str.indexOf('.') == -1) {
      if (str.indexOf('T') == -1) {
        return patternMap.get(TimestampPatterns.TIMESTAMP_PATTERN);
      } else {
        return patternMap.get(TimestampPatterns.TIMESTAMP_PATTERN_T);
      }
    } else {
      if (str.indexOf('T') == -1) {
        return patternMap.get(TimestampPatterns.TIMESTAMP_PATTERN_SUBSECOND);
      } else {
        return patternMap.get(TimestampPatterns.TIMESTAMP_PATTERN_SUBSECOND_T);
      }
    }
  }

  // https://cloud.google.com/bigquery/docs/reference/standard-sql/migrating-from-legacy-sql#timestamp_differences
  // 0001-01-01 00:00:00 to 9999-12-31 23:59:59.999999 UTC.
  // -62135596800000000 to 253402300799999999
  @SuppressWarnings("GoodTime")
  public static final Long MIN_UNIX_MILLIS = -62135596800000L;

  @SuppressWarnings("GoodTime")
  public static final Long MAX_UNIX_MILLIS = 253402300799999L;

  public static Instant parseTimeStampWithoutTimeZone(String str) {
    return Instant.parse(str);
  }

  public static DateTime parseTimestampWithUTCTimeZone(String str) {
    return findDateTimePattern(str).withZoneUTC().parseDateTime(str);
  }

  @SuppressWarnings("unused")
  public static DateTime parseTimestampWithLocalTimeZone(String str) {
    return findDateTimePattern(str).withZone(DateTimeZone.getDefault()).parseDateTime(str);
  }

  public static Value parseTimestampStringToValue(String timestampString) {
    Instant instant = Instant.parse(timestampString);
    return Value.createTimestampValueFromUnixMicros(
        instant.getEpochSecond() * MICROS_PER_SECOND + instant.getNano() / NANOS_PER_MICRO);
  }

  public static DateTime parseTimestampWithTimeZone(String str) {
    // for example, accept "1990-10-20 13:24:01+0730"
    if (str.indexOf('.') == -1) {
      return DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ssZ").parseDateTime(str);
    } else {
      return DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss.SSSZ").parseDateTime(str);
    }
  }

  public static String formatTimestampWithTimeZone(DateTime dt) {
    String resultWithoutZone;
    if (dt.getMillisOfSecond() == 0) {
      resultWithoutZone = dt.toString(DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss"));
    } else {
      resultWithoutZone = dt.toString(DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss.SSS"));
    }

    // ZetaSQL expects a 2-digit timezone offset (-05) if the minute part is zero, and it expects
    // a 4-digit timezone with a colon (-07:52) if the minute part is non-zero. None of the
    // variations on z,Z,ZZ,.. do this for us so we have to do it manually here.
    String zone = dt.toString(DateTimeFormat.forPattern("ZZ"));
    List<String> zoneParts = Lists.newArrayList(Splitter.on(':').limit(2).split(zone));
    if (zoneParts.size() == 2 && zoneParts.get(1).equals("00")) {
      zone = zoneParts.get(0);
    }

    return resultWithoutZone + zone;
  }

  @SuppressWarnings("unused")
  public static DateTime parseTimestampWithoutTimeZone(String str) {
    return DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss").parseDateTime(str);
  }

  public static DateTime parseDate(String str) {
    return DateTimeFormat.forPattern("yyyy-MM-dd").withZoneUTC().parseDateTime(str);
  }

  public static DateTime parseTime(String str) {
    // DateTimeFormat does not parse "08:10:10" for pattern "HH:mm:ss.SSS". In this case, '.' must
    // appear.
    if (str.indexOf('.') == -1) {
      return DateTimeFormat.forPattern("HH:mm:ss").withZoneUTC().parseDateTime(str);
    } else {
      return DateTimeFormat.forPattern("HH:mm:ss.SSS").withZoneUTC().parseDateTime(str);
    }
  }

  public static Value parseDateToValue(String dateString) {
    DateTime dateTime = parseDate(dateString);
    return Value.createDateValue((int) (dateTime.getMillis() / MILLIS_PER_DAY));
  }

  public static Value parseTimeToValue(String timeString) {
    LocalTime localTime = LocalTime.parse(timeString);
    return Value.createTimeValue(localTime);
  }

  /**
   * This function validates that Long representation of timestamp is compatible with ZetaSQL
   * timestamp values range.
   *
   * <p>Invoked via reflection. @see SqlOperators
   *
   * @param ts Timestamp to validate.
   * @return Unchanged timestamp sent for validation.
   */
  @SuppressWarnings("GoodTime")
  public static @Nullable Long validateTimestamp(@Nullable Long ts) {
    if (ts == null) {
      return null;
    }

    if ((ts < MIN_UNIX_MILLIS) || (ts > MAX_UNIX_MILLIS)) {
      throw Status.OUT_OF_RANGE
          .withDescription("Timestamp is out of valid range.")
          .asRuntimeException();
    }

    return ts;
  }

  /**
   * This function validates that interval is compatible with ZetaSQL timestamp values range.
   *
   * <p>ZetaSQL validates that if we represent interval in milliseconds, it will fit into Long.
   *
   * <p>In case of SECOND or smaller time unit, it converts timestamp to microseconds, so we need to
   * convert those to microsecond and verify that we do not cause overflow.
   *
   * <p>Invoked via reflection. @see SqlOperators
   *
   * @param arg Argument for the interval.
   * @param unit Time unit used in this interval.
   * @return Argument for the interval.
   */
  @SuppressWarnings("GoodTime")
  public static @Nullable Long validateTimeInterval(@Nullable Long arg, TimeUnit unit) {
    if (arg == null) {
      return null;
    }

    // multiplier to convert to milli or microseconds.
    long multiplier = unit.multiplier.longValue();
    switch (unit) {
      case SECOND:
      case MILLISECOND:
        multiplier *= 1000L; // Change multiplier from milliseconds to microseconds.
        break;
      default:
        break;
    }

    if ((arg > Long.MAX_VALUE / multiplier) || (arg < Long.MIN_VALUE / multiplier)) {
      throw Status.OUT_OF_RANGE
          .withDescription("Interval is out of valid range")
          .asRuntimeException();
    }

    return arg;
  }
}
