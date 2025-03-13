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
package org.apache.beam.runners.dataflow.util;

import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Strings;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.DateTime;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.joda.time.ReadableDuration;
import org.joda.time.ReadableInstant;
import org.joda.time.chrono.ISOChronology;

/**
 * A helper class for converting between Dataflow API and SDK time representations.
 *
 * <p>Dataflow API times are strings of the form {@code YYYY-MM-dd'T'HH:mm:ss[.nnnn]'Z'}: that is,
 * RFC 3339 strings with optional fractional seconds and a 'Z' offset.
 *
 * <p>Dataflow API durations are strings of the form {@code ['-']sssss[.nnnn]'s'}: that is, seconds
 * with optional fractional seconds and a literal 's' at the end.
 *
 * <p>In both formats, fractional seconds are either three digits (millisecond resolution), six
 * digits (microsecond resolution), or nine digits (nanosecond resolution).
 */
@SuppressWarnings({
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
public final class TimeUtil {
  private TimeUtil() {} // Non-instantiable.

  private static final Pattern DURATION_PATTERN = Pattern.compile("(\\d+)(?:\\.(\\d+))?s");
  private static final Pattern TIME_PATTERN =
      Pattern.compile("(\\d{4})-(\\d{2})-(\\d{2})T(\\d{2}):(\\d{2}):(\\d{2})(?:\\.(\\d+))?Z");

  /** Converts a {@link ReadableInstant} into a Dataflow API time value. */
  public static String toCloudTime(ReadableInstant instant) {
    // Note that since Joda objects use millisecond resolution, we always
    // produce either no fractional seconds or fractional seconds with
    // millisecond resolution.

    // Translate the ReadableInstant to a DateTime with ISOChronology.
    DateTime time = new DateTime(instant);

    int millis = time.getMillisOfSecond();
    if (millis == 0) {
      return String.format(
          "%04d-%02d-%02dT%02d:%02d:%02dZ",
          time.getYear(),
          time.getMonthOfYear(),
          time.getDayOfMonth(),
          time.getHourOfDay(),
          time.getMinuteOfHour(),
          time.getSecondOfMinute());
    } else {
      return String.format(
          "%04d-%02d-%02dT%02d:%02d:%02d.%03dZ",
          time.getYear(),
          time.getMonthOfYear(),
          time.getDayOfMonth(),
          time.getHourOfDay(),
          time.getMinuteOfHour(),
          time.getSecondOfMinute(),
          millis);
    }
  }

  /**
   * Converts a time value received via the Dataflow API into the corresponding {@link Instant}.
   *
   * @return the parsed time, or null if a parse error occurs
   */
  public static @Nullable Instant fromCloudTime(String time) {
    Matcher matcher = TIME_PATTERN.matcher(time);
    if (!matcher.matches()) {
      return null;
    }
    int year = Integer.parseInt(matcher.group(1));
    int month = Integer.parseInt(matcher.group(2));
    int day = Integer.parseInt(matcher.group(3));
    int hour = Integer.parseInt(matcher.group(4));
    int minute = Integer.parseInt(matcher.group(5));
    int second = Integer.parseInt(matcher.group(6));
    int millis = computeMillis(matcher.group(7));

    return new DateTime(
            year, month, day, hour, minute, second, millis, ISOChronology.getInstanceUTC())
        .toInstant();
  }

  private static int computeMillis(String frac) {
    if (frac == null) {
      return 0;
    }
    return Integer.parseInt(
        frac.length() > 3 ? frac.substring(0, 3) : Strings.padEnd(frac, 3, '0'));
  }

  /** Converts a {@link ReadableDuration} into a Dataflow API duration string. */
  public static String toCloudDuration(ReadableDuration duration) {
    // Note that since Joda objects use millisecond resolution, we always
    // produce either no fractional seconds or fractional seconds with
    // millisecond resolution.
    long millis = duration.getMillis();
    long seconds = millis / 1000;
    millis = millis % 1000;
    if (millis == 0) {
      return String.format("%ds", seconds);
    } else {
      return String.format("%d.%03ds", seconds, millis);
    }
  }

  /**
   * Converts a Dataflow API duration string into a {@link Duration}.
   *
   * @return the parsed duration, or null if a parse error occurs
   */
  public static @Nullable Duration fromCloudDuration(String duration) {
    Matcher matcher = DURATION_PATTERN.matcher(duration);
    if (!matcher.matches()) {
      return null;
    }
    long millis = Long.parseLong(matcher.group(1)) * 1000;
    String frac = matcher.group(2);
    if (frac != null) {
      long fracs = Long.parseLong(frac);
      if (frac.length() == 3) { // millisecond resolution
        millis += fracs;
      } else if (frac.length() == 6) { // microsecond resolution
        millis += fracs / 1000;
      } else if (frac.length() == 9) { // nanosecond resolution
        millis += fracs / 1000000;
      } else {
        return null;
      }
    }
    return Duration.millis(millis);
  }
}
