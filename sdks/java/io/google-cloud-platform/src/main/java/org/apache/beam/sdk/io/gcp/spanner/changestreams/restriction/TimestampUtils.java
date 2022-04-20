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
package org.apache.beam.sdk.io.gcp.spanner.changestreams.restriction;

import com.google.cloud.Timestamp;
import java.math.BigDecimal;
import java.util.concurrent.TimeUnit;

/**
 * Provides methods in order to convert timestamp to nanoseconds representation and back. Provides
 * method to increment a given timestamp nanoseconds by 1.
 */
public class TimestampUtils {
  private static final BigDecimal MIN_SECONDS =
      BigDecimal.valueOf(Timestamp.MIN_VALUE.getSeconds());
  private static final int NANOS_PER_SECOND = (int) TimeUnit.SECONDS.toNanos(1);

  /**
   * Converts the given timestamp to respective nanoseconds representation. This method always
   * returns a value >= 0.
   *
   * <p>Since the seconds part of a timestamp can be negative (if the timestamp represents a date
   * earlier than 1970-01-01), seconds are shifted by {@link Timestamp#MIN_VALUE} seconds by adding
   * the absolute value.
   *
   * @param timestamp the timestamp to be converted
   * @return positive number of nanoseconds from the {@link Timestamp#MIN_VALUE}
   */
  public static BigDecimal toNanos(Timestamp timestamp) {
    final BigDecimal secondsAsNanos =
        BigDecimal.valueOf(timestamp.getSeconds()).subtract(MIN_SECONDS).scaleByPowerOfTen(9);
    final BigDecimal nanos = BigDecimal.valueOf(timestamp.getNanos());

    return secondsAsNanos.add(nanos);
  }

  /**
   * Converts nanoseconds to their respective timestamp. It is assumed that the seconds part
   * represent the number of seconds from the {@link Timestamp#MIN_VALUE}. Thus, when converting,
   * they are shifted back to 1970-01-01, by subtracting the given seconds by the absolute value of
   * seconds from {@link Timestamp#MIN_VALUE}.
   *
   * @param bigDecimal the nanoseconds representation of a timestamp (from {@link
   *     Timestamp#MIN_VALUE})
   * @return the converted timestamp
   */
  public static Timestamp toTimestamp(BigDecimal bigDecimal) {
    final BigDecimal nanos = bigDecimal.remainder(BigDecimal.ONE.scaleByPowerOfTen(9));
    final BigDecimal seconds = bigDecimal.subtract(nanos).scaleByPowerOfTen(-9).add(MIN_SECONDS);

    return Timestamp.ofTimeSecondsAndNanos(seconds.longValue(), nanos.intValue());
  }

  /**
   * Adds one nanosecond to the given timestamp. If the timestamp given is {@link
   * Timestamp#MAX_VALUE}, {@link Timestamp#MAX_VALUE} is returned.
   *
   * @param timestamp the timestamp to have one nanosecond added to
   * @return input timestamp + 1 nanosecond
   */
  public static Timestamp next(Timestamp timestamp) {
    if (timestamp.equals(Timestamp.MAX_VALUE)) {
      return timestamp;
    }

    final int nanos = timestamp.getNanos();
    final long seconds = timestamp.getSeconds();
    if (nanos + 1 >= NANOS_PER_SECOND) {
      return Timestamp.ofTimeSecondsAndNanos(seconds + 1, 0);
    } else {
      return Timestamp.ofTimeSecondsAndNanos(seconds, nanos + 1);
    }
  }

  public static Timestamp previous(Timestamp timestamp) {
    if (timestamp.equals(Timestamp.MIN_VALUE)) {
      return timestamp;
    }

    final int nanos = timestamp.getNanos();
    final long seconds = timestamp.getSeconds();
    if (nanos - 1 >= 0) {
      return Timestamp.ofTimeSecondsAndNanos(seconds, nanos - 1);
    } else {
      return Timestamp.ofTimeSecondsAndNanos(seconds - 1, NANOS_PER_SECOND - 1);
    }
  }
}
