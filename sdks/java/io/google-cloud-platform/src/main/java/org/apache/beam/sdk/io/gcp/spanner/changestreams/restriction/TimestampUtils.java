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

class TimestampUtils {
  private static final BigDecimal MIN_SECONDS =
      BigDecimal.valueOf(Timestamp.MIN_VALUE.getSeconds());
  private static final long NANOS_PER_SECOND = TimeUnit.SECONDS.toNanos(1);

  static BigDecimal toNanos(Timestamp timestamp) {
    final BigDecimal secondsAsNanos =
        BigDecimal.valueOf(timestamp.getSeconds()).subtract(MIN_SECONDS).scaleByPowerOfTen(9);
    final BigDecimal nanos = BigDecimal.valueOf(timestamp.getNanos());

    return secondsAsNanos.add(nanos);
  }

  static Timestamp toTimestamp(BigDecimal bigDecimal) {
    final BigDecimal nanos = bigDecimal.remainder(BigDecimal.ONE.scaleByPowerOfTen(9));
    final BigDecimal seconds = bigDecimal.subtract(nanos).scaleByPowerOfTen(-9).add(MIN_SECONDS);

    return Timestamp.ofTimeSecondsAndNanos(seconds.longValue(), nanos.intValue());
  }

  static Timestamp next(Timestamp timestamp) {
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
}
