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
package org.apache.beam.sdk.io.gcp.spanner.changestreams;

import com.google.cloud.Timestamp;
import java.math.BigDecimal;

// TODO: javadocs
// TODO: This class probably does not belong in the cdc change
public class TimestampConverter {

  public static final long MAX_MICROS = timestampToMicros(Timestamp.MAX_VALUE);

  public static long timestampToMicros(Timestamp timestamp) {
    final BigDecimal seconds = BigDecimal.valueOf(timestamp.getSeconds());
    final BigDecimal nanos = BigDecimal.valueOf(timestamp.getNanos());
    final BigDecimal micros = nanos.scaleByPowerOfTen(-3);

    return seconds.scaleByPowerOfTen(6).add(micros).longValue();
  }

  public static Timestamp timestampFromMillis(long millis) {
    return Timestamp.ofTimeMicroseconds(millis * 1_000L);
  }

  public static Timestamp truncateNanos(Timestamp timestamp) {
    return Timestamp.ofTimeMicroseconds(timestampToMicros(timestamp));
  }
}
