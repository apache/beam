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

import static org.junit.Assert.assertEquals;

import com.google.cloud.Timestamp;
import org.junit.Test;

public class TimestampConverterTest {

  @Test
  public void testConvertTimestampToMicros() {
    final Timestamp timestamp = Timestamp.ofTimeMicroseconds(2_000_360L);

    assertEquals(2_000_360L, TimestampConverter.timestampToMicros(timestamp));
  }

  @Test
  public void testConvertTimestampZeroToMicros() {
    final Timestamp timestamp = Timestamp.ofTimeMicroseconds(0);

    assertEquals(0L, TimestampConverter.timestampToMicros(timestamp));
  }

  @Test
  public void testConvertTimestampMinToMicros() {
    final Timestamp timestamp = Timestamp.MIN_VALUE;

    assertEquals(-62135596800000000L, TimestampConverter.timestampToMicros(timestamp));
  }

  @Test
  public void testConvertTimestampMaxToMicros() {
    final Timestamp timestamp = Timestamp.MAX_VALUE;

    assertEquals(253402300799999999L, TimestampConverter.timestampToMicros(timestamp));
  }

  @Test
  public void testConvertMillisToTimestamp() {
    final Timestamp timestamp = Timestamp.ofTimeMicroseconds(1234_000L);

    assertEquals(timestamp, TimestampConverter.timestampFromMillis(1234L));
  }

  @Test
  public void testTruncateNanos() {
    final Timestamp timestamp = Timestamp.ofTimeSecondsAndNanos(10L, 123456789);
    final Timestamp expectedTimestamp = Timestamp.ofTimeSecondsAndNanos(10L, 123456000);

    assertEquals(expectedTimestamp, TimestampConverter.truncateNanos(timestamp));
  }
}
