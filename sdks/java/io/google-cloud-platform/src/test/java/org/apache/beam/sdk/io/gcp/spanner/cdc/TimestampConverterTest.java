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
package org.apache.beam.sdk.io.gcp.spanner.cdc;

import static org.junit.Assert.*;

import com.google.cloud.Timestamp;
import java.math.BigDecimal;
import org.junit.Test;

public class TimestampConverterTest {

  @Test
  public void testConvertTimestampToNanos() {
    final Timestamp timestamp = Timestamp.ofTimeSecondsAndNanos(10, 20);

    assertEquals(
        BigDecimal.valueOf(10_000_000_020L), TimestampConverter.timestampToNanos(timestamp));
  }

  @Test
  public void testConvertTimestampZeroToNanos() {
    final Timestamp timestamp = Timestamp.ofTimeSecondsAndNanos(0, 0);

    assertEquals(BigDecimal.valueOf(0L), TimestampConverter.timestampToNanos(timestamp));
  }

  @Test
  public void testConvertTimestampMinToNanos() {
    final Timestamp timestamp = Timestamp.MIN_VALUE;

    assertEquals(
        new BigDecimal("-62135596800000000000"), TimestampConverter.timestampToNanos(timestamp));
  }

  @Test
  public void testConvertTimestampMaxToNanos() {
    final Timestamp timestamp = Timestamp.MAX_VALUE;

    assertEquals(
        new BigDecimal("253402300799999999999"), TimestampConverter.timestampToNanos(timestamp));
  }

  @Test
  public void testConvertNanosToTimestamp() {
    final Timestamp timestamp = Timestamp.ofTimeSecondsAndNanos(10, 20);

    assertEquals(
        timestamp, TimestampConverter.timestampFromNanos(BigDecimal.valueOf(10_000_000_020L)));
  }

  @Test
  public void testConvertZeroNanosToTimestamp() {
    final Timestamp timestamp = Timestamp.ofTimeSecondsAndNanos(0, 0);

    assertEquals(timestamp, TimestampConverter.timestampFromNanos(BigDecimal.valueOf(0)));
  }

  @Test
  public void testConvertNanosToMinTimestamp() {
    assertEquals(
        Timestamp.MIN_VALUE,
        TimestampConverter.timestampFromNanos(new BigDecimal("-62135596800000000000")));
  }

  @Test
  public void testConvertNanosToMaxTimestamp() {
    assertEquals(
        Timestamp.MAX_VALUE,
        TimestampConverter.timestampFromNanos(new BigDecimal("253402300799999999999")));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testConvertUnderflowNanosToTimestamp() {
    TimestampConverter.timestampFromNanos(new BigDecimal("-62135596800000000001"));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testConvertOverflowNanosToTimestamp() {
    TimestampConverter.timestampFromNanos(new BigDecimal("253402300800000000000"));
  }

  @Test
  public void testConvertTimestampToMicros() {
    final Timestamp timestamp = Timestamp.ofTimeMicroseconds(2_000_360L);

    assertEquals(BigDecimal.valueOf(2_000_360L), TimestampConverter.timestampToMicros(timestamp));
  }

  @Test
  public void testConvertTimestampZeroToMicros() {
    final Timestamp timestamp = Timestamp.ofTimeMicroseconds(0);

    assertEquals(BigDecimal.valueOf(0), TimestampConverter.timestampToMicros(timestamp));
  }

  @Test
  public void testConvertTimestampMinToMicros() {
    final Timestamp timestamp = Timestamp.MIN_VALUE;

    assertEquals(
        new BigDecimal("-62135596800000000"), TimestampConverter.timestampToMicros(timestamp));
  }

  @Test
  public void testConvertTimestampMaxToMicros() {
    final Timestamp timestamp = Timestamp.MAX_VALUE;

    assertEquals(
        new BigDecimal("253402300799999999"), TimestampConverter.timestampToMicros(timestamp));
  }

  @Test
  public void testConvertMicrosToTimestamp() {
    final Timestamp timestamp = Timestamp.ofTimeMicroseconds(2_000_360L);

    assertEquals(timestamp, TimestampConverter.timestampFromMicros(BigDecimal.valueOf(2_000_360L)));
  }

  @Test
  public void testConvertZeroMicrosToTimestamp() {
    final Timestamp timestamp = Timestamp.ofTimeMicroseconds(0);

    assertEquals(timestamp, TimestampConverter.timestampFromMicros(BigDecimal.valueOf(0)));
  }

  @Test
  public void testConvertMicrosToMinTimestamp() {
    assertEquals(
        Timestamp.MIN_VALUE,
        TimestampConverter.timestampFromMicros(new BigDecimal("-62135596800000000")));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testConvertUnderflowMicrosToTimestamp() {
    TimestampConverter.timestampFromNanos(new BigDecimal("-62135596800000001"));
  }
}
