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
package org.apache.beam.sdk.io.clickhouse;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Unit tests for {@link ClickHouseWriter}. */
@RunWith(JUnit4.class)
public class ClickHouseWriterTest {

  private static final long MICROS_PER_SECOND = 1_000_000L;
  private static final long NANOS_PER_SECOND = 1_000_000_000L;

  // Shared test instant 2026-05-15T12:34:56Z; its .789012345 sub-second component exercises
  // every precision bucket.
  private static final long TEST_EPOCH_SECONDS = 1_778_848_496L;
  // Nano-of-second; the trailing 345 is not micro-aligned.
  private static final long TEST_NANOS_OF_SECOND = 789_012_345L;
  // The same sub-second component truncated to whole microseconds.
  private static final long TEST_MICROS_OF_SECOND = 789_012L;
  private static final long TEST_MICRO_ALIGNED_NANOS_OF_SECOND = TEST_MICROS_OF_SECOND * 1_000L;

  // Long.MAX_VALUE nanoseconds past the epoch: 2262-04-11T23:47:16.854775807Z, the last
  // instant representable in DateTime64(9).
  private static final long MAX_NANOS_EPOCH_SECONDS = 9_223_372_036L;
  private static final long MAX_NANOS_NANO_OF_SECOND = 854_775_807L;

  @Test
  public void encodeDateTime64MillisFromJoda() {
    DateTime jodaTs = new DateTime(2026, 5, 15, 12, 34, 56, 789, DateTimeZone.UTC);
    long expectedMillis = jodaTs.getMillis();
    assertEquals(expectedMillis, ClickHouseWriter.encodeDateTime64(jodaTs.toInstant(), 3));
  }

  @Test
  public void encodeDateTime64MicrosFromJavaInstant() {
    java.time.Instant ts =
        java.time.Instant.ofEpochSecond(TEST_EPOCH_SECONDS, TEST_MICRO_ALIGNED_NANOS_OF_SECOND);
    long expectedMicros = TEST_EPOCH_SECONDS * MICROS_PER_SECOND + TEST_MICROS_OF_SECOND;
    assertEquals(expectedMicros, ClickHouseWriter.encodeDateTime64(ts, 6));
  }

  @Test
  public void encodeDateTime64NanosFromJavaInstant() {
    // The non-micro-aligned trailing 345 must survive the encoding.
    java.time.Instant ts =
        java.time.Instant.ofEpochSecond(TEST_EPOCH_SECONDS, TEST_NANOS_OF_SECOND);
    long expectedNanos = TEST_EPOCH_SECONDS * NANOS_PER_SECOND + TEST_NANOS_OF_SECOND;
    assertEquals(expectedNanos, ClickHouseWriter.encodeDateTime64(ts, 9));
  }

  @Test
  public void encodeDateTime64Precision7TruncatesBelow100Nanos() {
    // Precision 7 means 100 ns ticks: .789012345 becomes 7890123 ticks, dropping the final 45.
    java.time.Instant ts =
        java.time.Instant.ofEpochSecond(TEST_EPOCH_SECONDS, TEST_NANOS_OF_SECOND);
    long expected = TEST_EPOCH_SECONDS * 10_000_000L + 7_890_123L;
    assertEquals(expected, ClickHouseWriter.encodeDateTime64(ts, 7));
  }

  @Test
  public void encodeDateTime64NanosTruncatesSubNanoFromJoda() {
    // Joda only carries ms precision, so encoding into nanos shifts left by 6 with no loss.
    DateTime jodaTs = new DateTime(2030, 1, 1, 0, 0, 0, 123, DateTimeZone.UTC);
    long expected = jodaTs.getMillis() * 1_000_000L;
    assertEquals(expected, ClickHouseWriter.encodeDateTime64(jodaTs.toInstant(), 9));
  }

  @Test
  public void encodeDateTime64HandlesNegativeMillisWithFloorDivision() {
    // -1ms maps to (-1s, +999ms), encoded at precision 3 should be exactly -1.
    org.joda.time.Instant jodaTs = new org.joda.time.Instant(-1L);
    assertEquals(-1L, ClickHouseWriter.encodeDateTime64(jodaTs, 3));
  }

  @Test
  public void encodeDateTime64ZeroPrecisionRoundsTowardEpochSeconds() {
    java.time.Instant ts = java.time.Instant.ofEpochSecond(42L, 999_999_999L);
    // Precision 0 means whole-second ticks; sub-second component is truncated.
    assertEquals(42L, ClickHouseWriter.encodeDateTime64(ts, 0));
  }

  @Test
  public void encodeDateTime64NanosMaxRepresentableInstant() {
    java.time.Instant ts =
        java.time.Instant.ofEpochSecond(MAX_NANOS_EPOCH_SECONDS, MAX_NANOS_NANO_OF_SECOND);
    assertEquals(Long.MAX_VALUE, ClickHouseWriter.encodeDateTime64(ts, 9));
  }

  @Test(expected = ArithmeticException.class)
  public void encodeDateTime64NanosOverflowsPastYear2262() {
    // Math.multiplyExact must fail loudly instead of silently wrapping around.
    java.time.Instant ts = java.time.Instant.ofEpochSecond(MAX_NANOS_EPOCH_SECONDS + 1, 0L);
    ClickHouseWriter.encodeDateTime64(ts, 9);
  }

  @Test(expected = ArithmeticException.class)
  public void encodeDateTime64NanosOverflowsOneNanoPastMax() {
    // One nanosecond past the last representable tick overflows in Math.addExact.
    java.time.Instant ts =
        java.time.Instant.ofEpochSecond(MAX_NANOS_EPOCH_SECONDS, MAX_NANOS_NANO_OF_SECOND + 1);
    ClickHouseWriter.encodeDateTime64(ts, 9);
  }

  @Test(expected = IllegalArgumentException.class)
  public void encodeDateTime64RejectsUnsupportedValue() {
    ClickHouseWriter.encodeDateTime64("not-a-timestamp", 3);
  }

  @Test
  public void encodeDateTime64RejectsNull() {
    IllegalArgumentException e =
        assertThrows(
            IllegalArgumentException.class, () -> ClickHouseWriter.encodeDateTime64(null, 3));
    assertEquals(
        "DateTime64 requires a Joda ReadableInstant or java.time.Instant, got null",
        e.getMessage());
  }
}
