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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

import com.clickhouse.data.ClickHouseOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.math.BigDecimal;
import org.apache.beam.sdk.io.clickhouse.TableSchema.ColumnType;
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

  private static byte[] writtenBytes(ColumnType columnType, Object value) throws IOException {
    ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    try (ClickHouseOutputStream stream = ClickHouseOutputStream.of(bytes)) {
      ClickHouseWriter.writeValue(stream, columnType, value);
    }
    return bytes.toByteArray();
  }

  @Test
  public void writeDecimal32AsLittleEndianUnscaledInt32() throws IOException {
    // Decimal(9, 2) is stored as Int32; 1.23 has unscaled value 123 at scale 2.
    assertArrayEquals(
        new byte[] {123, 0, 0, 0}, writtenBytes(ColumnType.decimal(9, 2), new BigDecimal("1.23")));
  }

  @Test
  public void writeDecimal64ScalesValueBelowColumnScale() throws IOException {
    // Decimal(18, 4) is stored as Int64; 2 becomes 20000 = 0x4E20 ticks.
    assertArrayEquals(
        new byte[] {0x20, 0x4E, 0, 0, 0, 0, 0, 0},
        writtenBytes(ColumnType.decimal(18, 4), new BigDecimal("2")));
  }

  @Test
  public void writeDecimal128NegativeOneIsSignExtended() throws IOException {
    // Decimal(38, 0) is stored as Int128; -1 is sixteen 0xFF bytes in two's complement.
    byte[] expected = new byte[16];
    java.util.Arrays.fill(expected, (byte) 0xFF);
    assertArrayEquals(expected, writtenBytes(ColumnType.decimal(38, 0), new BigDecimal("-1")));
  }

  @Test
  public void writeDecimal256OneAtScaleTwenty() throws IOException {
    // Decimal(76, 20) is stored as Int256; 1 becomes 10^20 = 0x056BC75E2D63100000 ticks,
    // little-endian in 32 bytes.
    byte[] expected = new byte[32];
    byte[] littleEndianTicks = {0x00, 0x00, 0x10, 0x63, 0x2D, 0x5E, (byte) 0xC7, 0x6B, 0x05};
    System.arraycopy(littleEndianTicks, 0, expected, 0, littleEndianTicks.length);
    assertArrayEquals(expected, writtenBytes(ColumnType.decimal(76, 20), new BigDecimal("1")));
  }

  @Test
  public void writeDecimalTruncatesExcessFractionTowardZero() throws IOException {
    // -1.239 at scale 2 is -123.9 ticks, truncated toward zero to -123 = 0xFFFFFF85.
    assertArrayEquals(
        new byte[] {(byte) 0x85, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF},
        writtenBytes(ColumnType.decimal(9, 2), new BigDecimal("-1.239")));
  }

  @Test
  public void writeDecimalRejectsValueBeyondStorageWidth() {
    // 10^7 at scale 2 is 10^9 ticks, outside Int32's Decimal range of ±(10^9 - 1).
    assertThrows(
        IllegalArgumentException.class,
        () -> writtenBytes(ColumnType.decimal(9, 2), new BigDecimal("10000000.00")));
  }

  @Test
  public void writeNullableDecimal() throws IOException {
    ByteArrayOutputStream nullBytes = new ByteArrayOutputStream();
    try (ClickHouseOutputStream stream = ClickHouseOutputStream.of(nullBytes)) {
      ClickHouseWriter.writeNullableValue(stream, ColumnType.decimal(9, 2), null);
    }
    assertArrayEquals(new byte[] {1}, nullBytes.toByteArray());

    ByteArrayOutputStream valueBytes = new ByteArrayOutputStream();
    try (ClickHouseOutputStream stream = ClickHouseOutputStream.of(valueBytes)) {
      ClickHouseWriter.writeNullableValue(stream, ColumnType.decimal(9, 2), new BigDecimal("1.23"));
    }
    assertArrayEquals(new byte[] {0, 123, 0, 0, 0}, valueBytes.toByteArray());
  }
}
