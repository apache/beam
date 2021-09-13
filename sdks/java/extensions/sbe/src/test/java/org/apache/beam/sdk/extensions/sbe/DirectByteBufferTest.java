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

import static java.nio.charset.StandardCharsets.US_ASCII;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.CharBuffer;
import java.util.Arrays;
import javax.annotation.Nonnull;
import org.agrona.DirectBuffer;
import org.agrona.ExpandableArrayBuffer;
import org.apache.beam.repackaged.core.org.apache.commons.lang3.ArrayUtils;
import org.apache.beam.sdk.extensions.sbe.DirectByteBuffer.CreateMode;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.primitives.Ints;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.primitives.Longs;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.primitives.Shorts;
import org.junit.Before;
import org.junit.Test;

/** Unit tests for {@link DirectByteBuffer}. */
public final class DirectByteBufferTest {
  /** Simple implementation for {@link DirectByteBuffer}. */
  static final class TestableDirectByteBuffer extends DirectByteBuffer {
    TestableDirectByteBuffer(@Nonnull ByteBuffer buffer, CreateMode mode) {
      super(buffer, mode);
    }

    /** Returns underlying instance. */
    ByteBuffer buffer() {
      return this.buffer;
    }

    /** Returns the offset value. */
    int offset() {
      return this.offset;
    }

    /** Returns the length value. */
    int length() {
      return this.length;
    }

    @Override
    public int compareTo(DirectBuffer o) {
      throw new UnsupportedOperationException("Not testing this");
    }
  }

  // Underlying data of length 10
  private static final byte[] DATA = new byte[] {0, 1, 2, 3, 4, 5, 6, 7, 8, 9};

  // Field to use for creating the DirectByteBuffer. Wraps DATA.
  private ByteBuffer underlying;

  @Before
  public void setUp() {
    // Create buffer at start of each test, since some tests may modify relative positions.
    underlying = ByteBuffer.wrap(DATA);
  }

  @Test
  public void testConstructor_createModeCopy() {
    TestableDirectByteBuffer buffer = new TestableDirectByteBuffer(underlying, CreateMode.COPY);

    byte[] actualData = new byte[DATA.length];
    buffer.buffer().get(actualData);

    assertNotSame(underlying, buffer.buffer());
    assertArrayEquals(DATA, actualData);
    assertEquals(0, buffer.offset());
    assertEquals(DATA.length, buffer.length());
    assertEquals(underlying.capacity(), buffer.capacity());
  }

  @Test
  public void testConstructor_createModeCopyOnCustomRange() {
    int offset = 1;
    int limit = 5;
    int length = limit - offset;
    underlying.position(offset);
    underlying.limit(limit);

    TestableDirectByteBuffer buffer = new TestableDirectByteBuffer(underlying, CreateMode.COPY);
    byte[] actualData = new byte[length];
    buffer.buffer().get(actualData);

    byte[] expectedData = new byte[length];
    System.arraycopy(DATA, offset, expectedData, 0, length);

    assertNotSame(underlying, buffer.buffer());
    assertArrayEquals(expectedData, actualData);
    assertEquals(0, buffer.offset());
    assertEquals(length, buffer.length());
    assertEquals(underlying.capacity(), buffer.capacity());
  }

  @Test
  public void testConstructor_createModeCopyForcesDirectness() {
    ByteBuffer buffer = ByteBuffer.allocate(10); // Not allocated as direct

    TestableDirectByteBuffer direct = new TestableDirectByteBuffer(buffer, CreateMode.COPY);

    assertTrue(direct.buffer().isDirect());
  }

  @Test
  public void testConstructor_createModeView() {
    TestableDirectByteBuffer buffer = new TestableDirectByteBuffer(underlying, CreateMode.VIEW);

    assertSame(underlying, buffer.buffer());
    assertEquals(0, buffer.offset());
    assertEquals(DATA.length, buffer.length());
    assertEquals(underlying.capacity(), buffer.capacity());
  }

  @Test
  public void testConstructor_createModeViewOnCustomRange() {
    int offset = 1;
    int limit = 5;
    int length = limit - offset;
    underlying.position(offset);
    underlying.limit(limit);

    TestableDirectByteBuffer buffer = new TestableDirectByteBuffer(underlying, CreateMode.VIEW);

    assertSame(underlying, buffer.buffer());
    assertEquals(offset, buffer.offset());
    assertEquals(length, buffer.length());
    assertEquals(underlying.capacity(), buffer.capacity());
  }

  @Test
  @SuppressWarnings("DoNotCall") // Make sure DoNotCall methods are still throwing exceptions
  public void testWrap() {
    TestableDirectByteBuffer buffer = new TestableDirectByteBuffer(underlying, CreateMode.VIEW);

    assertThrows(UnsupportedOperationException.class, () -> buffer.wrap(DATA));
    assertThrows(UnsupportedOperationException.class, () -> buffer.wrap(DATA, 1, 4));
    assertThrows(UnsupportedOperationException.class, () -> buffer.wrap(underlying));
    assertThrows(UnsupportedOperationException.class, () -> buffer.wrap(underlying, 1, 4));
    assertThrows(UnsupportedOperationException.class, () -> buffer.wrap(buffer));
    assertThrows(UnsupportedOperationException.class, () -> buffer.wrap(buffer, 1, 4));
    assertThrows(UnsupportedOperationException.class, () -> buffer.wrap(1, 4));
  }

  @Test
  @SuppressWarnings("DoNotCall")
  public void testAddressOffset() {
    TestableDirectByteBuffer buffer = new TestableDirectByteBuffer(underlying, CreateMode.VIEW);
    assertThrows(UnsupportedOperationException.class, buffer::addressOffset);
  }

  @Test
  @SuppressWarnings("DoNotCall")
  public void testGetUnderlyingBuffer() {
    TestableDirectByteBuffer buffer = new TestableDirectByteBuffer(underlying, CreateMode.VIEW);
    assertThrows(UnsupportedOperationException.class, buffer::byteArray);
    assertThrows(UnsupportedOperationException.class, buffer::byteBuffer);
  }

  @Test
  public void testGetCopyAsArray() {
    TestableDirectByteBuffer buffer = new TestableDirectByteBuffer(underlying, CreateMode.VIEW);
    assertArrayEquals(DATA, buffer.getCopyAsArray());
  }

  @Test
  public void testGetCopyAsBuffer() {
    TestableDirectByteBuffer buffer = new TestableDirectByteBuffer(underlying, CreateMode.VIEW);

    byte[] actual = new byte[DATA.length];
    buffer.getCopyAsBuffer().get(actual);

    assertArrayEquals(DATA, actual);
  }

  @Test
  public void testCheckLimit_success() {
    TestableDirectByteBuffer buffer = new TestableDirectByteBuffer(underlying, CreateMode.VIEW);

    buffer.checkLimit(0);
    buffer.checkLimit(DATA.length);
  }

  @Test
  public void testCheckLimit_failure() {
    TestableDirectByteBuffer buffer = new TestableDirectByteBuffer(underlying, CreateMode.VIEW);

    assertThrows(IndexOutOfBoundsException.class, () -> buffer.checkLimit(-1));
    assertThrows(IndexOutOfBoundsException.class, () -> buffer.checkLimit(DATA.length + 1));
  }

  @Test
  public void testGetLong() {
    long value = 42L;
    byte[] valueBytesBigEndian = Longs.toByteArray(value);
    byte[] valueBytesLittleEndian = Longs.toByteArray(Long.reverseBytes(value));

    TestableDirectByteBuffer beBuffer =
        new TestableDirectByteBuffer(ByteBuffer.wrap(valueBytesBigEndian), CreateMode.VIEW);
    TestableDirectByteBuffer leBuffer =
        new TestableDirectByteBuffer(ByteBuffer.wrap(valueBytesLittleEndian), CreateMode.VIEW);

    assertEquals(value, beBuffer.getLong(0));
    assertEquals(value, beBuffer.getLong(0, ByteOrder.BIG_ENDIAN));
    assertEquals(value, leBuffer.getLong(0, ByteOrder.LITTLE_ENDIAN));
  }

  @Test
  public void testGetLong_offset() {
    long value = 42L;
    byte[] valueBytesBigEndian = Longs.toByteArray(value);
    byte[] valueBytesLittleEndian = Longs.toByteArray(Long.reverseBytes(value));

    TestableDirectByteBuffer beBuffer =
        new TestableDirectByteBuffer(
            createBufferWithSurroundingBytes(valueBytesBigEndian), CreateMode.VIEW);
    TestableDirectByteBuffer leBuffer =
        new TestableDirectByteBuffer(
            createBufferWithSurroundingBytes(valueBytesLittleEndian), CreateMode.VIEW);

    assertEquals(value, beBuffer.getLong(1));
    assertEquals(value, beBuffer.getLong(1, ByteOrder.BIG_ENDIAN));
    assertEquals(value, leBuffer.getLong(1, ByteOrder.LITTLE_ENDIAN));
  }

  @Test
  public void testGetLong_badOffset() {
    long value = 42L;
    TestableDirectByteBuffer buffer =
        new TestableDirectByteBuffer(ByteBuffer.wrap(Longs.toByteArray(value)), CreateMode.VIEW);

    // Indexes that are always invalid
    assertThrows(IndexOutOfBoundsException.class, () -> buffer.getLong(-1));
    assertThrows(IndexOutOfBoundsException.class, () -> buffer.getLong(Long.BYTES + 1));

    // Index that is invalid for reading a long, but smaller values are still valid
    assertThrows(IndexOutOfBoundsException.class, () -> buffer.getLong(1));
  }

  @Test
  public void testGetInt() {
    int value = 42;
    byte[] valueBytesBigEndian = Ints.toByteArray(value);
    byte[] valueBytesLittleEndian = Ints.toByteArray(Integer.reverseBytes(value));

    TestableDirectByteBuffer beBuffer =
        new TestableDirectByteBuffer(ByteBuffer.wrap(valueBytesBigEndian), CreateMode.VIEW);
    TestableDirectByteBuffer leBuffer =
        new TestableDirectByteBuffer(ByteBuffer.wrap(valueBytesLittleEndian), CreateMode.VIEW);

    assertEquals(value, beBuffer.getInt(0));
    assertEquals(value, beBuffer.getInt(0, ByteOrder.BIG_ENDIAN));
    assertEquals(value, leBuffer.getInt(0, ByteOrder.LITTLE_ENDIAN));
  }

  @Test
  public void testGetInt_offset() {
    int value = 42;
    byte[] valueBytesBigEndian = Ints.toByteArray(value);
    byte[] valueBytesLittleEndian = Ints.toByteArray(Integer.reverseBytes(value));

    TestableDirectByteBuffer beBuffer =
        new TestableDirectByteBuffer(
            createBufferWithSurroundingBytes(valueBytesBigEndian), CreateMode.VIEW);
    TestableDirectByteBuffer leBuffer =
        new TestableDirectByteBuffer(
            createBufferWithSurroundingBytes(valueBytesLittleEndian), CreateMode.VIEW);

    assertEquals(value, beBuffer.getInt(1));
    assertEquals(value, beBuffer.getInt(1, ByteOrder.BIG_ENDIAN));
    assertEquals(value, leBuffer.getInt(1, ByteOrder.LITTLE_ENDIAN));
  }

  @Test
  public void testGetInt_badOffset() {
    int value = 42;
    TestableDirectByteBuffer buffer =
        new TestableDirectByteBuffer(ByteBuffer.wrap(Ints.toByteArray(value)), CreateMode.VIEW);

    // Indexes that are always invalid
    assertThrows(IndexOutOfBoundsException.class, () -> buffer.getLong(-1));
    assertThrows(IndexOutOfBoundsException.class, () -> buffer.getLong(Integer.BYTES + 1));

    // Index that is invalid for reading an int, but smaller values are still valid
    assertThrows(IndexOutOfBoundsException.class, () -> buffer.getLong(1));
  }

  @Test
  public void testParseNaturalIntAscii() {
    int value = 42;
    byte[] strBytes = Integer.toString(value).getBytes(US_ASCII);
    TestableDirectByteBuffer buffer =
        new TestableDirectByteBuffer(ByteBuffer.wrap(strBytes), CreateMode.VIEW);
    TestableDirectByteBuffer wrapped =
        new TestableDirectByteBuffer(createBufferWithSurroundingBytes(strBytes), CreateMode.VIEW);

    assertEquals(value, buffer.parseNaturalIntAscii(0, 2));
    assertEquals(value, wrapped.parseNaturalIntAscii(1, 2));
  }

  @Test
  public void testParseNaturalIntAscii_maxNaturalInt() {
    byte[] strBytes = Integer.toString(Integer.MAX_VALUE).getBytes(US_ASCII);
    TestableDirectByteBuffer buffer =
        new TestableDirectByteBuffer(ByteBuffer.wrap(strBytes), CreateMode.VIEW);

    assertEquals(Integer.MAX_VALUE, buffer.parseNaturalIntAscii(0, strBytes.length));
  }

  @Test
  public void testParseNaturalIntAscii_overflow() {
    byte[] strBytes = Long.toString(((long) Integer.MAX_VALUE) + 1).getBytes(US_ASCII);
    TestableDirectByteBuffer buffer =
        new TestableDirectByteBuffer(ByteBuffer.wrap(strBytes), CreateMode.VIEW);

    assertThrows(ArithmeticException.class, () -> buffer.parseNaturalIntAscii(0, strBytes.length));
  }

  @Test
  public void testParseNaturalIntAscii_negative() {
    byte[] strBytes = Integer.toString(-1).getBytes(US_ASCII);
    TestableDirectByteBuffer buffer =
        new TestableDirectByteBuffer(ByteBuffer.wrap(strBytes), CreateMode.VIEW);

    assertThrows(
        NumberFormatException.class, () -> buffer.parseNaturalIntAscii(0, strBytes.length));
  }

  // The bounds in this test are valid, but the string is too long to fit in an int
  @Test
  public void testParseNaturalIntAscii_stringTooLong() {
    byte[] strBytes = (Integer.toString(Integer.MAX_VALUE) + '0').getBytes(US_ASCII);
    TestableDirectByteBuffer buffer =
        new TestableDirectByteBuffer(ByteBuffer.wrap(strBytes), CreateMode.VIEW);

    assertThrows(ArithmeticException.class, () -> buffer.parseNaturalIntAscii(0, strBytes.length));
  }

  @Test
  public void testParseNaturalIntAscii_invalidBounds() {
    byte[] strBytes = Integer.toString(42).getBytes(US_ASCII);
    TestableDirectByteBuffer buffer =
        new TestableDirectByteBuffer(ByteBuffer.wrap(strBytes), CreateMode.VIEW);

    // Invalid values relative to the start and end of buffer
    assertThrows(
        IndexOutOfBoundsException.class, () -> buffer.parseNaturalIntAscii(-1, strBytes.length));
    assertThrows(
        IndexOutOfBoundsException.class,
        () -> buffer.parseNaturalIntAscii(strBytes.length + 1, strBytes.length));
    assertThrows(
        IndexOutOfBoundsException.class, () -> buffer.parseNaturalIntAscii(0, strBytes.length + 1));

    // Reading full length from index 1 will go outside of bounds
    assertThrows(
        IndexOutOfBoundsException.class, () -> buffer.parseNaturalIntAscii(1, strBytes.length));
  }

  @Test
  public void testParseNaturalLongAscii() {
    long value = 42;
    byte[] strBytes = Long.toString(value).getBytes(US_ASCII);
    TestableDirectByteBuffer buffer =
        new TestableDirectByteBuffer(ByteBuffer.wrap(strBytes), CreateMode.VIEW);
    TestableDirectByteBuffer wrapped =
        new TestableDirectByteBuffer(createBufferWithSurroundingBytes(strBytes), CreateMode.VIEW);

    assertEquals(value, buffer.parseNaturalLongAscii(0, 2));
    assertEquals(value, wrapped.parseNaturalLongAscii(1, 2));
  }

  @Test
  public void parseNaturalLongAscii_maxNaturalLong() {
    byte[] strBytes = Long.toString(Long.MAX_VALUE).getBytes(US_ASCII);
    TestableDirectByteBuffer buffer =
        new TestableDirectByteBuffer(ByteBuffer.wrap(strBytes), CreateMode.VIEW);

    assertEquals(Long.MAX_VALUE, buffer.parseNaturalLongAscii(0, strBytes.length));
  }

  @Test
  public void parseNaturalLongAscii_overflow() {
    byte[] strBytes = Long.toString(Long.MAX_VALUE).getBytes(US_ASCII);
    int index = strBytes.length - 1;
    strBytes[index] = (byte) (strBytes[index] + 1);
    TestableDirectByteBuffer buffer =
        new TestableDirectByteBuffer(ByteBuffer.wrap(strBytes), CreateMode.VIEW);

    assertThrows(ArithmeticException.class, () -> buffer.parseNaturalLongAscii(0, strBytes.length));
  }

  @Test
  public void parseNaturalLongAscii_negative() {
    byte[] strBytes = Long.toString(-1).getBytes(US_ASCII);
    TestableDirectByteBuffer buffer =
        new TestableDirectByteBuffer(ByteBuffer.wrap(strBytes), CreateMode.VIEW);

    assertThrows(
        NumberFormatException.class, () -> buffer.parseNaturalLongAscii(0, strBytes.length));
  }

  // The bounds in this test are valid, but the string is too long to fit in a long value
  @Test
  public void parseNaturalLongAscii_stringTooLong() {
    byte[] strBytes = (Long.toString(Long.MAX_VALUE) + '0').getBytes(US_ASCII);
    TestableDirectByteBuffer buffer =
        new TestableDirectByteBuffer(ByteBuffer.wrap(strBytes), CreateMode.VIEW);

    assertThrows(ArithmeticException.class, () -> buffer.parseNaturalLongAscii(0, strBytes.length));
  }

  @Test
  public void parseNaturalLongAscii_invalidBounds() {
    byte[] strBytes = Long.toString(42).getBytes(US_ASCII);
    TestableDirectByteBuffer buffer =
        new TestableDirectByteBuffer(ByteBuffer.wrap(strBytes), CreateMode.VIEW);

    // Invalid values relative to the start and end of buffer
    assertThrows(
        IndexOutOfBoundsException.class, () -> buffer.parseNaturalLongAscii(-1, strBytes.length));
    assertThrows(
        IndexOutOfBoundsException.class,
        () -> buffer.parseNaturalLongAscii(strBytes.length + 1, strBytes.length));
    assertThrows(
        IndexOutOfBoundsException.class,
        () -> buffer.parseNaturalLongAscii(0, strBytes.length + 1));

    // Reading full length from index 1 will go outside of bounds
    assertThrows(
        IndexOutOfBoundsException.class, () -> buffer.parseNaturalLongAscii(1, strBytes.length));
  }

  @Test
  public void testParseIntAscii() {
    int value = 42;
    byte[] strBytes = Integer.toString(value).getBytes(US_ASCII);
    TestableDirectByteBuffer buffer =
        new TestableDirectByteBuffer(ByteBuffer.wrap(strBytes), CreateMode.VIEW);
    TestableDirectByteBuffer wrapped =
        new TestableDirectByteBuffer(createBufferWithSurroundingBytes(strBytes), CreateMode.VIEW);

    assertEquals(value, buffer.parseIntAscii(0, 2));
    assertEquals(value, wrapped.parseIntAscii(1, 2));
  }

  @Test
  public void testParseIntAscii_maxValue() {
    byte[] strBytes = Integer.toString(Integer.MAX_VALUE).getBytes(US_ASCII);
    TestableDirectByteBuffer buffer =
        new TestableDirectByteBuffer(ByteBuffer.wrap(strBytes), CreateMode.VIEW);

    assertEquals(Integer.MAX_VALUE, buffer.parseIntAscii(0, strBytes.length));
  }

  @Test
  public void testParseIntAscii_minValue() {
    byte[] strBytes = Integer.toString(Integer.MIN_VALUE).getBytes(US_ASCII);
    TestableDirectByteBuffer buffer =
        new TestableDirectByteBuffer(ByteBuffer.wrap(strBytes), CreateMode.VIEW);

    assertEquals(Integer.MIN_VALUE, buffer.parseIntAscii(0, strBytes.length));
  }

  @Test
  public void testParseIntAscii_overflow() {
    byte[] strBytes = Long.toString(((long) Integer.MAX_VALUE) + 1).getBytes(US_ASCII);
    TestableDirectByteBuffer buffer =
        new TestableDirectByteBuffer(ByteBuffer.wrap(strBytes), CreateMode.VIEW);

    assertThrows(ArithmeticException.class, () -> buffer.parseIntAscii(0, strBytes.length));
  }

  @Test
  public void testParseIntAscii_underflow() {
    byte[] strBytes = Long.toString(((long) Integer.MIN_VALUE) - 1).getBytes(US_ASCII);
    TestableDirectByteBuffer buffer =
        new TestableDirectByteBuffer(ByteBuffer.wrap(strBytes), CreateMode.VIEW);

    assertThrows(ArithmeticException.class, () -> buffer.parseIntAscii(0, strBytes.length));
  }

  // The bounds in this test are valid, but the string is too long to fit in an int
  @Test
  public void testParseIntAscii_stringTooLong() {
    byte[] positiveBytes = (Integer.toString(Integer.MAX_VALUE) + '0').getBytes(US_ASCII);
    byte[] negativeBytes = (Integer.toString(Integer.MIN_VALUE) + '0').getBytes(US_ASCII);
    TestableDirectByteBuffer positive =
        new TestableDirectByteBuffer(ByteBuffer.wrap(positiveBytes), CreateMode.VIEW);
    TestableDirectByteBuffer negative =
        new TestableDirectByteBuffer(ByteBuffer.wrap(negativeBytes), CreateMode.VIEW);

    assertThrows(ArithmeticException.class, () -> positive.parseIntAscii(0, positiveBytes.length));
    assertThrows(ArithmeticException.class, () -> negative.parseIntAscii(0, negativeBytes.length));
  }

  @Test
  public void testParseIntAscii_invalidBounds() {
    byte[] strBytes = Integer.toString(42).getBytes(US_ASCII);
    TestableDirectByteBuffer buffer =
        new TestableDirectByteBuffer(ByteBuffer.wrap(strBytes), CreateMode.VIEW);

    // Invalid values relative to the start and end of buffer
    assertThrows(IndexOutOfBoundsException.class, () -> buffer.parseIntAscii(-1, strBytes.length));
    assertThrows(
        IndexOutOfBoundsException.class,
        () -> buffer.parseIntAscii(strBytes.length + 1, strBytes.length));
    assertThrows(
        IndexOutOfBoundsException.class, () -> buffer.parseIntAscii(0, strBytes.length + 1));

    // Reading full length from index 1 will go outside of bounds
    assertThrows(IndexOutOfBoundsException.class, () -> buffer.parseIntAscii(1, strBytes.length));
  }

  @Test
  public void testParseIntAscii_lengthTooShortForNegative() {
    int value = -4;
    byte[] strBytes = Integer.toString(value).getBytes(US_ASCII);
    TestableDirectByteBuffer buffer =
        new TestableDirectByteBuffer(ByteBuffer.wrap(strBytes), CreateMode.VIEW);

    // Length of 1 means only a negative sign will be read, which is bad
    assertEquals(value, buffer.parseIntAscii(0, 2));
    assertThrows(IllegalArgumentException.class, () -> buffer.parseIntAscii(0, 1));
  }

  @Test
  public void testParseLongAscii() {
    long value = 42;
    byte[] strBytes = Long.toString(value).getBytes(US_ASCII);
    TestableDirectByteBuffer buffer =
        new TestableDirectByteBuffer(ByteBuffer.wrap(strBytes), CreateMode.VIEW);
    TestableDirectByteBuffer wrapped =
        new TestableDirectByteBuffer(createBufferWithSurroundingBytes(strBytes), CreateMode.VIEW);

    assertEquals(value, buffer.parseLongAscii(0, 2));
    assertEquals(value, wrapped.parseLongAscii(1, 2));
  }

  @Test
  public void testParseLongAscii_maxValue() {
    byte[] strBytes = Long.toString(Long.MAX_VALUE).getBytes(US_ASCII);
    TestableDirectByteBuffer buffer =
        new TestableDirectByteBuffer(ByteBuffer.wrap(strBytes), CreateMode.VIEW);

    assertEquals(Long.MAX_VALUE, buffer.parseLongAscii(0, strBytes.length));
  }

  @Test
  public void testParseLongAscii_minValue() {
    byte[] strBytes = Long.toString(Long.MIN_VALUE).getBytes(US_ASCII);
    TestableDirectByteBuffer buffer =
        new TestableDirectByteBuffer(ByteBuffer.wrap(strBytes), CreateMode.VIEW);

    assertEquals(Long.MIN_VALUE, buffer.parseLongAscii(0, strBytes.length));
  }

  @Test
  public void testParseLongAscii_overflow() {
    byte[] strBytes = Long.toString(Long.MAX_VALUE).getBytes(US_ASCII);
    int index = strBytes.length - 1;
    strBytes[index] = (byte) (strBytes[index] + 1);
    TestableDirectByteBuffer buffer =
        new TestableDirectByteBuffer(ByteBuffer.wrap(strBytes), CreateMode.VIEW);

    assertThrows(ArithmeticException.class, () -> buffer.parseLongAscii(0, strBytes.length));
  }

  @Test
  public void testParseLongAscii_underflow() {
    byte[] strBytes = Long.toString(Long.MIN_VALUE).getBytes(US_ASCII);
    int index = strBytes.length - 1;
    strBytes[index] = (byte) (strBytes[index] + 1);
    TestableDirectByteBuffer buffer =
        new TestableDirectByteBuffer(ByteBuffer.wrap(strBytes), CreateMode.VIEW);

    assertThrows(ArithmeticException.class, () -> buffer.parseLongAscii(0, strBytes.length));
  }

  // The bounds in this test are valid, but the string is too long to fit in a long value
  @Test
  public void testParseLongAscii_stringTooLong() {
    byte[] strBytes = (Long.toString(Long.MAX_VALUE) + '0').getBytes(US_ASCII);
    TestableDirectByteBuffer buffer =
        new TestableDirectByteBuffer(ByteBuffer.wrap(strBytes), CreateMode.VIEW);

    assertThrows(ArithmeticException.class, () -> buffer.parseLongAscii(0, strBytes.length));
  }

  @Test
  public void testParseLongAscii_invalidBounds() {
    byte[] strBytes = Long.toString(42).getBytes(US_ASCII);
    TestableDirectByteBuffer buffer =
        new TestableDirectByteBuffer(ByteBuffer.wrap(strBytes), CreateMode.VIEW);

    // Invalid values relative to the start and end of buffer
    assertThrows(IndexOutOfBoundsException.class, () -> buffer.parseLongAscii(-1, strBytes.length));
    assertThrows(
        IndexOutOfBoundsException.class,
        () -> buffer.parseLongAscii(strBytes.length + 1, strBytes.length));
    assertThrows(
        IndexOutOfBoundsException.class, () -> buffer.parseLongAscii(0, strBytes.length + 1));

    // Reading full length from index 1 will go outside of bounds
    assertThrows(IndexOutOfBoundsException.class, () -> buffer.parseLongAscii(1, strBytes.length));
  }

  @Test
  public void testParseLongAscii_lengthTooShortForNegative() {
    long value = -4L;
    byte[] strBytes = Long.toString(value).getBytes(US_ASCII);
    TestableDirectByteBuffer buffer =
        new TestableDirectByteBuffer(ByteBuffer.wrap(strBytes), CreateMode.VIEW);

    // Length of 1 means only a negative sign will be read, which is bad
    assertEquals(value, buffer.parseLongAscii(0, 2));
    assertThrows(IllegalArgumentException.class, () -> buffer.parseLongAscii(0, 1));
  }

  @Test
  public void testGetDouble() {
    double value = 42.2;
    byte[] valueBytesBigEndian = getDoubleBytes(value, /* reverse= */ false);
    byte[] valueBytesLittleEndian = getDoubleBytes(value, /* reverse= */ true);

    TestableDirectByteBuffer beBuffer =
        new TestableDirectByteBuffer(ByteBuffer.wrap(valueBytesBigEndian), CreateMode.VIEW);
    TestableDirectByteBuffer leBuffer =
        new TestableDirectByteBuffer(ByteBuffer.wrap(valueBytesLittleEndian), CreateMode.VIEW);

    assertEquals(value, beBuffer.getDouble(0), 0);
    assertEquals(value, beBuffer.getDouble(0, ByteOrder.BIG_ENDIAN), 0);
    assertEquals(value, leBuffer.getDouble(0, ByteOrder.LITTLE_ENDIAN), 0);
  }

  @Test
  public void testGetDouble_offset() {
    double value = 42.2;
    byte[] valueBytesBigEndian = getDoubleBytes(value, /* reverse= */ false);
    byte[] valueBytesLittleEndian = getDoubleBytes(value, /* reverse= */ true);

    TestableDirectByteBuffer beBuffer =
        new TestableDirectByteBuffer(
            createBufferWithSurroundingBytes(valueBytesBigEndian), CreateMode.VIEW);
    TestableDirectByteBuffer leBuffer =
        new TestableDirectByteBuffer(
            createBufferWithSurroundingBytes(valueBytesLittleEndian), CreateMode.VIEW);

    assertEquals(value, beBuffer.getDouble(1), 0);
    assertEquals(value, beBuffer.getDouble(1, ByteOrder.BIG_ENDIAN), 0);
    assertEquals(value, leBuffer.getDouble(1, ByteOrder.LITTLE_ENDIAN), 0);
  }

  @Test
  public void testGetDouble_badOffset() {
    double value = 42.2;
    TestableDirectByteBuffer buffer =
        new TestableDirectByteBuffer(
            ByteBuffer.wrap(getDoubleBytes(value, /* reverse= */ false)), CreateMode.VIEW);

    // Indexes that are always invalid
    assertThrows(IndexOutOfBoundsException.class, () -> buffer.getDouble(-1));
    assertThrows(IndexOutOfBoundsException.class, () -> buffer.getDouble(Double.BYTES + 1));

    // Index that is invalid for reading a double, but smaller values are still valid
    assertThrows(IndexOutOfBoundsException.class, () -> buffer.getDouble(1));
  }

  @Test
  public void testGetFloat() {
    float value = 42.2F;
    byte[] valueBytesBigEndian = getFloatBytes(value, /* reverse= */ false);
    byte[] valueBytesLittleEndian = getFloatBytes(value, /* reverse= */ true);

    TestableDirectByteBuffer beBuffer =
        new TestableDirectByteBuffer(ByteBuffer.wrap(valueBytesBigEndian), CreateMode.VIEW);
    TestableDirectByteBuffer leBuffer =
        new TestableDirectByteBuffer(ByteBuffer.wrap(valueBytesLittleEndian), CreateMode.VIEW);

    assertEquals(value, beBuffer.getFloat(0), 0);
    assertEquals(value, beBuffer.getFloat(0, ByteOrder.BIG_ENDIAN), 0);
    assertEquals(value, leBuffer.getFloat(0, ByteOrder.LITTLE_ENDIAN), 0);
  }

  @Test
  public void testGetFloat_offset() {
    float value = 42.2F;
    byte[] valueBytesBigEndian = getFloatBytes(value, /* reverse= */ false);
    byte[] valueBytesLittleEndian = getFloatBytes(value, /* reverse= */ true);

    TestableDirectByteBuffer beBuffer =
        new TestableDirectByteBuffer(
            createBufferWithSurroundingBytes(valueBytesBigEndian), CreateMode.VIEW);
    TestableDirectByteBuffer leBuffer =
        new TestableDirectByteBuffer(
            createBufferWithSurroundingBytes(valueBytesLittleEndian), CreateMode.VIEW);

    assertEquals(value, beBuffer.getFloat(1), 0);
    assertEquals(value, beBuffer.getFloat(1, ByteOrder.BIG_ENDIAN), 0);
    assertEquals(value, leBuffer.getFloat(1, ByteOrder.LITTLE_ENDIAN), 0);
  }

  @Test
  public void testGetFloat_badOffset() {
    float value = 42.2F;
    TestableDirectByteBuffer buffer =
        new TestableDirectByteBuffer(
            ByteBuffer.wrap(getFloatBytes(value, /* reverse= */ false)), CreateMode.VIEW);

    // Indexes that are always invalid
    assertThrows(IndexOutOfBoundsException.class, () -> buffer.getFloat(-1));
    assertThrows(IndexOutOfBoundsException.class, () -> buffer.getFloat(Float.BYTES + 1));

    // Index that is invalid for reading a float, but smaller values are still valid
    assertThrows(IndexOutOfBoundsException.class, () -> buffer.getFloat(1));
  }

  @Test
  public void testGetShort() {
    short value = 42;
    byte[] valueBytesBigEndian = Shorts.toByteArray(value);
    byte[] valueBytesLittleEndian = Shorts.toByteArray(Short.reverseBytes(value));

    TestableDirectByteBuffer beBuffer =
        new TestableDirectByteBuffer(ByteBuffer.wrap(valueBytesBigEndian), CreateMode.VIEW);
    TestableDirectByteBuffer leBuffer =
        new TestableDirectByteBuffer(ByteBuffer.wrap(valueBytesLittleEndian), CreateMode.VIEW);

    assertEquals(value, beBuffer.getShort(0));
    assertEquals(value, beBuffer.getShort(0, ByteOrder.BIG_ENDIAN));
    assertEquals(value, leBuffer.getShort(0, ByteOrder.LITTLE_ENDIAN));
  }

  @Test
  public void testGetShort_offset() {
    short value = 42;
    byte[] valueBytesBigEndian = Shorts.toByteArray(value);
    byte[] valueBytesLittleEndian = Shorts.toByteArray(Short.reverseBytes(value));

    TestableDirectByteBuffer beBuffer =
        new TestableDirectByteBuffer(
            createBufferWithSurroundingBytes(valueBytesBigEndian), CreateMode.VIEW);
    TestableDirectByteBuffer leBuffer =
        new TestableDirectByteBuffer(
            createBufferWithSurroundingBytes(valueBytesLittleEndian), CreateMode.VIEW);

    assertEquals(value, beBuffer.getShort(1));
    assertEquals(value, beBuffer.getShort(1, ByteOrder.BIG_ENDIAN));
    assertEquals(value, leBuffer.getShort(1, ByteOrder.LITTLE_ENDIAN));
  }

  @Test
  public void testGetShort_badOffset() {
    byte[] shortBytes = Shorts.toByteArray((short) 42);
    TestableDirectByteBuffer buffer =
        new TestableDirectByteBuffer(ByteBuffer.wrap(shortBytes), CreateMode.VIEW);

    // Indexes that are always invalid
    assertThrows(IndexOutOfBoundsException.class, () -> buffer.getShort(-1));
    assertThrows(IndexOutOfBoundsException.class, () -> buffer.getShort(Float.BYTES + 1));

    // Index that is invalid for reading a short, but smaller values are still valid
    assertThrows(IndexOutOfBoundsException.class, () -> buffer.getShort(1));
  }

  @Test
  public void testGetChar() {
    char value = 'c';
    byte[] valueBytesBigEndian = getCharBytes(value, /* reverse= */ false);
    byte[] valueBytesLittleEndian = getCharBytes(value, /* reverse= */ true);

    TestableDirectByteBuffer beBuffer =
        new TestableDirectByteBuffer(ByteBuffer.wrap(valueBytesBigEndian), CreateMode.VIEW);
    TestableDirectByteBuffer leBuffer =
        new TestableDirectByteBuffer(ByteBuffer.wrap(valueBytesLittleEndian), CreateMode.VIEW);

    assertEquals(value, beBuffer.getChar(0));
    assertEquals(value, beBuffer.getChar(0, ByteOrder.BIG_ENDIAN));
    assertEquals(value, leBuffer.getChar(0, ByteOrder.LITTLE_ENDIAN));
  }

  @Test
  public void testGetChar_offset() {
    char value = 'c';
    byte[] valueBytesBigEndian = getCharBytes(value, /* reverse= */ false);
    byte[] valueBytesLittleEndian = getCharBytes(value, /* reverse= */ true);

    TestableDirectByteBuffer beBuffer =
        new TestableDirectByteBuffer(
            createBufferWithSurroundingBytes(valueBytesBigEndian), CreateMode.VIEW);
    TestableDirectByteBuffer leBuffer =
        new TestableDirectByteBuffer(
            createBufferWithSurroundingBytes(valueBytesLittleEndian), CreateMode.VIEW);

    assertEquals(value, beBuffer.getChar(1));
    assertEquals(value, beBuffer.getChar(1, ByteOrder.BIG_ENDIAN));
    assertEquals(value, leBuffer.getChar(1, ByteOrder.LITTLE_ENDIAN));
  }

  @Test
  public void testGetChar_badOffset() {
    TestableDirectByteBuffer buffer =
        new TestableDirectByteBuffer(
            ByteBuffer.wrap(getCharBytes('c', /* reverse= */ false)), CreateMode.VIEW);

    // Indexes that are always invalid
    assertThrows(IndexOutOfBoundsException.class, () -> buffer.getChar(-1));
    assertThrows(IndexOutOfBoundsException.class, () -> buffer.getChar(Character.BYTES + 1));

    // Index that is invalid for reading a char, but smaller values are still valid
    assertThrows(IndexOutOfBoundsException.class, () -> buffer.getChar(1));
  }

  @Test
  public void testGetByte() {
    byte value = 42;
    TestableDirectByteBuffer buffer =
        new TestableDirectByteBuffer(ByteBuffer.wrap(new byte[] {value}), CreateMode.VIEW);
    assertEquals(value, buffer.getByte(0));
  }

  @Test
  public void testGetByte_offset() {
    byte value = 42;
    TestableDirectByteBuffer buffer =
        new TestableDirectByteBuffer(
            createBufferWithSurroundingBytes(new byte[] {value}), CreateMode.VIEW);
    assertEquals(value, buffer.getByte(1));
  }

  @Test
  public void testGetByte_badOffset() {
    TestableDirectByteBuffer buffer =
        new TestableDirectByteBuffer(ByteBuffer.wrap(new byte[] {42}), CreateMode.VIEW);
    assertThrows(IndexOutOfBoundsException.class, () -> buffer.getByte(-1));
    assertThrows(IndexOutOfBoundsException.class, () -> buffer.getByte(1));
  }

  @Test
  public void testGetBytes() {
    TestableDirectByteBuffer buffer = new TestableDirectByteBuffer(underlying, CreateMode.VIEW);

    byte[] actual = new byte[DATA.length];
    buffer.getBytes(0, actual);

    assertArrayEquals(DATA, actual);
  }

  @Test
  public void testGetBytes_offset() {
    TestableDirectByteBuffer buffer =
        new TestableDirectByteBuffer(createBufferWithSurroundingBytes(DATA), CreateMode.VIEW);

    byte[] actual = new byte[DATA.length];
    buffer.getBytes(1, actual);

    assertArrayEquals(DATA, actual);
  }

  @Test
  public void testGetBytes_shorterArray() {
    int newLength = DATA.length / 2;
    TestableDirectByteBuffer buffer = new TestableDirectByteBuffer(underlying, CreateMode.VIEW);

    byte[] actual = new byte[newLength];
    buffer.getBytes(0, actual);

    byte[] expected = Arrays.copyOf(DATA, newLength);

    assertArrayEquals(expected, actual);
  }

  @Test
  public void testGetBytes_arrayTooLong() {
    byte[] tooLong = new byte[DATA.length + 1];
    TestableDirectByteBuffer buffer = new TestableDirectByteBuffer(underlying, CreateMode.VIEW);

    assertThrows(IndexOutOfBoundsException.class, () -> buffer.getBytes(0, tooLong));
  }

  @Test
  public void testGetBytes_badOffset() {
    byte[] target = new byte[DATA.length];
    TestableDirectByteBuffer buffer = new TestableDirectByteBuffer(underlying, CreateMode.VIEW);

    assertThrows(IndexOutOfBoundsException.class, () -> buffer.getBytes(-1, target));
    assertThrows(IndexOutOfBoundsException.class, () -> buffer.getBytes(DATA.length, target));
  }

  @Test
  public void testGetBytes_customSourceRange() {
    int sourceIndex = 1;
    int dstOffset = 0;
    int length = 4;
    TestableDirectByteBuffer buffer = new TestableDirectByteBuffer(underlying, CreateMode.VIEW);

    byte[] actual = new byte[length];
    buffer.getBytes(sourceIndex, actual, dstOffset, length);

    byte[] expected = new byte[length];
    System.arraycopy(DATA, sourceIndex, expected, dstOffset, length);

    assertArrayEquals(expected, actual);
  }

  @Test
  public void testGetBytes_customDstRange() {
    int sourceIndex = 0;
    int dstOffset = 1;
    int length = 4;
    int totalLength = length + dstOffset;
    TestableDirectByteBuffer buffer = new TestableDirectByteBuffer(underlying, CreateMode.VIEW);

    byte[] actual = new byte[totalLength];
    buffer.getBytes(sourceIndex, actual, dstOffset, length);

    byte[] expected = new byte[totalLength];
    System.arraycopy(DATA, sourceIndex, expected, dstOffset, length);

    assertArrayEquals(expected, actual);
  }

  @Test
  public void testGetBytes_customSourceAndDstRanges() {
    int sourceIndex = 1;
    int dstOffset = 1;
    int length = 4;
    int totalLength = length + dstOffset;
    TestableDirectByteBuffer buffer = new TestableDirectByteBuffer(underlying, CreateMode.VIEW);

    byte[] actual = new byte[totalLength];
    buffer.getBytes(sourceIndex, actual, dstOffset, length);

    byte[] expected = new byte[totalLength];
    System.arraycopy(DATA, sourceIndex, expected, dstOffset, length);

    assertArrayEquals(expected, actual);
  }

  @Test
  public void testGetBytes_badBounds() {
    int length = DATA.length - 2;
    byte[] dst = new byte[length];
    TestableDirectByteBuffer buffer = new TestableDirectByteBuffer(underlying, CreateMode.VIEW);

    assertThrows(IndexOutOfBoundsException.class, () -> buffer.getBytes(-1, dst, 0, length));
    assertThrows(IndexOutOfBoundsException.class, () -> buffer.getBytes(DATA.length, dst, 0, 1));
    assertThrows(IndexOutOfBoundsException.class, () -> buffer.getBytes(0, dst, -1, length));
    assertThrows(IndexOutOfBoundsException.class, () -> buffer.getBytes(0, dst, length, 1));
    assertThrows(IndexOutOfBoundsException.class, () -> buffer.getBytes(0, dst, 1, length));
  }

  @Test
  public void testGetBytes_mutableTarget() {
    int sourceIndex = 1;
    int dstOffset = 1;
    int length = 4;
    int totalLength = length + dstOffset;
    TestableDirectByteBuffer buffer = new TestableDirectByteBuffer(underlying, CreateMode.VIEW);

    ExpandableArrayBuffer actual = new ExpandableArrayBuffer();
    buffer.getBytes(sourceIndex, actual, dstOffset, length);
    byte[] actualArray = new byte[totalLength];
    actual.getBytes(0, actualArray); // Underlying array may be sized different

    byte[] expected = new byte[totalLength];
    System.arraycopy(DATA, sourceIndex, expected, dstOffset, length);

    assertArrayEquals(expected, actualArray);
  }

  // Skipping bounds checking on MutableDirectBuffer, since it will come down in part to the
  // implementation provided.

  @Test
  public void testGetBytes_byteBuffer() {
    int partialLength = DATA.length / 2;
    ByteBuffer full = ByteBuffer.wrap(new byte[DATA.length]);
    ByteBuffer partial = ByteBuffer.wrap(new byte[partialLength]);

    TestableDirectByteBuffer buffer = new TestableDirectByteBuffer(underlying, CreateMode.VIEW);
    buffer.getBytes(0, full, DATA.length);
    buffer.getBytes(0, partial, partialLength);

    assertArrayEquals(DATA, full.array());
    assertArrayEquals(Arrays.copyOf(DATA, partialLength), partial.array());
  }

  @Test
  public void testGetBytes_byteBufferSourceOffset() {
    int partialLength = DATA.length / 2;
    ByteBuffer full = ByteBuffer.wrap(new byte[DATA.length]);
    ByteBuffer partial = ByteBuffer.wrap(new byte[partialLength]);

    TestableDirectByteBuffer buffer =
        new TestableDirectByteBuffer(createBufferWithSurroundingBytes(DATA), CreateMode.VIEW);
    buffer.getBytes(1, full, DATA.length);
    buffer.getBytes(1, partial, partialLength);

    assertArrayEquals(DATA, full.array());
    assertArrayEquals(Arrays.copyOf(DATA, partialLength), partial.array());
  }

  @Test
  public void testGetBytes_badByteBufferBounds() {
    int allocate = 10;
    TestableDirectByteBuffer buffer = new TestableDirectByteBuffer(underlying, CreateMode.VIEW);

    assertThrows(
        IndexOutOfBoundsException.class,
        () -> buffer.getBytes(-1, ByteBuffer.allocate(allocate), allocate));
    assertThrows(
        IndexOutOfBoundsException.class,
        () -> buffer.getBytes(allocate, ByteBuffer.allocate(allocate), 1));
    assertThrows(
        IndexOutOfBoundsException.class,
        () -> buffer.getBytes(1, ByteBuffer.allocate(allocate), allocate));
  }

  @Test
  public void testGetBytes_byteBufferFullyCustomRange() {
    int sourceIndex = 1;
    int dstOffset = 1;
    int length = 4;
    int totalLength = length + dstOffset + 1;
    TestableDirectByteBuffer buffer = new TestableDirectByteBuffer(underlying, CreateMode.VIEW);

    ByteBuffer dst = ByteBuffer.wrap(new byte[totalLength]);
    buffer.getBytes(sourceIndex, dst, dstOffset, length);

    byte[] expected = new byte[totalLength];
    System.arraycopy(DATA, sourceIndex, expected, dstOffset, length);

    assertArrayEquals(expected, dst.array());
  }

  @Test
  public void testGetBytes_byteBufferBadBounds() {
    int length = DATA.length - 2;
    ByteBuffer dst = ByteBuffer.allocate(length);
    TestableDirectByteBuffer buffer = new TestableDirectByteBuffer(underlying, CreateMode.VIEW);

    assertThrows(IndexOutOfBoundsException.class, () -> buffer.getBytes(-1, dst, 0, length));
    assertThrows(IndexOutOfBoundsException.class, () -> buffer.getBytes(DATA.length, dst, 0, 1));
    assertThrows(IndexOutOfBoundsException.class, () -> buffer.getBytes(0, dst, -1, length));
    assertThrows(IndexOutOfBoundsException.class, () -> buffer.getBytes(0, dst, length, 1));
    assertThrows(IndexOutOfBoundsException.class, () -> buffer.getBytes(0, dst, 1, length));
  }

  @Test
  public void testGetStringAscii() {
    String value = "This statement is false.";
    TestableDirectByteBuffer buffer =
        new TestableDirectByteBuffer(
            ByteBuffer.wrap(getLengthPrefixedAsciiStringBytes(value, /* reverseLength= */ false)),
            CreateMode.VIEW);
    TestableDirectByteBuffer reversed =
        new TestableDirectByteBuffer(
            ByteBuffer.wrap(getLengthPrefixedAsciiStringBytes(value, /* reverseLength= */ true)),
            CreateMode.VIEW);

    assertEquals(value, buffer.getStringAscii(0));
    assertEquals(value, buffer.getStringAscii(0, ByteOrder.BIG_ENDIAN));
    assertEquals(value, reversed.getStringAscii(0, ByteOrder.LITTLE_ENDIAN));
  }

  @Test
  public void testGetStringAscii_wrapped() {
    String value = "This statement is false.";
    byte[] rawBytes = getLengthPrefixedAsciiStringBytes(value, /* reverseLength= */ false);
    byte[] reversedBytes = getLengthPrefixedAsciiStringBytes(value, /* reverseLength= */ true);
    TestableDirectByteBuffer buffer =
        new TestableDirectByteBuffer(createBufferWithSurroundingBytes(rawBytes), CreateMode.VIEW);
    TestableDirectByteBuffer reversed =
        new TestableDirectByteBuffer(
            createBufferWithSurroundingBytes(reversedBytes), CreateMode.VIEW);

    assertEquals(value, buffer.getStringAscii(1));
    assertEquals(value, buffer.getStringAscii(1, ByteOrder.BIG_ENDIAN));
    assertEquals(value, reversed.getStringAscii(1, ByteOrder.LITTLE_ENDIAN));
  }

  @Test
  public void testGetStringAscii_overrideLength() {
    String value = "This statement is false.";
    int customLength = 3;
    TestableDirectByteBuffer buffer =
        new TestableDirectByteBuffer(
            ByteBuffer.wrap(getLengthPrefixedAsciiStringBytes(value, /* reverseLength= */ false)),
            CreateMode.VIEW);
    // Reverse unnecessary, since the length prefix is skipped

    String expected = value.substring(0, customLength);

    assertEquals(expected, buffer.getStringAscii(0, customLength));
  }

  @Test
  public void testGetStringAscii_wrappedWithCustomLength() {
    String value = "This statement is false.";
    byte[] rawBytes = getLengthPrefixedAsciiStringBytes(value, /* reverseLength= */ false);
    int customLength = 3;
    TestableDirectByteBuffer buffer =
        new TestableDirectByteBuffer(createBufferWithSurroundingBytes(rawBytes), CreateMode.VIEW);
    // Reverse unnecessary, since the length prefix is skipped

    String expected = value.substring(0, customLength);

    assertEquals(expected, buffer.getStringAscii(1, customLength));
  }

  @Test
  public void testGetStringAscii_badBounds() {
    TestableDirectByteBuffer buffer = new TestableDirectByteBuffer(underlying, CreateMode.VIEW);

    assertThrows(IndexOutOfBoundsException.class, () -> buffer.getStringAscii(-1));
    // The integer read in will be 123, which will way exceed length when trying to read string
    assertThrows(IndexOutOfBoundsException.class, () -> buffer.getStringAscii(0));
    assertThrows(IndexOutOfBoundsException.class, () -> buffer.getStringAscii(DATA.length - 3));
    assertThrows(IndexOutOfBoundsException.class, () -> buffer.getStringAscii(-1, DATA.length));
    // Read starts from index 4 and will read 10, but there's only ten bytes
    assertThrows(IndexOutOfBoundsException.class, () -> buffer.getStringAscii(0, DATA.length));
    assertThrows(IndexOutOfBoundsException.class, () -> buffer.getStringAscii(DATA.length - 3, 1));
  }

  @Test
  public void testGetStringAscii_appendable() {
    String value = "This statement is false.";
    byte[] rawBytes = getLengthPrefixedAsciiStringBytes(value, /* reverseLength= */ false);
    byte[] reversedBytes = getLengthPrefixedAsciiStringBytes(value, /* reverseLength= */ true);
    TestableDirectByteBuffer buffer =
        new TestableDirectByteBuffer(ByteBuffer.wrap(rawBytes), CreateMode.VIEW);
    TestableDirectByteBuffer reversed =
        new TestableDirectByteBuffer(ByteBuffer.wrap(reversedBytes), CreateMode.VIEW);

    CharBuffer beDestination = CharBuffer.allocate(rawBytes.length);
    CharBuffer leDestination = CharBuffer.allocate(reversed.length);

    buffer.getStringAscii(0, beDestination);
    reversed.getStringAscii(0, leDestination, ByteOrder.LITTLE_ENDIAN);

    // Reposition for toString to return contents
    beDestination.position(0);
    leDestination.position(0);

    // The destinations likely have extra content beyond the write, so startsWith is used
    assertTrue(beDestination.toString().startsWith(value));
    assertTrue(leDestination.toString().startsWith(value));
  }

  @Test
  public void testGetStringAscii_appendableWrapped() {
    String value = "This statement is false.";
    byte[] rawBytes = getLengthPrefixedAsciiStringBytes(value, /* reverseLength= */ false);
    byte[] reversedBytes = getLengthPrefixedAsciiStringBytes(value, /* reverseLength= */ true);
    TestableDirectByteBuffer buffer =
        new TestableDirectByteBuffer(createBufferWithSurroundingBytes(rawBytes), CreateMode.VIEW);
    TestableDirectByteBuffer reversed =
        new TestableDirectByteBuffer(
            createBufferWithSurroundingBytes(reversedBytes), CreateMode.VIEW);

    CharBuffer beDestination = CharBuffer.allocate(rawBytes.length);
    CharBuffer leDestination = CharBuffer.allocate(reversed.length);

    buffer.getStringAscii(1, beDestination);
    reversed.getStringAscii(1, leDestination, ByteOrder.LITTLE_ENDIAN);

    beDestination.position(0);
    leDestination.position(0);

    // The destinations likely have extra content beyond the write, so startsWith is used
    assertTrue(beDestination.toString().startsWith(value));
    assertTrue(leDestination.toString().startsWith(value));
  }

  @Test
  public void testGetStringAscii_appendableCustomLength() {
    String value = "This statement is false.";
    int length = 3;
    byte[] rawBytes = getLengthPrefixedAsciiStringBytes(value, /* reverseLength= */ false);
    TestableDirectByteBuffer buffer =
        new TestableDirectByteBuffer(ByteBuffer.wrap(rawBytes), CreateMode.VIEW);

    CharBuffer destination = CharBuffer.allocate(length);
    buffer.getStringAscii(0, length, destination);
    destination.position(0); // Necessary for toString to capture the actual contents

    String expected = value.substring(0, length);

    assertEquals(expected, destination.toString());
  }

  @Test
  public void testGetStringWithoutLengthAscii() {
    String value = "This statement is false.";
    byte[] rawBytes = value.getBytes(US_ASCII);
    TestableDirectByteBuffer buffer =
        new TestableDirectByteBuffer(ByteBuffer.wrap(rawBytes), CreateMode.VIEW);

    assertEquals(value, buffer.getStringWithoutLengthAscii(0, rawBytes.length));
  }

  @Test
  public void testGetStringWithoutLengthAscii_appendable() {
    String value = "This statement is false.";
    byte[] rawBytes = value.getBytes(US_ASCII);
    TestableDirectByteBuffer buffer =
        new TestableDirectByteBuffer(ByteBuffer.wrap(rawBytes), CreateMode.VIEW);

    CharBuffer destination = CharBuffer.allocate(rawBytes.length);
    buffer.getStringWithoutLengthAscii(0, rawBytes.length, destination);
    destination.position(0); // Necessary for toString to capture the actual contents

    assertEquals(value, destination.toString());
  }

  @Test
  public void testGetStringWithoutLengthAscii_badBounds() {
    TestableDirectByteBuffer buffer = new TestableDirectByteBuffer(underlying, CreateMode.VIEW);

    assertThrows(
        IndexOutOfBoundsException.class, () -> buffer.getStringWithoutLengthAscii(-1, DATA.length));
    assertThrows(
        IndexOutOfBoundsException.class, () -> buffer.getStringWithoutLengthAscii(1, DATA.length));
    assertThrows(
        IndexOutOfBoundsException.class, () -> buffer.getStringWithoutLengthAscii(DATA.length, 1));
  }

  @Test
  public void testGetStringUtf8() {
    String value = "This statement is false.";
    TestableDirectByteBuffer buffer =
        new TestableDirectByteBuffer(
            ByteBuffer.wrap(getLengthPrefixedUtf8StringBytes(value, /* reverseLength= */ false)),
            CreateMode.VIEW);
    TestableDirectByteBuffer reversed =
        new TestableDirectByteBuffer(
            ByteBuffer.wrap(getLengthPrefixedUtf8StringBytes(value, /* reverseLength= */ true)),
            CreateMode.VIEW);

    assertEquals(value, buffer.getStringUtf8(0));
    assertEquals(value, buffer.getStringUtf8(0, ByteOrder.BIG_ENDIAN));
    assertEquals(value, reversed.getStringUtf8(0, ByteOrder.LITTLE_ENDIAN));
  }

  @Test
  public void testGetStringUtf8_wrapped() {
    String value = "This statement is false.";
    byte[] rawBytes = getLengthPrefixedUtf8StringBytes(value, /* reverseLength= */ false);
    byte[] reversedBytes = getLengthPrefixedUtf8StringBytes(value, /* reverseLength= */ true);
    TestableDirectByteBuffer buffer =
        new TestableDirectByteBuffer(createBufferWithSurroundingBytes(rawBytes), CreateMode.VIEW);
    TestableDirectByteBuffer reversed =
        new TestableDirectByteBuffer(
            createBufferWithSurroundingBytes(reversedBytes), CreateMode.VIEW);

    assertEquals(value, buffer.getStringUtf8(1));
    assertEquals(value, buffer.getStringUtf8(1, ByteOrder.BIG_ENDIAN));
    assertEquals(value, reversed.getStringUtf8(1, ByteOrder.LITTLE_ENDIAN));
  }

  @Test
  public void testGetStringUtf8_overrideLength() {
    String value = "This statement is false.";
    int customLength = 3;
    TestableDirectByteBuffer buffer =
        new TestableDirectByteBuffer(
            ByteBuffer.wrap(getLengthPrefixedUtf8StringBytes(value, /* reverseLength= */ false)),
            CreateMode.VIEW);
    // Reverse unnecessary, since the length prefix is skipped

    String expected = value.substring(0, customLength);

    assertEquals(expected, buffer.getStringUtf8(0, customLength));
  }

  @Test
  public void testGetStringUtf8_wrappedWithCustomLength() {
    String value = "This statement is false.";
    byte[] rawBytes = getLengthPrefixedUtf8StringBytes(value, /* reverseLength= */ false);
    int customLength = 3;
    TestableDirectByteBuffer buffer =
        new TestableDirectByteBuffer(createBufferWithSurroundingBytes(rawBytes), CreateMode.VIEW);
    // Reverse unnecessary, since the length prefix is skipped

    String expected = value.substring(0, customLength);

    assertEquals(expected, buffer.getStringUtf8(1, customLength));
  }

  @Test
  public void testGetStringUtf8_badBounds() {
    TestableDirectByteBuffer buffer = new TestableDirectByteBuffer(underlying, CreateMode.VIEW);

    assertThrows(IndexOutOfBoundsException.class, () -> buffer.getStringUtf8(-1));
    // The integer read in will be 123, which will way exceed length when trying to read string
    assertThrows(IndexOutOfBoundsException.class, () -> buffer.getStringUtf8(0));
    assertThrows(IndexOutOfBoundsException.class, () -> buffer.getStringUtf8(DATA.length - 3));
    assertThrows(IndexOutOfBoundsException.class, () -> buffer.getStringUtf8(-1, DATA.length));
    // Read starts from index 4 and will read 10, but there's only ten bytes
    assertThrows(IndexOutOfBoundsException.class, () -> buffer.getStringUtf8(0, DATA.length));
    assertThrows(IndexOutOfBoundsException.class, () -> buffer.getStringUtf8(DATA.length - 3, 1));
  }

  @Test
  public void testGetStringWithoutLengthUtf8() {
    String value = "This statement is false.";
    byte[] rawBytes = value.getBytes(UTF_8);
    TestableDirectByteBuffer buffer =
        new TestableDirectByteBuffer(ByteBuffer.wrap(rawBytes), CreateMode.VIEW);

    assertEquals(value, buffer.getStringWithoutLengthUtf8(0, rawBytes.length));
  }

  @Test
  public void testGetStringWithoutLengthUtf8_badBounds() {
    TestableDirectByteBuffer buffer = new TestableDirectByteBuffer(underlying, CreateMode.VIEW);

    assertThrows(
        IndexOutOfBoundsException.class, () -> buffer.getStringWithoutLengthUtf8(-1, DATA.length));
    assertThrows(
        IndexOutOfBoundsException.class, () -> buffer.getStringWithoutLengthUtf8(1, DATA.length));
    assertThrows(
        IndexOutOfBoundsException.class, () -> buffer.getStringWithoutLengthUtf8(DATA.length, 1));
  }

  @Test
  public void testCheckBounds_valid() {
    TestableDirectByteBuffer buffer = new TestableDirectByteBuffer(underlying, CreateMode.VIEW);
    buffer.boundsCheck(0, DATA.length);
    buffer.boundsCheck(1, DATA.length - 1);
  }

  @Test
  public void testCheckBounds_invalid() {
    TestableDirectByteBuffer buffer = new TestableDirectByteBuffer(underlying, CreateMode.VIEW);

    assertThrows(IndexOutOfBoundsException.class, () -> buffer.boundsCheck(-1, DATA.length));
    assertThrows(IndexOutOfBoundsException.class, () -> buffer.boundsCheck(DATA.length, 1));
    assertThrows(IndexOutOfBoundsException.class, () -> buffer.boundsCheck(0, -1));
    assertThrows(IndexOutOfBoundsException.class, () -> buffer.boundsCheck(1, DATA.length));
  }

  /** Converts {@code value} to a byte[]. The byte[] can be in reverse order. */
  private static byte[] getDoubleBytes(double value, boolean reverse) {
    long asLong = Double.doubleToLongBits(value);
    if (reverse) {
      asLong = Long.reverseBytes(asLong);
    }
    return Longs.toByteArray(asLong);
  }

  /** Converts {@code value} to a byte[]. The byte[] can be in reverse order. */
  private static byte[] getFloatBytes(float value, boolean reverse) {
    int asInt = Float.floatToIntBits(value);
    if (reverse) {
      asInt = Integer.reverseBytes(asInt);
    }
    return Ints.toByteArray(asInt);
  }

  /** Converts {@code value} to a byte[]. The byte[] can be in reverse order. */
  private static byte[] getCharBytes(char value, boolean reverse) {
    // We need two bytes, and using a string may only get us one, so a short is used.
    short asShort = (short) value;
    if (reverse) {
      asShort = Short.reverseBytes(asShort);
    }
    return Shorts.toByteArray(asShort);
  }

  /**
   * Get a byte[] for {@code value} but with four bytes prefixed to indicate original length.
   *
   * <p>The length can be reversed. {@code value}'s bytes cannot.
   */
  private static byte[] getLengthPrefixedAsciiStringBytes(String value, boolean reverseLength) {
    byte[] strBytes = value.getBytes(US_ASCII);

    int length = strBytes.length;
    if (reverseLength) {
      length = Integer.reverseBytes(length);
    }

    return ArrayUtils.addAll(Ints.toByteArray(length), strBytes);
  }

  /**
   * Get a byte[] for {@code value} but with four bytes prefixed to indicate original length.
   *
   * <p>The length can be reversed. {@code value}'s bytes cannot.
   */
  private static byte[] getLengthPrefixedUtf8StringBytes(String value, boolean reverseLength) {
    byte[] strBytes = value.getBytes(UTF_8);

    int length = strBytes.length;
    if (reverseLength) {
      length = Integer.reverseBytes(length);
    }

    return ArrayUtils.addAll(Ints.toByteArray(length), strBytes);
  }

  /** Creates a {@link ByteBuffer} with {@code value} surrounding by a single byte on both ends. */
  private static ByteBuffer createBufferWithSurroundingBytes(byte[] value) {
    byte surrounding = 0;
    ByteBuffer buffer = ByteBuffer.allocateDirect(value.length + 2);

    buffer.put(surrounding);
    buffer.put(value, 0, value.length);
    buffer.put(surrounding);
    buffer.position(0);

    return buffer;
  }
}
