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
package org.apache.beam.sdk.io.gcp.spanner;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import com.google.auto.value.AutoValue;
import com.google.common.collect.ImmutableList;
import com.google.common.io.BaseEncoding;
import com.google.common.primitives.Bytes;
import com.google.common.primitives.UnsignedBytes;
import com.google.common.primitives.UnsignedInteger;
import java.util.Arrays;
import java.util.List;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * A set of unit tests to verify {@link OrderedCode}.
 */
@RunWith(JUnit4.class)
public class OrderedCodeTest {
  /** Data for a generic coding test case with known encoded outputs. */
  abstract static class CodingTestCase<T> {
    /** The test value. */
    abstract T value();

    /**
     * Test value's encoding in increasing order (obtained from the C++
     * implementation).
     */
    abstract String increasingBytes();

    /**
     * Test value's encoding in dencreasing order (obtained from the C++
     * implementation).
     */
    abstract String decreasingBytes();

    // Helper methods to implement in concrete classes.

    abstract byte[] encodeIncreasing();
    abstract byte[] encodeDecreasing();

    T decodeIncreasing() {
      return decodeIncreasing(
          new OrderedCode(bytesFromHexString(increasingBytes())));
    }

    T decodeDecreasing() {
      return decodeDecreasing(
          new OrderedCode(bytesFromHexString(decreasingBytes())));
    }

    abstract T decodeIncreasing(OrderedCode orderedCode);
    abstract T decodeDecreasing(OrderedCode orderedCode);
  }

  @AutoValue
  abstract static class UnsignedNumber extends CodingTestCase<Long> {
    @Override
    byte[] encodeIncreasing() {
      OrderedCode orderedCode = new OrderedCode();
      orderedCode.writeNumIncreasing(value());
      return orderedCode.getEncodedBytes();
    }

    @Override
    byte[] encodeDecreasing() {
      OrderedCode orderedCode = new OrderedCode();
      orderedCode.writeNumDecreasing(value());
      return orderedCode.getEncodedBytes();
    }

    @Override
    Long decodeIncreasing(OrderedCode orderedCode) {
      return orderedCode.readNumIncreasing();
    }

    @Override
    Long decodeDecreasing(OrderedCode orderedCode) {
      return orderedCode.readNumDecreasing();
    }

    private static UnsignedNumber testCase(
        long value, String increasingBytes, String decreasingBytes) {
      return new AutoValue_OrderedCodeTest_UnsignedNumber(
          value, increasingBytes, decreasingBytes);
    }

    /** Test cases for unsigned numbers, in increasing (unsigned) order by value. */
    private static final ImmutableList<UnsignedNumber> TEST_CASES =
        ImmutableList.of(
            testCase(0, "00", "ff"),
            testCase(1, "0101", "fefe"),
            testCase(33, "0121", "fede"),
            testCase(55000, "02d6d8", "fd2927"),
            testCase(Integer.MAX_VALUE, "047fffffff", "fb80000000"),
            testCase(Long.MAX_VALUE, "087fffffffffffffff", "f78000000000000000"),
            testCase(Long.MIN_VALUE, "088000000000000000", "f77fffffffffffffff"),
            testCase(-100, "08ffffffffffffff9c", "f70000000000000063"),
            testCase(-1, "08ffffffffffffffff", "f70000000000000000"));
  }

  @AutoValue
  abstract static class BytesTest extends CodingTestCase<String> {
    @Override
    byte[] encodeIncreasing() {
      OrderedCode orderedCode = new OrderedCode();
      orderedCode.writeBytes(bytesFromHexString(value()));
      return orderedCode.getEncodedBytes();
    }

    @Override
    byte[] encodeDecreasing() {
      OrderedCode orderedCode = new OrderedCode();
      orderedCode.writeBytesDecreasing(bytesFromHexString(value()));
      return orderedCode.getEncodedBytes();
    }

    @Override
    String decodeIncreasing(OrderedCode orderedCode) {
      return bytesToHexString(orderedCode.readBytes());
    }

    @Override
    String decodeDecreasing(OrderedCode orderedCode) {
      return bytesToHexString(orderedCode.readBytesDecreasing());
    }

    private static BytesTest testCase(
        String value, String increasingBytes, String decreasingBytes) {
      return new AutoValue_OrderedCodeTest_BytesTest(
          value, increasingBytes, decreasingBytes);
    }

    /** Test cases for byte arrays, in increasing order by value. */
    private static final ImmutableList<BytesTest> TEST_CASES =
        ImmutableList.of(
            testCase("", "0001", "fffe"),
            testCase("00", "00ff0001", "ff00fffe"),
            testCase("0000", "00ff00ff0001", "ff00ff00fffe"),
            testCase("0001", "00ff010001", "ff00fefffe"),
            testCase("0041", "00ff410001", "ff00befffe"),
            testCase("00ff", "00ffff000001", "ff0000fffffe"),
            testCase("01", "010001", "fefffe"),
            testCase("0100", "0100ff0001", "feff00fffe"),
            testCase("6f776c", "6f776c0001", "908893fffe"),
            testCase("ff", "ff000001", "00fffffe"),
            testCase("ff00", "ff0000ff0001", "00ffff00fffe"),
            testCase("ff01", "ff00010001", "00fffefffe"),
            testCase("ffff", "ff00ff000001", "00ff00fffffe"),
            testCase("ffffff", "ff00ff00ff000001", "00ff00ff00fffffe"));
  }

  @Test
  public void testUnsignedEncoding() {
    testEncoding(UnsignedNumber.TEST_CASES);
  }

  @Test
  public void testUnsignedDecoding() {
    testDecoding(UnsignedNumber.TEST_CASES);
  }

  @Test
  public void testUnsignedOrdering() {
    testOrdering(UnsignedNumber.TEST_CASES);
  }

  @Test
  public void testBytesEncoding() {
    testEncoding(BytesTest.TEST_CASES);
  }

  @Test
  public void testBytesDecoding() {
    testDecoding(BytesTest.TEST_CASES);
  }

  @Test
  public void testBytesOrdering() {
    testOrdering(BytesTest.TEST_CASES);
  }

  private void testEncoding(List<? extends CodingTestCase<?>> testCases) {
    for (CodingTestCase<?> testCase : testCases) {
      byte[] actualIncreasing = testCase.encodeIncreasing();
      byte[] expectedIncreasing =
          bytesFromHexString(testCase.increasingBytes());
      assertEquals(0, compare(actualIncreasing, expectedIncreasing));

      byte[] actualDecreasing = testCase.encodeDecreasing();
      byte[] expectedDecreasing =
          bytesFromHexString(testCase.decreasingBytes());
      assertEquals(0, compare(actualDecreasing, expectedDecreasing));
    }
  }

  private void testDecoding(List<? extends CodingTestCase<?>> testCases) {
    for (CodingTestCase<?> testCase : testCases) {
      assertEquals(testCase.value(), testCase.decodeIncreasing());
      assertEquals(testCase.value(), testCase.decodeDecreasing());
    }
  }

  private void testOrdering(List<? extends CodingTestCase<?>> testCases) {
    // This is verifiable by inspection of the C++ encodings, but it seems
    // worth checking explicitly
    for (int caseIndex = 0; caseIndex < testCases.size() - 1; caseIndex++) {
      byte[] encodedValue = testCases.get(caseIndex).encodeIncreasing();
      byte[] nextEncodedValue = testCases.get(caseIndex + 1).encodeIncreasing();
      assertTrue(compare(encodedValue, nextEncodedValue) < 0);

      encodedValue = testCases.get(caseIndex).encodeDecreasing();
      nextEncodedValue = testCases.get(caseIndex + 1).encodeDecreasing();
      assertTrue(compare(encodedValue, nextEncodedValue) > 0);
    }
  }

  @Test
  public void testWriteInfinity() {
    OrderedCode orderedCode = new OrderedCode();
    try {
      orderedCode.readInfinity();
      fail("Expected IllegalArgumentException.");
    } catch (IllegalArgumentException e) {
      // expected
    }
    orderedCode.writeInfinity();
    assertTrue(orderedCode.readInfinity());
    try {
      orderedCode.readInfinity();
      fail("Expected IllegalArgumentException.");
    } catch (IllegalArgumentException e) {
      // expected
    }
  }

  @Test
  public void testWriteInfinityDecreasing() {
    OrderedCode orderedCode = new OrderedCode();
    try {
      orderedCode.readInfinityDecreasing();
      fail("Expected IllegalArgumentException.");
    } catch (IllegalArgumentException e) {
      // expected
    }
    orderedCode.writeInfinityDecreasing();
    assertTrue(orderedCode.readInfinityDecreasing());
    try {
      orderedCode.readInfinityDecreasing();
      fail("Expected IllegalArgumentException.");
    } catch (IllegalArgumentException e) {
      // expected
    }
  }

  @Test
  public void testWriteBytes() {
    byte[] first = { 'a', 'b', 'c'};
    byte[] second = { 'd', 'e', 'f'};
    byte[] last = { 'x', 'y', 'z'};
    OrderedCode orderedCode = new OrderedCode();
    orderedCode.writeBytes(first);
    byte[] firstEncoded = orderedCode.getEncodedBytes();
    assertTrue(Arrays.equals(orderedCode.readBytes(), first));

    orderedCode.writeBytes(first);
    orderedCode.writeBytes(second);
    orderedCode.writeBytes(last);
    byte[] allEncoded = orderedCode.getEncodedBytes();
    assertTrue(Arrays.equals(orderedCode.readBytes(), first));
    assertTrue(Arrays.equals(orderedCode.readBytes(), second));
    assertTrue(Arrays.equals(orderedCode.readBytes(), last));

    orderedCode = new OrderedCode(firstEncoded);
    orderedCode.writeBytes(second);
    orderedCode.writeBytes(last);
    assertTrue(Arrays.equals(orderedCode.getEncodedBytes(), allEncoded));
    assertTrue(Arrays.equals(orderedCode.readBytes(), first));
    assertTrue(Arrays.equals(orderedCode.readBytes(), second));
    assertTrue(Arrays.equals(orderedCode.readBytes(), last));

    orderedCode = new OrderedCode(allEncoded);
    assertTrue(Arrays.equals(orderedCode.readBytes(), first));
    assertTrue(Arrays.equals(orderedCode.readBytes(), second));
    assertTrue(Arrays.equals(orderedCode.readBytes(), last));
  }

  @Test
  public void testWriteBytesDecreasing() {
    byte[] first = { 'a', 'b', 'c'};
    byte[] second = { 'd', 'e', 'f'};
    byte[] last = { 'x', 'y', 'z'};
    OrderedCode orderedCode = new OrderedCode();
    orderedCode.writeBytesDecreasing(first);
    byte[] firstEncoded = orderedCode.getEncodedBytes();
    assertTrue(Arrays.equals(orderedCode.readBytesDecreasing(), first));

    orderedCode.writeBytesDecreasing(first);
    orderedCode.writeBytesDecreasing(second);
    orderedCode.writeBytesDecreasing(last);
    byte[] allEncoded = orderedCode.getEncodedBytes();
    assertTrue(Arrays.equals(orderedCode.readBytesDecreasing(), first));
    assertTrue(Arrays.equals(orderedCode.readBytesDecreasing(), second));
    assertTrue(Arrays.equals(orderedCode.readBytesDecreasing(), last));

    orderedCode = new OrderedCode(firstEncoded);
    orderedCode.writeBytesDecreasing(second);
    orderedCode.writeBytesDecreasing(last);
    assertTrue(Arrays.equals(orderedCode.getEncodedBytes(), allEncoded));
    assertTrue(Arrays.equals(orderedCode.readBytesDecreasing(), first));
    assertTrue(Arrays.equals(orderedCode.readBytesDecreasing(), second));
    assertTrue(Arrays.equals(orderedCode.readBytesDecreasing(), last));

    orderedCode = new OrderedCode(allEncoded);
    assertTrue(Arrays.equals(orderedCode.readBytesDecreasing(), first));
    assertTrue(Arrays.equals(orderedCode.readBytesDecreasing(), second));
    assertTrue(Arrays.equals(orderedCode.readBytesDecreasing(), last));
  }

  @Test
  public void testWriteNumIncreasing() {
    OrderedCode orderedCode = new OrderedCode();
    orderedCode.writeNumIncreasing(0);
    orderedCode.writeNumIncreasing(1);
    orderedCode.writeNumIncreasing(Long.MIN_VALUE);
    orderedCode.writeNumIncreasing(Long.MAX_VALUE);
    assertEquals(0, orderedCode.readNumIncreasing());
    assertEquals(1, orderedCode.readNumIncreasing());
    assertEquals(Long.MIN_VALUE, orderedCode.readNumIncreasing());
    assertEquals(Long.MAX_VALUE, orderedCode.readNumIncreasing());
  }

  @Test
  public void testWriteNumIncreasing_unsignedInt() {
    OrderedCode orderedCode = new OrderedCode();
    orderedCode.writeNumIncreasing(UnsignedInteger.fromIntBits(0));
    orderedCode.writeNumIncreasing(UnsignedInteger.fromIntBits(1));
    orderedCode.writeNumIncreasing(UnsignedInteger.fromIntBits(Integer.MIN_VALUE));
    orderedCode.writeNumIncreasing(UnsignedInteger.fromIntBits(Integer.MAX_VALUE));
    assertEquals(0, orderedCode.readNumIncreasing());
    assertEquals(1, orderedCode.readNumIncreasing());
    assertEquals((long) Integer.MAX_VALUE + 1L, orderedCode.readNumIncreasing());
    assertEquals(Integer.MAX_VALUE, orderedCode.readNumIncreasing());
  }

  @Test
  public void testWriteNumDecreasing() {
    OrderedCode orderedCode = new OrderedCode();
    orderedCode.writeNumDecreasing(0);
    orderedCode.writeNumDecreasing(1);
    orderedCode.writeNumDecreasing(Long.MIN_VALUE);
    orderedCode.writeNumDecreasing(Long.MAX_VALUE);
    assertEquals(0, orderedCode.readNumDecreasing());
    assertEquals(1, orderedCode.readNumDecreasing());
    assertEquals(Long.MIN_VALUE, orderedCode.readNumDecreasing());
    assertEquals(Long.MAX_VALUE, orderedCode.readNumDecreasing());
  }

  @Test
  public void testWriteNumDecreasing_unsignedInt() {
    OrderedCode orderedCode = new OrderedCode();
    orderedCode.writeNumDecreasing(UnsignedInteger.fromIntBits(0));
    orderedCode.writeNumDecreasing(UnsignedInteger.fromIntBits(1));
    orderedCode.writeNumDecreasing(UnsignedInteger.fromIntBits(Integer.MIN_VALUE));
    orderedCode.writeNumDecreasing(UnsignedInteger.fromIntBits(Integer.MAX_VALUE));
    assertEquals(0, orderedCode.readNumDecreasing());
    assertEquals(1, orderedCode.readNumDecreasing());
    assertEquals((long) Integer.MAX_VALUE + 1L, orderedCode.readNumDecreasing());
    assertEquals(Integer.MAX_VALUE, orderedCode.readNumDecreasing());
  }

  /**
   * Assert that encoding the specified long via
   * {@link OrderedCode#writeSignedNumIncreasing(long)} results in the bytes
   * represented by the specified string of hex digits.
   * E.g. assertSignedNumIncreasingEncodingEquals("3fbf", -65) asserts that
   * -65 is encoded as { (byte) 0x3f, (byte) 0xbf }.
   */
  private static void assertSignedNumIncreasingEncodingEquals(
      String expectedHexEncoding, long num) {
    OrderedCode orderedCode = new OrderedCode();
    orderedCode.writeSignedNumIncreasing(num);
    assertEquals(
        "Unexpected encoding for " + num,
        expectedHexEncoding,
        bytesToHexString(orderedCode.getEncodedBytes()));
  }

  /**
   * Assert that encoding various long values via
   * {@link OrderedCode#writeSignedNumIncreasing(long)} produces the expected
   * bytes. Expected byte sequences were generated via the c++ (authoritative)
   * implementation of OrderedCode::WriteSignedNumIncreasing.
   */
  @Test
  public void testSignedNumIncreasing_write() {
    assertSignedNumIncreasingEncodingEquals(
        "003f8000000000000000", Long.MIN_VALUE);
    assertSignedNumIncreasingEncodingEquals(
        "003f8000000000000001", Long.MIN_VALUE + 1);
    assertSignedNumIncreasingEncodingEquals(
        "077fffffff", Integer.MIN_VALUE - 1L);
    assertSignedNumIncreasingEncodingEquals("0780000000", Integer.MIN_VALUE);
    assertSignedNumIncreasingEncodingEquals(
        "0780000001", Integer.MIN_VALUE + 1);
    assertSignedNumIncreasingEncodingEquals("3fbf", -65);
    assertSignedNumIncreasingEncodingEquals("40", -64);
    assertSignedNumIncreasingEncodingEquals("41", -63);
    assertSignedNumIncreasingEncodingEquals("7d", -3);
    assertSignedNumIncreasingEncodingEquals("7e", -2);
    assertSignedNumIncreasingEncodingEquals("7f", -1);
    assertSignedNumIncreasingEncodingEquals("80", 0);
    assertSignedNumIncreasingEncodingEquals("81", 1);
    assertSignedNumIncreasingEncodingEquals("82", 2);
    assertSignedNumIncreasingEncodingEquals("83", 3);
    assertSignedNumIncreasingEncodingEquals("bf", 63);
    assertSignedNumIncreasingEncodingEquals("c040", 64);
    assertSignedNumIncreasingEncodingEquals("c041", 65);
    assertSignedNumIncreasingEncodingEquals(
        "f87ffffffe", Integer.MAX_VALUE - 1);
    assertSignedNumIncreasingEncodingEquals("f87fffffff", Integer.MAX_VALUE);
    assertSignedNumIncreasingEncodingEquals(
        "f880000000", Integer.MAX_VALUE + 1L);
    assertSignedNumIncreasingEncodingEquals(
        "ffc07ffffffffffffffe", Long.MAX_VALUE - 1);
    assertSignedNumIncreasingEncodingEquals(
        "ffc07fffffffffffffff", Long.MAX_VALUE);
  }

  /**
   * Convert a string of hex digits (e.g. "3fbf") to a byte[]
   * (e.g. { (byte) 0x3f, (byte) 0xbf }).
   */
  private static byte[] bytesFromHexString(String hexDigits) {
    return BaseEncoding.base16().lowerCase().decode(hexDigits);
  }

  /**
   * Convert a byte[] (e.g. { (byte) 0x3f, (byte) 0xbf }) to a string of hex
   * digits (e.g. "3fbf").
   */
  private static String bytesToHexString(byte[] bytes) {
    return BaseEncoding.base16().lowerCase().encode(bytes);
  }

  /**
   * Assert that decoding (via {@link OrderedCode#readSignedNumIncreasing()})
   * the bytes represented by the specified string of hex digits results in the
   * expected long value.
   * E.g. assertDecodedSignedNumIncreasingEquals(-65, "3fbf") asserts that the
   * byte array { (byte) 0x3f, (byte) 0xbf } is decoded as -65.
   */
  private static void assertDecodedSignedNumIncreasingEquals(
      long expectedNum, String encodedHexString) {
    OrderedCode orderedCode =
        new OrderedCode(bytesFromHexString(encodedHexString));
    assertEquals(
        "Unexpected value when decoding 0x" + encodedHexString,
        expectedNum,
        orderedCode.readSignedNumIncreasing());
    assertFalse(
        "Unexpected encoded bytes remain after decoding 0x" + encodedHexString,
        orderedCode.hasRemainingEncodedBytes());
  }

  /**
   * Assert that decoding various sequences of bytes via
   * {@link OrderedCode#readSignedNumIncreasing()} produces the expected long
   * value.
   * Input byte sequences were generated via the c++ (authoritative)
   * implementation of OrderedCode::WriteSignedNumIncreasing.
   */
  @Test
  public void testSignedNumIncreasing_read() {
    assertDecodedSignedNumIncreasingEquals(
        Long.MIN_VALUE, "003f8000000000000000");
    assertDecodedSignedNumIncreasingEquals(
        Long.MIN_VALUE + 1, "003f8000000000000001");
    assertDecodedSignedNumIncreasingEquals(
        Integer.MIN_VALUE - 1L, "077fffffff");
    assertDecodedSignedNumIncreasingEquals(Integer.MIN_VALUE, "0780000000");
    assertDecodedSignedNumIncreasingEquals(Integer.MIN_VALUE + 1, "0780000001");
    assertDecodedSignedNumIncreasingEquals(-65, "3fbf");
    assertDecodedSignedNumIncreasingEquals(-64, "40");
    assertDecodedSignedNumIncreasingEquals(-63, "41");
    assertDecodedSignedNumIncreasingEquals(-3, "7d");
    assertDecodedSignedNumIncreasingEquals(-2, "7e");
    assertDecodedSignedNumIncreasingEquals(-1, "7f");
    assertDecodedSignedNumIncreasingEquals(0, "80");
    assertDecodedSignedNumIncreasingEquals(1, "81");
    assertDecodedSignedNumIncreasingEquals(2, "82");
    assertDecodedSignedNumIncreasingEquals(3, "83");
    assertDecodedSignedNumIncreasingEquals(63, "bf");
    assertDecodedSignedNumIncreasingEquals(64, "c040");
    assertDecodedSignedNumIncreasingEquals(65, "c041");
    assertDecodedSignedNumIncreasingEquals(Integer.MAX_VALUE - 1, "f87ffffffe");
    assertDecodedSignedNumIncreasingEquals(Integer.MAX_VALUE, "f87fffffff");
    assertDecodedSignedNumIncreasingEquals(
        Integer.MAX_VALUE + 1L, "f880000000");
    assertDecodedSignedNumIncreasingEquals(
        Long.MAX_VALUE - 1, "ffc07ffffffffffffffe");
    assertDecodedSignedNumIncreasingEquals(
        Long.MAX_VALUE, "ffc07fffffffffffffff");
  }

  /**
   * Assert that encoding (via
   * {@link OrderedCode#writeSignedNumIncreasing(long)}) the specified long
   * value and then decoding (via {@link OrderedCode#readSignedNumIncreasing()})
   * results in the original value.
   */
  private static void assertSignedNumIncreasingWriteAndReadIsLossless(
      long num) {
    OrderedCode orderedCode = new OrderedCode();
    orderedCode.writeSignedNumIncreasing(num);
    assertEquals(
        "Unexpected result when decoding writeSignedNumIncreasing(" + num + ")",
        num,
        orderedCode.readSignedNumIncreasing());
    assertFalse("Unexpected remaining encoded bytes after decoding " + num,
        orderedCode.hasRemainingEncodedBytes());
  }

  /**
   * Assert that for various long values, encoding (via
   * {@link OrderedCode#writeSignedNumIncreasing(long)}) and then decoding (via
   * {@link OrderedCode#readSignedNumIncreasing()}) results in the original
   * value.
   */
  @Test
  public void testSignedNumIncreasing_writeAndRead() {
    assertSignedNumIncreasingWriteAndReadIsLossless(Long.MIN_VALUE);
    assertSignedNumIncreasingWriteAndReadIsLossless(Long.MIN_VALUE + 1);
    assertSignedNumIncreasingWriteAndReadIsLossless(Integer.MIN_VALUE - 1L);
    assertSignedNumIncreasingWriteAndReadIsLossless(Integer.MIN_VALUE);
    assertSignedNumIncreasingWriteAndReadIsLossless(Integer.MIN_VALUE + 1);
    assertSignedNumIncreasingWriteAndReadIsLossless(-65);
    assertSignedNumIncreasingWriteAndReadIsLossless(-64);
    assertSignedNumIncreasingWriteAndReadIsLossless(-63);
    assertSignedNumIncreasingWriteAndReadIsLossless(-3);
    assertSignedNumIncreasingWriteAndReadIsLossless(-2);
    assertSignedNumIncreasingWriteAndReadIsLossless(-1);
    assertSignedNumIncreasingWriteAndReadIsLossless(0);
    assertSignedNumIncreasingWriteAndReadIsLossless(1);
    assertSignedNumIncreasingWriteAndReadIsLossless(2);
    assertSignedNumIncreasingWriteAndReadIsLossless(3);
    assertSignedNumIncreasingWriteAndReadIsLossless(63);
    assertSignedNumIncreasingWriteAndReadIsLossless(64);
    assertSignedNumIncreasingWriteAndReadIsLossless(65);
    assertSignedNumIncreasingWriteAndReadIsLossless(Integer.MAX_VALUE - 1);
    assertSignedNumIncreasingWriteAndReadIsLossless(Integer.MAX_VALUE);
    assertSignedNumIncreasingWriteAndReadIsLossless(Integer.MAX_VALUE + 1L);
    assertSignedNumIncreasingWriteAndReadIsLossless(Long.MAX_VALUE - 1);
    assertSignedNumIncreasingWriteAndReadIsLossless(Long.MAX_VALUE);
  }

  /**
   * Assert that encoding (via
   * {@link OrderedCode#writeSignedNumDecreasing(long)}) the specified long
   * value and then decoding (via {@link OrderedCode#readSignedNumDecreasing()})
   * results in the original value.
   */
  private static void assertSignedNumDecreasingWriteAndReadIsLossless(
      long num) {
    OrderedCode orderedCode = new OrderedCode();
    orderedCode.writeSignedNumDecreasing(num);
    assertEquals(
        "Unexpected result when decoding writeSignedNumDecreasing(" + num + ")",
        num,
        orderedCode.readSignedNumDecreasing());
    assertFalse("Unexpected remaining encoded bytes after decoding " + num,
        orderedCode.hasRemainingEncodedBytes());
  }

  /**
   * Assert that for various long values, encoding (via
   * {@link OrderedCode#writeSignedNumDecreasing(long)}) and then decoding (via
   * {@link OrderedCode#readSignedNumDecreasing()}) results in the original
   * value.
   */
  @Test
  public void testSignedNumDecreasing_writeAndRead() {
    assertSignedNumDecreasingWriteAndReadIsLossless(Long.MIN_VALUE);
    assertSignedNumDecreasingWriteAndReadIsLossless(Long.MIN_VALUE + 1);
    assertSignedNumDecreasingWriteAndReadIsLossless(Integer.MIN_VALUE - 1L);
    assertSignedNumDecreasingWriteAndReadIsLossless(Integer.MIN_VALUE);
    assertSignedNumDecreasingWriteAndReadIsLossless(Integer.MIN_VALUE + 1);
    assertSignedNumDecreasingWriteAndReadIsLossless(-65);
    assertSignedNumDecreasingWriteAndReadIsLossless(-64);
    assertSignedNumDecreasingWriteAndReadIsLossless(-63);
    assertSignedNumDecreasingWriteAndReadIsLossless(-3);
    assertSignedNumDecreasingWriteAndReadIsLossless(-2);
    assertSignedNumDecreasingWriteAndReadIsLossless(-1);
    assertSignedNumDecreasingWriteAndReadIsLossless(0);
    assertSignedNumDecreasingWriteAndReadIsLossless(1);
    assertSignedNumDecreasingWriteAndReadIsLossless(2);
    assertSignedNumDecreasingWriteAndReadIsLossless(3);
    assertSignedNumDecreasingWriteAndReadIsLossless(63);
    assertSignedNumDecreasingWriteAndReadIsLossless(64);
    assertSignedNumDecreasingWriteAndReadIsLossless(65);
    assertSignedNumDecreasingWriteAndReadIsLossless(Integer.MAX_VALUE - 1);
    assertSignedNumDecreasingWriteAndReadIsLossless(Integer.MAX_VALUE);
    assertSignedNumDecreasingWriteAndReadIsLossless(Integer.MAX_VALUE + 1L);
    assertSignedNumDecreasingWriteAndReadIsLossless(Long.MAX_VALUE - 1);
    assertSignedNumDecreasingWriteAndReadIsLossless(Long.MAX_VALUE);
  }

  /** Ensures that numbers encoded as "decreasing" do indeed sort in reverse order. */
  @Test
  public void testDecreasing() {
    OrderedCode orderedCode = new OrderedCode();
    orderedCode.writeSignedNumDecreasing(10L);
    byte[] ten = orderedCode.getEncodedBytes();
    orderedCode = new OrderedCode();
    orderedCode.writeSignedNumDecreasing(20L);
    byte[] twenty = orderedCode.getEncodedBytes();
    // In decreasing order, twenty preceeds ten.
    assertTrue(compare(twenty, ten) < 0);
  }

  @Test
  public void testLog2Floor_Positive() {
    OrderedCode orderedCode = new OrderedCode();
    assertEquals(0, orderedCode.log2Floor(1));
    assertEquals(1, orderedCode.log2Floor(2));
    assertEquals(1, orderedCode.log2Floor(3));
    assertEquals(2, orderedCode.log2Floor(4));
    assertEquals(5, orderedCode.log2Floor(63));
    assertEquals(6, orderedCode.log2Floor(64));
    assertEquals(62, orderedCode.log2Floor(Long.MAX_VALUE));
  }

  /**
   * OrderedCode.log2Floor(long) is defined to return -1 given an input of zero
   * (because that's what Bits::Log2Floor64(uint64) does).
   */
  @Test
  public void testLog2Floor_zero() {
    OrderedCode orderedCode = new OrderedCode();
    assertEquals(-1, orderedCode.log2Floor(0));
  }

  @Test
  public void testLog2Floor_negative() {
    OrderedCode orderedCode = new OrderedCode();
    try {
      orderedCode.log2Floor(-1);
      fail("Expected an IllegalArgumentException.");
    } catch (IllegalArgumentException expected) {
      // Expected!
    }
  }

  @Test
  public void testGetSignedEncodingLength() {
    OrderedCode orderedCode = new OrderedCode();
    assertEquals(10, orderedCode.getSignedEncodingLength(Long.MIN_VALUE));
    assertEquals(10, orderedCode.getSignedEncodingLength(~(1L << 62)));
    assertEquals(9, orderedCode.getSignedEncodingLength(~(1L << 62) + 1));
    assertEquals(3, orderedCode.getSignedEncodingLength(-8193));
    assertEquals(2, orderedCode.getSignedEncodingLength(-8192));
    assertEquals(2, orderedCode.getSignedEncodingLength(-65));
    assertEquals(1, orderedCode.getSignedEncodingLength(-64));
    assertEquals(1, orderedCode.getSignedEncodingLength(-2));
    assertEquals(1, orderedCode.getSignedEncodingLength(-1));
    assertEquals(1, orderedCode.getSignedEncodingLength(0));
    assertEquals(1, orderedCode.getSignedEncodingLength(1));
    assertEquals(1, orderedCode.getSignedEncodingLength(63));
    assertEquals(2, orderedCode.getSignedEncodingLength(64));
    assertEquals(2, orderedCode.getSignedEncodingLength(8191));
    assertEquals(3, orderedCode.getSignedEncodingLength(8192));
    assertEquals(9, orderedCode.getSignedEncodingLength((1L << 62)) - 1);
    assertEquals(10, orderedCode.getSignedEncodingLength(1L << 62));
    assertEquals(10, orderedCode.getSignedEncodingLength(Long.MAX_VALUE));
  }

  @Test
  public void testWriteTrailingBytes() {
    byte[] escapeChars = new byte[] { OrderedCode.ESCAPE1,
        OrderedCode.NULL_CHARACTER, OrderedCode.SEPARATOR, OrderedCode.ESCAPE2,
        OrderedCode.INFINITY, OrderedCode.FF_CHARACTER};
    byte[] anotherArray = new byte[] { 'a', 'b', 'c', 'd', 'e' };

    OrderedCode orderedCode = new OrderedCode();
    orderedCode.writeTrailingBytes(escapeChars);
    assertTrue(Arrays.equals(orderedCode.getEncodedBytes(), escapeChars));
    assertTrue(Arrays.equals(orderedCode.readTrailingBytes(), escapeChars));
    try {
      orderedCode.readInfinity();
      fail("Expected IllegalArgumentException.");
    } catch (IllegalArgumentException e) {
      // expected
    }

    orderedCode = new OrderedCode();
    orderedCode.writeTrailingBytes(anotherArray);
    assertTrue(Arrays.equals(orderedCode.getEncodedBytes(), anotherArray));
    assertTrue(Arrays.equals(orderedCode.readTrailingBytes(), anotherArray));
  }

  @Test
  public void testMixedWrite() {
    byte[] first = { 'a', 'b', 'c'};
    byte[] second = { 'd', 'e', 'f'};
    byte[] last = { 'x', 'y', 'z'};
    byte[] escapeChars = new byte[] { OrderedCode.ESCAPE1,
        OrderedCode.NULL_CHARACTER, OrderedCode.SEPARATOR, OrderedCode.ESCAPE2,
        OrderedCode.INFINITY, OrderedCode.FF_CHARACTER};

    OrderedCode orderedCode = new OrderedCode();
    orderedCode.writeBytes(first);
    orderedCode.writeBytes(second);
    orderedCode.writeBytes(last);
    orderedCode.writeInfinity();
    orderedCode.writeNumIncreasing(0);
    orderedCode.writeNumIncreasing(1);
    orderedCode.writeNumIncreasing(Long.MIN_VALUE);
    orderedCode.writeNumIncreasing(Long.MAX_VALUE);
    orderedCode.writeSignedNumIncreasing(0);
    orderedCode.writeSignedNumIncreasing(1);
    orderedCode.writeSignedNumIncreasing(Long.MIN_VALUE);
    orderedCode.writeSignedNumIncreasing(Long.MAX_VALUE);
    orderedCode.writeTrailingBytes(escapeChars);
    byte[] allEncoded = orderedCode.getEncodedBytes();
    assertTrue(Arrays.equals(orderedCode.readBytes(), first));
    assertTrue(Arrays.equals(orderedCode.readBytes(), second));
    assertFalse(orderedCode.readInfinity());
    assertTrue(Arrays.equals(orderedCode.readBytes(), last));
    assertTrue(orderedCode.readInfinity());
    assertEquals(0, orderedCode.readNumIncreasing());
    assertEquals(1, orderedCode.readNumIncreasing());
    assertFalse(orderedCode.readInfinity());
    assertEquals(Long.MIN_VALUE, orderedCode.readNumIncreasing());
    assertEquals(Long.MAX_VALUE, orderedCode.readNumIncreasing());
    assertEquals(0, orderedCode.readSignedNumIncreasing());
    assertEquals(1, orderedCode.readSignedNumIncreasing());
    assertFalse(orderedCode.readInfinity());
    assertEquals(Long.MIN_VALUE, orderedCode.readSignedNumIncreasing());
    assertEquals(Long.MAX_VALUE, orderedCode.readSignedNumIncreasing());
    assertTrue(Arrays.equals(orderedCode.getEncodedBytes(), escapeChars));
    assertTrue(Arrays.equals(orderedCode.readTrailingBytes(), escapeChars));

    orderedCode = new OrderedCode(allEncoded);
    assertTrue(Arrays.equals(orderedCode.readBytes(), first));
    assertTrue(Arrays.equals(orderedCode.readBytes(), second));
    assertFalse(orderedCode.readInfinity());
    assertTrue(Arrays.equals(orderedCode.readBytes(), last));
    assertTrue(orderedCode.readInfinity());
    assertEquals(0, orderedCode.readNumIncreasing());
    assertEquals(1, orderedCode.readNumIncreasing());
    assertFalse(orderedCode.readInfinity());
    assertEquals(Long.MIN_VALUE, orderedCode.readNumIncreasing());
    assertEquals(Long.MAX_VALUE, orderedCode.readNumIncreasing());
    assertEquals(0, orderedCode.readSignedNumIncreasing());
    assertEquals(1, orderedCode.readSignedNumIncreasing());
    assertFalse(orderedCode.readInfinity());
    assertEquals(Long.MIN_VALUE, orderedCode.readSignedNumIncreasing());
    assertEquals(Long.MAX_VALUE, orderedCode.readSignedNumIncreasing());
    assertTrue(Arrays.equals(orderedCode.getEncodedBytes(), escapeChars));
    assertTrue(Arrays.equals(orderedCode.readTrailingBytes(), escapeChars));
  }

  @Test
  public void testEdgeCases() {
    byte[] ffChar = {OrderedCode.FF_CHARACTER};
    byte[] nullChar = {OrderedCode.NULL_CHARACTER};

    byte[] separatorEncoded = {OrderedCode.ESCAPE1, OrderedCode.SEPARATOR};
    byte[] ffCharEncoded = {OrderedCode.ESCAPE1, OrderedCode.NULL_CHARACTER};
    byte[] nullCharEncoded = {OrderedCode.ESCAPE2, OrderedCode.FF_CHARACTER};
    byte[] infinityEncoded  = {OrderedCode.ESCAPE2, OrderedCode.INFINITY};

    OrderedCode orderedCode = new OrderedCode();
    orderedCode.writeBytes(ffChar);
    orderedCode.writeBytes(nullChar);
    orderedCode.writeInfinity();
    assertTrue(Arrays.equals(orderedCode.getEncodedBytes(),
        Bytes.concat(ffCharEncoded, separatorEncoded,
            nullCharEncoded, separatorEncoded,
            infinityEncoded)));
    assertTrue(Arrays.equals(orderedCode.readBytes(), ffChar));
    assertTrue(Arrays.equals(orderedCode.readBytes(), nullChar));
    assertTrue(orderedCode.readInfinity());

    orderedCode = new OrderedCode(
        Bytes.concat(ffCharEncoded, separatorEncoded));
    assertTrue(Arrays.equals(orderedCode.readBytes(), ffChar));

    orderedCode = new OrderedCode(
        Bytes.concat(nullCharEncoded, separatorEncoded));
    assertTrue(Arrays.equals(orderedCode.readBytes(), nullChar));

    byte[] invalidEncodingForRead = {OrderedCode.ESCAPE2, OrderedCode.ESCAPE2,
        OrderedCode.ESCAPE1, OrderedCode.SEPARATOR};
    orderedCode = new OrderedCode(invalidEncodingForRead);
    try {
      orderedCode.readBytes();
      fail("Should have failed.");
    } catch (Exception e) {
      // Expected
    }
    assertTrue(orderedCode.hasRemainingEncodedBytes());
  }

  @Test
  public void testHasRemainingEncodedBytes() {
    byte[] bytes = { 'a', 'b', 'c'};
    long number = 12345;

    // Empty
    OrderedCode orderedCode = new OrderedCode();
    assertFalse(orderedCode.hasRemainingEncodedBytes());

    // First and only field of each type.
    orderedCode.writeBytes(bytes);
    assertTrue(orderedCode.hasRemainingEncodedBytes());
    assertTrue(Arrays.equals(orderedCode.readBytes(), bytes));
    assertFalse(orderedCode.hasRemainingEncodedBytes());

    orderedCode.writeNumIncreasing(number);
    assertTrue(orderedCode.hasRemainingEncodedBytes());
    assertEquals(orderedCode.readNumIncreasing(), number);
    assertFalse(orderedCode.hasRemainingEncodedBytes());

    orderedCode.writeSignedNumIncreasing(number);
    assertTrue(orderedCode.hasRemainingEncodedBytes());
    assertEquals(orderedCode.readSignedNumIncreasing(), number);
    assertFalse(orderedCode.hasRemainingEncodedBytes());

    orderedCode.writeInfinity();
    assertTrue(orderedCode.hasRemainingEncodedBytes());
    assertTrue(orderedCode.readInfinity());
    assertFalse(orderedCode.hasRemainingEncodedBytes());

    orderedCode.writeTrailingBytes(bytes);
    assertTrue(orderedCode.hasRemainingEncodedBytes());
    assertTrue(Arrays.equals(orderedCode.readTrailingBytes(), bytes));
    assertFalse(orderedCode.hasRemainingEncodedBytes());

    // Two fields of same type.
    orderedCode.writeBytes(bytes);
    orderedCode.writeBytes(bytes);
    assertTrue(orderedCode.hasRemainingEncodedBytes());
    assertTrue(Arrays.equals(orderedCode.readBytes(), bytes));
    assertTrue(Arrays.equals(orderedCode.readBytes(), bytes));
    assertFalse(orderedCode.hasRemainingEncodedBytes());
  }

  @Test
  public void testOrderingInfinity() {
    OrderedCode inf = new OrderedCode();
    inf.writeInfinity();

    OrderedCode negInf = new OrderedCode();
    negInf.writeInfinityDecreasing();

    OrderedCode longValue = new OrderedCode();
    longValue.writeSignedNumIncreasing(1);

    assertTrue(compare(inf.getEncodedBytes(), negInf.getEncodedBytes()) > 0);
    assertTrue(compare(longValue.getEncodedBytes(), negInf.getEncodedBytes()) > 0);
    assertTrue(compare(inf.getEncodedBytes(), longValue.getEncodedBytes()) > 0);
  }

  private int compare(byte[] bytes1, byte[] bytes2) {
    return UnsignedBytes.lexicographicalComparator().compare(bytes1, bytes2);
  }
}
