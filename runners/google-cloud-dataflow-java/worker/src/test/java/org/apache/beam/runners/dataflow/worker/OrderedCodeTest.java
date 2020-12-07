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
package org.apache.beam.runners.dataflow.worker;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.io.BaseEncoding;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.primitives.Bytes;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for OrderedCode. */
@RunWith(JUnit4.class)
public class OrderedCodeTest {
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
  public void testWriteBytes() {
    byte[] first = {'a', 'b', 'c'};
    byte[] second = {'d', 'e', 'f'};
    byte[] last = {'x', 'y', 'z'};
    OrderedCode orderedCode = new OrderedCode();
    orderedCode.writeBytes(first);
    byte[] firstEncoded = orderedCode.getEncodedBytes();
    assertArrayEquals(orderedCode.readBytes(), first);

    orderedCode.writeBytes(first);
    orderedCode.writeBytes(second);
    orderedCode.writeBytes(last);
    byte[] allEncoded = orderedCode.getEncodedBytes();
    assertArrayEquals(orderedCode.readBytes(), first);
    assertArrayEquals(orderedCode.readBytes(), second);
    assertArrayEquals(orderedCode.readBytes(), last);

    orderedCode = new OrderedCode(firstEncoded);
    orderedCode.writeBytes(second);
    orderedCode.writeBytes(last);
    assertArrayEquals(orderedCode.getEncodedBytes(), allEncoded);
    assertArrayEquals(orderedCode.readBytes(), first);
    assertArrayEquals(orderedCode.readBytes(), second);
    assertArrayEquals(orderedCode.readBytes(), last);

    orderedCode = new OrderedCode(allEncoded);
    assertArrayEquals(orderedCode.readBytes(), first);
    assertArrayEquals(orderedCode.readBytes(), second);
    assertArrayEquals(orderedCode.readBytes(), last);
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

  /**
   * Assert that encoding the specified long via {@link OrderedCode#writeSignedNumIncreasing(long)}
   * results in the bytes represented by the specified string of hex digits. E.g.
   * assertSignedNumIncreasingEncodingEquals("3fbf", -65) asserts that -65 is encoded as { (byte)
   * 0x3f, (byte) 0xbf }.
   */
  private static void assertSignedNumIncreasingEncodingEquals(
      String expectedHexEncoding, long num) {
    OrderedCode orderedCode = new OrderedCode();
    orderedCode.writeSignedNumIncreasing(num);
    assertEquals(
        "Unexpected encoding for " + num,
        expectedHexEncoding,
        BaseEncoding.base16().lowerCase().encode(orderedCode.getEncodedBytes()));
  }

  /**
   * Assert that encoding various long values via {@link OrderedCode#writeSignedNumIncreasing(long)}
   * produces the expected bytes. Expected byte sequences were generated via the c++ (authoritative)
   * implementation of OrderedCode::WriteSignedNumIncreasing.
   */
  @Test
  public void testSignedNumIncreasing_write() {
    assertSignedNumIncreasingEncodingEquals("003f8000000000000000", Long.MIN_VALUE);
    assertSignedNumIncreasingEncodingEquals("003f8000000000000001", Long.MIN_VALUE + 1);
    assertSignedNumIncreasingEncodingEquals("077fffffff", Integer.MIN_VALUE - 1L);
    assertSignedNumIncreasingEncodingEquals("0780000000", Integer.MIN_VALUE);
    assertSignedNumIncreasingEncodingEquals("0780000001", Integer.MIN_VALUE + 1);
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
    assertSignedNumIncreasingEncodingEquals("f87ffffffe", Integer.MAX_VALUE - 1);
    assertSignedNumIncreasingEncodingEquals("f87fffffff", Integer.MAX_VALUE);
    assertSignedNumIncreasingEncodingEquals("f880000000", Integer.MAX_VALUE + 1L);
    assertSignedNumIncreasingEncodingEquals("ffc07ffffffffffffffe", Long.MAX_VALUE - 1);
    assertSignedNumIncreasingEncodingEquals("ffc07fffffffffffffff", Long.MAX_VALUE);
  }

  /**
   * Convert a string of hex digits (e.g. "3fbf") to a byte[] (e.g. { (byte) 0x3f, (byte) 0xbf }).
   */
  private static byte[] bytesFromHexString(String hexDigits) {
    return new ByteString(BaseEncoding.base16().lowerCase().decode(hexDigits));
  }

  /**
   * Assert that decoding (via {@link OrderedCode#readSignedNumIncreasing()}) the bytes represented
   * by the specified string of hex digits results in the expected long value. E.g.
   * assertDecodedSignedNumIncreasingEquals(-65, "3fbf") asserts that the byte array { (byte) 0x3f,
   * (byte) 0xbf } is decoded as -65.
   */
  private static void assertDecodedSignedNumIncreasingEquals(
      long expectedNum, String encodedHexString) {
    OrderedCode orderedCode = new OrderedCode(bytesFromHexString(encodedHexString));
    assertEquals(
        "Unexpected value when decoding 0x" + encodedHexString,
        expectedNum,
        orderedCode.readSignedNumIncreasing());
    assertFalse(
        "Unexpected encoded bytes remain after decoding 0x" + encodedHexString,
        orderedCode.hasRemainingEncodedBytes());
  }

  /**
   * Assert that decoding various sequences of bytes via {@link
   * OrderedCode#readSignedNumIncreasing()} produces the expected long value. Input byte sequences
   * were generated via the c++ (authoritative) implementation of
   * OrderedCode::WriteSignedNumIncreasing.
   */
  @Test
  public void testSignedNumIncreasing_read() {
    assertDecodedSignedNumIncreasingEquals(Long.MIN_VALUE, "003f8000000000000000");
    assertDecodedSignedNumIncreasingEquals(Long.MIN_VALUE + 1, "003f8000000000000001");
    assertDecodedSignedNumIncreasingEquals(Integer.MIN_VALUE - 1L, "077fffffff");
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
    assertDecodedSignedNumIncreasingEquals(Integer.MAX_VALUE + 1L, "f880000000");
    assertDecodedSignedNumIncreasingEquals(Long.MAX_VALUE - 1, "ffc07ffffffffffffffe");
    assertDecodedSignedNumIncreasingEquals(Long.MAX_VALUE, "ffc07fffffffffffffff");
  }

  /**
   * Assert that encoding (via {@link OrderedCode#writeSignedNumIncreasing(long)}) the specified
   * long value and then decoding (via {@link OrderedCode#readSignedNumIncreasing()}) results in the
   * original value.
   */
  private static void assertSignedNumIncreasingWriteAndReadIsLossless(long num) {
    OrderedCode orderedCode = new OrderedCode();
    orderedCode.writeSignedNumIncreasing(num);
    assertEquals(
        "Unexpected result when decoding writeSignedNumIncreasing(" + num + ")",
        num,
        orderedCode.readSignedNumIncreasing());
    assertFalse(
        "Unexpected remaining encoded bytes after decoding " + num,
        orderedCode.hasRemainingEncodedBytes());
  }

  /**
   * Assert that for various long values, encoding (via {@link
   * OrderedCode#writeSignedNumIncreasing(long)}) and then decoding (via {@link
   * OrderedCode#readSignedNumIncreasing()}) results in the original value.
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

  /** OrderedCode.log2Floor(long) is defined to return -1 given an input of zero. */
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
    byte[] escapeChars =
        new byte[] {
          OrderedCode.ESCAPE1,
          OrderedCode.NULL_CHARACTER,
          OrderedCode.SEPARATOR,
          OrderedCode.ESCAPE2,
          OrderedCode.INFINITY,
          OrderedCode.FF_CHARACTER
        };
    byte[] anotherArray = new byte[] {'a', 'b', 'c', 'd', 'e'};

    OrderedCode orderedCode = new OrderedCode();
    orderedCode.writeTrailingBytes(escapeChars);
    assertArrayEquals(orderedCode.getEncodedBytes(), escapeChars);
    assertArrayEquals(orderedCode.readTrailingBytes(), escapeChars);
    try {
      orderedCode.readInfinity();
      fail("Expected IllegalArgumentException.");
    } catch (IllegalArgumentException e) {
      // expected
    }

    orderedCode = new OrderedCode();
    orderedCode.writeTrailingBytes(anotherArray);
    assertArrayEquals(orderedCode.getEncodedBytes(), anotherArray);
    assertArrayEquals(orderedCode.readTrailingBytes(), anotherArray);
  }

  @Test
  public void testMixedWrite() {
    byte[] first = {'a', 'b', 'c'};
    byte[] second = {'d', 'e', 'f'};
    byte[] last = {'x', 'y', 'z'};
    byte[] escapeChars =
        new byte[] {
          OrderedCode.ESCAPE1,
          OrderedCode.NULL_CHARACTER,
          OrderedCode.SEPARATOR,
          OrderedCode.ESCAPE2,
          OrderedCode.INFINITY,
          OrderedCode.FF_CHARACTER
        };

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
    assertArrayEquals(orderedCode.readBytes(), first);
    assertArrayEquals(orderedCode.readBytes(), second);
    assertFalse(orderedCode.readInfinity());
    assertArrayEquals(orderedCode.readBytes(), last);
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
    assertArrayEquals(orderedCode.getEncodedBytes(), escapeChars);
    assertArrayEquals(orderedCode.readTrailingBytes(), escapeChars);

    orderedCode = new OrderedCode(allEncoded);
    assertArrayEquals(orderedCode.readBytes(), first);
    assertArrayEquals(orderedCode.readBytes(), second);
    assertFalse(orderedCode.readInfinity());
    assertArrayEquals(orderedCode.readBytes(), last);
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
    assertArrayEquals(orderedCode.getEncodedBytes(), escapeChars);
    assertArrayEquals(orderedCode.readTrailingBytes(), escapeChars);
  }

  @Test
  public void testEdgeCases() {
    byte[] ffChar = {OrderedCode.FF_CHARACTER};
    byte[] nullChar = {OrderedCode.NULL_CHARACTER};

    byte[] separatorEncoded = {OrderedCode.ESCAPE1, OrderedCode.SEPARATOR};
    byte[] ffCharEncoded = {OrderedCode.ESCAPE1, OrderedCode.NULL_CHARACTER};
    byte[] nullCharEncoded = {OrderedCode.ESCAPE2, OrderedCode.FF_CHARACTER};
    byte[] infinityEncoded = {OrderedCode.ESCAPE2, OrderedCode.INFINITY};

    OrderedCode orderedCode = new OrderedCode();
    orderedCode.writeBytes(ffChar);
    orderedCode.writeBytes(nullChar);
    orderedCode.writeInfinity();
    assertArrayEquals(
        orderedCode.getEncodedBytes(),
        Bytes.concat(
            ffCharEncoded, separatorEncoded, nullCharEncoded, separatorEncoded, infinityEncoded));
    assertArrayEquals(orderedCode.readBytes(), ffChar);
    assertArrayEquals(orderedCode.readBytes(), nullChar);
    assertTrue(orderedCode.readInfinity());

    orderedCode = new OrderedCode(Bytes.concat(ffCharEncoded, separatorEncoded));
    assertArrayEquals(orderedCode.readBytes(), ffChar);

    orderedCode = new OrderedCode(Bytes.concat(nullCharEncoded, separatorEncoded));
    assertArrayEquals(orderedCode.readBytes(), nullChar);

    byte[] invalidEncodingForRead = {
      OrderedCode.ESCAPE2, OrderedCode.ESCAPE2, OrderedCode.ESCAPE1, OrderedCode.SEPARATOR
    };
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
    byte[] bytes = {'a', 'b', 'c'};
    long number = 12345;

    // Empty
    OrderedCode orderedCode = new OrderedCode();
    assertFalse(orderedCode.hasRemainingEncodedBytes());

    // First and only field of each type.
    orderedCode.writeBytes(bytes);
    assertTrue(orderedCode.hasRemainingEncodedBytes());
    assertArrayEquals(orderedCode.readBytes(), bytes);
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
    assertArrayEquals(orderedCode.readTrailingBytes(), bytes);
    assertFalse(orderedCode.hasRemainingEncodedBytes());

    // Two fields of same type.
    orderedCode.writeBytes(bytes);
    orderedCode.writeBytes(bytes);
    assertTrue(orderedCode.hasRemainingEncodedBytes());
    assertArrayEquals(orderedCode.readBytes(), bytes);
    assertArrayEquals(orderedCode.readBytes(), bytes);
    assertFalse(orderedCode.hasRemainingEncodedBytes());
  }
}
