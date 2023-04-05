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
package org.apache.beam.sdk.util;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertEquals;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.IOException;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Unit tests for {@link VarInt}. */
@RunWith(JUnit4.class)
public class VarIntTest {
  @Rule public final ExpectedException thrown = ExpectedException.none();

  // Long values to check for boundary cases.
  private static final long[] LONG_VALUES = {
    0,
    1,
    127,
    128,
    16383,
    16384,
    2097151,
    2097152,
    268435455,
    268435456,
    34359738367L,
    34359738368L,
    9223372036854775807L,
    -9223372036854775808L,
    -1,
  };

  // VarInt encoding of the above VALUES.
  private static final byte[][] LONG_ENCODED = {
    // 0
    {0x00},
    // 1
    {0x01},
    // 127
    {0x7f},
    // 128
    {(byte) 0x80, 0x01},
    // 16383
    {(byte) 0xff, 0x7f},
    // 16834
    {(byte) 0x80, (byte) 0x80, 0x01},
    // 2097151
    {(byte) 0xff, (byte) 0xff, 0x7f},
    // 2097152
    {(byte) 0x80, (byte) 0x80, (byte) 0x80, 0x01},
    // 268435455
    {(byte) 0xff, (byte) 0xff, (byte) 0xff, 0x7f},
    // 268435456
    {(byte) 0x80, (byte) 0x80, (byte) 0x80, (byte) 0x80, 0x01},
    // 34359738367
    {(byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xff, 0x7f},
    // 34359738368
    {(byte) 0x80, (byte) 0x80, (byte) 0x80, (byte) 0x80, (byte) 0x80, 0x01},
    // 9223372036854775807
    {
      (byte) 0xff,
      (byte) 0xff,
      (byte) 0xff,
      (byte) 0xff,
      (byte) 0xff,
      (byte) 0xff,
      (byte) 0xff,
      (byte) 0xff,
      (byte) 0x7f
    },
    // -9223372036854775808L
    {
      (byte) 0x80,
      (byte) 0x80,
      (byte) 0x80,
      (byte) 0x80,
      (byte) 0x80,
      (byte) 0x80,
      (byte) 0x80,
      (byte) 0x80,
      (byte) 0x80,
      0x01
    },
    // -1
    {
      (byte) 0xff,
      (byte) 0xff,
      (byte) 0xff,
      (byte) 0xff,
      (byte) 0xff,
      (byte) 0xff,
      (byte) 0xff,
      (byte) 0xff,
      (byte) 0xff,
      0x01
    }
  };

  // Integer values to check for boundary cases.
  private static final int[] INT_VALUES = {
    0,
    1,
    127,
    128,
    16383,
    16384,
    2097151,
    2097152,
    268435455,
    268435456,
    2147483647,
    -2147483648,
    -1,
  };

  // VarInt encoding of the above VALUES.
  private static final byte[][] INT_ENCODED = {
    // 0
    {(byte) 0x00},
    // 1
    {(byte) 0x01},
    // 127
    {(byte) 0x7f},
    // 128
    {(byte) 0x80, (byte) 0x01},
    // 16383
    {(byte) 0xff, (byte) 0x7f},
    // 16834
    {(byte) 0x80, (byte) 0x80, (byte) 0x01},
    // 2097151
    {(byte) 0xff, (byte) 0xff, (byte) 0x7f},
    // 2097152
    {(byte) 0x80, (byte) 0x80, (byte) 0x80, (byte) 0x01},
    // 268435455
    {(byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0x7f},
    // 268435456
    {(byte) 0x80, (byte) 0x80, (byte) 0x80, (byte) 0x80, (byte) 0x01},
    // 2147483647
    {(byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0x07},
    // -2147483648
    {(byte) 0x80, (byte) 0x80, (byte) 0x80, (byte) 0x80, (byte) 0x08},
    // -1
    {(byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0x0f}
  };

  private static byte[] encodeInt(int v) throws IOException {
    ByteArrayOutputStream stream = new ByteArrayOutputStream();
    VarInt.encode(v, stream);
    return stream.toByteArray();
  }

  private static byte[] encodeLong(long v) throws IOException {
    ByteArrayOutputStream stream = new ByteArrayOutputStream();
    VarInt.encode(v, stream);
    return stream.toByteArray();
  }

  private static int decodeInt(byte[] encoded) throws IOException {
    ByteArrayInputStream stream = new ByteArrayInputStream(encoded);
    return VarInt.decodeInt(stream);
  }

  private static long decodeLong(byte[] encoded) throws IOException {
    ByteArrayInputStream stream = new ByteArrayInputStream(encoded);
    return VarInt.decodeLong(stream);
  }

  @Test
  public void decodeValues() throws IOException {
    assertEquals(LONG_VALUES.length, LONG_ENCODED.length);
    for (int i = 0; i < LONG_ENCODED.length; ++i) {
      ByteArrayInputStream stream = new ByteArrayInputStream(LONG_ENCODED[i]);
      long parsed = VarInt.decodeLong(stream);
      assertEquals(LONG_VALUES[i], parsed);
      assertEquals(-1, stream.read());
    }

    assertEquals(INT_VALUES.length, INT_ENCODED.length);
    for (int i = 0; i < INT_ENCODED.length; ++i) {
      ByteArrayInputStream stream = new ByteArrayInputStream(INT_ENCODED[i]);
      int parsed = VarInt.decodeInt(stream);
      assertEquals(INT_VALUES[i], parsed);
      assertEquals(-1, stream.read());
    }
  }

  @Test
  public void encodeValuesAndGetLength() throws IOException {
    assertEquals(LONG_VALUES.length, LONG_ENCODED.length);
    for (int i = 0; i < LONG_VALUES.length; ++i) {
      byte[] encoded = encodeLong(LONG_VALUES[i]);
      assertThat(encoded, equalTo(LONG_ENCODED[i]));
      assertEquals(LONG_ENCODED[i].length, VarInt.getLength(LONG_VALUES[i]));
    }

    assertEquals(INT_VALUES.length, INT_ENCODED.length);
    for (int i = 0; i < INT_VALUES.length; ++i) {
      byte[] encoded = encodeInt(INT_VALUES[i]);
      assertThat(encoded, equalTo(INT_ENCODED[i]));
      assertEquals(INT_ENCODED[i].length, VarInt.getLength(INT_VALUES[i]));
    }
  }

  @Test
  public void decodeThrowsExceptionForOverflow() throws IOException {
    final byte[] tooLargeNumber = {
      (byte) 0xff,
      (byte) 0xff,
      (byte) 0xff,
      (byte) 0xff,
      (byte) 0xff,
      (byte) 0xff,
      (byte) 0xff,
      (byte) 0xff,
      (byte) 0xff,
      0x02
    };

    thrown.expect(IOException.class);
    decodeLong(tooLargeNumber);
  }

  @Test
  public void decodeThrowsExceptionForIntOverflow() throws IOException {
    byte[] encoded = encodeLong(1L << 32);

    thrown.expect(IOException.class);
    decodeInt(encoded);
  }

  @Test
  public void decodeThrowsExceptionForIntUnderflow() throws IOException {
    byte[] encoded = encodeLong(-1);

    thrown.expect(IOException.class);
    decodeInt(encoded);
  }

  @Test
  public void decodeThrowsExceptionForNonterminated() throws IOException {
    final byte[] nonTerminatedNumber = {(byte) 0xff, (byte) 0xff};

    thrown.expect(IOException.class);
    decodeLong(nonTerminatedNumber);
  }

  @Test
  public void decodeParsesEncodedValues() throws IOException {
    ByteArrayOutputStream outStream = new ByteArrayOutputStream();
    for (int i = 10; i < Integer.MAX_VALUE; i = (int) (i * 1.1)) {
      VarInt.encode(i, outStream);
      VarInt.encode(-i, outStream);
    }
    for (long i = 10; i < Long.MAX_VALUE; i = (long) (i * 1.1)) {
      VarInt.encode(i, outStream);
      VarInt.encode(-i, outStream);
    }

    ByteArrayInputStream inStream = new ByteArrayInputStream(outStream.toByteArray());
    for (int i = 10; i < Integer.MAX_VALUE; i = (int) (i * 1.1)) {
      assertEquals(i, VarInt.decodeInt(inStream));
      assertEquals(-i, VarInt.decodeInt(inStream));
    }
    for (long i = 10; i < Long.MAX_VALUE; i = (long) (i * 1.1)) {
      assertEquals(i, VarInt.decodeLong(inStream));
      assertEquals(-i, VarInt.decodeLong(inStream));
    }
  }

  @Test
  public void endOfFileThrowsException() throws Exception {
    ByteArrayInputStream inStream = new ByteArrayInputStream(new byte[0]);
    thrown.expect(EOFException.class);
    VarInt.decodeInt(inStream);
  }

  @Test
  public void unterminatedThrowsException() throws Exception {
    byte[] e = encodeLong(Long.MAX_VALUE);
    byte[] s = new byte[1];
    s[0] = e[0];
    ByteArrayInputStream inStream = new ByteArrayInputStream(s);
    thrown.expect(IOException.class);
    VarInt.decodeInt(inStream);
  }
}
