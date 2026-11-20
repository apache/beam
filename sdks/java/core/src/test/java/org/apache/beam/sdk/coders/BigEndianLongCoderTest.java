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
package org.apache.beam.sdk.coders;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertThrows;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.List;
import org.apache.beam.sdk.testing.CoderProperties;
import org.apache.beam.sdk.util.CoderUtils;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Test case for {@link BigEndianLongCoder}. */
@RunWith(JUnit4.class)
public class BigEndianLongCoderTest {

  private static final Coder<Long> TEST_CODER = BigEndianLongCoder.of();

  private static final List<Long> TEST_VALUES =
      Arrays.asList(
          -11L,
          -3L,
          -1L,
          0L,
          1L,
          5L,
          13L,
          29L,
          Integer.MAX_VALUE + 131L,
          Integer.MIN_VALUE - 29L,
          Long.MAX_VALUE,
          Long.MIN_VALUE);

  @Test
  public void testDecodeEncodeEqual() throws Exception {
    for (Long value : TEST_VALUES) {
      CoderProperties.coderDecodeEncodeEqual(TEST_CODER, value);
    }
  }

  /**
   * Generated data to check that the wire format has not changed. To regenerate, see {@link
   * org.apache.beam.sdk.coders.PrintBase64Encodings}.
   */
  private static final List<String> TEST_ENCODINGS =
      Arrays.asList(
          "__________U",
          "__________0",
          "__________8",
          "AAAAAAAAAAA",
          "AAAAAAAAAAE",
          "AAAAAAAAAAU",
          "AAAAAAAAAA0",
          "AAAAAAAAAB0",
          "AAAAAIAAAII",
          "_____3___-M",
          "f_________8",
          "gAAAAAAAAAA");

  @Test
  public void testWireFormatEncode() throws Exception {
    CoderProperties.coderEncodesBase64(TEST_CODER, TEST_VALUES, TEST_ENCODINGS);
  }

  @Rule public ExpectedException thrown = ExpectedException.none();

  @Test
  public void encodeNullThrowsCoderException() throws Exception {
    thrown.expect(CoderException.class);
    thrown.expectMessage("cannot encode a null Long");

    CoderUtils.encodeToBase64(TEST_CODER, null);
  }

  @Test
  public void testEncodedTypeDescriptor() throws Exception {
    assertThat(TEST_CODER.getEncodedTypeDescriptor(), equalTo(TypeDescriptor.of(Long.class)));
  }

  @Test
  public void testDecodeChunkedInputStreamAndEof() throws Exception {
    for (Long value : TEST_VALUES) {
      byte[] encoded = CoderUtils.encodeToByteArray(TEST_CODER, value);
      InputStream oneByteAtATime =
          new ByteArrayInputStream(encoded) {
            @Override
            public synchronized int read(byte[] b, int off, int len) {
              return super.read(b, off, Math.min(1, len));
            }
          };
      assertThat(TEST_CODER.decode(oneByteAtATime), equalTo(value));
    }

    // Empty stream hits EOF on the first read.
    assertThrows(
        CoderException.class, () -> TEST_CODER.decode(new ByteArrayInputStream(new byte[0])));

    // Truncated stream (3 bytes) hits EOF mid-long.
    byte[] truncated = new byte[] {1, 2, 3};
    assertThrows(
        CoderException.class,
        () ->
            TEST_CODER.decode(
                new ByteArrayInputStream(truncated) {
                  @Override
                  public synchronized int read(byte[] b, int off, int len) {
                    return super.read(b, off, Math.min(1, len));
                  }
                }));
  }

  @Test
  public void testReentrantWriteAndReadCallbacksDoNotCorruptData() throws Exception {
    long outerVal = 0x0102030405060708L;
    long innerVal = 0xF1F2F3F4F5F6F7F8L;
    byte[] expectedOuter = CoderUtils.encodeToByteArray(TEST_CODER, outerVal);
    byte[] expectedInner = CoderUtils.encodeToByteArray(TEST_CODER, innerVal);

    // Reentrant encode + decode triggered mid-write across a chunk boundary (e.g. DataStreams
    // flush).
    ByteArrayOutputStream outerOut = new ByteArrayOutputStream();
    ByteArrayOutputStream innerOut = new ByteArrayOutputStream();
    final Long[] innerDecodedDuringWrite = new Long[1];
    OutputStream chunkedReentrantOut =
        new OutputStream() {
          @Override
          public void write(int b) {
            outerOut.write(b);
          }

          @Override
          public void write(byte[] b, int off, int len) throws IOException {
            int firstHalf = len / 2;
            outerOut.write(b, off, firstHalf);
            TEST_CODER.encode(innerVal, innerOut);
            innerDecodedDuringWrite[0] = TEST_CODER.decode(new ByteArrayInputStream(expectedInner));
            outerOut.write(b, off + firstHalf, len - firstHalf);
          }
        };
    TEST_CODER.encode(outerVal, chunkedReentrantOut);
    assertArrayEquals(expectedOuter, outerOut.toByteArray());
    assertArrayEquals(expectedInner, innerOut.toByteArray());
    assertThat(innerDecodedDuringWrite[0], equalTo(innerVal));

    // Reentrant decode + encode triggered mid-read across a chunk boundary.
    ByteArrayOutputStream encodedDuringRead = new ByteArrayOutputStream();
    final Long[] innerDecodedDuringRead = new Long[1];
    InputStream chunkedReentrantIn =
        new ByteArrayInputStream(expectedOuter) {
          private boolean triggered;

          @Override
          public synchronized int read(byte[] b, int off, int len) {
            if (!triggered) {
              triggered = true;
              int n = super.read(b, off, Math.min(3, len));
              try {
                innerDecodedDuringRead[0] =
                    TEST_CODER.decode(new ByteArrayInputStream(expectedInner));
                TEST_CODER.encode(innerVal, encodedDuringRead);
              } catch (IOException e) {
                throw new RuntimeException(e);
              }
              return n;
            }
            return super.read(b, off, len);
          }
        };
    assertThat(TEST_CODER.decode(chunkedReentrantIn), equalTo(outerVal));
    assertThat(innerDecodedDuringRead[0], equalTo(innerVal));
    assertArrayEquals(expectedInner, encodedDuringRead.toByteArray());
  }
}
