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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import org.apache.beam.sdk.testing.CoderProperties;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.util.CoderUtils;
import org.apache.beam.sdk.util.StreamUtils;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link LengthPrefixCoder}. */
@RunWith(JUnit4.class)
public class LengthPrefixCoderTest {
  private static final StructuredCoder<byte[]> TEST_CODER =
      LengthPrefixCoder.of(ByteArrayCoder.of());

  private static final List<byte[]> TEST_VALUES =
      Arrays.asList(
          new byte[] {0xa, 0xb, 0xc}, new byte[] {0xd, 0x3}, new byte[] {0xd, 0xe}, new byte[] {});

  /**
   * Generated data to check that the wire format has not changed. To regenerate, see {@link
   * org.apache.beam.sdk.coders.PrintBase64Encodings}.
   */
  private static final ImmutableList<String> TEST_ENCODINGS =
      ImmutableList.of("AwoLDA", "Ag0D", "Ag0O", "AA");

  @Test
  public void testCoderSerializable() throws Exception {
    CoderProperties.coderSerializable(TEST_CODER);
  }

  @Test
  public void testCoderIsSerializableWithWellKnownCoderType() throws Exception {
    CoderProperties.coderSerializable(LengthPrefixCoder.of(GlobalWindow.Coder.INSTANCE));
  }

  @Test
  public void testEncodedSize() throws Exception {
    assertEquals(5L, TEST_CODER.getEncodedElementByteSize(TEST_VALUES.get(0)));
  }

  @Test
  public void testObserverIsCheap() throws Exception {
    LengthPrefixCoder<Double> coder = LengthPrefixCoder.of(DoubleCoder.of());
    assertTrue(coder.isRegisterByteSizeObserverCheap(5.0));
  }

  @Test
  public void testObserverIsNotCheap() throws Exception {
    LengthPrefixCoder<List<String>> coder =
        LengthPrefixCoder.of(ListCoder.of(StringUtf8Coder.of()));
    assertFalse(coder.isRegisterByteSizeObserverCheap(ImmutableList.of("hi", "test")));
  }

  @Test
  public void testDecodeEncodeEquals() throws Exception {
    for (byte[] value : TEST_VALUES) {
      CoderProperties.coderDecodeEncodeEqual(TEST_CODER, value);
    }
  }

  @Test
  public void testRegisterByteSizeObserver() throws Exception {
    CoderProperties.testByteCount(
        LengthPrefixCoder.of(VarIntCoder.of()), Coder.Context.NESTED, new Integer[] {0, 10, 1000});
  }

  @Test
  public void testStructuralValueConsistentWithEquals() throws Exception {
    for (byte[] value1 : TEST_VALUES) {
      for (byte[] value2 : TEST_VALUES) {
        CoderProperties.structuralValueConsistentWithEquals(TEST_CODER, value1, value2);
      }
    }
  }

  @Test
  public void testWireFormatEncode() throws Exception {
    CoderProperties.coderEncodesBase64(TEST_CODER, TEST_VALUES, TEST_ENCODINGS);
  }

  @Test
  public void testMultiCoderCycle() throws Exception {
    LengthPrefixCoder<Long> lengthPrefixedValueCoder =
        LengthPrefixCoder.of(BigEndianLongCoder.of());

    LengthPrefixCoder<byte[]> lengthPrefixedBytesCoder = LengthPrefixCoder.of(ByteArrayCoder.of());

    // [0x08, 0, 0, 0, 0, 0, 0, 0, 0x16]
    byte[] userEncoded = CoderUtils.encodeToByteArray(lengthPrefixedValueCoder, 22L);

    // [0, 0, 0, 0, 0, 0, 0, 0x16]
    byte[] decodedToBytes = CoderUtils.decodeFromByteArray(lengthPrefixedBytesCoder, userEncoded);

    // [0x08, 0, 0, 0, 0, 0, 0, 0, 0x16]
    byte[] reencodedBytes = CoderUtils.encodeToByteArray(lengthPrefixedBytesCoder, decodedToBytes);

    long userDecoded = CoderUtils.decodeFromByteArray(lengthPrefixedValueCoder, reencodedBytes);

    assertFalse(
        "Length-prefix decoding to bytes should drop the length",
        Arrays.equals(userEncoded, decodedToBytes));
    assertArrayEquals(userEncoded, reencodedBytes);
    assertEquals(22L, userDecoded);
  }

  @Test
  public void testByteArrayAndStringFastPath() throws Exception {
    LengthPrefixCoder<byte[]> bytesCoder = LengthPrefixCoder.of(ByteArrayCoder.of());
    ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();
    bytesCoder.encode(new byte[] {1, 2}, bytesOut);
    bytesCoder.encode(new byte[0], bytesOut);
    bytesCoder.encode(new byte[] {3, 4, 5}, bytesOut);
    ByteArrayInputStream bytesIn = new ByteArrayInputStream(bytesOut.toByteArray());
    assertArrayEquals(new byte[] {1, 2}, bytesCoder.decode(bytesIn));
    assertArrayEquals(new byte[0], bytesCoder.decode(bytesIn));
    assertArrayEquals(new byte[] {3, 4, 5}, bytesCoder.decode(bytesIn));
    assertEquals(0, bytesIn.available());
    assertThrows(CoderException.class, () -> CoderUtils.encodeToByteArray(bytesCoder, null));

    LengthPrefixCoder<String> stringCoder = LengthPrefixCoder.of(StringUtf8Coder.of());
    String unicode = "unicode-\u00e9-\u2603-\ud83d\ude00";
    assertArrayEquals(
        CoderUtils.encodeToByteArray(StringUtf8Coder.of(), unicode, Coder.Context.NESTED),
        CoderUtils.encodeToByteArray(stringCoder, unicode));
    ByteArrayOutputStream strOut = new ByteArrayOutputStream();
    stringCoder.encode("alpha", strOut);
    stringCoder.encode("", strOut);
    stringCoder.encode(unicode, strOut);
    ByteArrayInputStream strIn = new ByteArrayInputStream(strOut.toByteArray());
    assertEquals("alpha", stringCoder.decode(strIn));
    assertEquals("", stringCoder.decode(strIn));
    assertEquals(unicode, stringCoder.decode(strIn));
    assertEquals(0, strIn.available());
    assertThrows(CoderException.class, () -> CoderUtils.encodeToByteArray(stringCoder, null));
  }

  @Test
  public void testNestedLengthPrefixCoderReentrancy() throws Exception {
    LengthPrefixCoder<KV<String, Long>> nestedCoder =
        LengthPrefixCoder.of(
            KvCoder.of(
                LengthPrefixCoder.of(StringUtf8Coder.of()),
                LengthPrefixCoder.of(BigEndianLongCoder.of())));
    CoderProperties.coderDecodeEncodeEqual(nestedCoder, KV.of("reentrant-key", 123456789L));
  }

  @Test
  public void testLargePayloadExceedingPoolCap() throws Exception {
    LengthPrefixCoder<KV<byte[], Long>> coder =
        LengthPrefixCoder.of(KvCoder.of(ByteArrayCoder.of(), BigEndianLongCoder.of()));
    byte[] large = new byte[128 * 1024];
    Arrays.fill(large, (byte) 0x5a);
    CoderProperties.coderDecodeEncodeEqual(coder, KV.of(large, 42L));
    CoderProperties.coderDecodeEncodeEqual(coder, KV.of(new byte[] {1, 2, 3}, 7L));
  }

  @Test
  public void testPooledBufferResetAfterInnerEncodeException() throws Exception {
    Coder<String> failOnceCoder =
        new CustomCoder<String>() {
          @Override
          public void encode(String value, OutputStream outStream) throws IOException {
            outStream.write("stale-prefix-".getBytes(StandardCharsets.UTF_8));
            if ("fail".equals(value)) {
              throw new CoderException("simulated inner failure");
            }
            outStream.write(value.getBytes(StandardCharsets.UTF_8));
          }

          @Override
          public String decode(InputStream inStream) throws IOException {
            return new String(StreamUtils.getBytesWithoutClosing(inStream), StandardCharsets.UTF_8);
          }
        };
    LengthPrefixCoder<String> coder = LengthPrefixCoder.of(failOnceCoder);

    assertThrows(CoderException.class, () -> CoderUtils.encodeToByteArray(coder, "fail"));

    // Next encode reuses the pooled buffer; stale bytes from the failed encode must not leak.
    byte[] encoded = CoderUtils.encodeToByteArray(coder, "ok");
    assertEquals("stale-prefix-ok", CoderUtils.decodeFromByteArray(coder, encoded));
  }
}
