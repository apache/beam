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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.google.common.collect.ImmutableList;
import java.util.Arrays;
import java.util.List;
import org.apache.beam.sdk.testing.CoderProperties;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link LengthPrefixCoder}. */
@RunWith(JUnit4.class)
public class LengthPrefixCoderTest {
  private static final StructuredCoder<byte[]> TEST_CODER =
      LengthPrefixCoder.of(ByteArrayCoder.of());

  private static final List<byte[]> TEST_VALUES = Arrays.asList(
    new byte[]{ 0xa, 0xb, 0xc },
    new byte[]{ 0xd, 0x3 },
    new byte[]{ 0xd, 0xe },
    new byte[]{ });

  /**
   * Generated data to check that the wire format has not changed. To regenerate, see
   * {@link org.apache.beam.sdk.coders.PrintBase64Encodings}.
   */
  private static final List<String> TEST_ENCODINGS = ImmutableList.of(
      "AwoLDA",
      "Ag0D",
      "Ag0O",
      "AA");

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
    assertEquals(4L,
        TEST_CODER.getEncodedElementByteSize(TEST_VALUES.get(0), Coder.Context.NESTED));
    assertEquals(4L,
        TEST_CODER.getEncodedElementByteSize(TEST_VALUES.get(0), Coder.Context.OUTER));
  }

  @Test
  public void testObserverIsCheap() throws Exception {
    NullableCoder<Double> coder = NullableCoder.of(DoubleCoder.of());
    assertTrue(coder.isRegisterByteSizeObserverCheap(5.0, Coder.Context.OUTER));
  }

  @Test
  public void testObserverIsNotCheap() throws Exception {
    NullableCoder<List<String>> coder = NullableCoder.of(ListCoder.of(StringUtf8Coder.of()));
    assertFalse(coder.isRegisterByteSizeObserverCheap(
        ImmutableList.of("hi", "test"), Coder.Context.OUTER));
  }

  @Test
  public void testDecodeEncodeEquals() throws Exception {
    for (byte[] value : TEST_VALUES) {
      CoderProperties.coderDecodeEncodeEqual(TEST_CODER, value);
    }
  }

  @Test
  public void testRegisterByteSizeObserver() throws Exception {
    CoderProperties.testByteCount(TEST_CODER, Coder.Context.OUTER,
        new byte[][]{{ 0xa, 0xb, 0xc }});

    CoderProperties.testByteCount(TEST_CODER, Coder.Context.NESTED,
        new byte[][]{{ 0xa, 0xb, 0xc }, {}, {}, { 0xd, 0xe }, {}});
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
}
