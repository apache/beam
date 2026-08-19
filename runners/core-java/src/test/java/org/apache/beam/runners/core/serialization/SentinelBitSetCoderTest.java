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
package org.apache.beam.runners.core.serialization;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

import java.util.Arrays;
import java.util.BitSet;
import java.util.List;
import org.apache.beam.sdk.coders.BitSetCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.Coder.Context;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.testing.CoderProperties;
import org.apache.beam.sdk.util.CoderUtils;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link SentinelBitSetCoder}. */
@RunWith(JUnit4.class)
public class SentinelBitSetCoderTest {

  private static final Coder<BitSet> TEST_CODER = SentinelBitSetCoder.of();

  private static final List<BitSet> TEST_VALUES =
      Arrays.asList(
          BitSet.valueOf(new byte[] {0xa, 0xb, 0xc}),
          BitSet.valueOf(new byte[] {0xd, 0x3}),
          BitSet.valueOf(new byte[] {0xd, 0xe}),
          BitSet.valueOf(new byte[] {0}),
          BitSet.valueOf(new byte[] {}));

  @Test
  public void testDecodeEncodeEquals() throws Exception {
    for (BitSet value : TEST_VALUES) {
      CoderProperties.coderDecodeEncodeEqual(TEST_CODER, value);
    }
  }

  @Test
  public void testRegisterByteSizeObserver() throws Exception {
    CoderProperties.testByteCount(
        SentinelBitSetCoder.of(), Coder.Context.OUTER, TEST_VALUES.toArray(new BitSet[] {}));

    CoderProperties.testByteCount(
        SentinelBitSetCoder.of(), Coder.Context.NESTED, TEST_VALUES.toArray(new BitSet[] {}));
  }

  @Test
  public void testStructuralValueConsistentWithEquals() throws Exception {
    for (BitSet value1 : TEST_VALUES) {
      for (BitSet value2 : TEST_VALUES) {
        CoderProperties.structuralValueConsistentWithEquals(TEST_CODER, value1, value2);
      }
    }
  }

  /**
   * Generated data to check that the wire format has not changed. "CgsM" is {0xa, 0xb, 0xc} "DQM"
   * is {0xd, 0x3} "DQ4" is {0xd, 0xe} "AA==" is {0} (Sentinel for empty BitSet)
   */
  private static final List<String> TEST_ENCODINGS =
      Arrays.asList("CgsM", "DQM", "DQ4", "AA", "AA");

  @Test
  public void testWireFormatEncode() throws Exception {
    CoderProperties.coderEncodesBase64(TEST_CODER, TEST_VALUES, TEST_ENCODINGS);
  }

  @Rule public ExpectedException thrown = ExpectedException.none();

  @Test
  public void encodeNullThrowsCoderException() throws Exception {
    thrown.expect(CoderException.class);
    thrown.expectMessage("cannot encode a null BitSet");

    CoderUtils.encodeToBase64(TEST_CODER, null);
  }

  @Test
  public void testEncodedTypeDescriptor() throws Exception {
    assertThat(TEST_CODER.getEncodedTypeDescriptor(), equalTo(TypeDescriptor.of(BitSet.class)));
  }

  @Test
  public void testEmptyBitSetEncoding() throws Exception {
    {
      byte[] encoded = CoderUtils.encodeToByteArray(TEST_CODER, new BitSet());
      // ByteArrayCoder in OUTER context encodes as is.
      assertThat(encoded, equalTo(new byte[] {0}));
    }
    {
      byte[] encoded = CoderUtils.encodeToByteArray(TEST_CODER, new BitSet(), Context.NESTED);
      // Varint length = 1, data = 1
      assertThat(encoded, equalTo(new byte[] {1, 0}));
    }
  }

  @Test
  public void testCompatibilityWithBitSetCoder() throws Exception {
    BitSetCoder bitSetCoder = BitSetCoder.of();
    SentinelBitSetCoder sentinelCoder = SentinelBitSetCoder.of();

    for (BitSet bitset : TEST_VALUES) {
      for (Coder.Context context : Arrays.asList(Coder.Context.OUTER, Coder.Context.NESTED)) {
        // Test SentinelBitSetCoder can decode bytes encoded by BitSetCoder
        {
          byte[] encodedByBitSet = CoderUtils.encodeToByteArray(bitSetCoder, bitset, context);
          BitSet decodedBySentinel =
              CoderUtils.decodeFromByteArray(sentinelCoder, encodedByBitSet, context);
          assertThat(
              "Decoding BitSetCoder encoded value with context " + context,
              decodedBySentinel,
              equalTo(bitset));
        }

        // Test BitSetCoder can decode bytes encoded by SentinelBitSetCoder
        {
          byte[] encodedBySentinel = CoderUtils.encodeToByteArray(sentinelCoder, bitset, context);
          BitSet decodedByBitSet =
              CoderUtils.decodeFromByteArray(bitSetCoder, encodedBySentinel, context);
          assertThat(
              "Decoding SentinelBitSetCoder encoded value with context " + context,
              decodedByBitSet,
              equalTo(bitset));
        }
      }
    }
  }
}
