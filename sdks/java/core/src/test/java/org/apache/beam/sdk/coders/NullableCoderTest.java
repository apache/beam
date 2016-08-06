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

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.theInstance;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import org.apache.beam.sdk.testing.CoderProperties;

import com.google.common.collect.ImmutableList;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.Arrays;
import java.util.List;

/** Unit tests for {@link NullableCoder}. */
@RunWith(JUnit4.class)
public class NullableCoderTest {

  private static final Coder<String> TEST_CODER = NullableCoder.of(StringUtf8Coder.of());

  private static final List<String> TEST_VALUES = Arrays.<String>asList(
      "", "a", "13", "hello",
      null,
      "a longer string with spaces and all that",
      "a string with a \n newline",
      "スタリング");

  @Test
  public void testDecodeEncodeContentsInSameOrder() throws Exception {
    for (String value : TEST_VALUES) {
      CoderProperties.coderDecodeEncodeEqual(TEST_CODER, value);
    }
  }

  @Test
  public void testCoderSerializable() throws Exception {
    CoderProperties.coderSerializable(TEST_CODER);
  }

  // If this changes, it implies the binary format has changed.
  private static final String EXPECTED_ENCODING_ID = "";

  @Test
  public void testEncodingId() throws Exception {
    CoderProperties.coderHasEncodingId(TEST_CODER, EXPECTED_ENCODING_ID);
  }

  /**
   * Generated data to check that the wire format has not changed. To regenerate, see
   * {@code PrintBase64Encodings}.
   *
   * @see org.apache.beam.sdk.coders.PrintBase64Encodings
   */
  private static final List<String> TEST_ENCODINGS = Arrays.asList(
      "AQA",
      "AQFh",
      "AQIxMw",
      "AQVoZWxsbw",
      "AA",
      "AShhIGxvbmdlciBzdHJpbmcgd2l0aCBzcGFjZXMgYW5kIGFsbCB0aGF0",
      "ARlhIHN0cmluZyB3aXRoIGEgCiBuZXdsaW5l",
      "AQ_jgrnjgr_jg6rjg7PjgrA");

  @Test
  public void testWireFormatEncode() throws Exception {
    CoderProperties.coderEncodesBase64(TEST_CODER, TEST_VALUES, TEST_ENCODINGS);
  }

  @Test
  public void testEncodedSize() throws Exception {
    NullableCoder<Double> coder = NullableCoder.of(DoubleCoder.of());
    assertEquals(1, coder.getEncodedElementByteSize(null, Coder.Context.OUTER));
    assertEquals(9, coder.getEncodedElementByteSize(5.0, Coder.Context.OUTER));
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
  public void testObserverIsAlwaysCheapForNullValues() throws Exception {
    NullableCoder<List<String>> coder = NullableCoder.of(ListCoder.of(StringUtf8Coder.of()));
    assertTrue(coder.isRegisterByteSizeObserverCheap(null, Coder.Context.OUTER));
  }

  @Test
  public void testStructuralValueConsistentWithEquals() throws Exception {
    CoderProperties.structuralValueConsistentWithEquals(TEST_CODER, null, null);
  }

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  @Test
  public void testDecodingError() throws Exception {
    thrown.expect(CoderException.class);
    thrown.expectMessage(equalTo("NullableCoder expects either a byte valued 0 (null) "
        + "or 1 (present), got 5"));

    InputStream input = new ByteArrayInputStream(new byte[] {5});
    TEST_CODER.decode(input, Coder.Context.OUTER);
  }

  @Test
  public void testNestedNullableCoder() {
    NullableCoder<Double> coder = NullableCoder.of(DoubleCoder.of());
    assertThat(NullableCoder.of(coder), theInstance(coder));
  }
}
