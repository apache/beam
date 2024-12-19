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
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import org.apache.beam.sdk.testing.CoderProperties;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Unit tests for {@link OptionalCoder}. */
@RunWith(JUnit4.class)
public class OptionalCoderTest {

  private static final Coder<Optional<String>> TEST_CODER = OptionalCoder.of(StringUtf8Coder.of());

  private static final List<Optional<String>> TEST_VALUES =
      Arrays.asList(
          Optional.of(""),
          Optional.of("a"),
          Optional.of("13"),
          Optional.of("hello"),
          Optional.empty(),
          Optional.of("a longer string with spaces and all that"),
          Optional.of("a string with a \n newline"),
          Optional.of("スタリング"));

  @Test
  public void testDecodeEncodeContentsInSameOrder() throws Exception {
    for (Optional<String> value : TEST_VALUES) {
      CoderProperties.coderDecodeEncodeEqual(TEST_CODER, value);
    }
  }

  @Test
  public void testCoderSerializable() throws Exception {
    CoderProperties.coderSerializable(TEST_CODER);
  }

  @Test
  public void testCoderIsSerializableWithWellKnownCoderType() throws Exception {
    CoderProperties.coderSerializable(OptionalCoder.of(GlobalWindow.Coder.INSTANCE));
  }

  /**
   * Generated data to check that the wire format has not changed. To regenerate, see {@code
   * PrintBase64Encodings}.
   *
   * @see PrintBase64Encodings
   */
  private static final List<String> TEST_ENCODINGS =
      Arrays.asList(
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
    OptionalCoder<Double> coder = OptionalCoder.of(DoubleCoder.of());
    assertEquals(1, coder.getEncodedElementByteSize(Optional.empty()));
    assertEquals(9, coder.getEncodedElementByteSize(Optional.of(5.0)));
  }

  @Test
  public void testEncodedSizeNested() throws Exception {
    OptionalCoder<String> varLenCoder = OptionalCoder.of(StringUtf8Coder.of());
    assertEquals(1, varLenCoder.getEncodedElementByteSize(Optional.empty()));
    assertEquals(6, varLenCoder.getEncodedElementByteSize(Optional.of("spam")));
  }

  @Test
  public void testObserverIsCheap() throws Exception {
    OptionalCoder<Double> coder = OptionalCoder.of(DoubleCoder.of());
    assertTrue(coder.isRegisterByteSizeObserverCheap(Optional.of(5.0)));
  }

  @Test
  public void testObserverIsNotCheap() throws Exception {
    OptionalCoder<List<String>> coder = OptionalCoder.of(ListCoder.of(StringUtf8Coder.of()));
    assertFalse(coder.isRegisterByteSizeObserverCheap(Optional.of(ImmutableList.of("hi", "test"))));
  }

  @Test
  public void testObserverIsAlwaysCheapForEmptyValues() throws Exception {
    OptionalCoder<List<String>> coder = OptionalCoder.of(ListCoder.of(StringUtf8Coder.of()));
    assertTrue(coder.isRegisterByteSizeObserverCheap(Optional.empty()));
  }

  @Test
  public void testStructuralValueConsistentWithEquals() throws Exception {
    CoderProperties.structuralValueConsistentWithEquals(
        TEST_CODER, Optional.empty(), Optional.empty());
  }

  @Rule public ExpectedException thrown = ExpectedException.none();

  @Test
  public void testDecodingError() throws Exception {
    thrown.expect(CoderException.class);
    thrown.expectMessage(
        equalTo("OptionalCoder expects either a byte valued 0 (empty) " + "or 1 (present), got 5"));

    InputStream input = new ByteArrayInputStream(new byte[] {5});
    TEST_CODER.decode(input);
  }

  @Test
  public void testEncodedTypeDescriptor() throws Exception {
    TypeDescriptor<Optional<String>> expectedTypeDescriptor =
        new TypeDescriptor<Optional<String>>() {};
    assertThat(TEST_CODER.getEncodedTypeDescriptor(), equalTo(expectedTypeDescriptor));
  }
}
