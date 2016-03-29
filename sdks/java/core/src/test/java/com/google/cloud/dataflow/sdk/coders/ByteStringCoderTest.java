/*
 * Copyright (C) 2015 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.google.cloud.dataflow.sdk.coders;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.google.cloud.dataflow.sdk.coders.Coder.Context;
import com.google.cloud.dataflow.sdk.testing.CoderProperties;
import com.google.cloud.dataflow.sdk.util.CoderUtils;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.ByteString;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.util.Arrays;
import java.util.List;

/**
 * Test case for {@link ByteStringCoder}.
 */
@RunWith(JUnit4.class)
public class ByteStringCoderTest {

  private static final ByteStringCoder TEST_CODER = ByteStringCoder.of();

  private static final List<String> TEST_STRING_VALUES = Arrays.asList(
      "", "a", "13", "hello",
      "a longer string with spaces and all that",
      "a string with a \n newline",
      "???????????????");
  private static final ImmutableList<ByteString> TEST_VALUES;
  static {
    ImmutableList.Builder<ByteString> builder = ImmutableList.<ByteString>builder();
    for (String s : TEST_STRING_VALUES) {
      builder.add(ByteString.copyFrom(s.getBytes()));
    }
    TEST_VALUES = builder.build();
  }

  /**
   * Generated data to check that the wire format has not changed. To regenerate, see
   * {@link com.google.cloud.dataflow.sdk.coders.PrintBase64Encodings}.
   */
  private static final List<String> TEST_ENCODINGS = Arrays.asList(
      "",
      "YQ",
      "MTM",
      "aGVsbG8",
      "YSBsb25nZXIgc3RyaW5nIHdpdGggc3BhY2VzIGFuZCBhbGwgdGhhdA",
      "YSBzdHJpbmcgd2l0aCBhIAogbmV3bGluZQ",
      "Pz8_Pz8_Pz8_Pz8_Pz8_");

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  @Test
  public void testDecodeEncodeEqualInAllContexts() throws Exception {
    for (ByteString value : TEST_VALUES) {
      CoderProperties.coderDecodeEncodeEqual(TEST_CODER, value);
    }
  }

  @Test
  public void testWireFormatEncode() throws Exception {
    CoderProperties.coderEncodesBase64(TEST_CODER, TEST_VALUES, TEST_ENCODINGS);
  }

  @Test
  public void testCoderDeterministic() throws Throwable {
    TEST_CODER.verifyDeterministic();
  }

  @Test
  public void testConsistentWithEquals() {
    assertTrue(TEST_CODER.consistentWithEquals());
  }

  @Test
  public void testEncodeNullThrowsCoderException() throws Exception {
    thrown.expect(CoderException.class);
    thrown.expectMessage("cannot encode a null ByteString");

    CoderUtils.encodeToBase64(TEST_CODER, null);
  }

  @Test
  public void testNestedCoding() throws Throwable {
    Coder<List<ByteString>> listCoder = ListCoder.of(TEST_CODER);
    CoderProperties.coderDecodeEncodeContentsEqual(listCoder, TEST_VALUES);
    CoderProperties.coderDecodeEncodeContentsInSameOrder(listCoder, TEST_VALUES);
  }

  @Test
  public void testEncodedElementByteSizeInAllContexts() throws Throwable {
    for (Context context : CoderProperties.ALL_CONTEXTS) {
      for (ByteString value : TEST_VALUES) {
        byte[] encoded = CoderUtils.encodeToByteArray(TEST_CODER, value, context);
        assertEquals(encoded.length, TEST_CODER.getEncodedElementByteSize(value, context));
      }
    }
  }
}
