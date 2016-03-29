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

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertThat;

import com.google.cloud.dataflow.sdk.testing.CoderProperties;
import com.google.cloud.dataflow.sdk.util.CoderUtils;
import com.google.cloud.dataflow.sdk.util.common.CounterTestUtils;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

/**
 * Unit tests for {@link ByteArrayCoder}.
 */
@RunWith(JUnit4.class)
public class ByteArrayCoderTest {

  private static final ByteArrayCoder TEST_CODER = ByteArrayCoder.of();

  private static final List<byte[]> TEST_VALUES = Arrays.asList(
    new byte[]{0xa, 0xb, 0xc},
    new byte[]{0xd, 0x3},
    new byte[]{0xd, 0xe},
    new byte[]{});

  @Test
  public void testDecodeEncodeEquals() throws Exception {
    for (byte[] value : TEST_VALUES) {
      CoderProperties.coderDecodeEncodeEqual(TEST_CODER, value);
    }
  }

  @Test
  public void testRegisterByteSizeObserver() throws Exception {
    CounterTestUtils.testByteCount(ByteArrayCoder.of(), Coder.Context.OUTER,
                                   new byte[][]{{ 0xa, 0xb, 0xc }});

    CounterTestUtils.testByteCount(ByteArrayCoder.of(), Coder.Context.NESTED,
                                   new byte[][]{{ 0xa, 0xb, 0xc }, {}, {}, { 0xd, 0xe }, {}});
  }

  @Test
  public void testStructuralValueConsistentWithEquals() throws Exception {
    // We know that byte array coders are NOT compatible with equals
    // (aka injective w.r.t. Object.equals)
    for (byte[] value1 : TEST_VALUES) {
      for (byte[] value2 : TEST_VALUES) {
        CoderProperties.structuralValueConsistentWithEquals(TEST_CODER, value1, value2);
      }
    }
  }

  @Test
  public void testEncodeThenMutate() throws Exception {
    byte[] input = { 0x7, 0x3, 0xA, 0xf };
    byte[] encoded = CoderUtils.encodeToByteArray(TEST_CODER, input);
    input[1] = 0x9;
    byte[] decoded = CoderUtils.decodeFromByteArray(TEST_CODER, encoded);

    // now that I have mutated the input, the output should NOT match
    assertThat(input, not(equalTo(decoded)));
  }

  @Test
  public void testEncodeAndOwn() throws Exception {
    for (byte[] value : TEST_VALUES) {
      byte[] encodedSlow = CoderUtils.encodeToByteArray(TEST_CODER, value);
      byte[] encodedFast = encodeToByteArrayAndOwn(TEST_CODER, value);
      assertThat(encodedSlow, equalTo(encodedFast));
    }
  }

  private static byte[] encodeToByteArrayAndOwn(ByteArrayCoder coder, byte[] value)
      throws IOException {
    return encodeToByteArrayAndOwn(coder, value, Coder.Context.OUTER);
  }

  private static byte[] encodeToByteArrayAndOwn(
      ByteArrayCoder coder, byte[] value, Coder.Context context) throws IOException {
    ByteArrayOutputStream os = new ByteArrayOutputStream();
    coder.encodeAndOwn(value, os, context);
    return os.toByteArray();
  }

  // If this changes, it implies the binary format has changed.
  private static final String EXPECTED_ENCODING_ID = "";

  @Test
  public void testEncodingId() throws Exception {
    CoderProperties.coderHasEncodingId(TEST_CODER, EXPECTED_ENCODING_ID);
  }

  /**
   * Generated data to check that the wire format has not changed. To regenerate, see
   * {@link com.google.cloud.dataflow.sdk.coders.PrintBase64Encodings}.
   */
  private static final List<String> TEST_ENCODINGS = Arrays.asList(
      "CgsM",
      "DQM",
      "DQ4",
      "");

  @Test
  public void testWireFormatEncode() throws Exception {
    CoderProperties.coderEncodesBase64(TEST_CODER, TEST_VALUES, TEST_ENCODINGS);
  }

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  @Test
  public void encodeNullThrowsCoderException() throws Exception {
    thrown.expect(CoderException.class);
    thrown.expectMessage("cannot encode a null byte[]");

    CoderUtils.encodeToBase64(TEST_CODER, null);
  }
}
