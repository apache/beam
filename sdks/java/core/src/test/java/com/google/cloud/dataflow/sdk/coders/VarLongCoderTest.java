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

import com.google.cloud.dataflow.sdk.testing.CoderProperties;
import com.google.cloud.dataflow.sdk.util.CoderUtils;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.util.Arrays;
import java.util.List;

/**
 * Test case for {@link VarLongCoder}.
 */
@RunWith(JUnit4.class)
public class VarLongCoderTest {

  private static final Coder<Long> TEST_CODER = VarLongCoder.of();

  private static final List<Long> TEST_VALUES = Arrays.asList(
      -11L, -3L, -1L, 0L, 1L, 5L, 13L, 29L,
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
      "9f__________AQ",
      "_f__________AQ",
      "____________AQ",
      "AA",
      "AQ",
      "BQ",
      "DQ",
      "HQ",
      "goGAgAg",
      "4_____f_____AQ",
      "__________9_",
      "gICAgICAgICAAQ");

  @Test
  public void testWireFormatEncode() throws Exception {
    CoderProperties.coderEncodesBase64(TEST_CODER, TEST_VALUES, TEST_ENCODINGS);
  }

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  @Test
  public void encodeNullThrowsCoderException() throws Exception {
    thrown.expect(CoderException.class);
    thrown.expectMessage("cannot encode a null Long");

    CoderUtils.encodeToBase64(TEST_CODER, null);
  }
}
