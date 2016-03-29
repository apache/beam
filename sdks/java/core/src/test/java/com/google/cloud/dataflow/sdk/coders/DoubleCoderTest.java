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
 * Test case for {@link DoubleCoder}.
 */
@RunWith(JUnit4.class)
public class DoubleCoderTest {

  private static final Coder<Double> TEST_CODER = DoubleCoder.of();

  private static final List<Double> TEST_VALUES = Arrays.asList(
      0.0, -0.5, 0.5, 0.3, -0.3, 1.0, -43.89568740, 3.14159,
      Double.MAX_VALUE,
      Double.MIN_VALUE,
      Double.POSITIVE_INFINITY,
      Double.NEGATIVE_INFINITY,
      Double.NaN);

  @Test
  public void testDecodeEncodeEqual() throws Exception {
    for (Double value : TEST_VALUES) {
      CoderProperties.coderDecodeEncodeEqual(TEST_CODER, value);
    }
  }

  // This should never change. The format is fixed by Java.
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
      "AAAAAAAAAAA",
      "v-AAAAAAAAA",
      "P-AAAAAAAAA",
      "P9MzMzMzMzM",
      "v9MzMzMzMzM",
      "P_AAAAAAAAA",
      "wEXypeJ9ODo",
      "QAkh-fAbhm4",
      "f-________8",
      "AAAAAAAAAAE",
      "f_AAAAAAAAA",
      "__AAAAAAAAA",
      "f_gAAAAAAAA");

  @Test
  public void testWireFormatEncode() throws Exception {
    CoderProperties.coderEncodesBase64(TEST_CODER, TEST_VALUES, TEST_ENCODINGS);
  }

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  @Test
  public void encodeNullThrowsCoderException() throws Exception {
    thrown.expect(CoderException.class);
    thrown.expectMessage("cannot encode a null Double");

    CoderUtils.encodeToBase64(TEST_CODER, null);
  }
}
