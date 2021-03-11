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

/** Test case for {@link FloatCoder}. */
@RunWith(JUnit4.class)
public class FloatCoderTest {

  private static final Coder<Float> TEST_CODER = FloatCoder.of();

  private static final List<Float> TEST_VALUES =
      Arrays.asList(
          0.0f,
          -0.5f,
          0.5f,
          0.3f,
          -0.3f,
          1.0f,
          -43.895687f,
          (float) Math.PI,
          Float.MAX_VALUE,
          Float.MIN_VALUE,
          Float.POSITIVE_INFINITY,
          Float.NEGATIVE_INFINITY,
          Float.NaN);

  @Test
  public void testDecodeEncodeEqual() throws Exception {
    for (Float value : TEST_VALUES) {
      CoderProperties.coderDecodeEncodeEqual(TEST_CODER, value);
    }
  }

  /**
   * Generated data to check that the wire format has not changed. To regenerate, see {@link
   * org.apache.beam.sdk.coders.PrintBase64Encodings}.
   */
  private static final List<String> TEST_ENCODINGS =
      Arrays.asList(
          "AAAAAA", "vwAAAA", "PwAAAA", "PpmZmg", "vpmZmg", "P4AAAA", "wi-VLw", "QEkP2w", "f3___w",
          "AAAAAQ", "f4AAAA", "_4AAAA", "f8AAAA");

  @Test
  public void testWireFormatEncode() throws Exception {
    System.out.println(TEST_ENCODINGS);
    CoderProperties.coderEncodesBase64(TEST_CODER, TEST_VALUES, TEST_ENCODINGS);
  }

  @Rule public ExpectedException thrown = ExpectedException.none();

  @Test
  public void encodeNullThrowsCoderException() throws Exception {
    thrown.expect(CoderException.class);
    thrown.expectMessage("cannot encode a null Float");

    CoderUtils.encodeToBase64(TEST_CODER, null);
  }

  @Test
  public void testEncodedTypeDescriptor() throws Exception {
    assertThat(TEST_CODER.getEncodedTypeDescriptor(), equalTo(TypeDescriptor.of(Float.class)));
  }
}
