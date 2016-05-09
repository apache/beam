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

import org.apache.beam.sdk.testing.CoderProperties;
import org.apache.beam.sdk.util.CoderUtils;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.List;

/**
 * Test case for {@link BigDecimalCoder}.
 */
@RunWith(JUnit4.class)
public class BigDecimalCoderTest {

  private static final Coder<BigDecimal> TEST_CODER = BigDecimalCoder.of();

  private static final List<BigDecimal> TEST_VALUES = Arrays.asList(
      new BigDecimal(Double.MIN_VALUE),
      new BigDecimal(-11),
      new BigDecimal(-3),
      new BigDecimal(-1),
      new BigDecimal(0),
      new BigDecimal(1),
      new BigDecimal(5),
      new BigDecimal(13),
      new BigDecimal(29),
      new BigDecimal(Double.MAX_VALUE));

  @Test
  public void testDecodeEncodeEqual() throws Exception {
    for (BigDecimal value : TEST_VALUES) {
      CoderProperties.coderDecodeEncodeEqual(TEST_CODER, value);
    }
  }

  // This should never change. The definition of big endian encoding is fixed.
  private static final String EXPECTED_ENCODING_ID = "";

  @Test
  public void testEncodingId() throws Exception {
    CoderProperties.coderHasEncodingId(TEST_CODER, EXPECTED_ENCODING_ID);
  }

  /**
   * Generated data to check that the wire format has not changed. To regenerate, see
   * {@link org.apache.beam.sdk.coders.PrintBase64Encodings}.
   */
  private static final List<String> TEST_ENCODINGS = Arrays.asList(
      "AAAEMgAAATg12KOw51bHBNnjNkPn-wPiaWQ_AsohTe-mXyOGWcybUGt9TKi2FHqY2OH-gV0_GWqRbjNAGsSskI7K3" +
          "xf9JmTjf1ySZXuvF9S9PsgV3kT-sgypaRw_i1MK_orzcJVg_s3cEGTjTY1_Xor3JM9UBVKiQy3Vpulf7aN9LM" +
          "kiQEfO28mXQibyUtXL4yoLIwujoo8ArC9SayfbH5HmUxX9G0e506_cefoYIGByfq3M8GLp1_METj97ViU38je" +
          "xsXkggqxXrMG8PO6pCYNB8P_jcf9i5OagpPafem18giZ8-v3fWJPN63vkbuOtaHb9u9yGQfrN25aLpNW9ooU9" +
          "eYbL-1ewSBwENptcIT5SMhkulcVY6e9LyAqamGWdvnbevpwW84rTQpkeJePOkIt6G1_slfkQn6VBw7Jz3Vk",
      "AAAAAAAAAAH1",
      "AAAAAAAAAAH9",
      "AAAAAAAAAAH_",
      "AAAAAAAAAAEA",
      "AAAAAAAAAAEB",
      "AAAAAAAAAAEF",
      "AAAAAAAAAAEN",
      "AAAAAAAAAAEd",
      "AAAAAAAAAIEA________-AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA" +
          "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA" +
          "AAAAAAAAA");

  @Test
  public void testWireFormatEncode() throws Exception {
    CoderProperties.coderEncodesBase64(TEST_CODER, TEST_VALUES, TEST_ENCODINGS);
  }

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  @Test
  public void encodeNullThrowsCoderException() throws Exception {
    thrown.expect(CoderException.class);
    thrown.expectMessage("cannot encode a null BigDecimal");

    CoderUtils.encodeToBase64(TEST_CODER, null);
  }
}
