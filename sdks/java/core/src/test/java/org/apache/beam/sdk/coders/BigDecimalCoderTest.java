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
import static org.junit.Assert.assertThat;

import java.math.BigDecimal;
import org.apache.beam.sdk.testing.CoderProperties;
import org.apache.beam.sdk.testing.CoderProperties.TestElementByteSizeObserver;
import org.apache.beam.sdk.util.CoderUtils;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Test case for {@link BigDecimalCoder}. */
@RunWith(JUnit4.class)
public class BigDecimalCoderTest {

  @Rule public ExpectedException thrown = ExpectedException.none();

  private static final Coder<BigDecimal> TEST_CODER = BigDecimalCoder.of();

  private static final ImmutableList<BigDecimal> TEST_VALUES =
      ImmutableList.of(
          new BigDecimal(Double.MIN_VALUE).divide(BigDecimal.TEN),
          new BigDecimal(Double.MIN_VALUE),
          new BigDecimal(-10.5),
          new BigDecimal(-1),
          BigDecimal.ZERO,
          BigDecimal.ONE,
          new BigDecimal(13.258),
          new BigDecimal(Double.MAX_VALUE),
          new BigDecimal(Double.MAX_VALUE).multiply(BigDecimal.TEN));

  @Test
  public void testDecodeEncodeEqual() throws Exception {
    for (BigDecimal value : TEST_VALUES) {
      CoderProperties.coderDecodeEncodeEqual(TEST_CODER, value);
    }
  }

  /**
   * Generated data to check that the wire format has not changed. To regenerate, see {@link
   * org.apache.beam.sdk.coders.PrintBase64Encodings}.
   */
  private static final ImmutableList<String> TEST_ENCODINGS =
      ImmutableList.of(
          "swg12KOw51bHBNnjNkPn-wPiaWQ_AsohTe-mXyOGWcybUGt9TKi2FHqY2OH-gV0_GWqRbjNAGsSskI7K3xf9JmT"
              + "jf1ySZXuvF9S9PsgV3kT-sgypaRw_i1MK_orzcJVg_s3cEGTjTY1_Xor3JM9UBVKiQy3Vpulf7aN9LMki"
              + "QEfO28mXQibyUtXL4yoLIwujoo8ArC9SayfbH5HmUxX9G0e506_cefoYIGByfq3M8GLp1_METj97ViU38"
              + "jexsXkggqxXrMG8PO6pCYNB8P_jcf9i5OagpPafem18giZ8-v3fWJPN63vkbuOtaHb9u9yGQfrN25aLpN"
              + "W9ooU9eYbL-1ewSBwENptcIT5SMhkulcVY6e9LyAqamGWdvnbevpwW84rTQpkeJePOkIt6G1_slfkQn6V"
              + "Bw7Jz3Vk",
          "sgg12KOw51bHBNnjNkPn-wPiaWQ_AsohTe-mXyOGWcybUGt9TKi2FHqY2OH-gV0_GWqRbjNAGsSskI7K3xf9JmT"
              + "jf1ySZXuvF9S9PsgV3kT-sgypaRw_i1MK_orzcJVg_s3cEGTjTY1_Xor3JM9UBVKiQy3Vpulf7aN9LMki"
              + "QEfO28mXQibyUtXL4yoLIwujoo8ArC9SayfbH5HmUxX9G0e506_cefoYIGByfq3M8GLp1_METj97ViU38"
              + "jexsXkggqxXrMG8PO6pCYNB8P_jcf9i5OagpPafem18giZ8-v3fWJPN63vkbuOtaHb9u9yGQfrN25aLpN"
              + "W9ooU9eYbL-1ewSBwENptcIT5SMhkulcVY6e9LyAqamGWdvnbevpwW84rTQpkeJePOkIt6G1_slfkQn6V"
              + "Bw7Jz3Vk",
          "AZc",
          "AP8",
          "AAA",
          "AAE",
          "MAkSTUgCATUf0ayu7f08NmBSpPU2dQ",
          "AAD________4AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
              + "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
              + "AAAAAA",
          "AAn_______-wAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
              + "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
              + "AAAAAA");

  @Test
  public void testWireFormatEncode() throws Exception {
    CoderProperties.coderEncodesBase64(TEST_CODER, TEST_VALUES, TEST_ENCODINGS);
  }

  @Test
  public void testGetEncodedElementByteSize() throws Exception {
    TestElementByteSizeObserver observer = new TestElementByteSizeObserver();
    for (BigDecimal value : TEST_VALUES) {
      TEST_CODER.registerByteSizeObserver(value, observer);
      observer.advance();
      assertThat(
          observer.getSumAndReset(),
          equalTo(
              (long) CoderUtils.encodeToByteArray(TEST_CODER, value, Coder.Context.NESTED).length));
    }
  }

  @Test
  public void encodeNullThrowsException() throws Exception {
    thrown.expect(NullPointerException.class);
    thrown.expectMessage("cannot encode a null BigDecimal");

    CoderUtils.encodeToBase64(TEST_CODER, null);
  }
}
