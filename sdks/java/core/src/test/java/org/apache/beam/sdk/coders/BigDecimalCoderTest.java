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

import org.apache.beam.sdk.testing.CoderProperties;
import org.apache.beam.sdk.testing.CoderProperties.TestElementByteSizeObserver;
import org.apache.beam.sdk.util.CoderUtils;
import com.google.common.collect.ImmutableList;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import java.math.BigDecimal;
import java.util.List;

/**
 * Test case for {@link BigDecimalCoder}.
 */
@RunWith(JUnit4.class)
public class BigDecimalCoderTest {

  @Rule public ExpectedException thrown = ExpectedException.none();

  private static final Coder<BigDecimal> TEST_CODER = BigDecimalCoder.of();

  private static final List<BigDecimal> TEST_VALUES =
      ImmutableList.of(
          new BigDecimal(Double.MIN_VALUE).divide(BigDecimal.TEN),
          new BigDecimal(Double.MIN_VALUE),
          new BigDecimal(-10.5),
          new BigDecimal(-1),
          new BigDecimal(0),
          new BigDecimal(1),
          new BigDecimal(13.258),
          new BigDecimal(Double.MAX_VALUE),
          new BigDecimal(Double.MAX_VALUE).multiply(BigDecimal.TEN));

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
  private static final List<String> TEST_ENCODINGS =
      ImmutableList.of(
          "swi4AjXYo7DnVscE2eM2Q-f7A-JpZD8CyiFN76ZfI4ZZzJtQa31MqLYUepjY4f6BXT8ZapFuM"
              + "0AaxKyQjsrfF_0mZON_XJJle68X1L0-yBXeRP6yDKlpHD-LUwr-ivNwlWD-zdwQZONNjX9"
              + "eivckz1QFUqJDLdWm6V_to30sySJAR87byZdCJvJS1cvjKgsjC6OijwCsL1JrJ9sfkeZTF"
              + "f0bR7nTr9x5-hggYHJ-rczwYunX8wROP3tWJTfyN7GxeSCCrFeswbw87qkJg0Hw_-Nx_2L"
              + "k5qCk9p96bXyCJnz6_d9Yk83re-Ru461odv273IZB-s3blouk1b2ihT15hsv7V7BIHAQ2m"
              + "1whPlIyGS6VxVjp70vICpqYZZ2-dt6-nBbzitNCmR4l486Qi3obX-yV-RCfpUHDsnPdWQ",
          "sgi4AjXYo7DnVscE2eM2Q-f7A-JpZD8CyiFN76ZfI4ZZzJtQa31MqLYUepjY4f6BXT8ZapFu"
              + "M0AaxKyQjsrfF_0mZON_XJJle68X1L0-yBXeRP6yDKlpHD-LUwr-ivNwlWD-zdwQZONNj"
              + "X9eivckz1QFUqJDLdWm6V_to30sySJAR87byZdCJvJS1cvjKgsjC6OijwCsL1JrJ9sfke"
              + "ZTFf0bR7nTr9x5-hggYHJ-rczwYunX8wROP3tWJTfyN7GxeSCCrFeswbw87qkJg0Hw_-Nx"
              + "_2Lk5qCk9p96bXyCJnz6_d9Yk83re-Ru461odv273IZB-s3blouk1b2ihT15hsv7V7BIHA"
              + "Q2m1whPlIyGS6VxVjp70vICpqYZZ2-dt6-nBbzitNCmR4l486Qi3obX-yV-RCfpUHDsnPdWQ",
          "AQGX",
          "AAH_",
          "AAEA",
          "AAEB",
          "MBUJEk1IAgE1H9Gsru39PDZgUqT1NnU",
          "AIEBAP________gAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
              + "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
              + "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
          "AIEBCf_______7AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
              + "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
              + "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA");

  @Test
  public void testWireFormatEncode() throws Exception {
    CoderProperties.coderEncodesBase64(TEST_CODER, TEST_VALUES, TEST_ENCODINGS);
  }

  @Test
  public void testGetEncodedElementByteSize() throws Exception {
    TestElementByteSizeObserver observer = new TestElementByteSizeObserver();
    for (BigDecimal value : TEST_VALUES) {
      TEST_CODER.registerByteSizeObserver(value, observer, Coder.Context.OUTER);
      observer.advance();
      assertThat(observer.getSumAndReset(),
          equalTo((long) CoderUtils.encodeToByteArray(TEST_CODER, value).length));
    }
  }

  @Test
  public void encodeNullThrowsException() throws Exception {
    thrown.expect(NullPointerException.class);
    thrown.expectMessage("cannot encode a null BigDecimal");

    CoderUtils.encodeToBase64(TEST_CODER, null);
  }

  /**
   * This is a change-detector test. If this test fails, then the encoding id of
   * {@link BigDecimalCoder} must change.
   */
  @Test
  public void testCoderIdDependencies() {
    assertThat(VarIntCoder.of().getEncodingId(), equalTo(""));
    assertThat(BigIntegerCoder.of().getEncodingId(), equalTo(""));
  }
}
