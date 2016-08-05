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
import java.math.BigInteger;
import java.util.List;

/**
 * Test case for {@link BigIntegerCoder}.
 */
@RunWith(JUnit4.class)
public class BigIntegerCoderTest {

  @Rule public ExpectedException thrown = ExpectedException.none();

  private static final Coder<BigInteger> TEST_CODER = BigIntegerCoder.of();

  private static final List<BigInteger> TEST_VALUES =
      ImmutableList.of(
          BigInteger.valueOf(Integer.MIN_VALUE).subtract(BigInteger.valueOf(Integer.MAX_VALUE)),
          BigInteger.valueOf(Integer.MIN_VALUE).subtract(BigInteger.ONE),
          BigInteger.valueOf(-1),
          BigInteger.ZERO,
          BigInteger.valueOf(1),
          BigInteger.valueOf(Integer.MAX_VALUE).add(BigInteger.ONE),
          BigInteger.valueOf(Integer.MAX_VALUE).multiply(BigInteger.TEN));

  @Test
  public void testDecodeEncodeEqual() throws Exception {
    for (BigInteger value : TEST_VALUES) {
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
      ImmutableList.of("_wAAAAE", "_3____8", "_w", "AA", "AQ", "AIAAAAA", "BP____Y");

  @Test
  public void testWireFormatEncode() throws Exception {
    CoderProperties.coderEncodesBase64(TEST_CODER, TEST_VALUES, TEST_ENCODINGS);
  }

  @Test
  public void testGetEncodedElementByteSize() throws Exception {
    TestElementByteSizeObserver observer = new TestElementByteSizeObserver();
    for (BigInteger value : TEST_VALUES) {
      TEST_CODER.registerByteSizeObserver(value, observer, Coder.Context.OUTER);
      observer.advance();
      assertThat(
          observer.getSumAndReset(),
          equalTo((long) CoderUtils.encodeToByteArray(TEST_CODER, value).length));
    }
  }

  @Test
  public void encodeNullThrowsCoderException() throws Exception {
    thrown.expect(NullPointerException.class);
    thrown.expectMessage("cannot encode a null BigInteger");

    CoderUtils.encodeToBase64(TEST_CODER, null);
  }

  /**
   * This is a change-detector test. If this test fails, then the encoding id of
   * {@link BigIntegerCoder} must change.
   */
  @Test
  public void testCoderIdDependencies() {
    assertThat(VarIntCoder.of().getEncodingId(), equalTo(""));
    assertThat(ByteArrayCoder.of().getEncodingId(), equalTo(""));
  }
}
