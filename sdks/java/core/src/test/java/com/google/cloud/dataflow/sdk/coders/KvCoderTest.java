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
package com.google.cloud.dataflow.sdk.coders;

import com.google.cloud.dataflow.sdk.testing.CoderProperties;
import com.google.cloud.dataflow.sdk.util.CoderUtils;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.common.collect.ImmutableMap;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Test case for {@link KvCoder}.
 */
@RunWith(JUnit4.class)
public class KvCoderTest {

  private static final Map<Coder<?>, Iterable<?>> TEST_DATA =
      new ImmutableMap.Builder<Coder<?>, Iterable<?>>()
      .put(VarIntCoder.of(),
          Arrays.asList(-1, 0, 1, 13, Integer.MAX_VALUE, Integer.MIN_VALUE))
      .put(BigEndianLongCoder.of(),
          Arrays.asList(-1L, 0L, 1L, 13L, Long.MAX_VALUE, Long.MIN_VALUE))
      .put(StringUtf8Coder.of(),
          Arrays.asList("", "hello", "goodbye", "1"))
      .put(KvCoder.of(StringUtf8Coder.of(), VarIntCoder.of()),
          Arrays.asList(KV.of("", -1), KV.of("hello", 0), KV.of("goodbye", Integer.MAX_VALUE)))
      .put(ListCoder.of(VarLongCoder.of()),
          Arrays.asList(
              Arrays.asList(1L, 2L, 3L),
              Collections.emptyList()))
       .build();

  @Test
  public void testDecodeEncodeEqual() throws Exception {
    for (Map.Entry<Coder<?>, Iterable<?>> entry : TEST_DATA.entrySet()) {
      // The coder and corresponding values must be the same type.
      // If someone messes this up in the above test data, the test
      // will fail anyhow (unless the coder magically works on data
      // it does not understand).
      @SuppressWarnings("unchecked")
      Coder<Object> coder = (Coder<Object>) entry.getKey();
      Iterable<?> values = entry.getValue();
      for (Object value : values) {
        CoderProperties.coderDecodeEncodeEqual(coder, value);
      }
    }
  }

  // If this changes, it implies the binary format has changed!
  private static final String EXPECTED_ENCODING_ID = "";

  @Test
  public void testEncodingId() throws Exception {
    CoderProperties.coderHasEncodingId(
        KvCoder.of(VarIntCoder.of(), VarIntCoder.of()),
        EXPECTED_ENCODING_ID);
  }

  /**
   * Homogeneously typed test value for ease of use with the wire format test utility.
   */
  private static final Coder<KV<String, Integer>> TEST_CODER =
      KvCoder.of(StringUtf8Coder.of(), VarIntCoder.of());

  private static final List<KV<String, Integer>> TEST_VALUES = Arrays.asList(
      KV.of("", -1),
      KV.of("hello", 0),
      KV.of("goodbye", Integer.MAX_VALUE));

  /**
   * Generated data to check that the wire format has not changed. To regenerate, see
   * {@link com.google.cloud.dataflow.sdk.coders.PrintBase64Encodings}.
   */
  private static final List<String> TEST_ENCODINGS = Arrays.asList(
      "AP____8P",
      "BWhlbGxvAA",
      "B2dvb2RieWX_____Bw");

  @Test
  public void testWireFormatEncode() throws Exception {
    CoderProperties.coderEncodesBase64(TEST_CODER, TEST_VALUES, TEST_ENCODINGS);
  }

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  @Test
  public void encodeNullThrowsCoderException() throws Exception {
    thrown.expect(CoderException.class);
    thrown.expectMessage("cannot encode a null KV");

    CoderUtils.encodeToBase64(TEST_CODER, null);
  }
}
