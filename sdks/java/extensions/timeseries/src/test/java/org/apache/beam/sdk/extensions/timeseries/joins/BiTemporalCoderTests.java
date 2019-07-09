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
package org.apache.beam.sdk.extensions.timeseries.joins;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.beam.sdk.coders.*;
import org.apache.beam.sdk.testing.CoderProperties;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.util.CoderUtils;
import org.apache.beam.sdk.values.KV;
import org.joda.time.Instant;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@RunWith(JUnit4.class)
public class BiTemporalCoderTests implements Serializable {

  private static final Logger LOG = LoggerFactory.getLogger(BiTemporalCoderTests.class);

  @Rule public transient TestPipeline p = TestPipeline.create();

  @Test
  public void testBeforeValues() {}

  private static class CoderAndData<T> {
    Coder<T> coder;
    List<T> data;
  }

  private static class AnyCoderAndData {
    private CoderAndData<?> coderAndData;
  }

  private static <T> AnyCoderAndData coderAndData(Coder<T> coder, List<T> data) {
    CoderAndData<T> coderAndData = new CoderAndData<>();
    coderAndData.coder = coder;
    coderAndData.data = data;
    AnyCoderAndData res = new AnyCoderAndData();
    res.coderAndData = coderAndData;
    return res;
  }

  private static final List<AnyCoderAndData> TEST_DATA =
      Arrays.asList(
          coderAndData(
              VarIntCoder.of(), Arrays.asList(-1, 0, 1, 13, Integer.MAX_VALUE, Integer.MIN_VALUE)),
          coderAndData(
              BigEndianLongCoder.of(),
              Arrays.asList(-1L, 0L, 1L, 13L, Long.MAX_VALUE, Long.MIN_VALUE)),
          coderAndData(StringUtf8Coder.of(), Arrays.asList("", "hello", "goodbye", "1")),
          coderAndData(
              KvCoder.of(StringUtf8Coder.of(), VarIntCoder.of()),
              Arrays.asList(KV.of("", -1), KV.of("hello", 0), KV.of("goodbye", Integer.MAX_VALUE))),
          coderAndData(
              ListCoder.of(VarLongCoder.of()),
              Arrays.asList(Arrays.asList(1L, 2L, 3L), Collections.emptyList())));

  @Test
  @SuppressWarnings("rawtypes")
  public void testDecodeEncodeEqual() throws Exception {

    Instant time = new Instant("2000-01-01");

    for (AnyCoderAndData keyCoderAndData : TEST_DATA) {
      Coder keyCoder = keyCoderAndData.coderAndData.coder;
      for (Object key : keyCoderAndData.coderAndData.data) {
        for (AnyCoderAndData valueCoderAndData : TEST_DATA) {
          Coder valueCoder = valueCoderAndData.coderAndData.coder;
          for (Object value : valueCoderAndData.coderAndData.data) {
            CoderProperties.coderDecodeEncodeEqual(
                BiTemporalJoinResultCoder.of(keyCoder, valueCoder, valueCoder),
                BiTemporalJoinResult.of()
                    .setLeftData(KV.of(key, value), time)
                    .setRightData(KV.of(key, value), time));
          }
        }
      }
    }
  }

  @Test
  public void testCoderIsSerializableWithWellKnownCoderType() throws Exception {
    CoderProperties.coderSerializable(
        BiTemporalJoinResultCoder.of(
            GlobalWindow.Coder.INSTANCE, GlobalWindow.Coder.INSTANCE, GlobalWindow.Coder.INSTANCE));
  }

  /** Homogeneously typed test value for ease of use with the wire format test utility. */
  private static final Coder<BiTemporalJoinResult<String, Integer, Integer>> TEST_CODER =
      BiTemporalJoinResultCoder.of(StringUtf8Coder.of(), VarIntCoder.of(), VarIntCoder.of());

  private static final List<BiTemporalJoinResult> TEST_VALUES =
      Arrays.asList(
          BiTemporalJoinResult.of()
              .setLeftData(KV.of("key", -1), Instant.ofEpochMilli(Long.MAX_VALUE))
              .setRightData(KV.of("key", 0), Instant.ofEpochMilli(Long.MAX_VALUE)));

  /**
   * Generated data to check that the wire format has not changed. To regenerate, see {@link
   * org.apache.beam.sdk.coders.PrintBase64Encodings}.
   */
  private static final List<String> TEST_ENCODINGS =
      Arrays.asList("AP____8P", "BWhlbGxvAA", "B2dvb2RieWX_____Bw");

  @Rule public ExpectedException thrown = ExpectedException.none();

  @Test
  public void encodeNullThrowsCoderException() throws Exception {

    thrown.expect(CoderException.class);
    thrown.expectMessage("cannot encode a null");

    CoderUtils.encodeToBase64(TEST_CODER, null);
  }

  @Test
  public void encodeNullKeyThrowsCoderException() throws Exception {

    thrown.expect(CoderException.class);
    thrown.expectMessage("cannot encode a null key");

    CoderUtils.encodeToBase64(TEST_CODER, new BiTemporalJoinResult<>());
  }
}
