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
package org.apache.beam.runners.core;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import org.apache.beam.sdk.coders.BigEndianLongCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.ListCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.coders.VarLongCoder;
import org.apache.beam.sdk.testing.CoderProperties;
import org.apache.beam.sdk.util.CoderUtils;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;

/**
 * Tests for {@link ElementAndRestrictionCoder}. Parroted from {@link
 * org.apache.beam.sdk.coders.KvCoderTest}.
 */
@RunWith(Parameterized.class)
public class ElementAndRestrictionCoderTest<K, V> {
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
              ElementAndRestrictionCoder.of(StringUtf8Coder.of(), VarIntCoder.of()),
              Arrays.asList(
                  ElementAndRestriction.of("", -1),
                  ElementAndRestriction.of("hello", 0),
                  ElementAndRestriction.of("goodbye", Integer.MAX_VALUE))),
          coderAndData(
              ListCoder.of(VarLongCoder.of()),
              Arrays.asList(Arrays.asList(1L, 2L, 3L), Collections.<Long>emptyList())));

  @Parameterized.Parameters(name = "{index}: keyCoder={0} key={1} valueCoder={2} value={3}")
  public static Collection<Object[]> data() {
    List<Object[]> parameters = new ArrayList<>();
    for (AnyCoderAndData keyCoderAndData : TEST_DATA) {
      Coder keyCoder = keyCoderAndData.coderAndData.coder;
      for (Object key : keyCoderAndData.coderAndData.data) {
        for (AnyCoderAndData valueCoderAndData : TEST_DATA) {
          Coder valueCoder = valueCoderAndData.coderAndData.coder;
          for (Object value : valueCoderAndData.coderAndData.data) {
            parameters.add(new Object[] {keyCoder, key, valueCoder, value});
          }
        }
      }
    }
    return parameters;
  }

  @Parameter(0)
  public Coder<K> keyCoder;
  @Parameter(1)
  public K key;
  @Parameter(2)
  public Coder<V> valueCoder;
  @Parameter(3)
  public V value;

  @Test
  @SuppressWarnings("rawtypes")
  public void testDecodeEncodeEqual() throws Exception {
    CoderProperties.coderDecodeEncodeEqual(
        ElementAndRestrictionCoder.of(keyCoder, valueCoder),
        ElementAndRestriction.of(key, value));
  }

  @Rule public ExpectedException thrown = ExpectedException.none();

  @Test
  public void encodeNullThrowsCoderException() throws Exception {
    thrown.expect(CoderException.class);
    thrown.expectMessage("cannot encode a null ElementAndRestriction");

    CoderUtils.encodeToBase64(
        ElementAndRestrictionCoder.of(StringUtf8Coder.of(), VarIntCoder.of()), null);
  }
}
