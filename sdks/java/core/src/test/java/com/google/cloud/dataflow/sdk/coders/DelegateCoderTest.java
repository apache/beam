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
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/** Unit tests for {@link DelegateCoder}. */
@RunWith(JUnit4.class)
public class DelegateCoderTest implements Serializable {

  private static final List<Set<Integer>> TEST_VALUES = Arrays.<Set<Integer>>asList(
      Collections.<Integer>emptySet(),
      Collections.singleton(13),
      new HashSet<>(Arrays.asList(31, -5, 83)));

  private static final DelegateCoder<Set<Integer>, List<Integer>> TEST_CODER = DelegateCoder.of(
      ListCoder.of(VarIntCoder.of()),
      new DelegateCoder.CodingFunction<Set<Integer>, List<Integer>>() {
        @Override
        public List<Integer> apply(Set<Integer> input) {
          return Lists.newArrayList(input);
        }
      },
      new DelegateCoder.CodingFunction<List<Integer>, Set<Integer>>() {
        @Override
        public Set<Integer> apply(List<Integer> input) {
          return Sets.newHashSet(input);
        }
      });

  @Test
  public void testDeterministic() throws Exception {
    for (Set<Integer> value : TEST_VALUES) {
      CoderProperties.coderDeterministic(
          TEST_CODER, value, Sets.newHashSet(value));
    }
  }

  @Test
  public void testDecodeEncodeEqual() throws Exception {
    for (Set<Integer> value : TEST_VALUES) {
      CoderProperties.coderDecodeEncodeEqual(TEST_CODER, value);
    }
  }

  @Test
  public void testSerializable() throws Exception {
    CoderProperties.coderSerializable(TEST_CODER);
  }

  private static final String TEST_ENCODING_ID = "test-encoding-id";
  private static final String TEST_ALLOWED_ENCODING = "test-allowed-encoding";

  private static class TestAllowedEncodingsCoder extends StandardCoder<Integer> {

    @Override
    public void encode(Integer value, OutputStream outstream, Context context) {
      throw new UnsupportedOperationException();
    }

    @Override
    public Integer decode(InputStream instream, Context context) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void verifyDeterministic() {
      throw new UnsupportedOperationException();
    }

    @Override
    public List<? extends Coder<?>> getCoderArguments() {
      return Collections.emptyList();
    }

    @Override
    public String getEncodingId() {
      return TEST_ENCODING_ID;
    }

    @Override
    public Collection<String> getAllowedEncodings() {
      return Collections.singletonList(TEST_ALLOWED_ENCODING);
    }
  }

  @Test
  public void testEncodingId() throws Exception {
    Coder<Integer> underlyingCoder = new TestAllowedEncodingsCoder();

    Coder<Integer> trivialDelegateCoder = DelegateCoder.of(
      underlyingCoder,
      new DelegateCoder.CodingFunction<Integer, Integer>() {
        @Override
        public Integer apply(Integer input) {
          return input;
        }
      },
      new DelegateCoder.CodingFunction<Integer, Integer>() {
        @Override
        public Integer apply(Integer input) {
          return input;
        }
      });
    CoderProperties.coderHasEncodingId(
        trivialDelegateCoder, TestAllowedEncodingsCoder.class.getName() + ":" + TEST_ENCODING_ID);
    CoderProperties.coderAllowsEncoding(
        trivialDelegateCoder,
        TestAllowedEncodingsCoder.class.getName() + ":" + TEST_ALLOWED_ENCODING);
  }
}
