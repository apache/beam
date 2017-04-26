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
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertThat;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.beam.sdk.testing.CoderProperties;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Unit tests for {@link DelegateCoder}. */
@RunWith(JUnit4.class)
public class DelegateCoderTest implements Serializable {

  private static final List<Set<Integer>> TEST_VALUES = Arrays.<Set<Integer>>asList(
      Collections.<Integer>emptySet(),
      Collections.singleton(13),
      new HashSet<>(Arrays.asList(31, -5, 83)));

  private static final TypeDescriptor<Set<Integer>> SET_INTEGER_TYPE_DESCRIPTOR =
      new TypeDescriptor<Set<Integer>>() {};

  private static final DelegateCoder<Set<Integer>, List<Integer>> TEST_CODER = DelegateCoder.of(
      ListCoder.of(VarIntCoder.of()),
      new DelegateCoder.CodingFunction<Set<Integer>, List<Integer>>() {
        @Override
        public List<Integer> apply(Set<Integer> input) {
          return Lists.newArrayList(input);
        }

        @Override
        public boolean equals(Object o) {
          return o != null && this.getClass() == o.getClass();
        }

        @Override
        public int hashCode() {
          return this.getClass().hashCode();
        }
      },
      new DelegateCoder.CodingFunction<List<Integer>, Set<Integer>>() {
        @Override
        public Set<Integer> apply(List<Integer> input) {
          return Sets.newHashSet(input);
        }

        @Override
        public boolean equals(Object o) {
          return o != null && this.getClass() == o.getClass();
        }

        @Override
        public int hashCode() {
          return this.getClass().hashCode();
        }
      }, SET_INTEGER_TYPE_DESCRIPTOR);

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

  private static class TestAllowedEncodingsCoder extends CustomCoder<Integer> {

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
  }

  @Test
  public void testCoderEquals() throws Exception {
    DelegateCoder.CodingFunction<Integer, Integer> identityFn =
        new DelegateCoder.CodingFunction<Integer, Integer>() {
          @Override
          public Integer apply(Integer input) {
            return input;
          }
        };
    Coder<Integer> varIntCoder1 = DelegateCoder.of(VarIntCoder.of(), identityFn, identityFn);
    Coder<Integer> varIntCoder2 = DelegateCoder.of(VarIntCoder.of(), identityFn, identityFn);
    Coder<Integer> bigEndianIntegerCoder =
        DelegateCoder.of(BigEndianIntegerCoder.of(), identityFn, identityFn);

    assertEquals(varIntCoder1, varIntCoder2);
    assertEquals(varIntCoder1.hashCode(), varIntCoder2.hashCode());
    assertNotEquals(varIntCoder1, bigEndianIntegerCoder);
    assertNotEquals(varIntCoder1.hashCode(), bigEndianIntegerCoder.hashCode());
  }

  @Test
  public void testEncodedTypeDescriptorSimpleEncodedType() throws Exception {
    assertThat(
        DelegateCoder.of(
            StringUtf8Coder.of(),
            new DelegateCoder.CodingFunction<Integer, String>() {
              @Override
              public String apply(Integer input) {
                return String.valueOf(input);
              }
            },
            new DelegateCoder.CodingFunction<String, Integer>() {
              @Override
              public Integer apply(String input) {
                return Integer.valueOf(input);
              }
            },
            new TypeDescriptor<Integer>(){}).getEncodedTypeDescriptor(),
        equalTo(TypeDescriptor.of(Integer.class)));
  }

  @Test
  public void testEncodedTypeDescriptor() throws Exception {
    TypeDescriptor<Set<Integer>> typeDescriptor = new TypeDescriptor<Set<Integer>>() {};
    assertThat(TEST_CODER.getEncodedTypeDescriptor(), equalTo(typeDescriptor));
  }
}
