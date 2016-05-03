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
package org.apache.beam.sdk.values;

import static org.junit.Assert.assertEquals;

import com.google.common.reflect.TypeToken;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.math.BigDecimal;
import java.util.List;
import java.util.Set;

/**
 * Tests for TypeDescriptors.
 */
@RunWith(JUnit4.class)
public class TypeDescriptorsTest {
  @Test
  public void testTypeDescriptorsIterables() throws Exception {
    TypeDescriptor<Iterable<String>> descriptor =
        TypeDescriptors.iterables(TypeDescriptors.strings());
    TypeToken<Iterable<String>> token = new TypeToken<Iterable<String>>(){};
    assertEquals(token.getType(), descriptor.getType());
  }

  @Test
  public void testTypeDescriptorsSets() throws Exception {
    TypeDescriptor<Set<String>> descriptor =
        TypeDescriptors.sets(TypeDescriptors.strings());
    TypeToken<Set<String>> token = new TypeToken<Set<String>>(){};
    assertEquals(token.getType(), descriptor.getType());
  }

  @Test
  public void testTypeDescriptorsKV() throws Exception {
    TypeDescriptor<KV<String, Integer>> descriptor =
        TypeDescriptors.kv(TypeDescriptors.strings(), TypeDescriptors.integers());
    TypeToken<KV<String, Integer>> token = new TypeToken<KV<String, Integer>>(){};
    assertEquals(token.getType(), descriptor.getType());
  }

  @Test
  public void testTypeDescriptorsLists() throws Exception {
    TypeDescriptor<List<String>> descriptor =
        TypeDescriptors.lists(TypeDescriptors.strings());
    TypeToken<List<String>> token = new TypeToken<List<String>>(){};
    assertEquals(token.getType(), descriptor.getType());
  }

  @Test
  public void testTypeDescriptorsBoolean() throws Exception {
    TypeDescriptor<Boolean> descriptor = new TypeDescriptor<Boolean>(){};
    assertEquals(Boolean.class, descriptor.getRawType());
  }

  @Test
  public void testTypeDescriptorsDouble() throws Exception {
    TypeDescriptor<Double> descriptor = new TypeDescriptor<Double>(){};
    assertEquals(Double.class, descriptor.getRawType());
  }

  @Test
  public void testTypeDescriptorsFloat() throws Exception {
    TypeDescriptor<Float> descriptor = new TypeDescriptor<Float>(){};
    assertEquals(Float.class, descriptor.getRawType());
  }

  @Test
  public void testTypeDescriptorsInteger() throws Exception {
    TypeDescriptor<Integer> descriptor = new TypeDescriptor<Integer>(){};
    assertEquals(Integer.class, descriptor.getRawType());
  }

  @Test
  public void testTypeDescriptorsLong() throws Exception {
    TypeDescriptor<Long> descriptor = new TypeDescriptor<Long>(){};
    assertEquals(Long.class, descriptor.getRawType());
  }

  @Test
  public void testTypeDescriptorsShort() throws Exception {
    TypeDescriptor<Short> descriptor = new TypeDescriptor<Short>(){};
    assertEquals(Short.class, descriptor.getRawType());
  }

  @Test
  public void testTypeDescriptorsBigDecimal() throws Exception {
    TypeDescriptor<BigDecimal> descriptor = new TypeDescriptor<BigDecimal>(){};
    assertEquals(BigDecimal.class, descriptor.getRawType());
  }

  @Test
  public void testTypeDescriptorsString() throws Exception {
    TypeDescriptor<String> descriptor = new TypeDescriptor<String>(){};
    assertEquals(String.class, descriptor.getRawType());
  }
}
