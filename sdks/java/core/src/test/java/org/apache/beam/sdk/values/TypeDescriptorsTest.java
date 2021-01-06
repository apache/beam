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

import static org.apache.beam.sdk.values.TypeDescriptors.integers;
import static org.apache.beam.sdk.values.TypeDescriptors.iterables;
import static org.apache.beam.sdk.values.TypeDescriptors.kvs;
import static org.apache.beam.sdk.values.TypeDescriptors.lists;
import static org.apache.beam.sdk.values.TypeDescriptors.sets;
import static org.apache.beam.sdk.values.TypeDescriptors.strings;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;

import java.util.List;
import java.util.Set;
import org.hamcrest.CoreMatchers;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link TypeDescriptors}. */
@RunWith(JUnit4.class)
public class TypeDescriptorsTest {
  @Test
  public void testTypeDescriptorsIterables() throws Exception {
    TypeDescriptor<Iterable<String>> descriptor = iterables(strings());
    assertEquals(descriptor, new TypeDescriptor<Iterable<String>>() {});
  }

  @Test
  public void testTypeDescriptorsSets() throws Exception {
    TypeDescriptor<Set<String>> descriptor = sets(strings());
    assertEquals(descriptor, new TypeDescriptor<Set<String>>() {});
  }

  @Test
  public void testTypeDescriptorsKV() throws Exception {
    TypeDescriptor<KV<String, Integer>> descriptor = kvs(strings(), integers());
    assertEquals(descriptor, new TypeDescriptor<KV<String, Integer>>() {});
  }

  @Test
  public void testTypeDescriptorsLists() throws Exception {
    TypeDescriptor<List<String>> descriptor = lists(strings());
    assertEquals(descriptor, new TypeDescriptor<List<String>>() {});
    assertNotEquals(descriptor, new TypeDescriptor<List<Boolean>>() {});
  }

  @Test
  public void testTypeDescriptorsListsOfLists() throws Exception {
    TypeDescriptor<List<List<String>>> descriptor = lists(lists(strings()));
    assertEquals(descriptor, new TypeDescriptor<List<List<String>>>() {});
    assertNotEquals(descriptor, new TypeDescriptor<List<String>>() {});
    assertNotEquals(descriptor, new TypeDescriptor<List<Boolean>>() {});
  }

  private interface Generic<FooT, BarT> {}

  private static <ActualFooT> Generic<ActualFooT, String> typeErasedGeneric() {
    return new Generic<ActualFooT, String>() {};
  }

  private static <ActualFooT, ActualBarT> TypeDescriptor<ActualFooT> extractFooT(
      Generic<ActualFooT, ActualBarT> instance) {
    return TypeDescriptors.extractFromTypeParameters(
        instance,
        Generic.class,
        new TypeDescriptors.TypeVariableExtractor<
            Generic<ActualFooT, ActualBarT>, ActualFooT>() {});
  }

  private static <ActualFooT, ActualBarT> TypeDescriptor<ActualBarT> extractBarT(
      Generic<ActualFooT, ActualBarT> instance) {
    return TypeDescriptors.extractFromTypeParameters(
        instance,
        Generic.class,
        new TypeDescriptors.TypeVariableExtractor<
            Generic<ActualFooT, ActualBarT>, ActualBarT>() {});
  }

  private static <ActualFooT, ActualBarT> TypeDescriptor<KV<ActualFooT, ActualBarT>> extractKV(
      Generic<ActualFooT, ActualBarT> instance) {
    return TypeDescriptors.extractFromTypeParameters(
        instance,
        Generic.class,
        new TypeDescriptors.TypeVariableExtractor<
            Generic<ActualFooT, ActualBarT>, KV<ActualFooT, ActualBarT>>() {});
  }

  @Test
  public void testTypeDescriptorsTypeParameterOf() throws Exception {
    assertEquals(strings(), extractFooT(new Generic<String, Integer>() {}));
    assertEquals(integers(), extractBarT(new Generic<String, Integer>() {}));
    assertEquals(kvs(strings(), integers()), extractKV(new Generic<String, Integer>() {}));
  }

  @Test
  public void testTypeDescriptorsTypeParameterOfErased() throws Exception {
    Generic<Integer, String> instance = TypeDescriptorsTest.typeErasedGeneric();

    TypeDescriptor<Integer> fooT = extractFooT(instance);
    assertNotNull(fooT);
    // Using toString() assertions because verifying the contents of a Type is very cumbersome,
    // and the expected types can not be easily constructed directly.
    assertEquals("ActualFooT", fooT.toString());

    assertEquals(strings(), extractBarT(instance));

    TypeDescriptor<KV<Integer, String>> kvT = extractKV(instance);
    assertNotNull(kvT);
    assertThat(kvT.toString(), CoreMatchers.containsString("KV<ActualFooT, java.lang.String>"));
  }
}
