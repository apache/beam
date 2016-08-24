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
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

import java.util.List;
import java.util.Set;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Tests for {@link TypeDescriptors}.
 */
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
    TypeDescriptor<KV<String, Integer>> descriptor =
        kvs(strings(), integers());
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
}
