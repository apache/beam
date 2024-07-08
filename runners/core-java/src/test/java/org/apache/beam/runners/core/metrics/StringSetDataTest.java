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
package org.apache.beam.runners.core.metrics;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.util.Collections;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableSet;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

/** Tests for {@link StringSetData}. */
public class StringSetDataTest {
  @Rule public ExpectedException exception = ExpectedException.none();

  @Test
  public void testCreate() {
    // test empty stringset creation
    assertTrue(StringSetData.create(Collections.emptySet()).stringSet().isEmpty());
    // single element test
    ImmutableSet<String> singleElement = ImmutableSet.of("ab");
    StringSetData setData = StringSetData.create(singleElement);
    assertEquals(setData.stringSet(), singleElement);

    // multiple element test
    ImmutableSet<String> multipleElement = ImmutableSet.of("cd", "ef");
    setData = StringSetData.create(multipleElement);
    assertEquals(setData.stringSet(), multipleElement);
  }

  @Test
  public void testCombine() {
    StringSetData singleElement = StringSetData.create(ImmutableSet.of("ab"));
    StringSetData multipleElement = StringSetData.create(ImmutableSet.of("cd", "ef"));
    StringSetData result = singleElement.combine(multipleElement);
    assertEquals(result.stringSet(), ImmutableSet.of("cd", "ef", "ab"));

    // original sets in stringsetdata should have remained the same
    assertEquals(singleElement.stringSet(), ImmutableSet.of("ab"));
    assertEquals(multipleElement.stringSet(), ImmutableSet.of("cd", "ef"));
  }

  @Test
  public void testCombineWithEmpty() {
    StringSetData empty = StringSetData.empty();
    StringSetData multipleElement = StringSetData.create(ImmutableSet.of("cd", "ef"));
    StringSetData result = empty.combine(multipleElement);
    assertEquals(result.stringSet(), ImmutableSet.of("cd", "ef"));
    // original sets in stringsetdata should have remained the same
    assertTrue(empty.stringSet().isEmpty());
    assertEquals(multipleElement.stringSet(), ImmutableSet.of("cd", "ef"));
  }

  @Test
  public void testEmpty() {
    StringSetData empty = StringSetData.empty();
    assertTrue(empty.stringSet().isEmpty());
  }

  @Test
  public void testStringSetDataEmptyIsImmutable() {
    StringSetData empty = StringSetData.empty();
    assertThrows(UnsupportedOperationException.class, () -> empty.stringSet().add("aa"));
  }

  @Test
  public void testEmptyExtract() {
    assertTrue(StringSetData.empty().extractResult().getStringSet().isEmpty());
  }

  @Test
  public void testExtract() {
    ImmutableSet<String> contents = ImmutableSet.of("ab", "cd");
    StringSetData stringSetData = StringSetData.create(contents);
    assertEquals(stringSetData.stringSet(), contents);
  }

  @Test
  public void testExtractReturnsImmutable() {
    StringSetData stringSetData = StringSetData.create(ImmutableSet.of("ab", "cd"));
    // check that immutable copy is returned
    assertThrows(UnsupportedOperationException.class, () -> stringSetData.stringSet().add("aa"));
  }
}
