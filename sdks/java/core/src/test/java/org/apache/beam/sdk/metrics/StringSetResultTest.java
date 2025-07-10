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
package org.apache.beam.sdk.metrics;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Sets;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Sets.SetView;
import org.junit.Test;

public class StringSetResultTest {

  @Test
  public void getStringSet() {
    // Test that getStringSet gives an immutable set
    HashSet<String> initialSet = new HashSet<>(Arrays.asList("ab", "cd"));
    Set<String> stringSetResultSet = StringSetResult.create(initialSet).getStringSet();
    assertEquals(initialSet, stringSetResultSet);
    assertThrows(UnsupportedOperationException.class, () -> stringSetResultSet.add("should-fail"));
  }

  @Test
  public void create() {
    // Test that create makes an immutable copy of the given set
    HashSet<String> modifiableSet = new HashSet<>(Arrays.asList("ab", "cd"));
    StringSetResult stringSetResult = StringSetResult.create(modifiableSet);
    // change the initial set.
    modifiableSet.add("ef");
    SetView<String> difference = Sets.difference(modifiableSet, stringSetResult.getStringSet());
    assertEquals(1, difference.size());
    assertEquals("ef", difference.iterator().next());
    assertTrue(Sets.difference(stringSetResult.getStringSet(), modifiableSet).isEmpty());
  }

  @Test
  public void empty() {
    // Test empty returns an immutable set
    StringSetResult empptyStringSetResult = StringSetResult.empty();
    assertTrue(empptyStringSetResult.getStringSet().isEmpty());
    assertThrows(
        UnsupportedOperationException.class,
        () -> empptyStringSetResult.getStringSet().add("should-fail"));
  }
}
