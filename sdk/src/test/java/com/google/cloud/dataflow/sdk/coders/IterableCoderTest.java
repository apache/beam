/*
 * Copyright (C) 2014 Google Inc.
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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

/** Unit tests for {@link IterableCoder}. */
@RunWith(JUnit4.class)
public class IterableCoderTest {

  private static final List<Iterable<Integer>> TEST_VALUES = Arrays.<Iterable<Integer>>asList(
      Collections.<Integer>emptyList(),
      Collections.<Integer>singletonList(13),
      Arrays.<Integer>asList(1, 2, 3, 4),
      new LinkedList<Integer>(Arrays.asList(7, 6, 5)));

  @Test
  public void testDecodeEncodeContentsInSameOrder() throws Exception {
    Coder<Iterable<Integer>> coder = IterableCoder.of(VarIntCoder.of());
    for (Iterable<Integer> value : TEST_VALUES) {
      CoderProperties.<Integer, Iterable<Integer>>coderDecodeEncodeContentsInSameOrder(
          coder, value);
    }
  }

  @Test
  public void testGetInstanceComponentsNonempty() {
    Iterable<Integer> iterable = Arrays.asList(2, 58, 99, 5);
    List<Object> components = IterableCoder.getInstanceComponents(iterable);
    assertEquals(1, components.size());
    assertEquals(2, components.get(0));
  }

  @Test
  public void testGetInstanceComponentsEmpty() {
    Iterable<Integer> iterable = Arrays.asList();
    List<Object> components = IterableCoder.getInstanceComponents(iterable);
    assertNull(components);
  }
}
