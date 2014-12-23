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

/** Unit tests for {@link ListCoder}. */
@RunWith(JUnit4.class)
public class ListCoderTest {

  private static final List<List<Integer>> TEST_VALUES = Arrays.<List<Integer>>asList(
      Collections.<Integer>emptyList(),
      Collections.singletonList(43),
      Arrays.asList(1, 2, 3, 4),
      new LinkedList<Integer>(Arrays.asList(7, 6, 5)));

  @Test
  public void testDecodeEncodeContentsInSameOrder() throws Exception {
    Coder<List<Integer>> coder = ListCoder.of(VarIntCoder.of());
    for (List<Integer> value : TEST_VALUES) {
      CoderProperties.<Integer, List<Integer>>coderDecodeEncodeContentsInSameOrder(coder, value);
    }
  }

  @Test
  public void testGetInstanceComponentsNonempty() throws Exception {
    List<Integer> list = Arrays.asList(21, 5, 3, 5);
    List<Object> components = ListCoder.getInstanceComponents(list);
    assertEquals(1, components.size());
    assertEquals(21, components.get(0));
  }

  @Test
  public void testGetInstanceComponentsEmpty() throws Exception {
    List<Integer> list = Arrays.asList();
    List<Object> components = ListCoder.getInstanceComponents(list);
    assertNull(components);
  }

  @Test
  public void testEmptyList() throws Exception {
    List<Integer> list = Collections.emptyList();
    Coder<List<Integer>> coder = ListCoder.of(VarIntCoder.of());
    CoderProperties.<List<Integer>>coderDecodeEncodeEqual(coder, list);
  }
}
