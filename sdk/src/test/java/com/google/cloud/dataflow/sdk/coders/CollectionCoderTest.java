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

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;

/**
 * Test case for {@link CollectionCoder}.
 */
@RunWith(JUnit4.class)
public class CollectionCoderTest {

  private static final List<Collection<Integer>> TEST_VALUES = Arrays.<Collection<Integer>>asList(
      Collections.<Integer>emptyList(),
      Collections.<Integer>emptySet(),
      Collections.singletonList(13),
      Arrays.asList(1, 2, 3, 4),
      new LinkedList<>(Arrays.asList(7, 6, 5)),
      new HashSet<>(Arrays.asList(31, -5, 83)));

  @Test
  public void testDecodeEncodeContentsEqual() throws Exception {
    Coder<Collection<Integer>> coder = CollectionCoder.of(VarIntCoder.of());
    for (Collection<Integer> value : TEST_VALUES) {
      CoderProperties.coderDecodeEncodeContentsEqual(coder, value);
    }
  }
}
