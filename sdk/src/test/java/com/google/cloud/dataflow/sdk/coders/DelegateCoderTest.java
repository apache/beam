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

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/** Unit tests for {@link DelegateCoder}. */
@RunWith(JUnit4.class)
public class DelegateCoderTest {

  private static final List<Set<Integer>> TEST_VALUES = Arrays.<Set<Integer>>asList(
      Collections.<Integer>emptySet(),
      Collections.singleton(13),
      new HashSet<>(Arrays.asList(31, -5, 83)));

  private static final Coder<Set<Integer>> coder = DelegateCoder.of(
      ListCoder.of(VarIntCoder.of()),
      new DelegateCoder.CodingFunction<Set<Integer>, List<Integer>>() {
        private static final long serialVersionUID = 0;

        public List<Integer> apply(Set<Integer> input) {
          return Lists.newArrayList(input);
        }
      },
      new DelegateCoder.CodingFunction<List<Integer>, Set<Integer>>() {
        private static final long serialVersionUID = 0;

        public Set<Integer> apply(List<Integer> input) {
          return Sets.newHashSet(input);
        }
      });

  @Test
  public void testDeterministic() throws Exception {
    for (Set<Integer> value : TEST_VALUES) {
      CoderProperties.coderDeterministic(
          coder, value, Sets.newHashSet(value));
    }
  }

  @Test
  public void testDecodeEncodeEqual() throws Exception {
    for (Set<Integer> value : TEST_VALUES) {
      CoderProperties.coderDecodeEncodeEqual(coder, value);
    }
  }

  @Test
  public void testSerializable() throws Exception {
    CoderProperties.coderSerializable(coder);
  }
}
