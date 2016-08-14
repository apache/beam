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
package org.apache.beam.sdk.transforms;


import static org.apache.beam.sdk.TestUtils.checkCombineFn;
import static org.apache.beam.sdk.transforms.display.DisplayDataMatchers.hasDisplayItem;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;

import org.apache.beam.sdk.transforms.display.DisplayData;

import com.google.common.collect.Lists;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Tests for Min.
 */
@RunWith(JUnit4.class)
public class MinTest {
  @Test
  public void testMeanGetNames() {
    assertEquals("Min.Globally", Min.integersGlobally().getName());
    assertEquals("Min.Globally", Min.doublesGlobally().getName());
    assertEquals("Min.Globally", Min.longsGlobally().getName());
    assertEquals("Min.PerKey", Min.integersPerKey().getName());
    assertEquals("Min.PerKey", Min.doublesPerKey().getName());
    assertEquals("Min.PerKey", Min.longsPerKey().getName());
  }

  @Test
  public void testMinIntegerFn() {
    checkCombineFn(
        new Min.MinIntegerFn(),
        Lists.newArrayList(1, 2, 3, 4),
        1);
  }

  @Test
  public void testMinLongFn() {
    checkCombineFn(
        new Min.MinLongFn(),
        Lists.newArrayList(1L, 2L, 3L, 4L),
        1L);
  }

  @Test
  public void testMinDoubleFn() {
    checkCombineFn(
        new Min.MinDoubleFn(),
        Lists.newArrayList(1.0, 2.0, 3.0, 4.0),
        1.0);
  }

  @Test
  public void testDisplayData() {
    Top.Smallest<Integer> comparer = new Top.Smallest<>();

    Combine.Globally<Integer, Integer> min = Min.globally(comparer);
    assertThat(DisplayData.from(min), hasDisplayItem("comparer", comparer.getClass()));
  }
}
