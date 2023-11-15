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

import static org.apache.beam.sdk.testing.CombineFnTester.testCombineFn;
import static org.apache.beam.sdk.transforms.display.DisplayDataMatchers.hasDisplayItem;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;

import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Lists;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for Min. */
@RunWith(JUnit4.class)
public class MinTest {
  @Test
  public void testMinGetNames() {
    assertEquals("Combine.globally(MinInteger)", Min.integersGlobally().getName());
    assertEquals("Combine.globally(MinDouble)", Min.doublesGlobally().getName());
    assertEquals("Combine.globally(MinLong)", Min.longsGlobally().getName());
    assertEquals("Combine.perKey(MinInteger)", Min.integersPerKey().getName());
    assertEquals("Combine.perKey(MinDouble)", Min.doublesPerKey().getName());
    assertEquals("Combine.perKey(MinLong)", Min.longsPerKey().getName());
  }

  @Test
  public void testMinIntegerFn() {
    testCombineFn(Min.ofIntegers(), Lists.newArrayList(1, 2, 3, 4), 1);
  }

  @Test
  public void testMinLongFn() {
    testCombineFn(Min.ofLongs(), Lists.newArrayList(1L, 2L, 3L, 4L), 1L);
  }

  @Test
  public void testMinDoubleFn() {
    testCombineFn(Min.ofDoubles(), Lists.newArrayList(1.0, 2.0, 3.0, 4.0), 1.0);
  }

  @Test
  public void testMinDoubleFnInfinity() {
    testCombineFn(
        Min.ofDoubles(),
        Lists.newArrayList(Double.NEGATIVE_INFINITY, 2.0, 3.0, Double.POSITIVE_INFINITY),
        Double.NEGATIVE_INFINITY);
  }

  @Test
  public void testMinDoubleFnNan() {
    testCombineFn(
        Min.ofDoubles(),
        Lists.newArrayList(Double.NEGATIVE_INFINITY, 2.0, 3.0, Double.NaN),
        Double.NaN);
  }

  @Test
  public void testDisplayData() {
    Top.Reversed<Integer> comparer = new Top.Reversed<>();

    Combine.Globally<Integer, Integer> min = Min.globally(comparer);
    assertThat(DisplayData.from(min), hasDisplayItem("comparer", comparer.getClass()));
  }
}
