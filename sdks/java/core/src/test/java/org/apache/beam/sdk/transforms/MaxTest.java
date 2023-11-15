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

/** Tests for Max. */
@RunWith(JUnit4.class)
public class MaxTest {
  @Test
  public void testMaxGetNames() {
    assertEquals("Combine.globally(MaxInteger)", Max.integersGlobally().getName());
    assertEquals("Combine.globally(MaxDouble)", Max.doublesGlobally().getName());
    assertEquals("Combine.globally(MaxLong)", Max.longsGlobally().getName());
    assertEquals("Combine.perKey(MaxInteger)", Max.integersPerKey().getName());
    assertEquals("Combine.perKey(MaxDouble)", Max.doublesPerKey().getName());
    assertEquals("Combine.perKey(MaxLong)", Max.longsPerKey().getName());
  }

  @Test
  public void testMaxIntegerFn() {
    testCombineFn(Max.ofIntegers(), Lists.newArrayList(1, 2, 3, 4), 4);
  }

  @Test
  public void testMaxLongFn() {
    testCombineFn(Max.ofLongs(), Lists.newArrayList(1L, 2L, 3L, 4L), 4L);
  }

  @Test
  public void testMaxDoubleFn() {
    testCombineFn(Max.ofDoubles(), Lists.newArrayList(1.0, 2.0, 3.0, 4.0), 4.0);
  }

  @Test
  public void testMaxDoubleFnInfinity() {
    testCombineFn(
        Max.ofDoubles(),
        Lists.newArrayList(Double.NEGATIVE_INFINITY, 2.0, 3.0, Double.POSITIVE_INFINITY),
        Double.POSITIVE_INFINITY);
  }

  @Test
  public void testMaxDoubleFnNan() {
    testCombineFn(
        Max.ofDoubles(),
        Lists.newArrayList(Double.NaN, 2.0, 3.0, Double.POSITIVE_INFINITY),
        Double.NaN);
  }

  @Test
  public void testDisplayData() {
    Top.Natural<Integer> comparer = new Top.Natural<>();

    Combine.Globally<Integer, Integer> max = Max.globally(comparer);
    assertThat(DisplayData.from(max), hasDisplayItem("comparer", comparer.getClass()));
  }
}
