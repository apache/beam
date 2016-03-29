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

package com.google.cloud.dataflow.sdk.transforms;

import static com.google.cloud.dataflow.sdk.TestUtils.checkCombineFn;
import static org.junit.Assert.assertEquals;

import com.google.common.collect.Lists;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Tests for Max.
 */
@RunWith(JUnit4.class)
public class MaxTest {
  @Test
  public void testMeanGetNames() {
    assertEquals("Max.Globally", Max.integersGlobally().getName());
    assertEquals("Max.Globally", Max.doublesGlobally().getName());
    assertEquals("Max.Globally", Max.longsGlobally().getName());
    assertEquals("Max.PerKey", Max.integersPerKey().getName());
    assertEquals("Max.PerKey", Max.doublesPerKey().getName());
    assertEquals("Max.PerKey", Max.longsPerKey().getName());
  }

  @Test
  public void testMaxIntegerFn() {
    checkCombineFn(
        new Max.MaxIntegerFn(),
        Lists.newArrayList(1, 2, 3, 4),
        4);
  }

  @Test
  public void testMaxLongFn() {
    checkCombineFn(
        new Max.MaxLongFn(),
        Lists.newArrayList(1L, 2L, 3L, 4L),
        4L);
  }

  @Test
  public void testMaxDoubleFn() {
    checkCombineFn(
        new Max.MaxDoubleFn(),
        Lists.newArrayList(1.0, 2.0, 3.0, 4.0),
        4.0);
  }
}
