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

import com.google.cloud.dataflow.sdk.coders.Coder;
import com.google.cloud.dataflow.sdk.testing.CoderProperties;
import com.google.cloud.dataflow.sdk.transforms.Mean.CountSum;
import com.google.cloud.dataflow.sdk.transforms.Mean.CountSumCoder;
import com.google.common.collect.Lists;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.util.Arrays;
import java.util.List;

/**
 * Tests for Mean.
 */
@RunWith(JUnit4.class)
public class MeanTest {
  @Test
  public void testMeanGetNames() {
    assertEquals("Mean.Globally", Mean.globally().getName());
    assertEquals("Mean.PerKey", Mean.perKey().getName());
  }

  private static final Coder<CountSum<Number>> TEST_CODER = new CountSumCoder<>();

  private static final List<CountSum<Number>> TEST_VALUES = Arrays.asList(
      new CountSum<>(1, 5.7),
      new CountSum<>(42, 42.0),
      new CountSum<>(29, 2.2));

  @Test
  public void testCountSumCoderEncodeDecode() throws Exception {
    for (CountSum<Number> value : TEST_VALUES) {
      CoderProperties.coderDecodeEncodeEqual(TEST_CODER, value);
    }
  }

  @Test
  public void testCountSumCoderSerializable() throws Exception {
    CoderProperties.coderSerializable(TEST_CODER);
  }

  @Test
  public void testMeanFn() throws Exception {
    checkCombineFn(
        new Mean.MeanFn<Integer>(),
        Lists.newArrayList(1, 2, 3, 4),
        2.5);
  }
}
