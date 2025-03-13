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
import static org.junit.Assert.assertEquals;

import java.util.Arrays;
import java.util.List;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.testing.CoderProperties;
import org.apache.beam.sdk.transforms.Mean.CountSum;
import org.apache.beam.sdk.transforms.Mean.CountSumCoder;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Lists;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for Mean. */
@RunWith(JUnit4.class)
public class MeanTest {

  @Test
  public void testMeanGetNames() {
    assertEquals("Combine.globally(Mean)", Mean.globally().getName());
    assertEquals("Combine.perKey(Mean)", Mean.perKey().getName());
  }

  private static final Coder<CountSum<Number>> TEST_CODER = new CountSumCoder<>();

  private static final List<CountSum<Number>> TEST_VALUES =
      Arrays.asList(new CountSum<>(1, 5.7), new CountSum<>(42, 42.0), new CountSum<>(29, 2.2));

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
    testCombineFn(Mean.of(), Lists.newArrayList(1, 2, 3, 4), 2.5);
  }
}
