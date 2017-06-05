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
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

import com.google.common.collect.Lists;
import org.apache.beam.sdk.coders.BigEndianIntegerCoder;
import org.apache.beam.sdk.coders.BigEndianLongCoder;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.coders.DoubleCoder;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.coders.VarLongCoder;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Tests for Sum.
 */
@RunWith(JUnit4.class)
public class SumTest {
  private static final CoderRegistry STANDARD_REGISTRY = CoderRegistry.createDefault();

  @Test
  public void testSumGetNames() {
    assertEquals("Combine.globally(SumInteger)", Sum.integersGlobally().getName());
    assertEquals("Combine.globally(SumDouble)", Sum.doublesGlobally().getName());
    assertEquals("Combine.globally(SumLong)", Sum.longsGlobally().getName());
    assertEquals("Combine.perKey(SumInteger)", Sum.integersPerKey().getName());
    assertEquals("Combine.perKey(SumDouble)", Sum.doublesPerKey().getName());
    assertEquals("Combine.perKey(SumLong)", Sum.longsPerKey().getName());
  }

  @Test
  public void testSumIntegerFn() {
    checkCombineFn(
        Sum.ofIntegers(),
        Lists.newArrayList(1, 2, 3, 4),
        10);
  }

  @Test
  public void testSumLongFn() {
    checkCombineFn(
        Sum.ofLongs(),
        Lists.newArrayList(1L, 2L, 3L, 4L),
        10L);
  }

  @Test
  public void testSumDoubleFn() {
    checkCombineFn(
        Sum.ofDoubles(),
        Lists.newArrayList(1.0, 2.0, 3.0, 4.0),
        10.0);
  }

  @Test
  public void testGetAccumulatorCoderEquals() {
    Combine.BinaryCombineIntegerFn sumIntegerFn = Sum.ofIntegers();
    assertEquals(
        sumIntegerFn.getAccumulatorCoder(STANDARD_REGISTRY, VarIntCoder.of()),
        sumIntegerFn.getAccumulatorCoder(STANDARD_REGISTRY, VarIntCoder.of()));
    assertNotEquals(
        sumIntegerFn.getAccumulatorCoder(STANDARD_REGISTRY, VarIntCoder.of()),
        sumIntegerFn.getAccumulatorCoder(STANDARD_REGISTRY, BigEndianIntegerCoder.of()));

    Combine.BinaryCombineLongFn sumLongFn = Sum.ofLongs();
    assertEquals(
        sumLongFn.getAccumulatorCoder(STANDARD_REGISTRY, VarLongCoder.of()),
        sumLongFn.getAccumulatorCoder(STANDARD_REGISTRY, VarLongCoder.of()));
    assertNotEquals(
        sumLongFn.getAccumulatorCoder(STANDARD_REGISTRY, VarLongCoder.of()),
        sumLongFn.getAccumulatorCoder(STANDARD_REGISTRY, BigEndianLongCoder.of()));

    Combine.BinaryCombineDoubleFn sumDoubleFn = Sum.ofDoubles();
    assertEquals(
        sumDoubleFn.getAccumulatorCoder(STANDARD_REGISTRY, DoubleCoder.of()),
        sumDoubleFn.getAccumulatorCoder(STANDARD_REGISTRY, DoubleCoder.of()));
  }
}
