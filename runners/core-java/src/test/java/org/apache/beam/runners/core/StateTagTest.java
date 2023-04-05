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
package org.apache.beam.runners.core;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

import org.apache.beam.sdk.coders.BigEndianIntegerCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.Max;
import org.apache.beam.sdk.transforms.Min;
import org.apache.beam.sdk.transforms.windowing.TimestampCombiner;
import org.apache.beam.sdk.util.CombineFnUtil;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link StateTag}. */
@RunWith(JUnit4.class)
public class StateTagTest {
  @Test
  public void testValueEquality() {
    StateTag<?> fooVarInt1 = StateTags.value("foo", VarIntCoder.of());
    StateTag<?> fooVarInt2 = StateTags.value("foo", VarIntCoder.of());
    StateTag<?> fooBigEndian = StateTags.value("foo", BigEndianIntegerCoder.of());
    StateTag<?> barVarInt = StateTags.value("bar", VarIntCoder.of());

    assertEquals(fooVarInt1, fooVarInt2);
    assertEquals(fooVarInt1, fooBigEndian);
    assertNotEquals(fooVarInt1, barVarInt);
  }

  @Test
  public void testBagEquality() {
    StateTag<?> fooVarInt1 = StateTags.bag("foo", VarIntCoder.of());
    StateTag<?> fooVarInt2 = StateTags.bag("foo", VarIntCoder.of());
    StateTag<?> fooBigEndian = StateTags.bag("foo", BigEndianIntegerCoder.of());
    StateTag<?> barVarInt = StateTags.bag("bar", VarIntCoder.of());

    assertEquals(fooVarInt1, fooVarInt2);
    assertEquals(fooVarInt1, fooBigEndian);
    assertNotEquals(fooVarInt1, barVarInt);
  }

  @Test
  public void testSetEquality() {
    StateTag<?> fooVarInt1 = StateTags.set("foo", VarIntCoder.of());
    StateTag<?> fooVarInt2 = StateTags.set("foo", VarIntCoder.of());
    StateTag<?> fooBigEndian = StateTags.set("foo", BigEndianIntegerCoder.of());
    StateTag<?> barVarInt = StateTags.set("bar", VarIntCoder.of());

    assertEquals(fooVarInt1, fooVarInt2);
    assertEquals(fooVarInt1, fooBigEndian);
    assertNotEquals(fooVarInt1, barVarInt);
  }

  @Test
  public void testMapEquality() {
    StateTag<?> fooStringVarInt1 = StateTags.map("foo", StringUtf8Coder.of(), VarIntCoder.of());
    StateTag<?> fooStringVarInt2 = StateTags.map("foo", StringUtf8Coder.of(), VarIntCoder.of());
    StateTag<?> fooStringBigEndian =
        StateTags.map("foo", StringUtf8Coder.of(), BigEndianIntegerCoder.of());
    StateTag<?> fooVarIntBigEndian =
        StateTags.map("foo", VarIntCoder.of(), BigEndianIntegerCoder.of());
    StateTag<?> barStringVarInt = StateTags.map("bar", StringUtf8Coder.of(), VarIntCoder.of());

    assertEquals(fooStringVarInt1, fooStringVarInt2);
    assertEquals(fooStringVarInt1, fooStringBigEndian);
    assertEquals(fooStringBigEndian, fooVarIntBigEndian);
    assertEquals(fooStringVarInt1, fooVarIntBigEndian);
    assertNotEquals(fooStringVarInt1, barStringVarInt);
  }

  @Test
  public void testWatermarkBagEquality() {
    StateTag<?> foo1 = StateTags.watermarkStateInternal("foo", TimestampCombiner.EARLIEST);
    StateTag<?> foo2 = StateTags.watermarkStateInternal("foo", TimestampCombiner.EARLIEST);
    StateTag<?> bar = StateTags.watermarkStateInternal("bar", TimestampCombiner.EARLIEST);

    StateTag<?> bar2 = StateTags.watermarkStateInternal("bar", TimestampCombiner.LATEST);

    // Same id, same fn.
    assertEquals(foo1, foo2);
    // Different id, same fn.
    assertNotEquals(foo1, bar);
    // Same id, different fn.
    assertEquals(bar, bar2);
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  @Test
  public void testCombiningValueEquality() {
    Combine.BinaryCombineIntegerFn maxFn = Max.ofIntegers();
    Coder<Integer> input1 = VarIntCoder.of();
    Coder<Integer> input2 = BigEndianIntegerCoder.of();
    Combine.BinaryCombineIntegerFn minFn = Min.ofIntegers();

    StateTag<?> fooCoder1Max1 = StateTags.combiningValueFromInputInternal("foo", input1, maxFn);
    StateTag<?> fooCoder1Max2 = StateTags.combiningValueFromInputInternal("foo", input1, maxFn);
    StateTag<?> fooCoder1Min = StateTags.combiningValueFromInputInternal("foo", input1, minFn);

    StateTag<?> fooCoder2Max = StateTags.combiningValueFromInputInternal("foo", input2, maxFn);
    StateTag<?> barCoder1Max = StateTags.combiningValueFromInputInternal("bar", input1, maxFn);

    // Same name, coder and combineFn
    assertEquals(fooCoder1Max1, fooCoder1Max2);
    assertEquals(
        StateTags.convertToBagTagInternal((StateTag) fooCoder1Max1),
        StateTags.convertToBagTagInternal((StateTag) fooCoder1Max2));

    // Different combineFn, but we treat them as equal since we only serialize the bits.
    assertEquals(fooCoder1Max1, fooCoder1Min);
    assertEquals(
        StateTags.convertToBagTagInternal((StateTag) fooCoder1Max1),
        StateTags.convertToBagTagInternal((StateTag) fooCoder1Min));

    // Different input coder coder.
    assertEquals(fooCoder1Max1, fooCoder2Max);
    assertEquals(
        StateTags.convertToBagTagInternal((StateTag) fooCoder1Max1),
        StateTags.convertToBagTagInternal((StateTag) fooCoder2Max));

    // These StateTags have different IDs.
    assertNotEquals(fooCoder1Max1, barCoder1Max);
    assertNotEquals(
        StateTags.convertToBagTagInternal((StateTag) fooCoder1Max1),
        StateTags.convertToBagTagInternal((StateTag) barCoder1Max));
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  @Test
  public void testCombiningValueWithContextEquality() {
    CoderRegistry registry = CoderRegistry.createDefault();

    Combine.BinaryCombineIntegerFn maxFn = Max.ofIntegers();
    Combine.BinaryCombineIntegerFn minFn = Min.ofIntegers();

    Coder<int[]> accum1 = maxFn.getAccumulatorCoder(registry, VarIntCoder.of());
    Coder<int[]> accum2 = minFn.getAccumulatorCoder(registry, BigEndianIntegerCoder.of());

    StateTag<?> fooCoder1Max1 =
        StateTags.combiningValueWithContext("foo", accum1, CombineFnUtil.toFnWithContext(maxFn));
    StateTag<?> fooCoder1Max2 =
        StateTags.combiningValueWithContext("foo", accum1, CombineFnUtil.toFnWithContext(maxFn));
    StateTag<?> fooCoder1Min =
        StateTags.combiningValueWithContext("foo", accum1, CombineFnUtil.toFnWithContext(minFn));

    StateTag<?> fooCoder2Max =
        StateTags.combiningValueWithContext("foo", accum2, CombineFnUtil.toFnWithContext(maxFn));
    StateTag<?> barCoder1Max =
        StateTags.combiningValueWithContext("bar", accum1, CombineFnUtil.toFnWithContext(maxFn));

    // Same name, coder and combineFn
    assertEquals(fooCoder1Max1, fooCoder1Max2);
    assertEquals(
        StateTags.convertToBagTagInternal((StateTag) fooCoder1Max1),
        StateTags.convertToBagTagInternal((StateTag) fooCoder1Max2));
    // Different combineFn, but we treat them as equal since we only serialize the bits.
    assertEquals(fooCoder1Max1, fooCoder1Min);
    assertEquals(
        StateTags.convertToBagTagInternal((StateTag) fooCoder1Max1),
        StateTags.convertToBagTagInternal((StateTag) fooCoder1Min));

    // Different input coder coder.
    assertEquals(fooCoder1Max1, fooCoder2Max);
    assertEquals(
        StateTags.convertToBagTagInternal((StateTag) fooCoder1Max1),
        StateTags.convertToBagTagInternal((StateTag) fooCoder2Max));

    // These StateTags have different IDs.
    assertNotEquals(fooCoder1Max1, barCoder1Max);
    assertNotEquals(
        StateTags.convertToBagTagInternal((StateTag) fooCoder1Max1),
        StateTags.convertToBagTagInternal((StateTag) barCoder1Max));
  }
}
