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
package com.google.cloud.dataflow.sdk.util.state;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;

import com.google.cloud.dataflow.sdk.coders.StringUtf8Coder;
import com.google.cloud.dataflow.sdk.coders.VarIntCoder;
import com.google.cloud.dataflow.sdk.transforms.Sum;

import org.hamcrest.Matchers;
import org.joda.time.Instant;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.util.Arrays;

/**
 * Tests for {@link InMemoryStateInternals}.
 */
@RunWith(JUnit4.class)
public class InMemoryStateInternalsTest {

  private static final StateNamespace NAMESPACE_1 = new StateNamespaceForTest("ns1");
  private static final StateNamespace NAMESPACE_2 = new StateNamespaceForTest("ns2");
  private static final StateNamespace NAMESPACE_3 = new StateNamespaceForTest("ns3");

  private static final StateTag<ValueState<String>> STRING_VALUE_ADDR =
      StateTags.value("stringValue", StringUtf8Coder.of());
  private static final StateTag<CombiningValueState<Integer, Integer>> SUM_INTEGER_ADDR =
      StateTags.combiningValueFromInputInternal(
          "sumInteger", VarIntCoder.of(), new Sum.SumIntegerFn());
  private static final StateTag<BagState<String>> STRING_BAG_ADDR =
      StateTags.bag("stringBag", StringUtf8Coder.of());
  private static final StateTag<WatermarkStateInternal> WATERMARK_BAG_ADDR =
      StateTags.watermarkStateInternal("watermark");

  InMemoryStateInternals underTest = new InMemoryStateInternals();

  @Test
  public void testValue() throws Exception {
    ValueState<String> value = underTest.state(NAMESPACE_1, STRING_VALUE_ADDR);

    // State instances are cached, but depend on the namespace.
    assertThat(underTest.state(NAMESPACE_1, STRING_VALUE_ADDR), Matchers.sameInstance(value));
    assertThat(underTest.state(NAMESPACE_2, STRING_VALUE_ADDR),
        Matchers.not(Matchers.sameInstance(value)));

    assertThat(value.get().read(), Matchers.nullValue());
    StateContents<String> readFuture = value.get();
    value.set("hello");
    assertThat(readFuture.read(), Matchers.equalTo("hello"));
    assertThat(value.get().read(), Matchers.equalTo("hello"));
    value.set("world");
    assertThat(readFuture.read(), Matchers.equalTo("world"));

    value.clear();
    assertThat(value.get().read(), Matchers.nullValue());
    assertThat(underTest.state(NAMESPACE_1, STRING_VALUE_ADDR), Matchers.sameInstance(value));
  }

  @Test
  public void testBag() throws Exception {
    BagState<String> value = underTest.state(NAMESPACE_1, STRING_BAG_ADDR);

    // State instances are cached, but depend on the namespace.
    assertEquals(value, underTest.state(NAMESPACE_1, STRING_BAG_ADDR));
    assertFalse(value.equals(underTest.state(NAMESPACE_2, STRING_BAG_ADDR)));

    assertThat(value.get().read(), Matchers.emptyIterable());
    StateContents<Iterable<String>> readFuture = value.get();
    value.add("hello");
    assertThat(readFuture.read(), Matchers.containsInAnyOrder("hello"));
    assertThat(value.get().read(), Matchers.containsInAnyOrder("hello"));

    value.add("world");
    assertThat(value.get().read(), Matchers.containsInAnyOrder("hello", "world"));

    value.clear();
    assertThat(value.get().read(), Matchers.emptyIterable());
    assertThat(underTest.state(NAMESPACE_1, STRING_BAG_ADDR), Matchers.sameInstance(value));
  }

  @Test
  public void testBagIsEmpty() throws Exception {
    BagState<String> value = underTest.state(NAMESPACE_1, STRING_BAG_ADDR);

    assertThat(value.isEmpty().read(), Matchers.is(true));
    StateContents<Boolean> readFuture = value.isEmpty();
    value.add("hello");
    assertThat(readFuture.read(), Matchers.is(false));

    value.clear();
    assertThat(readFuture.read(), Matchers.is(true));
  }

  @Test
  public void testMergeBagIntoSource() throws Exception {
    BagState<String> bag1 = underTest.state(NAMESPACE_1, STRING_BAG_ADDR);
    BagState<String> bag2 = underTest.state(NAMESPACE_2, STRING_BAG_ADDR);

    bag1.add("Hello");
    bag2.add("World");
    bag1.add("!");

    BagState<String> merged = underTest.mergedState(
        Arrays.asList(NAMESPACE_1, NAMESPACE_2), NAMESPACE_1, STRING_BAG_ADDR);

    // Reading the merged bag gets both the contents
    assertThat(merged.get().read(), Matchers.containsInAnyOrder("Hello", "World", "!"));

    // Adding to the merged bag adds to namespace 1
    merged.add("...");
    assertThat(merged.get().read(), Matchers.containsInAnyOrder("Hello", "World", "!", "..."));
    assertThat(bag1.get().read(), Matchers.containsInAnyOrder("Hello", "!", "..."));
    assertThat(bag2.get().read(), Matchers.not(Matchers.contains("...")));
  }

  @Test
  public void testMergeBagIntoNewNamespace() throws Exception {
    BagState<String> bag1 = underTest.state(NAMESPACE_1, STRING_BAG_ADDR);
    BagState<String> bag2 = underTest.state(NAMESPACE_2, STRING_BAG_ADDR);

    bag1.add("Hello");
    bag2.add("World");
    bag1.add("!");

    BagState<String> merged = underTest.mergedState(
        Arrays.asList(NAMESPACE_1, NAMESPACE_2), NAMESPACE_3, STRING_BAG_ADDR);

    // Reading the merged bag gets both the contents
    assertThat(merged.get().read(), Matchers.containsInAnyOrder("Hello", "World", "!"));

    // Adding to the merged bag adds to namespace 3
    merged.add("...");
    assertThat(merged.get().read(), Matchers.containsInAnyOrder("Hello", "World", "!", "..."));
    assertThat(bag1.get().read(), Matchers.not(Matchers.contains("...")));
    assertThat(bag2.get().read(), Matchers.not(Matchers.contains("...")));
    assertThat(
        underTest.state(NAMESPACE_3, STRING_BAG_ADDR).get().read(), Matchers.contains("..."));
  }

  @Test
  public void testCombiningValue() throws Exception {
    CombiningValueState<Integer, Integer> value = underTest.state(NAMESPACE_1, SUM_INTEGER_ADDR);

    // State instances are cached, but depend on the namespace.
    assertEquals(value, underTest.state(NAMESPACE_1, SUM_INTEGER_ADDR));
    assertFalse(value.equals(underTest.state(NAMESPACE_2, SUM_INTEGER_ADDR)));

    assertThat(value.get().read(), Matchers.equalTo(0));
    StateContents<Integer> readFuture = value.get();
    value.add(2);
    assertThat(readFuture.read(), Matchers.equalTo(2));
    assertThat(value.get().read(), Matchers.equalTo(2));

    value.add(3);
    assertThat(readFuture.read(), Matchers.equalTo(5));

    value.clear();
    assertThat(readFuture.read(), Matchers.equalTo(0));
    assertThat(underTest.state(NAMESPACE_1, SUM_INTEGER_ADDR), Matchers.sameInstance(value));
  }

  @Test
  public void testCombiningIsEmpty() throws Exception {
    CombiningValueState<Integer, Integer> value = underTest.state(NAMESPACE_1, SUM_INTEGER_ADDR);

    assertThat(value.isEmpty().read(), Matchers.is(true));
    StateContents<Boolean> readFuture = value.isEmpty();
    value.add(5);
    assertThat(readFuture.read(), Matchers.is(false));

    value.clear();
    assertThat(readFuture.read(), Matchers.is(true));
  }

  @Test
  public void testMergeCombiningValueIntoSource() throws Exception {
    CombiningValueState<Integer, Integer> value1 = underTest.state(NAMESPACE_1, SUM_INTEGER_ADDR);
    CombiningValueState<Integer, Integer> value2 = underTest.state(NAMESPACE_2, SUM_INTEGER_ADDR);

    value1.add(5);
    value2.add(10);
    value1.add(6);

    assertThat(value1.get().read(), Matchers.equalTo(11));
    assertThat(value2.get().read(), Matchers.equalTo(10));

    CombiningValueState<Integer, Integer> merged = underTest.mergedState(
        Arrays.asList(NAMESPACE_1, NAMESPACE_2), NAMESPACE_1, SUM_INTEGER_ADDR);

    assertThat(value1.get().read(), Matchers.equalTo(11));
    assertThat(value2.get().read(), Matchers.equalTo(10));
    assertThat(merged.get().read(), Matchers.equalTo(21));

    // Reading the merged value compressed the old values.
    assertThat(value1.get().read(), Matchers.equalTo(21));
    assertThat(value2.get().read(), Matchers.equalTo(0));

    merged.add(8);
    assertThat(merged.get().read(), Matchers.equalTo(29));
    assertThat(value1.get().read(), Matchers.equalTo(29));
    assertThat(value2.get().read(), Matchers.equalTo(0));
  }

  @Test
  public void testMergeCombiningValueIntoNewNamespace() throws Exception {
    CombiningValueState<Integer, Integer> value1 = underTest.state(NAMESPACE_1, SUM_INTEGER_ADDR);
    CombiningValueState<Integer, Integer> value2 = underTest.state(NAMESPACE_2, SUM_INTEGER_ADDR);

    value1.add(5);
    value2.add(10);
    value1.add(6);

    assertThat(value1.get().read(), Matchers.equalTo(11));
    assertThat(value2.get().read(), Matchers.equalTo(10));

    CombiningValueState<Integer, Integer> merged = underTest.mergedState(
        Arrays.asList(NAMESPACE_1, NAMESPACE_2), NAMESPACE_3, SUM_INTEGER_ADDR);

    assertThat(value1.get().read(), Matchers.equalTo(11));
    assertThat(value2.get().read(), Matchers.equalTo(10));
    assertThat(merged.get().read(), Matchers.equalTo(21));

    // Reading the merged value compressed the old values.
    CombiningValueState<Integer, Integer> value3 = underTest.state(NAMESPACE_3, SUM_INTEGER_ADDR);
    assertThat(value1.get().read(), Matchers.equalTo(0));
    assertThat(value2.get().read(), Matchers.equalTo(0));
    assertThat(value3.get().read(), Matchers.equalTo(21));

    merged.add(8);
    assertThat(merged.get().read(), Matchers.equalTo(29));
    assertThat(value1.get().read(), Matchers.equalTo(0));
    assertThat(value2.get().read(), Matchers.equalTo(0));
    assertThat(value3.get().read(), Matchers.equalTo(29));
  }

  @Test
  public void testWatermarkState() throws Exception {
    WatermarkStateInternal value = underTest.state(NAMESPACE_1, WATERMARK_BAG_ADDR);

    // State instances are cached, but depend on the namespace.
    assertEquals(value, underTest.state(NAMESPACE_1, WATERMARK_BAG_ADDR));
    assertFalse(value.equals(underTest.state(NAMESPACE_2, WATERMARK_BAG_ADDR)));

    assertThat(value.get().read(), Matchers.nullValue());
    StateContents<Instant> readFuture = value.get();
    value.add(new Instant(2000));
    assertThat(readFuture.read(), Matchers.equalTo(new Instant(2000)));
    assertThat(value.get().read(), Matchers.equalTo(new Instant(2000)));

    value.add(new Instant(3000));
    assertThat(readFuture.read(), Matchers.equalTo(new Instant(2000)));
    assertThat(value.get().read(), Matchers.equalTo(new Instant(2000)));

    value.add(new Instant(1000));
    assertThat(readFuture.read(), Matchers.equalTo(new Instant(1000)));
    assertThat(value.get().read(), Matchers.equalTo(new Instant(1000)));

    value.clear();
    assertThat(readFuture.read(), Matchers.equalTo(null));
    assertThat(underTest.state(NAMESPACE_1, WATERMARK_BAG_ADDR), Matchers.sameInstance(value));
  }

  @Test
  public void testWatermarkStateIsEmpty() throws Exception {
    WatermarkStateInternal value = underTest.state(NAMESPACE_1, WATERMARK_BAG_ADDR);

    assertThat(value.isEmpty().read(), Matchers.is(true));
    StateContents<Boolean> readFuture = value.isEmpty();
    value.add(new Instant(1000));
    assertThat(readFuture.read(), Matchers.is(false));

    value.clear();
    assertThat(readFuture.read(), Matchers.is(true));
  }

  @Test
  public void testMergeWatermarkIntoSource() throws Exception {
    WatermarkStateInternal value1 = underTest.state(NAMESPACE_1, WATERMARK_BAG_ADDR);
    WatermarkStateInternal value2 = underTest.state(NAMESPACE_2, WATERMARK_BAG_ADDR);

    value1.add(new Instant(3000));
    value2.add(new Instant(5000));
    value1.add(new Instant(4000));
    value2.add(new Instant(2000));

    WatermarkStateInternal merged = underTest.mergedState(
        Arrays.asList(NAMESPACE_1, NAMESPACE_2), NAMESPACE_1, WATERMARK_BAG_ADDR);

    assertThat(value1.get().read(), Matchers.equalTo(new Instant(3000)));
    assertThat(value2.get().read(), Matchers.equalTo(new Instant(2000)));
    assertThat(merged.get().read(), Matchers.equalTo(new Instant(2000)));

    // Reading the merged value compressed the old values
    assertThat(value1.get().read(), Matchers.equalTo(new Instant(2000)));
    assertThat(value2.get().read(), Matchers.equalTo(null));

    merged.add(new Instant(1000));
    assertThat(merged.get().read(), Matchers.equalTo(new Instant(1000)));
  }
}
