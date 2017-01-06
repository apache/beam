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
package org.apache.beam.sdk.util.state;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;

import java.util.Arrays;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.transforms.Sum;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.transforms.windowing.OutputTimeFns;
import org.hamcrest.Matchers;
import org.joda.time.Instant;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Tests for {@link InMemoryStateInternals}.
 */
@RunWith(JUnit4.class)
public class InMemoryStateInternalsTest {
  private static final BoundedWindow WINDOW_1 = new IntervalWindow(new Instant(0), new Instant(10));
  private static final StateNamespace NAMESPACE_1 = new StateNamespaceForTest("ns1");
  private static final StateNamespace NAMESPACE_2 = new StateNamespaceForTest("ns2");
  private static final StateNamespace NAMESPACE_3 = new StateNamespaceForTest("ns3");

  private static final StateTag<Object, ValueState<String>> STRING_VALUE_ADDR =
      StateTags.value("stringValue", StringUtf8Coder.of());
  private static final StateTag<Object, AccumulatorCombiningState<Integer, int[], Integer>>
      SUM_INTEGER_ADDR = StateTags.combiningValueFromInputInternal(
          "sumInteger", VarIntCoder.of(), Sum.ofIntegers());
  private static final StateTag<Object, BagState<String>> STRING_BAG_ADDR =
      StateTags.bag("stringBag", StringUtf8Coder.of());
  private static final StateTag<Object, WatermarkHoldState<BoundedWindow>>
      WATERMARK_EARLIEST_ADDR =
      StateTags.watermarkStateInternal("watermark", OutputTimeFns.outputAtEarliestInputTimestamp());
  private static final StateTag<Object, WatermarkHoldState<BoundedWindow>>
      WATERMARK_LATEST_ADDR =
      StateTags.watermarkStateInternal("watermark", OutputTimeFns.outputAtLatestInputTimestamp());
  private static final StateTag<Object, WatermarkHoldState<BoundedWindow>> WATERMARK_EOW_ADDR =
      StateTags.watermarkStateInternal("watermark", OutputTimeFns.outputAtEndOfWindow());

  InMemoryStateInternals<String> underTest = InMemoryStateInternals.forKey("dummyKey");

  @Test
  public void testValue() throws Exception {
    ValueState<String> value = underTest.state(NAMESPACE_1, STRING_VALUE_ADDR);

    // State instances are cached, but depend on the namespace.
    assertThat(underTest.state(NAMESPACE_1, STRING_VALUE_ADDR), Matchers.sameInstance(value));
    assertThat(
        underTest.state(NAMESPACE_2, STRING_VALUE_ADDR),
        Matchers.not(Matchers.sameInstance(value)));

    assertThat(value.read(), Matchers.nullValue());
    value.write("hello");
    assertThat(value.read(), Matchers.equalTo("hello"));
    value.write("world");
    assertThat(value.read(), Matchers.equalTo("world"));

    value.clear();
    assertThat(value.read(), Matchers.nullValue());
    assertThat(underTest.state(NAMESPACE_1, STRING_VALUE_ADDR), Matchers.sameInstance(value));
  }

  @Test
  public void testBag() throws Exception {
    BagState<String> value = underTest.state(NAMESPACE_1, STRING_BAG_ADDR);

    // State instances are cached, but depend on the namespace.
    assertEquals(value, underTest.state(NAMESPACE_1, STRING_BAG_ADDR));
    assertFalse(value.equals(underTest.state(NAMESPACE_2, STRING_BAG_ADDR)));

    assertThat(value.read(), Matchers.emptyIterable());
    value.add("hello");
    assertThat(value.read(), Matchers.containsInAnyOrder("hello"));

    value.add("world");
    assertThat(value.read(), Matchers.containsInAnyOrder("hello", "world"));

    value.clear();
    assertThat(value.read(), Matchers.emptyIterable());
    assertThat(underTest.state(NAMESPACE_1, STRING_BAG_ADDR), Matchers.sameInstance(value));
  }

  @Test
  public void testBagIsEmpty() throws Exception {
    BagState<String> value = underTest.state(NAMESPACE_1, STRING_BAG_ADDR);

    assertThat(value.isEmpty().read(), Matchers.is(true));
    ReadableState<Boolean> readFuture = value.isEmpty();
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

    StateMerging.mergeBags(Arrays.asList(bag1, bag2), bag1);

    // Reading the merged bag gets both the contents
    assertThat(bag1.read(), Matchers.containsInAnyOrder("Hello", "World", "!"));
    assertThat(bag2.read(), Matchers.emptyIterable());
  }

  @Test
  public void testMergeBagIntoNewNamespace() throws Exception {
    BagState<String> bag1 = underTest.state(NAMESPACE_1, STRING_BAG_ADDR);
    BagState<String> bag2 = underTest.state(NAMESPACE_2, STRING_BAG_ADDR);
    BagState<String> bag3 = underTest.state(NAMESPACE_3, STRING_BAG_ADDR);

    bag1.add("Hello");
    bag2.add("World");
    bag1.add("!");

    StateMerging.mergeBags(Arrays.asList(bag1, bag2, bag3), bag3);

    // Reading the merged bag gets both the contents
    assertThat(bag3.read(), Matchers.containsInAnyOrder("Hello", "World", "!"));
    assertThat(bag1.read(), Matchers.emptyIterable());
    assertThat(bag2.read(), Matchers.emptyIterable());
  }

  @Test
  public void testCombiningValue() throws Exception {
    CombiningState<Integer, Integer> value = underTest.state(NAMESPACE_1, SUM_INTEGER_ADDR);

    // State instances are cached, but depend on the namespace.
    assertEquals(value, underTest.state(NAMESPACE_1, SUM_INTEGER_ADDR));
    assertFalse(value.equals(underTest.state(NAMESPACE_2, SUM_INTEGER_ADDR)));

    assertThat(value.read(), Matchers.equalTo(0));
    value.add(2);
    assertThat(value.read(), Matchers.equalTo(2));

    value.add(3);
    assertThat(value.read(), Matchers.equalTo(5));

    value.clear();
    assertThat(value.read(), Matchers.equalTo(0));
    assertThat(underTest.state(NAMESPACE_1, SUM_INTEGER_ADDR), Matchers.sameInstance(value));
  }

  @Test
  public void testCombiningIsEmpty() throws Exception {
    CombiningState<Integer, Integer> value = underTest.state(NAMESPACE_1, SUM_INTEGER_ADDR);

    assertThat(value.isEmpty().read(), Matchers.is(true));
    ReadableState<Boolean> readFuture = value.isEmpty();
    value.add(5);
    assertThat(readFuture.read(), Matchers.is(false));

    value.clear();
    assertThat(readFuture.read(), Matchers.is(true));
  }

  @Test
  public void testMergeCombiningValueIntoSource() throws Exception {
    AccumulatorCombiningState<Integer, int[], Integer> value1 =
        underTest.state(NAMESPACE_1, SUM_INTEGER_ADDR);
    AccumulatorCombiningState<Integer, int[], Integer> value2 =
        underTest.state(NAMESPACE_2, SUM_INTEGER_ADDR);

    value1.add(5);
    value2.add(10);
    value1.add(6);

    assertThat(value1.read(), Matchers.equalTo(11));
    assertThat(value2.read(), Matchers.equalTo(10));

    // Merging clears the old values and updates the result value.
    StateMerging.mergeCombiningValues(Arrays.asList(value1, value2), value1);

    assertThat(value1.read(), Matchers.equalTo(21));
    assertThat(value2.read(), Matchers.equalTo(0));
  }

  @Test
  public void testMergeCombiningValueIntoNewNamespace() throws Exception {
    AccumulatorCombiningState<Integer, int[], Integer> value1 =
        underTest.state(NAMESPACE_1, SUM_INTEGER_ADDR);
    AccumulatorCombiningState<Integer, int[], Integer> value2 =
        underTest.state(NAMESPACE_2, SUM_INTEGER_ADDR);
    AccumulatorCombiningState<Integer, int[], Integer> value3 =
        underTest.state(NAMESPACE_3, SUM_INTEGER_ADDR);

    value1.add(5);
    value2.add(10);
    value1.add(6);

    StateMerging.mergeCombiningValues(Arrays.asList(value1, value2), value3);

    // Merging clears the old values and updates the result value.
    assertThat(value1.read(), Matchers.equalTo(0));
    assertThat(value2.read(), Matchers.equalTo(0));
    assertThat(value3.read(), Matchers.equalTo(21));
  }

  @Test
  public void testWatermarkEarliestState() throws Exception {
    WatermarkHoldState<BoundedWindow> value =
        underTest.state(NAMESPACE_1, WATERMARK_EARLIEST_ADDR);

    // State instances are cached, but depend on the namespace.
    assertEquals(value, underTest.state(NAMESPACE_1, WATERMARK_EARLIEST_ADDR));
    assertFalse(value.equals(underTest.state(NAMESPACE_2, WATERMARK_EARLIEST_ADDR)));

    assertThat(value.read(), Matchers.nullValue());
    value.add(new Instant(2000));
    assertThat(value.read(), Matchers.equalTo(new Instant(2000)));

    value.add(new Instant(3000));
    assertThat(value.read(), Matchers.equalTo(new Instant(2000)));

    value.add(new Instant(1000));
    assertThat(value.read(), Matchers.equalTo(new Instant(1000)));

    value.clear();
    assertThat(value.read(), Matchers.equalTo(null));
    assertThat(underTest.state(NAMESPACE_1, WATERMARK_EARLIEST_ADDR), Matchers.sameInstance(value));
  }

  @Test
  public void testWatermarkLatestState() throws Exception {
    WatermarkHoldState<BoundedWindow> value =
        underTest.state(NAMESPACE_1, WATERMARK_LATEST_ADDR);

    // State instances are cached, but depend on the namespace.
    assertEquals(value, underTest.state(NAMESPACE_1, WATERMARK_LATEST_ADDR));
    assertFalse(value.equals(underTest.state(NAMESPACE_2, WATERMARK_LATEST_ADDR)));

    assertThat(value.read(), Matchers.nullValue());
    value.add(new Instant(2000));
    assertThat(value.read(), Matchers.equalTo(new Instant(2000)));

    value.add(new Instant(3000));
    assertThat(value.read(), Matchers.equalTo(new Instant(3000)));

    value.add(new Instant(1000));
    assertThat(value.read(), Matchers.equalTo(new Instant(3000)));

    value.clear();
    assertThat(value.read(), Matchers.equalTo(null));
    assertThat(underTest.state(NAMESPACE_1, WATERMARK_LATEST_ADDR), Matchers.sameInstance(value));
  }

  @Test
  public void testWatermarkEndOfWindowState() throws Exception {
    WatermarkHoldState<BoundedWindow> value = underTest.state(NAMESPACE_1, WATERMARK_EOW_ADDR);

    // State instances are cached, but depend on the namespace.
    assertEquals(value, underTest.state(NAMESPACE_1, WATERMARK_EOW_ADDR));
    assertFalse(value.equals(underTest.state(NAMESPACE_2, WATERMARK_EOW_ADDR)));

    assertThat(value.read(), Matchers.nullValue());
    value.add(new Instant(2000));
    assertThat(value.read(), Matchers.equalTo(new Instant(2000)));

    value.clear();
    assertThat(value.read(), Matchers.equalTo(null));
    assertThat(underTest.state(NAMESPACE_1, WATERMARK_EOW_ADDR), Matchers.sameInstance(value));
  }

  @Test
  public void testWatermarkStateIsEmpty() throws Exception {
    WatermarkHoldState<BoundedWindow> value =
        underTest.state(NAMESPACE_1, WATERMARK_EARLIEST_ADDR);

    assertThat(value.isEmpty().read(), Matchers.is(true));
    ReadableState<Boolean> readFuture = value.isEmpty();
    value.add(new Instant(1000));
    assertThat(readFuture.read(), Matchers.is(false));

    value.clear();
    assertThat(readFuture.read(), Matchers.is(true));
  }

  @Test
  public void testMergeEarliestWatermarkIntoSource() throws Exception {
    WatermarkHoldState<BoundedWindow> value1 =
        underTest.state(NAMESPACE_1, WATERMARK_EARLIEST_ADDR);
    WatermarkHoldState<BoundedWindow> value2 =
        underTest.state(NAMESPACE_2, WATERMARK_EARLIEST_ADDR);

    value1.add(new Instant(3000));
    value2.add(new Instant(5000));
    value1.add(new Instant(4000));
    value2.add(new Instant(2000));

    // Merging clears the old values and updates the merged value.
    StateMerging.mergeWatermarks(Arrays.asList(value1, value2), value1, WINDOW_1);

    assertThat(value1.read(), Matchers.equalTo(new Instant(2000)));
    assertThat(value2.read(), Matchers.equalTo(null));
  }

  @Test
  public void testMergeLatestWatermarkIntoSource() throws Exception {
    WatermarkHoldState<BoundedWindow> value1 =
        underTest.state(NAMESPACE_1, WATERMARK_LATEST_ADDR);
    WatermarkHoldState<BoundedWindow> value2 =
        underTest.state(NAMESPACE_2, WATERMARK_LATEST_ADDR);
    WatermarkHoldState<BoundedWindow> value3 =
        underTest.state(NAMESPACE_3, WATERMARK_LATEST_ADDR);

    value1.add(new Instant(3000));
    value2.add(new Instant(5000));
    value1.add(new Instant(4000));
    value2.add(new Instant(2000));

    // Merging clears the old values and updates the result value.
    StateMerging.mergeWatermarks(Arrays.asList(value1, value2), value3, WINDOW_1);

    // Merging clears the old values and updates the result value.
    assertThat(value3.read(), Matchers.equalTo(new Instant(5000)));
    assertThat(value1.read(), Matchers.equalTo(null));
    assertThat(value2.read(), Matchers.equalTo(null));
  }
}
