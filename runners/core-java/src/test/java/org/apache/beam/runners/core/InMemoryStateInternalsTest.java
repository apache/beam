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

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.Map;
import java.util.Objects;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.transforms.Sum;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.transforms.windowing.OutputTimeFns;
import org.apache.beam.sdk.util.state.BagState;
import org.apache.beam.sdk.util.state.CombiningState;
import org.apache.beam.sdk.util.state.GroupingState;
import org.apache.beam.sdk.util.state.MapState;
import org.apache.beam.sdk.util.state.ReadableState;
import org.apache.beam.sdk.util.state.SetState;
import org.apache.beam.sdk.util.state.ValueState;
import org.apache.beam.sdk.util.state.WatermarkHoldState;
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
  private static final StateTag<Object, CombiningState<Integer, int[], Integer>>
      SUM_INTEGER_ADDR = StateTags.combiningValueFromInputInternal(
          "sumInteger", VarIntCoder.of(), Sum.ofIntegers());
  private static final StateTag<Object, BagState<String>> STRING_BAG_ADDR =
      StateTags.bag("stringBag", StringUtf8Coder.of());
  private static final StateTag<Object, SetState<String>> STRING_SET_ADDR =
      StateTags.set("stringSet", StringUtf8Coder.of());
  private static final StateTag<Object, MapState<String, Integer>> STRING_MAP_ADDR =
      StateTags.map("stringMap", StringUtf8Coder.of(), VarIntCoder.of());
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
    assertThat(value.read(), equalTo("hello"));
    value.write("world");
    assertThat(value.read(), equalTo("world"));

    value.clear();
    assertThat(value.read(), Matchers.nullValue());
    assertThat(underTest.state(NAMESPACE_1, STRING_VALUE_ADDR), Matchers.sameInstance(value));
  }

  @Test
  public void testBag() throws Exception {
    BagState<String> value = underTest.state(NAMESPACE_1, STRING_BAG_ADDR);

    // State instances are cached, but depend on the namespace.
    assertThat(value, equalTo(underTest.state(NAMESPACE_1, STRING_BAG_ADDR)));
    assertThat(value, not(equalTo(underTest.state(NAMESPACE_2, STRING_BAG_ADDR))));

    assertThat(value.read(), Matchers.emptyIterable());
    value.add("hello");
    assertThat(value.read(), containsInAnyOrder("hello"));

    value.add("world");
    assertThat(value.read(), containsInAnyOrder("hello", "world"));

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
    assertThat(bag1.read(), containsInAnyOrder("Hello", "World", "!"));
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
    assertThat(bag3.read(), containsInAnyOrder("Hello", "World", "!"));
    assertThat(bag1.read(), Matchers.emptyIterable());
    assertThat(bag2.read(), Matchers.emptyIterable());
  }

  @Test
  public void testSet() throws Exception {
    SetState<String> value = underTest.state(NAMESPACE_1, STRING_SET_ADDR);

    // State instances are cached, but depend on the namespace.
    assertThat(value, equalTo(underTest.state(NAMESPACE_1, STRING_SET_ADDR)));
    assertThat(value, not(equalTo(underTest.state(NAMESPACE_2, STRING_SET_ADDR))));

    // empty
    assertThat(value.read(), Matchers.emptyIterable());
    assertFalse(value.contains("A").read());

    // add
    value.add("A");
    value.add("B");
    value.add("A");
    assertFalse(value.addIfAbsent("B").read());
    assertThat(value.read(), containsInAnyOrder("A", "B"));

    // remove
    value.remove("A");
    assertThat(value.read(), containsInAnyOrder("B"));
    value.remove("C");
    assertThat(value.read(), containsInAnyOrder("B"));

    // contains
    assertFalse(value.contains("A").read());
    assertTrue(value.contains("B").read());
    value.add("C");
    value.add("D");

    // readLater
    assertThat(value.readLater().read(), containsInAnyOrder("B", "C", "D"));
    SetState<String> later = value.readLater();
    assertThat(later.read(), hasItems("C", "D"));
    assertFalse(later.contains("A").read());

    // clear
    value.clear();
    assertThat(value.read(), Matchers.emptyIterable());
    assertThat(underTest.state(NAMESPACE_1, STRING_SET_ADDR), Matchers.sameInstance(value));

  }

  @Test
  public void testSetIsEmpty() throws Exception {
    SetState<String> value = underTest.state(NAMESPACE_1, STRING_SET_ADDR);

    assertThat(value.isEmpty().read(), Matchers.is(true));
    ReadableState<Boolean> readFuture = value.isEmpty();
    value.add("hello");
    assertThat(readFuture.read(), Matchers.is(false));

    value.clear();
    assertThat(readFuture.read(), Matchers.is(true));
  }

  @Test
  public void testMergeSetIntoSource() throws Exception {
    SetState<String> set1 = underTest.state(NAMESPACE_1, STRING_SET_ADDR);
    SetState<String> set2 = underTest.state(NAMESPACE_2, STRING_SET_ADDR);

    set1.add("Hello");
    set2.add("Hello");
    set2.add("World");
    set1.add("!");

    StateMerging.mergeSets(Arrays.asList(set1, set2), set1);

    // Reading the merged set gets both the contents
    assertThat(set1.read(), containsInAnyOrder("Hello", "World", "!"));
    assertThat(set2.read(), Matchers.emptyIterable());
  }

  @Test
  public void testMergeSetIntoNewNamespace() throws Exception {
    SetState<String> set1 = underTest.state(NAMESPACE_1, STRING_SET_ADDR);
    SetState<String> set2 = underTest.state(NAMESPACE_2, STRING_SET_ADDR);
    SetState<String> set3 = underTest.state(NAMESPACE_3, STRING_SET_ADDR);

    set1.add("Hello");
    set2.add("Hello");
    set2.add("World");
    set1.add("!");

    StateMerging.mergeSets(Arrays.asList(set1, set2, set3), set3);

    // Reading the merged set gets both the contents
    assertThat(set3.read(), containsInAnyOrder("Hello", "World", "!"));
    assertThat(set1.read(), Matchers.emptyIterable());
    assertThat(set2.read(), Matchers.emptyIterable());
  }

  // for testMap
  private static class MapEntry<K, V> implements Map.Entry<K, V> {
    private K key;
    private V value;

    private MapEntry(K key, V value) {
      this.key = key;
      this.value = value;
    }

    static <K, V> Map.Entry<K, V> of(K k, V v) {
      return new MapEntry<>(k, v);
    }

    public final K getKey() {
      return key;
    }
    public final V getValue() {
      return value;
    }

    public final String toString() {
      return key + "=" + value;
    }

    public final int hashCode() {
      return Objects.hashCode(key) ^ Objects.hashCode(value);
    }

    public final V setValue(V newValue) {
      V oldValue = value;
      value = newValue;
      return oldValue;
    }

    public final boolean equals(Object o) {
      if (o == this) {
        return true;
      }
      if (o instanceof Map.Entry) {
        Map.Entry<?, ?> e = (Map.Entry<?, ?>) o;
        if (Objects.equals(key, e.getKey())
            && Objects.equals(value, e.getValue())) {
          return true;
        }
      }
      return false;
    }
  }

  @Test
  public void testMap() throws Exception {
    MapState<String, Integer> value = underTest.state(NAMESPACE_1, STRING_MAP_ADDR);

    // State instances are cached, but depend on the namespace.
    assertThat(value, equalTo(underTest.state(NAMESPACE_1, STRING_MAP_ADDR)));
    assertThat(value, not(equalTo(underTest.state(NAMESPACE_2, STRING_MAP_ADDR))));

    // put
    assertThat(value.entries().read(), Matchers.emptyIterable());
    value.put("A", 1);
    value.put("B", 2);
    value.put("A", 11);
    assertThat(value.putIfAbsent("B", 22).read(), equalTo(2));
    assertThat(value.entries().read(), containsInAnyOrder(MapEntry.of("A", 11),
        MapEntry.of("B", 2)));

    // remove
    value.remove("A");
    assertThat(value.entries().read(), containsInAnyOrder(MapEntry.of("B", 2)));
    value.remove("C");
    assertThat(value.entries().read(), containsInAnyOrder(MapEntry.of("B", 2)));

    // get
    assertNull(value.get("A").read());
    assertThat(value.get("B").read(), equalTo(2));
    value.put("C", 3);
    value.put("D", 4);
    assertThat(value.get("C").read(), equalTo(3));

    // iterate
    value.put("E", 5);
    value.remove("C");
    assertThat(value.keys().read(), containsInAnyOrder("B", "D", "E"));
    assertThat(value.values().read(), containsInAnyOrder(2, 4, 5));
    assertThat(
        value.entries().read(),
        containsInAnyOrder(MapEntry.of("B", 2), MapEntry.of("D", 4), MapEntry.of("E", 5)));

    // readLater
    assertThat(value.get("B").readLater().read(), equalTo(2));
    assertNull(value.get("A").readLater().read());
    assertThat(
        value.entries().readLater().read(),
        containsInAnyOrder(MapEntry.of("B", 2), MapEntry.of("D", 4), MapEntry.of("E", 5)));

    // clear
    value.clear();
    assertThat(value.entries().read(), Matchers.emptyIterable());
    assertThat(underTest.state(NAMESPACE_1, STRING_MAP_ADDR), Matchers.sameInstance(value));
  }

  @Test
  public void testCombiningValue() throws Exception {
    GroupingState<Integer, Integer> value = underTest.state(NAMESPACE_1, SUM_INTEGER_ADDR);

    // State instances are cached, but depend on the namespace.
    assertEquals(value, underTest.state(NAMESPACE_1, SUM_INTEGER_ADDR));
    assertFalse(value.equals(underTest.state(NAMESPACE_2, SUM_INTEGER_ADDR)));

    assertThat(value.read(), equalTo(0));
    value.add(2);
    assertThat(value.read(), equalTo(2));

    value.add(3);
    assertThat(value.read(), equalTo(5));

    value.clear();
    assertThat(value.read(), equalTo(0));
    assertThat(underTest.state(NAMESPACE_1, SUM_INTEGER_ADDR), Matchers.sameInstance(value));
  }

  @Test
  public void testCombiningIsEmpty() throws Exception {
    GroupingState<Integer, Integer> value = underTest.state(NAMESPACE_1, SUM_INTEGER_ADDR);

    assertThat(value.isEmpty().read(), Matchers.is(true));
    ReadableState<Boolean> readFuture = value.isEmpty();
    value.add(5);
    assertThat(readFuture.read(), Matchers.is(false));

    value.clear();
    assertThat(readFuture.read(), Matchers.is(true));
  }

  @Test
  public void testMergeCombiningValueIntoSource() throws Exception {
    CombiningState<Integer, int[], Integer> value1 =
        underTest.state(NAMESPACE_1, SUM_INTEGER_ADDR);
    CombiningState<Integer, int[], Integer> value2 =
        underTest.state(NAMESPACE_2, SUM_INTEGER_ADDR);

    value1.add(5);
    value2.add(10);
    value1.add(6);

    assertThat(value1.read(), equalTo(11));
    assertThat(value2.read(), equalTo(10));

    // Merging clears the old values and updates the result value.
    StateMerging.mergeCombiningValues(Arrays.asList(value1, value2), value1);

    assertThat(value1.read(), equalTo(21));
    assertThat(value2.read(), equalTo(0));
  }

  @Test
  public void testMergeCombiningValueIntoNewNamespace() throws Exception {
    CombiningState<Integer, int[], Integer> value1 =
        underTest.state(NAMESPACE_1, SUM_INTEGER_ADDR);
    CombiningState<Integer, int[], Integer> value2 =
        underTest.state(NAMESPACE_2, SUM_INTEGER_ADDR);
    CombiningState<Integer, int[], Integer> value3 =
        underTest.state(NAMESPACE_3, SUM_INTEGER_ADDR);

    value1.add(5);
    value2.add(10);
    value1.add(6);

    StateMerging.mergeCombiningValues(Arrays.asList(value1, value2), value3);

    // Merging clears the old values and updates the result value.
    assertThat(value1.read(), equalTo(0));
    assertThat(value2.read(), equalTo(0));
    assertThat(value3.read(), equalTo(21));
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
    assertThat(value.read(), equalTo(new Instant(2000)));

    value.add(new Instant(3000));
    assertThat(value.read(), equalTo(new Instant(2000)));

    value.add(new Instant(1000));
    assertThat(value.read(), equalTo(new Instant(1000)));

    value.clear();
    assertThat(value.read(), equalTo(null));
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
    assertThat(value.read(), equalTo(new Instant(2000)));

    value.add(new Instant(3000));
    assertThat(value.read(), equalTo(new Instant(3000)));

    value.add(new Instant(1000));
    assertThat(value.read(), equalTo(new Instant(3000)));

    value.clear();
    assertThat(value.read(), equalTo(null));
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
    assertThat(value.read(), equalTo(new Instant(2000)));

    value.clear();
    assertThat(value.read(), equalTo(null));
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

    assertThat(value1.read(), equalTo(new Instant(2000)));
    assertThat(value2.read(), equalTo(null));
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
    assertThat(value3.read(), equalTo(new Instant(5000)));
    assertThat(value1.read(), equalTo(null));
    assertThat(value2.read(), equalTo(null));
  }
}
