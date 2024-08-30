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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.state.BagState;
import org.apache.beam.sdk.state.CombiningState;
import org.apache.beam.sdk.state.GroupingState;
import org.apache.beam.sdk.state.MapState;
import org.apache.beam.sdk.state.ReadableState;
import org.apache.beam.sdk.state.SetState;
import org.apache.beam.sdk.state.ValueState;
import org.apache.beam.sdk.state.WatermarkHoldState;
import org.apache.beam.sdk.transforms.CombineWithContext;
import org.apache.beam.sdk.transforms.Sum;
import org.apache.beam.sdk.transforms.windowing.TimestampCombiner;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Iterables;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.hamcrest.Matchers;
import org.joda.time.Instant;
import org.junit.Before;
import org.junit.Test;

/** Tests for {@link StateInternals}. */
public abstract class StateInternalsTest {

  private static final StateNamespace NAMESPACE_1 = new StateNamespaceForTest("ns1");
  private static final StateNamespace NAMESPACE_2 = new StateNamespaceForTest("ns2");
  private static final StateNamespace NAMESPACE_3 = new StateNamespaceForTest("ns3");

  private static final StateTag<ValueState<String>> STRING_VALUE_ADDR =
      StateTags.value("stringValue", StringUtf8Coder.of());
  private static final StateTag<CombiningState<Integer, int[], Integer>> SUM_INTEGER_ADDR =
      StateTags.combiningValueFromInputInternal("sumInteger", VarIntCoder.of(), Sum.ofIntegers());
  private static final StateTag<CombiningState<Integer, Integer, Integer>>
      SUM_INTEGER_CONTEXT_ADDR =
          StateTags.combiningValueWithContext(
              "sumIntegerWithContext", VarIntCoder.of(), new SummingContextFn());
  private static final StateTag<BagState<String>> STRING_BAG_ADDR =
      StateTags.bag("stringBag", StringUtf8Coder.of());
  private static final StateTag<SetState<String>> STRING_SET_ADDR =
      StateTags.set("stringSet", StringUtf8Coder.of());
  private static final StateTag<MapState<String, Integer>> STRING_MAP_ADDR =
      StateTags.map("stringMap", StringUtf8Coder.of(), VarIntCoder.of());
  private static final StateTag<WatermarkHoldState> WATERMARK_EARLIEST_ADDR =
      StateTags.watermarkStateInternal("watermark", TimestampCombiner.EARLIEST);
  private static final StateTag<WatermarkHoldState> WATERMARK_LATEST_ADDR =
      StateTags.watermarkStateInternal("watermark", TimestampCombiner.LATEST);
  private static final StateTag<WatermarkHoldState> WATERMARK_EOW_ADDR =
      StateTags.watermarkStateInternal("watermark", TimestampCombiner.END_OF_WINDOW);

  // Two distinct tags because they have non-equals() coders
  private static final StateTag<BagState<String>> STRING_BAG_ADDR1 =
      StateTags.bag("badStringBag", new StringCoderWithIdentityEquality());

  private static final StateTag<BagState<String>> STRING_BAG_ADDR2 =
      StateTags.bag("badStringBag", new StringCoderWithIdentityEquality());

  private StateInternals underTest;

  @Before
  public void setUp() {
    this.underTest = createStateInternals();
  }

  protected abstract StateInternals createStateInternals();

  @Test
  public void testValue() throws Exception {
    ValueState<String> value = underTest.state(NAMESPACE_1, STRING_VALUE_ADDR);

    // State instances are cached, but depend on the namespace.
    assertThat(underTest.state(NAMESPACE_1, STRING_VALUE_ADDR), equalTo(value));
    assertThat(underTest.state(NAMESPACE_2, STRING_VALUE_ADDR), not(equalTo(value)));

    assertThat(value.read(), Matchers.nullValue());
    value.write("hello");
    assertThat(value.read(), equalTo("hello"));
    value.write("world");
    assertThat(value.read(), equalTo("world"));

    value.clear();
    assertThat(value.read(), Matchers.nullValue());
    assertThat(underTest.state(NAMESPACE_1, STRING_VALUE_ADDR), equalTo(value));
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
    assertThat(underTest.state(NAMESPACE_1, STRING_BAG_ADDR), equalTo(value));
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
    assertThat(underTest.state(NAMESPACE_1, STRING_SET_ADDR), equalTo(value));
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

    @Override
    public final K getKey() {
      return key;
    }

    @Override
    public final V getValue() {
      return value;
    }

    @Override
    public final String toString() {
      return key + "=" + value;
    }

    @Override
    public final int hashCode() {
      return Objects.hashCode(key) ^ Objects.hashCode(value);
    }

    @Override
    public final V setValue(V newValue) {
      V oldValue = value;
      value = newValue;
      return oldValue;
    }

    @Override
    public final boolean equals(Object o) {
      if (o == this) {
        return true;
      }
      if (o instanceof Map.Entry) {
        Map.Entry<?, ?> e = (Map.Entry<?, ?>) o;
        if (Objects.equals(key, e.getKey()) && Objects.equals(value, e.getValue())) {
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
    assertThat(
        value.entries().read(), containsInAnyOrder(MapEntry.of("A", 11), MapEntry.of("B", 2)));

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

    // isEmpty
    assertFalse(value.isEmpty().read());

    // clear
    value.clear();
    assertThat(value.entries().read(), Matchers.emptyIterable());
    assertThat(underTest.state(NAMESPACE_1, STRING_MAP_ADDR), equalTo(value));

    // isEmpty
    assertTrue(value.isEmpty().read());
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
    assertThat(underTest.state(NAMESPACE_1, SUM_INTEGER_ADDR), equalTo(value));
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
    CombiningState<Integer, int[], Integer> value1 = underTest.state(NAMESPACE_1, SUM_INTEGER_ADDR);
    CombiningState<Integer, int[], Integer> value2 = underTest.state(NAMESPACE_2, SUM_INTEGER_ADDR);

    assertThat(value1.getAccum(), Matchers.is(notNullValue()));
    assertThat(value2.getAccum(), Matchers.is(notNullValue()));

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
    CombiningState<Integer, int[], Integer> value1 = underTest.state(NAMESPACE_1, SUM_INTEGER_ADDR);
    CombiningState<Integer, int[], Integer> value2 = underTest.state(NAMESPACE_2, SUM_INTEGER_ADDR);
    CombiningState<Integer, int[], Integer> value3 = underTest.state(NAMESPACE_3, SUM_INTEGER_ADDR);

    assertThat(value1.getAccum(), Matchers.is(notNullValue()));
    assertThat(value2.getAccum(), Matchers.is(notNullValue()));
    assertThat(value3.getAccum(), Matchers.is(notNullValue()));

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
  public void testMergeCombiningWithContextValueIntoSource() throws Exception {
    CombiningState<Integer, Integer, Integer> value1 =
        underTest.state(NAMESPACE_1, SUM_INTEGER_CONTEXT_ADDR);
    CombiningState<Integer, Integer, Integer> value2 =
        underTest.state(NAMESPACE_2, SUM_INTEGER_CONTEXT_ADDR);

    assertThat(value1.getAccum(), Matchers.is(notNullValue()));
    assertThat(value2.getAccum(), Matchers.is(notNullValue()));

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
  public void testMergeCombiningWithContextValueIntoNewNamespace() throws Exception {
    CombiningState<Integer, Integer, Integer> value1 =
        underTest.state(NAMESPACE_1, SUM_INTEGER_CONTEXT_ADDR);
    CombiningState<Integer, Integer, Integer> value2 =
        underTest.state(NAMESPACE_2, SUM_INTEGER_CONTEXT_ADDR);
    CombiningState<Integer, Integer, Integer> value3 =
        underTest.state(NAMESPACE_3, SUM_INTEGER_CONTEXT_ADDR);

    assertThat(value1.getAccum(), Matchers.is(notNullValue()));
    assertThat(value2.getAccum(), Matchers.is(notNullValue()));
    assertThat(value3.getAccum(), Matchers.is(notNullValue()));

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
    WatermarkHoldState value = underTest.state(NAMESPACE_1, WATERMARK_EARLIEST_ADDR);

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
    assertThat(underTest.state(NAMESPACE_1, WATERMARK_EARLIEST_ADDR), equalTo(value));
  }

  @Test
  public void testWatermarkLatestState() throws Exception {
    WatermarkHoldState value = underTest.state(NAMESPACE_1, WATERMARK_LATEST_ADDR);

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
    assertThat(underTest.state(NAMESPACE_1, WATERMARK_LATEST_ADDR), equalTo(value));
  }

  @Test
  public void testWatermarkEndOfWindowState() throws Exception {
    WatermarkHoldState value = underTest.state(NAMESPACE_1, WATERMARK_EOW_ADDR);

    // State instances are cached, but depend on the namespace.
    assertEquals(value, underTest.state(NAMESPACE_1, WATERMARK_EOW_ADDR));
    assertFalse(value.equals(underTest.state(NAMESPACE_2, WATERMARK_EOW_ADDR)));

    assertThat(value.read(), Matchers.nullValue());
    value.add(new Instant(2000));
    assertThat(value.read(), equalTo(new Instant(2000)));

    value.clear();
    assertThat(value.read(), equalTo(null));
    assertThat(underTest.state(NAMESPACE_1, WATERMARK_EOW_ADDR), equalTo(value));
  }

  @Test
  public void testWatermarkStateIsEmpty() throws Exception {
    WatermarkHoldState value = underTest.state(NAMESPACE_1, WATERMARK_EARLIEST_ADDR);

    assertThat(value.isEmpty().read(), Matchers.is(true));
    ReadableState<Boolean> readFuture = value.isEmpty();
    value.add(new Instant(1000));
    assertThat(readFuture.read(), Matchers.is(false));

    value.clear();
    assertThat(readFuture.read(), Matchers.is(true));
  }

  @Test
  public void testSetReadable() throws Exception {
    SetState<String> value = underTest.state(NAMESPACE_1, STRING_SET_ADDR);

    // test contains
    ReadableState<Boolean> readable = value.contains("A");
    value.add("A");
    assertFalse(readable.read());

    // test addIfAbsent
    value.addIfAbsent("B");
    assertTrue(value.contains("B").read());
  }

  @Test
  public void testMapReadable() throws Exception {
    MapState<String, Integer> value = underTest.state(NAMESPACE_1, STRING_MAP_ADDR);

    // test iterable, should just return a iterable view of the values contained in this map.
    // The iterable is backed by the map, so changes to the map are reflected in the iterable.
    ReadableState<Iterable<String>> keys = value.keys();
    ReadableState<Iterable<Integer>> values = value.values();
    ReadableState<Iterable<Map.Entry<String, Integer>>> entries = value.entries();
    value.put("A", 1);
    assertFalse(Iterables.isEmpty(keys.read()));
    assertFalse(Iterables.isEmpty(values.read()));
    assertFalse(Iterables.isEmpty(entries.read()));

    // test get
    ReadableState<Integer> get = value.get("B");
    value.put("B", 2);
    assertThat(get.read(), equalTo(2));

    // test addIfAbsent
    value.putIfAbsent("C", 3);
    assertThat(value.get("C").read(), equalTo(3));
  }

  @Test
  public void testBagWithBadCoderEquality() throws Exception {
    // Ensure two instances of the bad coder are distinct; models user who fails to
    // override equals() or inherit from CustomCoder for StructuredCoder
    assertThat(
        new StringCoderWithIdentityEquality(), not(equalTo(new StringCoderWithIdentityEquality())));

    BagState<String> state1 = underTest.state(NAMESPACE_1, STRING_BAG_ADDR1);
    state1.add("hello");

    BagState<String> state2 = underTest.state(NAMESPACE_1, STRING_BAG_ADDR2);
    assertThat(state2.read(), containsInAnyOrder("hello"));
  }

  private static class StringCoderWithIdentityEquality extends Coder<String> {

    private final StringUtf8Coder realCoder = StringUtf8Coder.of();

    @Override
    public void encode(String value, OutputStream outStream) throws CoderException, IOException {
      realCoder.encode(value, outStream);
    }

    @Override
    public String decode(InputStream inStream) throws CoderException, IOException {
      return realCoder.decode(inStream);
    }

    @Override
    public List<? extends Coder<?>> getCoderArguments() {
      return null;
    }

    @Override
    public void verifyDeterministic() throws NonDeterministicException {}

    @Override
    public boolean equals(@Nullable Object other) {
      return other == this;
    }

    @Override
    public int hashCode() {
      return super.hashCode();
    }
  }

  private static class SummingContextFn
      extends CombineWithContext.CombineFnWithContext<Integer, Integer, Integer> {

    @Override
    public Integer createAccumulator(CombineWithContext.Context c) {
      return 0;
    }

    @Override
    public Integer addInput(Integer accumulator, Integer input, CombineWithContext.Context c) {
      return accumulator + input;
    }

    @Override
    public Integer mergeAccumulators(Iterable<Integer> accumulators, CombineWithContext.Context c) {
      int sum = createAccumulator(c);
      for (Integer accumulator : accumulators) {
        sum += accumulator;
      }
      return sum;
    }

    @Override
    public Integer extractOutput(Integer accumulator, CombineWithContext.Context c) {
      return accumulator;
    }
  }
}
