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
package org.apache.beam.runners.spark.structuredstreaming.translation.streaming.state;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.beam.runners.core.StateNamespace;
import org.apache.beam.runners.core.StateNamespaces;
import org.apache.beam.runners.core.StateTag;
import org.apache.beam.runners.core.StateTags;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.state.BagState;
import org.apache.beam.sdk.state.CombiningState;
import org.apache.beam.sdk.state.MapState;
import org.apache.beam.sdk.state.SetState;
import org.apache.beam.sdk.state.ValueState;
import org.apache.beam.sdk.state.WatermarkHoldState;
import org.apache.beam.sdk.transforms.Sum;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.transforms.windowing.TimestampCombiner;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Lists;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Instant;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Unit tests for the Beam {@code StateInternals} bridge, exercised against an in-memory {@link
 * BytesKV} rather than a live Spark {@code MapState}.
 *
 * <p>That is the whole point of the {@link BytesKV} seam: everything below it is Spark's problem,
 * everything above it is Beam semantics and can be tested in milliseconds.
 */
@RunWith(JUnit4.class)
public class TwsStateInternalsTest {

  private static final StateNamespace NS_A = StateNamespaces.global();
  private static final StateNamespace NS_B =
      StateNamespaces.window(GlobalWindow.Coder.INSTANCE, GlobalWindow.INSTANCE);

  /** A {@link BytesKV} backed by a plain {@link LinkedHashMap}, the test double for Spark state. */
  public static final class InMemoryBytesKV implements BytesKV {
    private final Map<String, byte[]> map = new LinkedHashMap<>();

    @Override
    public byte @Nullable [] get(String key) {
      return map.get(key);
    }

    @Override
    public void put(String key, byte[] value) {
      map.put(key, value);
    }

    @Override
    public void remove(String key) {
      map.remove(key);
    }

    @Override
    public Iterable<Map.Entry<String, byte[]>> entries() {
      return new ArrayList<>(map.entrySet());
    }

    /** Returns the raw store keys currently present, for addressing assertions. */
    public Iterable<String> keys() {
      return new ArrayList<>(map.keySet());
    }

    public int size() {
      return map.size();
    }
  }

  private InMemoryBytesKV store() {
    return new InMemoryBytesKV();
  }

  private TwsStateInternals<String> internals(BytesKV store) {
    return TwsStateInternals.forKey("key", store);
  }

  @Test
  public void testKeyIsExposed() {
    assertEquals("key", internals(store()).getKey());
  }

  @Test
  public void testStoreKeyLayout() {
    assertEquals(NS_A.stringKey() + "+" + "tag", TwsStateInternals.storeKey(NS_A, "tag"));
  }

  @Test
  public void testValueStateRoundTrip() {
    InMemoryBytesKV store = store();
    StateTag<ValueState<String>> tag = StateTags.value("v", StringUtf8Coder.of());

    ValueState<String> state = internals(store).state(NS_A, tag);
    assertNull("an unwritten value state reads null", state.read());

    state.write("hello");
    assertEquals("hello", state.read());

    // A fresh bridge over the same store must see the same value, nothing is cached in memory.
    assertEquals("hello", internals(store).state(NS_A, tag).read());

    state.clear();
    assertNull(internals(store).state(NS_A, tag).read());
    assertEquals("clear must remove the cell, not blank it", 0, store.size());
  }

  @Test
  public void testValueStateIsAddressedByNamespaceAndTag() {
    InMemoryBytesKV store = store();
    StateTag<ValueState<String>> tag = StateTags.value("v", StringUtf8Coder.of());

    internals(store).state(NS_A, tag).write("hello");

    assertEquals(1, store.size());
    assertEquals(TwsStateInternals.storeKey(NS_A, "v"), Lists.newArrayList(store.keys()).get(0));
  }

  @Test
  public void testNamespacesAreIsolated() {
    InMemoryBytesKV store = store();
    StateTag<ValueState<String>> tag = StateTags.value("v", StringUtf8Coder.of());

    internals(store).state(NS_A, tag).write("a");
    internals(store).state(NS_B, tag).write("b");

    assertEquals(2, store.size());
    assertEquals("a", internals(store).state(NS_A, tag).read());
    assertEquals("b", internals(store).state(NS_B, tag).read());

    internals(store).state(NS_A, tag).clear();
    assertNull(internals(store).state(NS_A, tag).read());
    assertEquals(
        "clearing one namespace must not touch the other",
        "b",
        internals(store).state(NS_B, tag).read());
  }

  @Test
  public void testTagsAreIsolatedWithinANamespace() {
    InMemoryBytesKV store = store();
    internals(store).state(NS_A, StateTags.value("one", StringUtf8Coder.of())).write("1");
    internals(store).state(NS_A, StateTags.value("two", StringUtf8Coder.of())).write("2");

    assertEquals(2, store.size());
    assertEquals(
        "1", internals(store).state(NS_A, StateTags.value("one", StringUtf8Coder.of())).read());
    assertEquals(
        "2", internals(store).state(NS_A, StateTags.value("two", StringUtf8Coder.of())).read());
  }

  @Test
  public void testBagStateRoundTrip() {
    InMemoryBytesKV store = store();
    StateTag<BagState<Integer>> tag = StateTags.bag("b", VarIntCoder.of());

    BagState<Integer> state = internals(store).state(NS_A, tag);
    assertTrue("an unwritten bag is empty", state.isEmpty().read());
    assertEquals(0, Lists.newArrayList(state.read()).size());

    state.add(1);
    state.add(2);
    state.add(2);
    assertFalse(state.isEmpty().read());
    assertEquals(
        Lists.newArrayList(1, 2, 2), Lists.newArrayList(internals(store).state(NS_A, tag).read()));

    state.clear();
    assertTrue(internals(store).state(NS_A, tag).isEmpty().read());
    assertEquals(0, store.size());
  }

  @Test
  public void testCombiningStateRoundTrip() {
    InMemoryBytesKV store = store();
    StateTag<CombiningState<Integer, int[], Integer>> tag =
        StateTags.combiningValueFromInputInternal("c", VarIntCoder.of(), Sum.ofIntegers());

    CombiningState<Integer, int[], Integer> state = internals(store).state(NS_A, tag);
    assertTrue(state.isEmpty().read());
    assertEquals(Integer.valueOf(0), state.read());

    state.add(3);
    state.add(4);
    assertFalse(state.isEmpty().read());
    assertEquals(
        "the accumulator must be persisted, not kept in memory",
        Integer.valueOf(7),
        internals(store).state(NS_A, tag).read());

    internals(store).state(NS_A, tag).add(1);
    assertEquals(Integer.valueOf(8), internals(store).state(NS_A, tag).read());

    state.clear();
    assertEquals(0, store.size());
  }

  @Test
  public void testWatermarkHoldStateRoundTrip() {
    InMemoryBytesKV store = store();
    StateTag<WatermarkHoldState> tag =
        StateTags.watermarkStateInternal("hold", TimestampCombiner.EARLIEST);

    WatermarkHoldState state = internals(store).state(NS_A, tag);
    assertTrue(state.isEmpty().read());
    assertNull(state.read());
    assertEquals(TimestampCombiner.EARLIEST, state.getTimestampCombiner());

    state.add(new Instant(50));
    state.add(new Instant(20));
    state.add(new Instant(70));
    assertEquals("EARLIEST must win", new Instant(20), internals(store).state(NS_A, tag).read());

    state.clear();
    assertTrue(internals(store).state(NS_A, tag).isEmpty().read());
    assertEquals(0, store.size());
  }

  @Test
  public void testMapStateRoundTrip() {
    InMemoryBytesKV store = store();
    StateTag<MapState<String, Integer>> tag =
        StateTags.map("m", StringUtf8Coder.of(), VarIntCoder.of());

    MapState<String, Integer> state = internals(store).state(NS_A, tag);
    assertTrue(state.isEmpty().read());
    assertNull(state.get("a").read());
    assertEquals(Integer.valueOf(7), state.getOrDefault("a", 7).read());

    state.put("a", 1);
    state.put("b", 2);
    assertEquals(Integer.valueOf(1), internals(store).state(NS_A, tag).get("a").read());

    List<String> keys = Lists.newArrayList(internals(store).state(NS_A, tag).keys().read());
    assertEquals(2, keys.size());
    assertTrue(keys.contains("a"));
    assertTrue(keys.contains("b"));
    assertEquals(2, Lists.newArrayList(internals(store).state(NS_A, tag).values().read()).size());
    assertEquals(2, Lists.newArrayList(internals(store).state(NS_A, tag).entries().read()).size());

    assertEquals(
        "computeIfAbsent must not overwrite",
        Integer.valueOf(1),
        internals(store).state(NS_A, tag).computeIfAbsent("a", k -> 99).read());
    assertNull(internals(store).state(NS_A, tag).computeIfAbsent("c", k -> 3).read());
    assertEquals(Integer.valueOf(3), internals(store).state(NS_A, tag).get("c").read());

    internals(store).state(NS_A, tag).remove("a");
    assertNull(internals(store).state(NS_A, tag).get("a").read());

    internals(store).state(NS_A, tag).clear();
    assertTrue(internals(store).state(NS_A, tag).isEmpty().read());
    assertEquals(0, store.size());
  }

  @Test
  public void testSetStateRoundTrip() {
    InMemoryBytesKV store = store();
    StateTag<SetState<String>> tag = StateTags.set("s", StringUtf8Coder.of());

    SetState<String> state = internals(store).state(NS_A, tag);
    assertTrue(state.isEmpty().read());
    assertFalse(state.contains("a").read());

    assertTrue("addIfAbsent returns true the first time", state.addIfAbsent("a").read());
    assertFalse("addIfAbsent returns false the second time", state.addIfAbsent("a").read());
    state.add("b");

    assertTrue(internals(store).state(NS_A, tag).contains("a").read());
    assertEquals(2, Lists.newArrayList(internals(store).state(NS_A, tag).read()).size());

    internals(store).state(NS_A, tag).remove("a");
    assertFalse(internals(store).state(NS_A, tag).contains("a").read());

    internals(store).state(NS_A, tag).clear();
    assertEquals(0, store.size());
  }

  @Test
  public void testDifferentKeysUseDifferentStores() {
    // Spark scopes a MapState to the grouping key, so the bridge does not encode the key into the
    // store key. Two keys are two stores, which this test pins down as an explicit contract.
    InMemoryBytesKV storeOne = store();
    InMemoryBytesKV storeTwo = store();
    StateTag<ValueState<String>> tag = StateTags.value("v", StringUtf8Coder.of());

    TwsStateInternals.forKey("one", storeOne).state(NS_A, tag).write("1");
    TwsStateInternals.forKey("two", storeTwo).state(NS_A, tag).write("2");

    assertEquals("1", TwsStateInternals.forKey("one", storeOne).state(NS_A, tag).read());
    assertEquals("2", TwsStateInternals.forKey("two", storeTwo).state(NS_A, tag).read());
    assertEquals(
        "the store key must not depend on the Beam key",
        Lists.newArrayList(storeOne.keys()),
        Lists.newArrayList(storeTwo.keys()));
  }

  @Test
  public void testWindowNamespacesAreDistinct() {
    InMemoryBytesKV store = store();
    StateTag<ValueState<String>> tag = StateTags.value("v", StringUtf8Coder.of());
    StateNamespace first =
        StateNamespaces.window(
            IntervalWindow.getCoder(), new IntervalWindow(new Instant(0), new Instant(10)));
    StateNamespace second =
        StateNamespaces.window(
            IntervalWindow.getCoder(), new IntervalWindow(new Instant(10), new Instant(20)));

    internals(store).state(first, tag).write("first");
    internals(store).state(second, tag).write("second");

    assertEquals(2, store.size());
    assertEquals("first", internals(store).state(first, tag).read());
    assertEquals("second", internals(store).state(second, tag).read());
  }

  @Test
  public void testUnsupportedStateTypesFailLoudly() {
    InMemoryBytesKV store = store();
    assertThrows(
        UnsupportedOperationException.class,
        () ->
            internals(store)
                .state(NS_A, StateTags.multimap("mm", StringUtf8Coder.of(), VarIntCoder.of())));
    assertThrows(
        UnsupportedOperationException.class,
        () -> internals(store).state(NS_A, StateTags.orderedList("ol", VarIntCoder.of())));
  }
}
