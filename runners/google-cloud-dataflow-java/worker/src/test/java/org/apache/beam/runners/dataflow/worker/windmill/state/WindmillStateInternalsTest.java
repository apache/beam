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
package org.apache.beam.runners.dataflow.worker.windmill.state;

import static org.apache.beam.runners.dataflow.worker.DataflowMatchers.ByteStringMatcher.byteStringEq;
import static org.apache.beam.sdk.testing.SystemNanoTimeSleeper.sleepMillis;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.when;

import java.io.Closeable;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.AbstractMap;
import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.apache.beam.runners.core.StateNamespace;
import org.apache.beam.runners.core.StateNamespaceForTest;
import org.apache.beam.runners.core.StateTag;
import org.apache.beam.runners.core.StateTags;
import org.apache.beam.runners.dataflow.options.DataflowWorkerHarnessOptions;
import org.apache.beam.runners.dataflow.worker.WindmillComputationKey;
import org.apache.beam.runners.dataflow.worker.WindmillStateTestUtils;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.TagBag;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.TagSortedListUpdateRequest;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.TagValue;
import org.apache.beam.sdk.coders.ByteArrayCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.Coder.Context;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.state.BagState;
import org.apache.beam.sdk.state.CombiningState;
import org.apache.beam.sdk.state.GroupingState;
import org.apache.beam.sdk.state.MapState;
import org.apache.beam.sdk.state.MultimapState;
import org.apache.beam.sdk.state.OrderedListState;
import org.apache.beam.sdk.state.ReadableState;
import org.apache.beam.sdk.state.ValueState;
import org.apache.beam.sdk.state.WatermarkHoldState;
import org.apache.beam.sdk.transforms.Sum;
import org.apache.beam.sdk.transforms.windowing.TimestampCombiner;
import org.apache.beam.sdk.util.ByteStringOutputStream;
import org.apache.beam.sdk.util.CoderUtils;
import org.apache.beam.sdk.values.TimestampedValue;
import org.apache.beam.vendor.grpc.v1p60p1.com.google.protobuf.ByteString;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Supplier;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ArrayListMultimap;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Iterables;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Multimap;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Range;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.RangeSet;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.util.concurrent.Futures;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.util.concurrent.SettableFuture;
import org.hamcrest.Matchers;
import org.hamcrest.core.CombinableMatcher;
import org.joda.time.Instant;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

/** Tests for {@link WindmillStateInternals}. */
@RunWith(JUnit4.class)
@SuppressWarnings({
  "rawtypes", // TODO(https://github.com/apache/beam/issues/20447)
})
public class WindmillStateInternalsTest {
  @Rule public transient Timeout globalTimeout = Timeout.seconds(600);
  public static final Range<Long> FULL_ORDERED_LIST_RANGE =
      Range.closedOpen(WindmillOrderedList.MIN_TS_MICROS, WindmillOrderedList.MAX_TS_MICROS);
  private static final StateNamespace NAMESPACE = new StateNamespaceForTest("ns");
  private static final String STATE_FAMILY = "family";
  private static final StateTag<CombiningState<Integer, int[], Integer>> COMBINING_ADDR =
      StateTags.combiningValueFromInputInternal("combining", VarIntCoder.of(), Sum.ofIntegers());
  private static final ByteString COMBINING_KEY = key(NAMESPACE, "combining");
  private final Coder<int[]> accumCoder =
      Sum.ofIntegers().getAccumulatorCoder(null, VarIntCoder.of());
  DataflowWorkerHarnessOptions options;
  private long workToken = 0;
  @Mock private WindmillStateReader mockReader;
  private WindmillStateInternals<String> underTest;
  private WindmillStateInternals<String> underTestNewKey;
  private WindmillStateInternals<String> underTestMapViaMultimap;
  private WindmillStateCache cache;
  private WindmillStateCache cacheViaMultimap;
  @Mock private Supplier<Closeable> readStateSupplier;

  private static ByteString key(StateNamespace namespace, String addrId) {
    return ByteString.copyFromUtf8(namespace.stringKey() + "+u" + addrId);
  }

  private static ByteString systemKey(StateNamespace namespace, String addrId) {
    return ByteString.copyFromUtf8(namespace.stringKey() + "+s" + addrId);
  }

  private static <T> ByteString encodeWithCoder(T key, Coder<T> coder) {
    ByteStringOutputStream out = new ByteStringOutputStream();
    try {
      coder.encode(key, out, Context.OUTER);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return out.toByteString();
  }

  // We use the structural value of the Multimap keys to differentiate between different keys. So we
  // mix using the original key object and a duplicate but same key object so make sure the
  // correctness.
  private static byte[] dup(byte[] key) {
    byte[] res = new byte[key.length];
    System.arraycopy(key, 0, res, 0, key.length);
    return res;
  }

  private static Map.Entry<ByteString, Iterable<Integer>> multimapEntry(
      byte[] key, Integer... values) {
    return new AbstractMap.SimpleEntry<>(
        encodeWithCoder(key, ByteArrayCoder.of()), Arrays.asList(values));
  }

  @SafeVarargs
  private static <T> List<T> weightedList(T... entries) {
    WeightedList<T> list = new WeightedList<>(new ArrayList<>());
    for (T entry : entries) {
      list.addWeighted(entry, 1);
    }
    return list;
  }

  private static CombinableMatcher<Object> multimapEntryMatcher(byte[] key, Integer value) {
    return Matchers.both(Matchers.hasProperty("key", Matchers.equalTo(key)))
        .and(Matchers.hasProperty("value", Matchers.equalTo(value)));
  }

  private static MultimapEntryUpdate decodeTagMultimapEntry(Windmill.TagMultimapEntry entryProto) {
    try {
      String key = StringUtf8Coder.of().decode(entryProto.getEntryName().newInput(), Context.OUTER);
      List<Integer> values = new ArrayList<>();
      for (ByteString value : entryProto.getValuesList()) {
        values.add(VarIntCoder.of().decode(value.newInput(), Context.OUTER));
      }
      return new MultimapEntryUpdate(key, values, entryProto.getDeleteAll());
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private static void assertTagMultimapUpdates(
      Windmill.TagMultimapUpdateRequest.Builder updates, MultimapEntryUpdate... expected) {
    assertThat(
        updates.getUpdatesList().stream()
            .map(WindmillStateInternalsTest::decodeTagMultimapEntry)
            .collect(Collectors.toList()),
        Matchers.containsInAnyOrder(expected));
  }

  @Before
  public void setUp() {
    MockitoAnnotations.initMocks(this);
    options = PipelineOptionsFactory.as(DataflowWorkerHarnessOptions.class);
    cache = WindmillStateCache.builder().setSizeMb(options.getWorkerCacheMb()).build();
    cacheViaMultimap =
        WindmillStateCache.builder()
            .setSizeMb(options.getWorkerCacheMb())
            .setSupportMapViaMultimap(true)
            .build();
    resetUnderTest();
  }

  public void resetUnderTest() {
    workToken++;
    underTest =
        new WindmillStateInternals<>(
            "dummyKey",
            STATE_FAMILY,
            mockReader,
            false,
            cache
                .forComputation("comp")
                .forKey(
                    WindmillComputationKey.create(
                        "comp", ByteString.copyFrom("dummyKey", StandardCharsets.UTF_8), 123),
                    17L,
                    workToken)
                .forFamily(STATE_FAMILY),
            readStateSupplier);
    underTestNewKey =
        new WindmillStateInternals<String>(
            "dummyNewKey",
            STATE_FAMILY,
            mockReader,
            true,
            cache
                .forComputation("comp")
                .forKey(
                    WindmillComputationKey.create(
                        "comp", ByteString.copyFrom("dummyNewKey", StandardCharsets.UTF_8), 123),
                    17L,
                    workToken)
                .forFamily(STATE_FAMILY),
            readStateSupplier);
    underTestMapViaMultimap =
        new WindmillStateInternals<String>(
            "dummyNewKey",
            STATE_FAMILY,
            mockReader,
            false,
            cacheViaMultimap
                .forComputation("comp")
                .forKey(
                    WindmillComputationKey.create(
                        "comp", ByteString.copyFrom("dummyNewKey", StandardCharsets.UTF_8), 123),
                    17L,
                    workToken)
                .forFamily(STATE_FAMILY),
            readStateSupplier);
  }

  @After
  public void tearDown() throws Exception {
    // Make sure no WindmillStateReader (a per-WorkItem object) escapes into the cache
    // (a global object).
    WindmillStateTestUtils.assertNoReference(cache, WindmillStateReader.class);
    WindmillStateTestUtils.assertNoReference(cacheViaMultimap, WindmillStateReader.class);
  }

  private <T> void waitAndSet(final SettableFuture<T> future, final T value, final long millis) {
    new Thread(
            () -> {
              try {
                sleepMillis(millis);
              } catch (InterruptedException e) {
                throw new RuntimeException("Interrupted before setting", e);
              }
              future.set(value);
            })
        .run();
  }

  private WeightedList<String> weightedList(String... elems) {
    WeightedList<String> result = new WeightedList<>(new ArrayList<String>(elems.length));
    for (String elem : elems) {
      result.addWeighted(elem, elem.length());
    }
    return result;
  }

  private <K> ByteString protoKeyFromUserKey(@Nullable K tag, Coder<K> keyCoder)
      throws IOException {
    ByteStringOutputStream keyStream = new ByteStringOutputStream();
    key(NAMESPACE, "map").writeTo(keyStream);
    if (tag != null) {
      keyCoder.encode(tag, keyStream, Context.OUTER);
    }
    return keyStream.toByteString();
  }

  private <K> K userKeyFromProtoKey(ByteString tag, Coder<K> keyCoder) throws IOException {
    ByteString keyBytes = tag.substring(key(NAMESPACE, "map").size());
    return keyCoder.decode(keyBytes.newInput(), Context.OUTER);
  }

  @Test
  public void testMapAddBeforeGet() throws Exception {
    StateTag<MapState<String, Integer>> addr =
        StateTags.map("map", StringUtf8Coder.of(), VarIntCoder.of());
    MapState<String, Integer> mapState = underTest.state(NAMESPACE, addr);

    final String tag = "tag";
    SettableFuture<Integer> future = SettableFuture.create();
    when(mockReader.valueFuture(
            protoKeyFromUserKey(tag, StringUtf8Coder.of()), STATE_FAMILY, VarIntCoder.of()))
        .thenReturn(future);

    ReadableState<Integer> result = mapState.get("tag");
    result = result.readLater();
    waitAndSet(future, 1, 200);
    assertEquals(1, (int) result.read());
    mapState.put("tag", 2);
    assertEquals(2, (int) result.read());
  }

  @Test
  public void testMapAddClearBeforeGet() throws Exception {
    StateTag<MapState<String, Integer>> addr =
        StateTags.map("map", StringUtf8Coder.of(), VarIntCoder.of());
    MapState<String, Integer> mapState = underTest.state(NAMESPACE, addr);

    final String tag = "tag";

    SettableFuture<Iterable<Map.Entry<ByteString, Integer>>> prefixFuture = SettableFuture.create();
    when(mockReader.valuePrefixFuture(
            protoKeyFromUserKey(null, StringUtf8Coder.of()), STATE_FAMILY, VarIntCoder.of()))
        .thenReturn(prefixFuture);

    ReadableState<Integer> result = mapState.get("tag");
    result = result.readLater();
    waitAndSet(
        prefixFuture,
        ImmutableList.of(
            new AbstractMap.SimpleEntry<>(protoKeyFromUserKey(tag, StringUtf8Coder.of()), 1)),
        50);
    assertFalse(mapState.isEmpty().read());
    mapState.clear();
    assertTrue(mapState.isEmpty().read());
    assertNull(mapState.get("tag").read());
    mapState.put("tag", 2);
    assertFalse(mapState.isEmpty().read());
    assertEquals(2, (int) result.read());
  }

  @Test
  public void testMapLocalAddOverridesStorage() throws Exception {
    StateTag<MapState<String, Integer>> addr =
        StateTags.map("map", StringUtf8Coder.of(), VarIntCoder.of());
    MapState<String, Integer> mapState = underTest.state(NAMESPACE, addr);

    final String tag = "tag";

    SettableFuture<Integer> future = SettableFuture.create();
    when(mockReader.valueFuture(
            protoKeyFromUserKey(tag, StringUtf8Coder.of()), STATE_FAMILY, VarIntCoder.of()))
        .thenReturn(future);
    SettableFuture<Iterable<Map.Entry<ByteString, Integer>>> prefixFuture = SettableFuture.create();
    when(mockReader.valuePrefixFuture(
            protoKeyFromUserKey(null, StringUtf8Coder.of()), STATE_FAMILY, VarIntCoder.of()))
        .thenReturn(prefixFuture);

    waitAndSet(future, 1, 50);
    waitAndSet(
        prefixFuture,
        ImmutableList.of(
            new AbstractMap.SimpleEntry<>(protoKeyFromUserKey(tag, StringUtf8Coder.of()), 1)),
        50);
    mapState.put(tag, 42);
    assertEquals(42, (int) mapState.get(tag).read());
    assertThat(
        mapState.entries().read(),
        Matchers.containsInAnyOrder(new AbstractMap.SimpleEntry<>(tag, 42)));
  }

  @Test
  public void testMapLocalRemoveOverridesStorage() throws Exception {
    StateTag<MapState<String, Integer>> addr =
        StateTags.map("map", StringUtf8Coder.of(), VarIntCoder.of());
    MapState<String, Integer> mapState = underTest.state(NAMESPACE, addr);

    final String tag1 = "tag1";
    final String tag2 = "tag2";

    SettableFuture<Integer> future = SettableFuture.create();
    when(mockReader.valueFuture(
            protoKeyFromUserKey(tag1, StringUtf8Coder.of()), STATE_FAMILY, VarIntCoder.of()))
        .thenReturn(future);
    SettableFuture<Iterable<Map.Entry<ByteString, Integer>>> prefixFuture = SettableFuture.create();
    when(mockReader.valuePrefixFuture(
            protoKeyFromUserKey(null, StringUtf8Coder.of()), STATE_FAMILY, VarIntCoder.of()))
        .thenReturn(prefixFuture);

    waitAndSet(future, 1, 50);
    waitAndSet(
        prefixFuture,
        ImmutableList.of(
            new AbstractMap.SimpleEntry<>(protoKeyFromUserKey(tag1, StringUtf8Coder.of()), 1),
            new AbstractMap.SimpleEntry<>(protoKeyFromUserKey(tag2, StringUtf8Coder.of()), 2)),
        50);
    mapState.remove(tag1);
    assertNull(mapState.get(tag1).read());
    assertThat(
        mapState.entries().read(),
        Matchers.containsInAnyOrder(new AbstractMap.SimpleEntry<>(tag2, 2)));

    mapState.remove(tag2);
    assertTrue(mapState.isEmpty().read());
  }

  @Test
  public void testMapLocalClearOverridesStorage() throws Exception {
    StateTag<MapState<String, Integer>> addr =
        StateTags.map("map", StringUtf8Coder.of(), VarIntCoder.of());
    MapState<String, Integer> mapState = underTest.state(NAMESPACE, addr);

    final String tag1 = "tag1";
    final String tag2 = "tag2";

    SettableFuture<Integer> future1 = SettableFuture.create();
    SettableFuture<Integer> future2 = SettableFuture.create();

    when(mockReader.valueFuture(
            protoKeyFromUserKey(tag1, StringUtf8Coder.of()), STATE_FAMILY, VarIntCoder.of()))
        .thenReturn(future1);
    when(mockReader.valueFuture(
            protoKeyFromUserKey(tag2, StringUtf8Coder.of()), STATE_FAMILY, VarIntCoder.of()))
        .thenReturn(future2);
    SettableFuture<Iterable<Map.Entry<ByteString, Integer>>> prefixFuture = SettableFuture.create();
    when(mockReader.valuePrefixFuture(
            protoKeyFromUserKey(null, StringUtf8Coder.of()), STATE_FAMILY, VarIntCoder.of()))
        .thenReturn(prefixFuture);

    waitAndSet(future1, 1, 50);
    waitAndSet(future2, 2, 50);
    waitAndSet(
        prefixFuture,
        ImmutableList.of(
            new AbstractMap.SimpleEntry<>(protoKeyFromUserKey(tag1, StringUtf8Coder.of()), 1),
            new AbstractMap.SimpleEntry<>(protoKeyFromUserKey(tag2, StringUtf8Coder.of()), 2)),
        50);
    mapState.clear();
    assertNull(mapState.get(tag1).read());
    assertNull(mapState.get(tag2).read());
    assertThat(mapState.entries().read(), Matchers.emptyIterable());
    assertTrue(mapState.isEmpty().read());
  }

  @Test
  public void testMapAddBeforeRead() throws Exception {
    StateTag<MapState<String, Integer>> addr =
        StateTags.map("map", StringUtf8Coder.of(), VarIntCoder.of());
    MapState<String, Integer> mapState = underTest.state(NAMESPACE, addr);

    final String tag1 = "tag1";
    final String tag2 = "tag2";
    final String tag3 = "tag3";
    SettableFuture<Iterable<Map.Entry<ByteString, Integer>>> prefixFuture = SettableFuture.create();
    when(mockReader.valuePrefixFuture(
            protoKeyFromUserKey(null, StringUtf8Coder.of()), STATE_FAMILY, VarIntCoder.of()))
        .thenReturn(prefixFuture);

    ReadableState<Iterable<Map.Entry<String, Integer>>> result = mapState.entries();
    result = result.readLater();

    mapState.put(tag1, 1);
    waitAndSet(
        prefixFuture,
        ImmutableList.of(
            new AbstractMap.SimpleEntry<>(protoKeyFromUserKey(tag2, StringUtf8Coder.of()), 2)),
        200);
    Iterable<Map.Entry<String, Integer>> readData = result.read();
    assertThat(
        readData,
        Matchers.containsInAnyOrder(
            new AbstractMap.SimpleEntry<>(tag1, 1), new AbstractMap.SimpleEntry<>(tag2, 2)));

    mapState.put(tag3, 3);
    assertThat(
        result.read(),
        Matchers.containsInAnyOrder(
            new AbstractMap.SimpleEntry<>(tag1, 1),
            new AbstractMap.SimpleEntry<>(tag2, 2),
            new AbstractMap.SimpleEntry<>(tag3, 3)));
  }

  @Test
  public void testMapPutIfAbsentSucceeds() throws Exception {
    StateTag<MapState<String, Integer>> addr =
        StateTags.map("map", StringUtf8Coder.of(), VarIntCoder.of());
    MapState<String, Integer> mapState = underTest.state(NAMESPACE, addr);

    final String tag1 = "tag1";
    SettableFuture<Integer> future = SettableFuture.create();
    when(mockReader.valueFuture(
            protoKeyFromUserKey(tag1, StringUtf8Coder.of()), STATE_FAMILY, VarIntCoder.of()))
        .thenReturn(future);
    waitAndSet(future, null, 50);

    assertNull(mapState.putIfAbsent(tag1, 42).read());
    assertEquals(42, (int) mapState.get(tag1).read());
  }

  @Test
  public void testMapPutIfAbsentFails() throws Exception {
    StateTag<MapState<String, Integer>> addr =
        StateTags.map("map", StringUtf8Coder.of(), VarIntCoder.of());
    MapState<String, Integer> mapState = underTest.state(NAMESPACE, addr);

    final String tag1 = "tag1";
    mapState.put(tag1, 1);
    assertEquals(1, (int) mapState.putIfAbsent(tag1, 42).read());
    assertEquals(1, (int) mapState.get(tag1).read());

    final String tag2 = "tag2";
    SettableFuture<Integer> future = SettableFuture.create();
    when(mockReader.valueFuture(
            protoKeyFromUserKey(tag2, StringUtf8Coder.of()), STATE_FAMILY, VarIntCoder.of()))
        .thenReturn(future);
    waitAndSet(future, 2, 50);
    assertEquals(2, (int) mapState.putIfAbsent(tag2, 42).read());
    assertEquals(2, (int) mapState.get(tag2).read());
  }

  @Test
  public void testMapPutIfAbsentNoReadSucceeds() throws Exception {
    StateTag<MapState<String, Integer>> addr =
        StateTags.map("map", StringUtf8Coder.of(), VarIntCoder.of());
    MapState<String, Integer> mapState = underTest.state(NAMESPACE, addr);

    final String tag1 = "tag1";
    SettableFuture<Integer> future = SettableFuture.create();
    when(mockReader.valueFuture(
            protoKeyFromUserKey(tag1, StringUtf8Coder.of()), STATE_FAMILY, VarIntCoder.of()))
        .thenReturn(future);
    waitAndSet(future, null, 50);
    ReadableState<Integer> readableState = mapState.putIfAbsent(tag1, 42);
    assertEquals(42, (int) mapState.get(tag1).read());
    assertNull(readableState.read());
  }

  @Test
  public void testMapPutIfAbsentNoReadFails() throws Exception {
    StateTag<MapState<String, Integer>> addr =
        StateTags.map("map", StringUtf8Coder.of(), VarIntCoder.of());
    MapState<String, Integer> mapState = underTest.state(NAMESPACE, addr);

    final String tag1 = "tag1";
    mapState.put(tag1, 1);
    ReadableState<Integer> readableState = mapState.putIfAbsent(tag1, 42);
    assertEquals(1, (int) mapState.get(tag1).read());
    assertEquals(1, (int) readableState.read());

    final String tag2 = "tag2";
    SettableFuture<Integer> future = SettableFuture.create();
    when(mockReader.valueFuture(
            protoKeyFromUserKey(tag2, StringUtf8Coder.of()), STATE_FAMILY, VarIntCoder.of()))
        .thenReturn(future);
    waitAndSet(future, 2, 50);
    readableState = mapState.putIfAbsent(tag2, 42);
    assertEquals(2, (int) mapState.get(tag2).read());
    assertEquals(2, (int) readableState.read());
  }

  @Test
  public void testMapMultiplePutIfAbsentNoRead() throws Exception {
    StateTag<MapState<String, Integer>> addr =
        StateTags.map("map", StringUtf8Coder.of(), VarIntCoder.of());
    MapState<String, Integer> mapState = underTest.state(NAMESPACE, addr);

    final String tag1 = "tag1";
    SettableFuture<Integer> future = SettableFuture.create();
    when(mockReader.valueFuture(
            protoKeyFromUserKey(tag1, StringUtf8Coder.of()), STATE_FAMILY, VarIntCoder.of()))
        .thenReturn(future);
    waitAndSet(future, null, 50);
    ReadableState<Integer> readableState = mapState.putIfAbsent(tag1, 42);
    assertEquals(42, (int) mapState.get(tag1).read());
    ReadableState<Integer> readableState2 = mapState.putIfAbsent(tag1, 43);
    mapState.put(tag1, 1);
    ReadableState<Integer> readableState3 = mapState.putIfAbsent(tag1, 44);
    assertEquals(1, (int) mapState.get(tag1).read());
    assertNull(readableState.read());
    assertEquals(42, (int) readableState2.read());
    assertEquals(1, (int) readableState3.read());
  }

  @Test
  public void testMapNegativeCache() throws Exception {
    StateTag<MapState<String, Integer>> addr =
        StateTags.map("map", StringUtf8Coder.of(), VarIntCoder.of());
    MapState<String, Integer> mapState = underTest.state(NAMESPACE, addr);

    final String tag = "tag";
    SettableFuture<Integer> future = SettableFuture.create();
    when(mockReader.valueFuture(
            protoKeyFromUserKey(tag, StringUtf8Coder.of()), STATE_FAMILY, VarIntCoder.of()))
        .thenReturn(future);
    waitAndSet(future, null, 200);
    assertNull(mapState.get(tag).read());
    future.set(42);
    assertNull(mapState.get(tag).read());
  }

  private <K, V> Map.Entry<K, V> fromTagValue(
      TagValue tagValue, Coder<K> keyCoder, Coder<V> valueCoder) {
    try {
      V value =
          !tagValue.getValue().getData().isEmpty()
              ? valueCoder.decode(tagValue.getValue().getData().newInput())
              : null;
      return new AbstractMap.SimpleEntry<>(userKeyFromProtoKey(tagValue.getTag(), keyCoder), value);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Test
  public void testMapAddPersist() throws Exception {
    StateTag<MapState<String, Integer>> addr =
        StateTags.map("map", StringUtf8Coder.of(), VarIntCoder.of());
    MapState<String, Integer> mapState = underTest.state(NAMESPACE, addr);

    final String tag1 = "tag1";
    final String tag2 = "tag2";
    mapState.put(tag1, 1);
    mapState.put(tag2, 2);

    Windmill.WorkItemCommitRequest.Builder commitBuilder =
        Windmill.WorkItemCommitRequest.newBuilder();
    underTest.persist(commitBuilder);

    assertEquals(2, commitBuilder.getValueUpdatesCount());
    assertThat(
        commitBuilder.getValueUpdatesList().stream()
            .map(tv -> fromTagValue(tv, StringUtf8Coder.of(), VarIntCoder.of()))
            .collect(Collectors.toList()),
        Matchers.containsInAnyOrder(new SimpleEntry<>(tag1, 1), new SimpleEntry<>(tag2, 2)));
  }

  @Test
  public void testMapRemovePersist() throws Exception {
    StateTag<MapState<String, Integer>> addr =
        StateTags.map("map", StringUtf8Coder.of(), VarIntCoder.of());
    MapState<String, Integer> mapState = underTest.state(NAMESPACE, addr);

    final String tag1 = "tag1";
    final String tag2 = "tag2";
    mapState.remove(tag1);
    mapState.remove(tag2);

    Windmill.WorkItemCommitRequest.Builder commitBuilder =
        Windmill.WorkItemCommitRequest.newBuilder();
    underTest.persist(commitBuilder);

    assertEquals(2, commitBuilder.getValueUpdatesCount());
    assertThat(
        commitBuilder.getValueUpdatesList().stream()
            .map(tv -> fromTagValue(tv, StringUtf8Coder.of(), VarIntCoder.of()))
            .collect(Collectors.toList()),
        Matchers.containsInAnyOrder(new SimpleEntry<>(tag1, null), new SimpleEntry<>(tag2, null)));
  }

  @Test
  public void testMapClearPersist() throws Exception {
    StateTag<MapState<String, Integer>> addr =
        StateTags.map("map", StringUtf8Coder.of(), VarIntCoder.of());
    MapState<String, Integer> mapState = underTest.state(NAMESPACE, addr);

    final String tag1 = "tag1";
    final String tag2 = "tag2";
    mapState.put(tag1, 1);
    mapState.put(tag2, 2);
    mapState.clear();

    Windmill.WorkItemCommitRequest.Builder commitBuilder =
        Windmill.WorkItemCommitRequest.newBuilder();
    underTest.persist(commitBuilder);

    assertEquals(0, commitBuilder.getValueUpdatesCount());
    assertEquals(1, commitBuilder.getTagValuePrefixDeletesCount());
    System.err.println(commitBuilder);
    assertEquals(STATE_FAMILY, commitBuilder.getTagValuePrefixDeletes(0).getStateFamily());
    assertEquals(
        protoKeyFromUserKey(null, StringUtf8Coder.of()),
        commitBuilder.getTagValuePrefixDeletes(0).getTagPrefix());
  }

  @Test
  public void testMapComplexPersist() throws Exception {
    StateTag<MapState<String, Integer>> addr =
        StateTags.map("map", StringUtf8Coder.of(), VarIntCoder.of());
    MapState<String, Integer> mapState = underTest.state(NAMESPACE, addr);

    final String tag1 = "tag1";
    final String tag2 = "tag2";
    final String tag3 = "tag3";
    final String tag4 = "tag4";

    mapState.put(tag1, 1);
    mapState.clear();
    mapState.put(tag2, 2);
    mapState.put(tag3, 3);
    mapState.remove(tag2);
    mapState.remove(tag4);

    Windmill.WorkItemCommitRequest.Builder commitBuilder =
        Windmill.WorkItemCommitRequest.newBuilder();
    underTest.persist(commitBuilder);
    assertEquals(1, commitBuilder.getTagValuePrefixDeletesCount());
    assertEquals(STATE_FAMILY, commitBuilder.getTagValuePrefixDeletes(0).getStateFamily());
    assertEquals(
        protoKeyFromUserKey(null, StringUtf8Coder.of()),
        commitBuilder.getTagValuePrefixDeletes(0).getTagPrefix());
    assertThat(
        commitBuilder.getValueUpdatesList().stream()
            .map(tv -> fromTagValue(tv, StringUtf8Coder.of(), VarIntCoder.of()))
            .collect(Collectors.toList()),
        Matchers.containsInAnyOrder(
            new SimpleEntry<>(tag3, 3),
            new SimpleEntry<>(tag2, null),
            new SimpleEntry<>(tag4, null)));

    // Once persist has been called, calling persist again should be a noop.
    commitBuilder = Windmill.WorkItemCommitRequest.newBuilder();
    assertEquals(0, commitBuilder.getTagValuePrefixDeletesCount());
    assertEquals(0, commitBuilder.getValueUpdatesCount());
  }

  @Test
  public void testMultimapGet() {
    final String tag = "multimap";
    StateTag<MultimapState<byte[], Integer>> addr =
        StateTags.multimap(tag, ByteArrayCoder.of(), VarIntCoder.of());
    MultimapState<byte[], Integer> multimapState = underTest.state(NAMESPACE, addr);

    final byte[] key = "key".getBytes(StandardCharsets.UTF_8);
    SettableFuture<Iterable<Integer>> future = SettableFuture.create();
    when(mockReader.multimapFetchSingleEntryFuture(
            encodeWithCoder(key, ByteArrayCoder.of()),
            key(NAMESPACE, tag),
            STATE_FAMILY,
            VarIntCoder.of()))
        .thenReturn(future);

    ReadableState<Iterable<Integer>> result = multimapState.get(dup(key)).readLater();
    waitAndSet(future, Arrays.asList(1, 2, 3), 30);
    assertThat(result.read(), Matchers.containsInAnyOrder(1, 2, 3));
  }

  @Test
  public void testMapViaMultimapGet() {
    final String tag = "map";
    StateTag<MapState<byte[], Integer>> addr =
        StateTags.map(tag, ByteArrayCoder.of(), VarIntCoder.of());
    MapState<byte[], Integer> mapViaMultiMapState = underTestMapViaMultimap.state(NAMESPACE, addr);

    final byte[] key1 = "key1".getBytes(StandardCharsets.UTF_8);
    final byte[] key2 = "key2".getBytes(StandardCharsets.UTF_8);
    SettableFuture<Iterable<Integer>> future1 = SettableFuture.create();
    when(mockReader.multimapFetchSingleEntryFuture(
            encodeWithCoder(key1, ByteArrayCoder.of()),
            key(NAMESPACE, tag),
            STATE_FAMILY,
            VarIntCoder.of()))
        .thenReturn(future1);
    SettableFuture<Iterable<Integer>> future2 = SettableFuture.create();
    when(mockReader.multimapFetchSingleEntryFuture(
            encodeWithCoder(key2, ByteArrayCoder.of()),
            key(NAMESPACE, tag),
            STATE_FAMILY,
            VarIntCoder.of()))
        .thenReturn(future2);

    ReadableState<Integer> result1 = mapViaMultiMapState.get(dup(key1)).readLater();
    ReadableState<Integer> result2 = mapViaMultiMapState.get(dup(key2)).readLater();
    waitAndSet(future1, Collections.singletonList(1), 30);
    waitAndSet(future2, Collections.emptyList(), 1);
    assertEquals(Integer.valueOf(1), result1.read());
    assertNull(result2.read());
  }

  @Test
  public void testMultimapPutAndGet() {
    final String tag = "multimap";
    StateTag<MultimapState<byte[], Integer>> addr =
        StateTags.multimap(tag, ByteArrayCoder.of(), VarIntCoder.of());
    MultimapState<byte[], Integer> multimapState = underTest.state(NAMESPACE, addr);

    final byte[] key = "key".getBytes(StandardCharsets.UTF_8);
    SettableFuture<Iterable<Integer>> future = SettableFuture.create();
    when(mockReader.multimapFetchSingleEntryFuture(
            encodeWithCoder(key, ByteArrayCoder.of()),
            key(NAMESPACE, tag),
            STATE_FAMILY,
            VarIntCoder.of()))
        .thenReturn(future);

    multimapState.put(key, 1);
    ReadableState<Iterable<Integer>> result = multimapState.get(dup(key)).readLater();
    waitAndSet(future, Arrays.asList(1, 2, 3), 30);
    assertThat(result.read(), Matchers.containsInAnyOrder(1, 1, 2, 3));

    multimapState.remove(key);
    multimapState.put(key, 4);
    multimapState.remove(key);
    multimapState.put(key, 5);
    assertThat(result.read(), Matchers.containsInAnyOrder(5));
    multimapState.clear();
    assertThat(multimapState.get(key).read(), Matchers.emptyIterable());
  }

  @Test
  public void testMapViaMultimapPutAndGet() {
    final String tag = "map";
    StateTag<MapState<byte[], Integer>> addr =
        StateTags.map(tag, ByteArrayCoder.of(), VarIntCoder.of());
    MapState<byte[], Integer> mapViaMultiMapState = underTestMapViaMultimap.state(NAMESPACE, addr);

    final byte[] key = "key".getBytes(StandardCharsets.UTF_8);
    SettableFuture<Iterable<Integer>> future = SettableFuture.create();
    when(mockReader.multimapFetchSingleEntryFuture(
            encodeWithCoder(key, ByteArrayCoder.of()),
            key(NAMESPACE, tag),
            STATE_FAMILY,
            VarIntCoder.of()))
        .thenReturn(future);

    mapViaMultiMapState.put(key, 1);
    ReadableState<Integer> result = mapViaMultiMapState.get(dup(key)).readLater();
    waitAndSet(future, Collections.singletonList(2), 30);
    assertEquals(Integer.valueOf(1), result.read());

    mapViaMultiMapState.put(key, 3);
    assertEquals(Integer.valueOf(3), mapViaMultiMapState.get(key).read());
    mapViaMultiMapState.clear();
    assertNull(mapViaMultiMapState.get(key).read());
  }

  @Test
  public void testMultimapRemoveAndGet() {
    final String tag = "multimap";
    StateTag<MultimapState<byte[], Integer>> addr =
        StateTags.multimap(tag, ByteArrayCoder.of(), VarIntCoder.of());
    MultimapState<byte[], Integer> multimapState = underTest.state(NAMESPACE, addr);

    final byte[] key = "key".getBytes(StandardCharsets.UTF_8);
    SettableFuture<Iterable<Integer>> future = SettableFuture.create();
    when(mockReader.multimapFetchSingleEntryFuture(
            encodeWithCoder(key, ByteArrayCoder.of()),
            key(NAMESPACE, tag),
            STATE_FAMILY,
            VarIntCoder.of()))
        .thenReturn(future);

    ReadableState<Iterable<Integer>> result1 = multimapState.get(key).readLater();
    ReadableState<Iterable<Integer>> result2 = multimapState.get(dup(key)).readLater();
    waitAndSet(future, Arrays.asList(1, 2, 3), 30);

    assertTrue(multimapState.containsKey(key).read());
    assertThat(result1.read(), Matchers.containsInAnyOrder(1, 2, 3));

    multimapState.remove(key);
    assertFalse(multimapState.containsKey(dup(key)).read());
    assertThat(result2.read(), Matchers.emptyIterable());
  }

  @Test
  public void testMapViaMultimapRemoveAndGet() {
    final String tag = "map";
    StateTag<MapState<byte[], Integer>> addr =
        StateTags.map(tag, ByteArrayCoder.of(), VarIntCoder.of());
    MapState<byte[], Integer> mapViaMultiMapState = underTestMapViaMultimap.state(NAMESPACE, addr);

    final byte[] key = "key".getBytes(StandardCharsets.UTF_8);
    SettableFuture<Iterable<Integer>> future = SettableFuture.create();
    when(mockReader.multimapFetchSingleEntryFuture(
            encodeWithCoder(key, ByteArrayCoder.of()),
            key(NAMESPACE, tag),
            STATE_FAMILY,
            VarIntCoder.of()))
        .thenReturn(future);

    ReadableState<Integer> result1 = mapViaMultiMapState.get(key).readLater();
    ReadableState<Integer> result2 = mapViaMultiMapState.get(dup(key)).readLater();
    waitAndSet(future, Collections.singletonList(1), 30);

    assertEquals(Integer.valueOf(1), result1.read());

    mapViaMultiMapState.remove(key);
    assertNull(mapViaMultiMapState.get(dup(key)).read());
    assertNull(result2.read());
  }

  @Test
  public void testMultimapRemoveThenPut() {
    final String tag = "multimap";
    StateTag<MultimapState<byte[], Integer>> addr =
        StateTags.multimap(tag, ByteArrayCoder.of(), VarIntCoder.of());
    MultimapState<byte[], Integer> multimapState = underTest.state(NAMESPACE, addr);

    final byte[] key = "key".getBytes(StandardCharsets.UTF_8);
    SettableFuture<Iterable<Integer>> future = SettableFuture.create();
    when(mockReader.multimapFetchSingleEntryFuture(
            encodeWithCoder(key, ByteArrayCoder.of()),
            key(NAMESPACE, tag),
            STATE_FAMILY,
            VarIntCoder.of()))
        .thenReturn(future);

    ReadableState<Iterable<Integer>> result = multimapState.get(key).readLater();
    waitAndSet(future, Arrays.asList(1, 2, 3), 30);
    multimapState.remove(dup(key));
    multimapState.put(key, 4);
    multimapState.put(dup(key), 5);
    assertThat(result.read(), Matchers.containsInAnyOrder(4, 5));
  }

  @Test
  public void testMultimapRemovePersistPut() {
    final String tag = "multimap";
    StateTag<MultimapState<String, Integer>> addr =
        StateTags.multimap(tag, StringUtf8Coder.of(), VarIntCoder.of());
    MultimapState<String, Integer> multimapState = underTest.state(NAMESPACE, addr);

    final String key = "key";
    multimapState.put(key, 1);
    multimapState.put(key, 2);

    Windmill.WorkItemCommitRequest.Builder commitBuilder =
        Windmill.WorkItemCommitRequest.newBuilder();

    // After key is removed, this key is cache complete and no need to read backend.
    multimapState.remove(key);
    multimapState.put(key, 4);
    // Since key is cache complete, value 4 in localAdditions should be added to cached values,
    /// instead of being cleared from cache after persisted.
    underTest.persist(commitBuilder);
    assertTagMultimapUpdates(
        Iterables.getOnlyElement(commitBuilder.getMultimapUpdatesBuilderList()),
        new MultimapEntryUpdate(key, Collections.singletonList(4), true));

    multimapState.put(key, 5);
    assertThat(multimapState.get(key).read(), Matchers.containsInAnyOrder(4, 5));
  }

  @Test
  public void testMultimapGetLocalCombineStorage() {
    final String tag = "multimap";
    StateTag<MultimapState<byte[], Integer>> addr =
        StateTags.multimap(tag, ByteArrayCoder.of(), VarIntCoder.of());
    MultimapState<byte[], Integer> multimapState = underTest.state(NAMESPACE, addr);

    final byte[] key = "key".getBytes(StandardCharsets.UTF_8);
    SettableFuture<Iterable<Integer>> future = SettableFuture.create();
    when(mockReader.multimapFetchSingleEntryFuture(
            encodeWithCoder(key, ByteArrayCoder.of()),
            key(NAMESPACE, tag),
            STATE_FAMILY,
            VarIntCoder.of()))
        .thenReturn(future);

    ReadableState<Iterable<Integer>> result = multimapState.get(dup(key)).readLater();
    waitAndSet(future, Arrays.asList(1, 2), 30);
    multimapState.put(key, 3);
    multimapState.put(dup(key), 4);
    assertFalse(multimapState.isEmpty().read());
    assertThat(result.read(), Matchers.containsInAnyOrder(1, 2, 3, 4));
  }

  @Test
  public void testMultimapLocalRemoveOverrideStorage() {
    final String tag = "multimap";
    StateTag<MultimapState<byte[], Integer>> addr =
        StateTags.multimap(tag, ByteArrayCoder.of(), VarIntCoder.of());
    MultimapState<byte[], Integer> multimapState = underTest.state(NAMESPACE, addr);

    final byte[] key = "key".getBytes(StandardCharsets.UTF_8);
    SettableFuture<Iterable<Integer>> future = SettableFuture.create();
    when(mockReader.multimapFetchSingleEntryFuture(
            encodeWithCoder(key, ByteArrayCoder.of()),
            key(NAMESPACE, tag),
            STATE_FAMILY,
            VarIntCoder.of()))
        .thenReturn(future);

    ReadableState<Iterable<Integer>> result = multimapState.get(key).readLater();
    waitAndSet(future, Arrays.asList(1, 2), 30);
    multimapState.remove(dup(key));
    assertThat(result.read(), Matchers.emptyIterable());
    multimapState.put(key, 3);
    multimapState.put(dup(key), 4);
    assertFalse(multimapState.isEmpty().read());
    assertThat(result.read(), Matchers.containsInAnyOrder(3, 4));
  }

  @Test
  public void testMultimapLocalClearOverrideStorage() {
    final String tag = "multimap";
    StateTag<MultimapState<byte[], Integer>> addr =
        StateTags.multimap(tag, ByteArrayCoder.of(), VarIntCoder.of());
    MultimapState<byte[], Integer> multimapState = underTest.state(NAMESPACE, addr);

    final byte[] key1 = "key1".getBytes(StandardCharsets.UTF_8);
    final byte[] key2 = "key2".getBytes(StandardCharsets.UTF_8);
    SettableFuture<Iterable<Integer>> future = SettableFuture.create();
    when(mockReader.multimapFetchSingleEntryFuture(
            encodeWithCoder(key1, ByteArrayCoder.of()),
            key(NAMESPACE, tag),
            STATE_FAMILY,
            VarIntCoder.of()))
        .thenReturn(future);
    SettableFuture<Iterable<Integer>> future2 = SettableFuture.create();
    when(mockReader.multimapFetchSingleEntryFuture(
            encodeWithCoder(key2, ByteArrayCoder.of()),
            key(NAMESPACE, tag),
            STATE_FAMILY,
            VarIntCoder.of()))
        .thenReturn(future2);

    ReadableState<Iterable<Integer>> result1 = multimapState.get(key1).readLater();
    ReadableState<Iterable<Integer>> result2 = multimapState.get(dup(key2)).readLater();
    multimapState.clear();
    waitAndSet(future, Arrays.asList(1, 2), 30);
    assertThat(result1.read(), Matchers.emptyIterable());
    assertThat(result2.read(), Matchers.emptyIterable());
    assertThat(multimapState.keys().read(), Matchers.emptyIterable());
    assertThat(multimapState.entries().read(), Matchers.emptyIterable());
    assertTrue(multimapState.isEmpty().read());
  }

  @Test
  public void testMultimapBasicEntriesAndKeys() {
    final String tag = "multimap";
    StateTag<MultimapState<byte[], Integer>> addr =
        StateTags.multimap(tag, ByteArrayCoder.of(), VarIntCoder.of());
    MultimapState<byte[], Integer> multimapState = underTest.state(NAMESPACE, addr);

    final byte[] key1 = "key1".getBytes(StandardCharsets.UTF_8);
    final byte[] key2 = "key2".getBytes(StandardCharsets.UTF_8);

    SettableFuture<Iterable<Map.Entry<ByteString, Iterable<Integer>>>> entriesFuture =
        SettableFuture.create();
    when(mockReader.multimapFetchAllFuture(
            false, key(NAMESPACE, tag), STATE_FAMILY, VarIntCoder.of()))
        .thenReturn(entriesFuture);
    SettableFuture<Iterable<Map.Entry<ByteString, Iterable<Integer>>>> keysFuture =
        SettableFuture.create();
    when(mockReader.multimapFetchAllFuture(
            true, key(NAMESPACE, tag), STATE_FAMILY, VarIntCoder.of()))
        .thenReturn(keysFuture);

    ReadableState<Iterable<Map.Entry<byte[], Integer>>> entriesResult =
        multimapState.entries().readLater();
    ReadableState<Iterable<byte[]>> keysResult = multimapState.keys().readLater();
    waitAndSet(
        entriesFuture,
        Arrays.asList(multimapEntry(key1, 1, 2, 3), multimapEntry(key2, 2, 3, 4)),
        30);
    waitAndSet(keysFuture, Arrays.asList(multimapEntry(key1), multimapEntry(key2)), 30);

    Iterable<Map.Entry<byte[], Integer>> entries = entriesResult.read();
    assertEquals(6, Iterables.size(entries));
    assertThat(
        entries,
        Matchers.containsInAnyOrder(
            multimapEntryMatcher(key1, 1),
            multimapEntryMatcher(key1, 2),
            multimapEntryMatcher(key1, 3),
            multimapEntryMatcher(key2, 4),
            multimapEntryMatcher(key2, 2),
            multimapEntryMatcher(key2, 3)));

    Iterable<byte[]> keys = keysResult.read();
    assertEquals(2, Iterables.size(keys));
    assertThat(keys, Matchers.containsInAnyOrder(key1, key2));
  }

  @Test
  public void testMultimapEntriesAndKeysMergeLocalAdd() {
    final String tag = "multimap";
    StateTag<MultimapState<byte[], Integer>> addr =
        StateTags.multimap(tag, ByteArrayCoder.of(), VarIntCoder.of());
    MultimapState<byte[], Integer> multimapState = underTest.state(NAMESPACE, addr);

    final byte[] key1 = "key1".getBytes(StandardCharsets.UTF_8);
    final byte[] key2 = "key2".getBytes(StandardCharsets.UTF_8);
    final byte[] key3 = "key3".getBytes(StandardCharsets.UTF_8);

    SettableFuture<Iterable<Map.Entry<ByteString, Iterable<Integer>>>> entriesFuture =
        SettableFuture.create();
    when(mockReader.multimapFetchAllFuture(
            false, key(NAMESPACE, tag), STATE_FAMILY, VarIntCoder.of()))
        .thenReturn(entriesFuture);
    SettableFuture<Iterable<Map.Entry<ByteString, Iterable<Integer>>>> keysFuture =
        SettableFuture.create();
    when(mockReader.multimapFetchAllFuture(
            true, key(NAMESPACE, tag), STATE_FAMILY, VarIntCoder.of()))
        .thenReturn(keysFuture);

    ReadableState<Iterable<Map.Entry<byte[], Integer>>> entriesResult =
        multimapState.entries().readLater();
    ReadableState<Iterable<byte[]>> keysResult = multimapState.keys().readLater();
    waitAndSet(
        entriesFuture,
        Arrays.asList(multimapEntry(key1, 1, 2, 3), multimapEntry(key2, 2, 3, 4)),
        30);
    waitAndSet(keysFuture, Arrays.asList(multimapEntry(key1), multimapEntry(key2)), 30);

    multimapState.put(key1, 7);
    multimapState.put(dup(key2), 8);
    multimapState.put(dup(key3), 8);

    Iterable<Map.Entry<byte[], Integer>> entries = entriesResult.read();
    assertEquals(9, Iterables.size(entries));
    assertThat(
        entries,
        Matchers.containsInAnyOrder(
            multimapEntryMatcher(key1, 1),
            multimapEntryMatcher(key1, 2),
            multimapEntryMatcher(key1, 3),
            multimapEntryMatcher(key1, 7),
            multimapEntryMatcher(key2, 4),
            multimapEntryMatcher(key2, 2),
            multimapEntryMatcher(key2, 3),
            multimapEntryMatcher(key2, 8),
            multimapEntryMatcher(key3, 8)));

    Iterable<byte[]> keys = keysResult.read();
    assertEquals(3, Iterables.size(keys));
    assertThat(keys, Matchers.containsInAnyOrder(key1, key2, key3));
  }

  @Test
  public void testMapViaMultimapEntriesAndKeysMergeLocalAddRemoveClear() {
    final String tag = "map";
    StateTag<MapState<byte[], Integer>> addr =
        StateTags.map(tag, ByteArrayCoder.of(), VarIntCoder.of());
    MapState<byte[], Integer> mapState = underTestMapViaMultimap.state(NAMESPACE, addr);

    final byte[] key1 = "key1".getBytes(StandardCharsets.UTF_8);
    final byte[] key2 = "key2".getBytes(StandardCharsets.UTF_8);
    final byte[] key3 = "key3".getBytes(StandardCharsets.UTF_8);
    final byte[] key4 = "key4".getBytes(StandardCharsets.UTF_8);

    SettableFuture<Iterable<Map.Entry<ByteString, Iterable<Integer>>>> entriesFuture =
        SettableFuture.create();
    when(mockReader.multimapFetchAllFuture(
            false, key(NAMESPACE, tag), STATE_FAMILY, VarIntCoder.of()))
        .thenReturn(entriesFuture);
    SettableFuture<Iterable<Map.Entry<ByteString, Iterable<Integer>>>> keysFuture =
        SettableFuture.create();
    when(mockReader.multimapFetchAllFuture(
            true, key(NAMESPACE, tag), STATE_FAMILY, VarIntCoder.of()))
        .thenReturn(keysFuture);

    ReadableState<Iterable<Map.Entry<byte[], Integer>>> entriesResult =
        mapState.entries().readLater();
    ReadableState<Iterable<byte[]>> keysResult = mapState.keys().readLater();
    waitAndSet(entriesFuture, Arrays.asList(multimapEntry(key1, 3), multimapEntry(key2, 4)), 30);
    waitAndSet(keysFuture, Arrays.asList(multimapEntry(key1), multimapEntry(key2)), 30);

    mapState.put(key1, 7);
    mapState.put(dup(key3), 8);
    mapState.put(key4, 1);
    mapState.remove(key4);

    Iterable<Map.Entry<byte[], Integer>> entries = entriesResult.read();
    assertEquals(3, Iterables.size(entries));
    assertThat(
        entries,
        Matchers.containsInAnyOrder(
            multimapEntryMatcher(key1, 7),
            multimapEntryMatcher(key2, 4),
            multimapEntryMatcher(key3, 8)));

    Iterable<byte[]> keys = keysResult.read();
    assertEquals(3, Iterables.size(keys));
    assertThat(keys, Matchers.containsInAnyOrder(key1, key2, key3));
    assertFalse(mapState.isEmpty().read());

    mapState.clear();
    assertTrue(mapState.isEmpty().read());
    assertTrue(Iterables.isEmpty(mapState.keys().read()));
    assertTrue(Iterables.isEmpty(mapState.entries().read()));

    // Previously read iterable should still have the same result.
    assertEquals(3, Iterables.size(keys));
    assertThat(keys, Matchers.containsInAnyOrder(key1, key2, key3));
  }

  @Test
  public void testMultimapEntriesAndKeysMergeLocalRemove() {
    final String tag = "multimap";
    StateTag<MultimapState<byte[], Integer>> addr =
        StateTags.multimap(tag, ByteArrayCoder.of(), VarIntCoder.of());
    MultimapState<byte[], Integer> multimapState = underTest.state(NAMESPACE, addr);

    final byte[] key1 = "key1".getBytes(StandardCharsets.UTF_8);
    final byte[] key2 = "key2".getBytes(StandardCharsets.UTF_8);
    final byte[] key3 = "key3".getBytes(StandardCharsets.UTF_8);

    SettableFuture<Iterable<Map.Entry<ByteString, Iterable<Integer>>>> entriesFuture =
        SettableFuture.create();
    when(mockReader.multimapFetchAllFuture(
            false, key(NAMESPACE, tag), STATE_FAMILY, VarIntCoder.of()))
        .thenReturn(entriesFuture);
    SettableFuture<Iterable<Map.Entry<ByteString, Iterable<Integer>>>> keysFuture =
        SettableFuture.create();
    when(mockReader.multimapFetchAllFuture(
            true, key(NAMESPACE, tag), STATE_FAMILY, VarIntCoder.of()))
        .thenReturn(keysFuture);

    ReadableState<Iterable<Map.Entry<byte[], Integer>>> entriesResult =
        multimapState.entries().readLater();
    ReadableState<Iterable<byte[]>> keysResult = multimapState.keys().readLater();
    waitAndSet(
        entriesFuture,
        Arrays.asList(multimapEntry(key1, 1, 2, 3), multimapEntry(key2, 2, 3, 4)),
        30);
    waitAndSet(keysFuture, Arrays.asList(multimapEntry(key1), multimapEntry(key2)), 30);

    multimapState.remove(dup(key1));
    multimapState.put(key2, 8);
    multimapState.put(dup(key3), 8);

    Iterable<Map.Entry<byte[], Integer>> entries = entriesResult.read();
    assertEquals(5, Iterables.size(entries));
    assertThat(
        entries,
        Matchers.containsInAnyOrder(
            multimapEntryMatcher(key2, 4),
            multimapEntryMatcher(key2, 2),
            multimapEntryMatcher(key2, 3),
            multimapEntryMatcher(key2, 8),
            multimapEntryMatcher(key3, 8)));

    Iterable<byte[]> keys = keysResult.read();
    assertThat(keys, Matchers.containsInAnyOrder(key2, key3));
  }

  @Test
  public void testMapViaMultimapEntriesAndKeysMergeLocalRemove() {
    final String tag = "map";
    StateTag<MapState<byte[], Integer>> addr =
        StateTags.map(tag, ByteArrayCoder.of(), VarIntCoder.of());
    MapState<byte[], Integer> mapState = underTestMapViaMultimap.state(NAMESPACE, addr);

    final byte[] key1 = "key1".getBytes(StandardCharsets.UTF_8);
    final byte[] key2 = "key2".getBytes(StandardCharsets.UTF_8);
    final byte[] key3 = "key3".getBytes(StandardCharsets.UTF_8);

    SettableFuture<Iterable<Map.Entry<ByteString, Iterable<Integer>>>> entriesFuture =
        SettableFuture.create();
    when(mockReader.multimapFetchAllFuture(
            false, key(NAMESPACE, tag), STATE_FAMILY, VarIntCoder.of()))
        .thenReturn(entriesFuture);
    SettableFuture<Iterable<Map.Entry<ByteString, Iterable<Integer>>>> keysFuture =
        SettableFuture.create();
    when(mockReader.multimapFetchAllFuture(
            true, key(NAMESPACE, tag), STATE_FAMILY, VarIntCoder.of()))
        .thenReturn(keysFuture);

    ReadableState<Iterable<Map.Entry<byte[], Integer>>> entriesResult =
        mapState.entries().readLater();
    ReadableState<Iterable<byte[]>> keysResult = mapState.keys().readLater();
    waitAndSet(entriesFuture, Arrays.asList(multimapEntry(key1, 1), multimapEntry(key2, 2)), 30);
    waitAndSet(keysFuture, Arrays.asList(multimapEntry(key1), multimapEntry(key2)), 30);

    mapState.remove(dup(key1));
    mapState.put(key2, 8);
    mapState.put(dup(key3), 9);

    Iterable<Map.Entry<byte[], Integer>> entries = entriesResult.read();
    assertEquals(2, Iterables.size(entries));
    assertThat(
        entries,
        Matchers.containsInAnyOrder(multimapEntryMatcher(key2, 8), multimapEntryMatcher(key3, 9)));

    Iterable<byte[]> keys = keysResult.read();
    assertThat(keys, Matchers.containsInAnyOrder(key2, key3));
  }

  @Test
  public void testMultimapCacheComplete() {
    final String tag = "multimap";
    StateTag<MultimapState<byte[], Integer>> addr =
        StateTags.multimap(tag, ByteArrayCoder.of(), VarIntCoder.of());
    MultimapState<byte[], Integer> multimapState = underTest.state(NAMESPACE, addr);

    final byte[] key = "key".getBytes(StandardCharsets.UTF_8);

    SettableFuture<Iterable<Map.Entry<ByteString, Iterable<Integer>>>> entriesFuture =
        SettableFuture.create();
    when(mockReader.multimapFetchAllFuture(
            false, key(NAMESPACE, tag), STATE_FAMILY, VarIntCoder.of()))
        .thenReturn(entriesFuture);

    // to set up the multimap as cache complete
    waitAndSet(entriesFuture, weightedList(multimapEntry(key, 1, 2, 3)), 30);
    multimapState.entries().read();

    multimapState.put(key, 2);

    when(mockReader.multimapFetchAllFuture(
            anyBoolean(), eq(key(NAMESPACE, tag)), eq(STATE_FAMILY), eq(VarIntCoder.of())))
        .thenThrow(
            new RuntimeException(
                "The multimap is cache complete and should not perform any windmill read."));
    when(mockReader.multimapFetchSingleEntryFuture(
            any(), eq(key(NAMESPACE, tag)), eq(STATE_FAMILY), eq(VarIntCoder.of())))
        .thenThrow(
            new RuntimeException(
                "The multimap is cache complete and should not perform any windmill read."));

    Iterable<Map.Entry<byte[], Integer>> entries = multimapState.entries().read();
    assertEquals(4, Iterables.size(entries));
    assertThat(
        entries,
        Matchers.containsInAnyOrder(
            multimapEntryMatcher(key, 1),
            multimapEntryMatcher(key, 2),
            multimapEntryMatcher(key, 3),
            multimapEntryMatcher(key, 2)));

    Iterable<byte[]> keys = multimapState.keys().read();
    assertThat(keys, Matchers.containsInAnyOrder(key));

    Iterable<Integer> values = multimapState.get(dup(key)).read();
    assertThat(values, Matchers.containsInAnyOrder(1, 2, 2, 3));
  }

  @Test
  public void testMultimapCachedSingleEntry() {
    final String tag = "multimap";
    StateTag<MultimapState<byte[], Integer>> addr =
        StateTags.multimap(tag, ByteArrayCoder.of(), VarIntCoder.of());
    MultimapState<byte[], Integer> multimapState = underTest.state(NAMESPACE, addr);

    final byte[] key = "key".getBytes(StandardCharsets.UTF_8);

    SettableFuture<Iterable<Integer>> entryFuture = SettableFuture.create();
    when(mockReader.multimapFetchSingleEntryFuture(
            encodeWithCoder(key, ByteArrayCoder.of()),
            key(NAMESPACE, tag),
            STATE_FAMILY,
            VarIntCoder.of()))
        .thenReturn(entryFuture);

    // to set up the entry key as cache complete and add some local changes
    waitAndSet(entryFuture, weightedList(1, 2, 3), 30);
    multimapState.get(key).read();
    multimapState.put(key, 2);

    when(mockReader.multimapFetchSingleEntryFuture(
            eq(encodeWithCoder(key, ByteArrayCoder.of())),
            eq(key(NAMESPACE, tag)),
            eq(STATE_FAMILY),
            eq(VarIntCoder.of())))
        .thenThrow(
            new RuntimeException(
                "The multimap is cache complete for "
                    + Arrays.toString(key)
                    + " and should not perform any windmill read."));

    Iterable<Integer> values = multimapState.get(dup(key)).read();
    assertThat(values, Matchers.containsInAnyOrder(1, 2, 2, 3));
    assertTrue(multimapState.containsKey(key).read());
  }

  @Test
  public void testMultimapCachedPartialEntry() {
    final String tag = "multimap";
    StateTag<MultimapState<byte[], Integer>> addr =
        StateTags.multimap(tag, ByteArrayCoder.of(), VarIntCoder.of());
    MultimapState<byte[], Integer> multimapState = underTest.state(NAMESPACE, addr);

    final byte[] key1 = "key1".getBytes(StandardCharsets.UTF_8);
    final byte[] key2 = "key2".getBytes(StandardCharsets.UTF_8);
    final byte[] key3 = "key3".getBytes(StandardCharsets.UTF_8);

    SettableFuture<Iterable<Integer>> entryFuture = SettableFuture.create();
    when(mockReader.multimapFetchSingleEntryFuture(
            encodeWithCoder(key1, ByteArrayCoder.of()),
            key(NAMESPACE, tag),
            STATE_FAMILY,
            VarIntCoder.of()))
        .thenReturn(entryFuture);

    // to set up the entry key1 as cache complete and add some local changes
    waitAndSet(entryFuture, weightedList(1, 2, 3), 30);
    multimapState.get(key1).read();
    multimapState.put(key1, 2);
    multimapState.put(key3, 20);

    SettableFuture<Iterable<Map.Entry<ByteString, Iterable<Integer>>>> entriesFuture =
        SettableFuture.create();
    when(mockReader.multimapFetchAllFuture(
            false, key(NAMESPACE, tag), STATE_FAMILY, VarIntCoder.of()))
        .thenReturn(entriesFuture);

    // windmill contains extra entry key2
    waitAndSet(
        entriesFuture,
        weightedList(multimapEntry(key1, 1, 2, 3), multimapEntry(key2, 4, 5, 6)),
        30);

    // key1 exist in both cache and windmill; key2 exists only in windmill; key3 exists only in
    // cache. They should all be merged.
    Iterable<Map.Entry<byte[], Integer>> entries = multimapState.entries().read();

    assertEquals(8, Iterables.size(entries));
    assertThat(
        entries,
        Matchers.containsInAnyOrder(
            multimapEntryMatcher(key1, 1),
            multimapEntryMatcher(key1, 2),
            multimapEntryMatcher(key1, 2),
            multimapEntryMatcher(key1, 3),
            multimapEntryMatcher(key2, 4),
            multimapEntryMatcher(key2, 5),
            multimapEntryMatcher(key2, 6),
            multimapEntryMatcher(key3, 20)));

    assertThat(multimapState.keys().read(), Matchers.containsInAnyOrder(key1, key2, key3));
  }

  @Test
  public void testMultimapEntriesCombineCacheAndWindmill() {
    final String tag = "multimap";
    StateTag<MultimapState<byte[], Integer>> addr =
        StateTags.multimap(tag, ByteArrayCoder.of(), VarIntCoder.of());
    MultimapState<byte[], Integer> multimapState = underTest.state(NAMESPACE, addr);

    final byte[] key1 = "key1".getBytes(StandardCharsets.UTF_8);
    final byte[] key2 = "key2".getBytes(StandardCharsets.UTF_8);
    final byte[] key3 = "key3".getBytes(StandardCharsets.UTF_8);

    SettableFuture<Iterable<Integer>> entryFuture = SettableFuture.create();
    when(mockReader.multimapFetchSingleEntryFuture(
            encodeWithCoder(key1, ByteArrayCoder.of()),
            key(NAMESPACE, tag),
            STATE_FAMILY,
            VarIntCoder.of()))
        .thenReturn(entryFuture);

    // to set up the entry key1 as cache complete and add some local changes
    waitAndSet(entryFuture, weightedList(1, 2, 3), 30);
    multimapState.get(key1).read();
    multimapState.put(dup(key1), 2);
    multimapState.put(dup(key3), 20);

    SettableFuture<Iterable<Map.Entry<ByteString, Iterable<Integer>>>> entriesFuture =
        SettableFuture.create();
    when(mockReader.multimapFetchAllFuture(
            false, key(NAMESPACE, tag), STATE_FAMILY, VarIntCoder.of()))
        .thenReturn(entriesFuture);
    SettableFuture<Iterable<Map.Entry<ByteString, Iterable<Integer>>>> keysFuture =
        SettableFuture.create();
    when(mockReader.multimapFetchAllFuture(
            true, key(NAMESPACE, tag), STATE_FAMILY, VarIntCoder.of()))
        .thenReturn(keysFuture);

    // windmill contains extra entry key2, and this time the entries returned should not be cached.
    waitAndSet(
        entriesFuture,
        Arrays.asList(multimapEntry(key1, 1, 2, 3), multimapEntry(key2, 4, 5, 6)),
        30);
    waitAndSet(keysFuture, Arrays.asList(multimapEntry(key1), multimapEntry(key2)), 30);

    // key1 exist in both cache and windmill; key2 exists only in windmill; key3 exists only in
    // cache. They should all be merged.
    Iterable<Map.Entry<byte[], Integer>> entries = multimapState.entries().read();

    assertEquals(8, Iterables.size(entries));
    assertThat(
        entries,
        Matchers.containsInAnyOrder(
            multimapEntryMatcher(key1, 1),
            multimapEntryMatcher(key1, 2),
            multimapEntryMatcher(key1, 2),
            multimapEntryMatcher(key1, 3),
            multimapEntryMatcher(key2, 4),
            multimapEntryMatcher(key2, 5),
            multimapEntryMatcher(key2, 6),
            multimapEntryMatcher(key3, 20)));

    assertThat(multimapState.keys().read(), Matchers.containsInAnyOrder(key1, key2, key3));
  }

  @Test
  public void testMultimapModifyAfterReadDoesNotAffectResult() {
    final String tag = "multimap";
    StateTag<MultimapState<byte[], Integer>> addr =
        StateTags.multimap(tag, ByteArrayCoder.of(), VarIntCoder.of());
    MultimapState<byte[], Integer> multimapState = underTest.state(NAMESPACE, addr);

    final byte[] key1 = "key1".getBytes(StandardCharsets.UTF_8);
    final byte[] key2 = "key2".getBytes(StandardCharsets.UTF_8);
    final byte[] key3 = "key3".getBytes(StandardCharsets.UTF_8);
    final byte[] key4 = "key4".getBytes(StandardCharsets.UTF_8);

    SettableFuture<Iterable<Map.Entry<ByteString, Iterable<Integer>>>> entriesFuture =
        SettableFuture.create();
    when(mockReader.multimapFetchAllFuture(
            false, key(NAMESPACE, tag), STATE_FAMILY, VarIntCoder.of()))
        .thenReturn(entriesFuture);
    SettableFuture<Iterable<Map.Entry<ByteString, Iterable<Integer>>>> keysFuture =
        SettableFuture.create();
    when(mockReader.multimapFetchAllFuture(
            true, key(NAMESPACE, tag), STATE_FAMILY, VarIntCoder.of()))
        .thenReturn(keysFuture);
    SettableFuture<Iterable<Integer>> getKey1Future = SettableFuture.create();
    SettableFuture<Iterable<Integer>> getKey2Future = SettableFuture.create();
    SettableFuture<Iterable<Integer>> getKey4Future = SettableFuture.create();
    when(mockReader.multimapFetchSingleEntryFuture(
            encodeWithCoder(key1, ByteArrayCoder.of()),
            key(NAMESPACE, tag),
            STATE_FAMILY,
            VarIntCoder.of()))
        .thenReturn(getKey1Future);
    when(mockReader.multimapFetchSingleEntryFuture(
            encodeWithCoder(key2, ByteArrayCoder.of()),
            key(NAMESPACE, tag),
            STATE_FAMILY,
            VarIntCoder.of()))
        .thenReturn(getKey2Future);
    when(mockReader.multimapFetchSingleEntryFuture(
            encodeWithCoder(key4, ByteArrayCoder.of()),
            key(NAMESPACE, tag),
            STATE_FAMILY,
            VarIntCoder.of()))
        .thenReturn(getKey4Future);

    ReadableState<Iterable<Map.Entry<byte[], Integer>>> entriesResult =
        multimapState.entries().readLater();
    ReadableState<Iterable<byte[]>> keysResult = multimapState.keys().readLater();
    waitAndSet(
        entriesFuture,
        Arrays.asList(multimapEntry(key1, 1, 2, 3), multimapEntry(key2, 2, 3, 4)),
        200);
    waitAndSet(keysFuture, Arrays.asList(multimapEntry(key1), multimapEntry(key2)), 200);

    // make key4 to be known nonexistent.
    multimapState.remove(key4);

    ReadableState<Iterable<Integer>> key1Future = multimapState.get(key1).readLater();
    waitAndSet(getKey1Future, Arrays.asList(1, 2, 3), 200);
    ReadableState<Iterable<Integer>> key2Future = multimapState.get(key2).readLater();
    waitAndSet(getKey2Future, Arrays.asList(2, 3, 4), 200);
    ReadableState<Iterable<Integer>> key4Future = multimapState.get(key4).readLater();
    waitAndSet(getKey4Future, Collections.emptyList(), 200);

    multimapState.put(key1, 7);
    multimapState.put(dup(key2), 8);
    multimapState.put(dup(key3), 8);

    Iterable<Map.Entry<byte[], Integer>> entries = entriesResult.read();
    Iterable<byte[]> keys = keysResult.read();
    Iterable<Integer> key1Values = key1Future.read();
    Iterable<Integer> key2Values = key2Future.read();
    Iterable<Integer> key4Values = key4Future.read();

    // values added/removed after read should not be reflected in result
    multimapState.remove(key1);
    multimapState.put(key2, 9);
    multimapState.put(key4, 10);

    assertEquals(9, Iterables.size(entries));
    assertThat(
        entries,
        Matchers.containsInAnyOrder(
            multimapEntryMatcher(key1, 1),
            multimapEntryMatcher(key1, 2),
            multimapEntryMatcher(key1, 3),
            multimapEntryMatcher(key1, 7),
            multimapEntryMatcher(key2, 4),
            multimapEntryMatcher(key2, 2),
            multimapEntryMatcher(key2, 3),
            multimapEntryMatcher(key2, 8),
            multimapEntryMatcher(key3, 8)));

    assertEquals(3, Iterables.size(keys));
    assertThat(keys, Matchers.containsInAnyOrder(key1, key2, key3));

    assertEquals(4, Iterables.size(key1Values));
    assertThat(key1Values, Matchers.containsInAnyOrder(1, 2, 3, 7));

    assertEquals(4, Iterables.size(key2Values));
    assertThat(key2Values, Matchers.containsInAnyOrder(2, 3, 4, 8));

    assertTrue(Iterables.isEmpty(key4Values));
  }

  @Test
  public void testMultimapLazyIterateHugeEntriesResult() {
    // A multimap with 1 million keys with a total of 10GBs data
    final String tag = "multimap";
    StateTag<MultimapState<byte[], Integer>> addr =
        StateTags.multimap(tag, ByteArrayCoder.of(), VarIntCoder.of());
    MultimapState<byte[], Integer> multimapState = underTest.state(NAMESPACE, addr);

    SettableFuture<Iterable<Map.Entry<ByteString, Iterable<Integer>>>> entriesFuture =
        SettableFuture.create();
    when(mockReader.multimapFetchAllFuture(
            false, key(NAMESPACE, tag), STATE_FAMILY, VarIntCoder.of()))
        .thenReturn(entriesFuture);

    waitAndSet(
        entriesFuture,
        () ->
            new Iterator<Map.Entry<ByteString, Iterable<Integer>>>() {
              final int targetEntries = 1_000_000; // return 1 million entries, which is 10 GBs
              final byte[] entryKey = new byte[10_000]; // each key is 10KB
              final Random rand = new Random();
              int returnedEntries = 0;

              @Override
              public boolean hasNext() {
                return returnedEntries < targetEntries;
              }

              @Override
              public Map.Entry<ByteString, Iterable<Integer>> next() {
                returnedEntries++;
                rand.nextBytes(entryKey);
                return multimapEntry(entryKey, 1);
              }
            },
        200);
    Iterable<Map.Entry<byte[], Integer>> entries = multimapState.entries().read();
    assertEquals(1_000_000, Iterables.size(entries));
  }

  @Test
  public void testMultimapLazyIterateHugeKeysResult() {
    // A multimap with 1 million keys with a total of 10GBs data
    final String tag = "multimap";
    StateTag<MultimapState<byte[], Integer>> addr =
        StateTags.multimap(tag, ByteArrayCoder.of(), VarIntCoder.of());
    MultimapState<byte[], Integer> multimapState = underTest.state(NAMESPACE, addr);

    SettableFuture<Iterable<Map.Entry<ByteString, Iterable<Integer>>>> keysFuture =
        SettableFuture.create();
    when(mockReader.multimapFetchAllFuture(
            true, key(NAMESPACE, tag), STATE_FAMILY, VarIntCoder.of()))
        .thenReturn(keysFuture);

    waitAndSet(
        keysFuture,
        () ->
            new Iterator<Map.Entry<ByteString, Iterable<Integer>>>() {
              final int targetEntries = 1_000_000; // return 1 million entries, which is 10 GBs
              final byte[] entryKey = new byte[10_000]; // each key is 10KB
              final Random rand = new Random();
              int returnedEntries = 0;

              @Override
              public boolean hasNext() {
                return returnedEntries < targetEntries;
              }

              @Override
              public Map.Entry<ByteString, Iterable<Integer>> next() {
                returnedEntries++;
                rand.nextBytes(entryKey);
                return multimapEntry(entryKey);
              }
            },
        200);
    Iterable<byte[]> keys = multimapState.keys().read();
    assertEquals(1_000_000, Iterables.size(keys));
  }

  @Test
  public void testMultimapLazyIterateHugeEntriesResultSingleEntry() {
    // A multimap with 1 key and 1 million values and a total of 10GBs data
    final String tag = "multimap";
    final Integer key = 100;
    StateTag<MultimapState<Integer, byte[]>> addr =
        StateTags.multimap(tag, VarIntCoder.of(), ByteArrayCoder.of());
    MultimapState<Integer, byte[]> multimapState = underTest.state(NAMESPACE, addr);

    SettableFuture<Iterable<Map.Entry<ByteString, Iterable<byte[]>>>> entriesFuture =
        SettableFuture.create();
    when(mockReader.multimapFetchAllFuture(
            false, key(NAMESPACE, tag), STATE_FAMILY, ByteArrayCoder.of()))
        .thenReturn(entriesFuture);
    SettableFuture<Iterable<byte[]>> getKeyFuture = SettableFuture.create();
    when(mockReader.multimapFetchSingleEntryFuture(
            encodeWithCoder(key, VarIntCoder.of()),
            key(NAMESPACE, tag),
            STATE_FAMILY,
            ByteArrayCoder.of()))
        .thenReturn(getKeyFuture);

    // a not weighted iterators that returns tons of data
    Iterable<byte[]> values =
        () ->
            new Iterator<byte[]>() {
              final int targetValues = 1_000_000; // return 1 million values, which is 10 GBs
              final byte[] value = new byte[10_000]; // each value is 10KB
              final Random rand = new Random();
              int returnedValues = 0;

              @Override
              public boolean hasNext() {
                return returnedValues < targetValues;
              }

              @Override
              public byte[] next() {
                returnedValues++;
                rand.nextBytes(value);
                return value;
              }
            };

    waitAndSet(
        entriesFuture,
        Collections.singletonList(
            new SimpleEntry<>(encodeWithCoder(key, VarIntCoder.of()), values)),
        200);
    waitAndSet(getKeyFuture, values, 200);

    Iterable<Map.Entry<Integer, byte[]>> entries = multimapState.entries().read();
    assertEquals(1_000_000, Iterables.size(entries));

    Iterable<byte[]> valueResult = multimapState.get(key).read();
    assertEquals(1_000_000, Iterables.size(valueResult));
  }

  @Test
  public void testMultimapPutAndPersist() {
    final String tag = "multimap";
    StateTag<MultimapState<String, Integer>> addr =
        StateTags.multimap(tag, StringUtf8Coder.of(), VarIntCoder.of());
    MultimapState<String, Integer> multimapState = underTest.state(NAMESPACE, addr);

    final String key1 = "key1";
    final String key2 = "key2";

    multimapState.put(key1, 1);
    multimapState.put(key1, 2);
    multimapState.put(key2, 2);

    Windmill.WorkItemCommitRequest.Builder commitBuilder =
        Windmill.WorkItemCommitRequest.newBuilder();
    underTest.persist(commitBuilder);

    assertEquals(1, commitBuilder.getMultimapUpdatesCount());
    Windmill.TagMultimapUpdateRequest.Builder builder =
        Iterables.getOnlyElement(commitBuilder.getMultimapUpdatesBuilderList());
    assertTagMultimapUpdates(
        builder,
        new MultimapEntryUpdate(key1, Arrays.asList(1, 2), false),
        new MultimapEntryUpdate(key2, Collections.singletonList(2), false));
  }

  @Test
  public void testMultimapRemovePutAndPersist() {
    final String tag = "multimap";
    StateTag<MultimapState<String, Integer>> addr =
        StateTags.multimap(tag, StringUtf8Coder.of(), VarIntCoder.of());
    MultimapState<String, Integer> multimapState = underTest.state(NAMESPACE, addr);

    final String key1 = "key1";
    final String key2 = "key2";

    // we should add 1 and 2 to key1
    multimapState.remove(key1);
    multimapState.put(key1, 1);
    multimapState.put(key1, 2);
    // we should not add 2 to key 2
    multimapState.put(key2, 2);
    multimapState.remove(key2);
    // we should add 4 to key 2
    multimapState.put(key2, 4);

    Windmill.WorkItemCommitRequest.Builder commitBuilder =
        Windmill.WorkItemCommitRequest.newBuilder();
    underTest.persist(commitBuilder);

    assertEquals(1, commitBuilder.getMultimapUpdatesCount());
    Windmill.TagMultimapUpdateRequest.Builder builder =
        Iterables.getOnlyElement(commitBuilder.getMultimapUpdatesBuilderList());
    assertTagMultimapUpdates(
        builder,
        new MultimapEntryUpdate(key1, Arrays.asList(1, 2), true),
        new MultimapEntryUpdate(key2, Collections.singletonList(4), true));
  }

  @Test
  public void testMultimapRemoveAndPersist() {
    final String tag = "multimap";
    StateTag<MultimapState<String, Integer>> addr =
        StateTags.multimap(tag, StringUtf8Coder.of(), VarIntCoder.of());
    MultimapState<String, Integer> multimapState = underTest.state(NAMESPACE, addr);

    final String key1 = "key1";
    final String key2 = "key2";

    multimapState.remove(key1);
    multimapState.remove(key2);

    Windmill.WorkItemCommitRequest.Builder commitBuilder =
        Windmill.WorkItemCommitRequest.newBuilder();
    underTest.persist(commitBuilder);

    assertEquals(1, commitBuilder.getMultimapUpdatesCount());
    Windmill.TagMultimapUpdateRequest.Builder builder =
        Iterables.getOnlyElement(commitBuilder.getMultimapUpdatesBuilderList());
    assertTagMultimapUpdates(
        builder,
        new MultimapEntryUpdate(key1, Collections.emptyList(), true),
        new MultimapEntryUpdate(key2, Collections.emptyList(), true));
  }

  @Test
  public void testMultimapPutRemoveClearAndPersist() {
    final String tag = "multimap";
    StateTag<MultimapState<String, Integer>> addr =
        StateTags.multimap(tag, StringUtf8Coder.of(), VarIntCoder.of());
    MultimapState<String, Integer> multimapState = underTest.state(NAMESPACE, addr);

    final String key1 = "key1";
    final String key2 = "key2";
    final String key3 = "key3";

    // no need to send any put/remove if clear is called later
    multimapState.put(key1, 1);
    multimapState.put(key2, 2);
    multimapState.remove(key2);
    multimapState.clear();
    // remove without put sent after clear should also not be added: we are cache complete after
    // clear, so we know we can skip unnecessary remove.
    multimapState.remove(key3);

    Windmill.WorkItemCommitRequest.Builder commitBuilder =
        Windmill.WorkItemCommitRequest.newBuilder();
    underTest.persist(commitBuilder);

    assertEquals(1, commitBuilder.getMultimapUpdatesCount());
    Windmill.TagMultimapUpdateRequest.Builder builder =
        Iterables.getOnlyElement(commitBuilder.getMultimapUpdatesBuilderList());
    assertEquals(0, builder.getUpdatesCount());
    assertTrue(builder.getDeleteAll());
  }

  @Test
  public void testMultimapPutRemoveAndPersistWhenComplete() {
    final String tag = "multimap";
    StateTag<MultimapState<String, Integer>> addr =
        StateTags.multimap(tag, StringUtf8Coder.of(), VarIntCoder.of());
    MultimapState<String, Integer> multimapState = underTest.state(NAMESPACE, addr);

    SettableFuture<Iterable<Map.Entry<ByteString, Iterable<Integer>>>> entriesFuture =
        SettableFuture.create();
    when(mockReader.multimapFetchAllFuture(
            false, key(NAMESPACE, tag), STATE_FAMILY, VarIntCoder.of()))
        .thenReturn(entriesFuture);

    // to set up the multimap as cache complete
    waitAndSet(entriesFuture, Collections.emptyList(), 30);
    multimapState.entries().read();

    final String key1 = "key1";
    final String key2 = "key2";

    // put when complete should be sent
    multimapState.put(key1, 4);

    // put-then-remove when complete should not be sent
    multimapState.put(key2, 5);
    multimapState.remove(key2);

    Windmill.WorkItemCommitRequest.Builder commitBuilder =
        Windmill.WorkItemCommitRequest.newBuilder();
    underTest.persist(commitBuilder);

    assertEquals(1, commitBuilder.getMultimapUpdatesCount());
    Windmill.TagMultimapUpdateRequest.Builder builder =
        Iterables.getOnlyElement(commitBuilder.getMultimapUpdatesBuilderList());
    assertTagMultimapUpdates(
        builder, new MultimapEntryUpdate(key1, Collections.singletonList(4), false));
  }

  @Test
  public void testMultimapRemoveAndKeysAndPersist() throws IOException {
    final String tag = "multimap";
    StateTag<MultimapState<byte[], Integer>> addr =
        StateTags.multimap(tag, ByteArrayCoder.of(), VarIntCoder.of());
    MultimapState<byte[], Integer> multimapState = underTest.state(NAMESPACE, addr);

    final byte[] key1 = "key1".getBytes(StandardCharsets.UTF_8);
    final byte[] key2 = "key2".getBytes(StandardCharsets.UTF_8);

    SettableFuture<Iterable<Map.Entry<ByteString, Iterable<Integer>>>> keysFuture =
        SettableFuture.create();
    when(mockReader.multimapFetchAllFuture(
            true, key(NAMESPACE, tag), STATE_FAMILY, VarIntCoder.of()))
        .thenReturn(keysFuture);

    ReadableState<Iterable<byte[]>> keysResult = multimapState.keys().readLater();
    waitAndSet(
        keysFuture,
        new WeightedList<>(Arrays.asList(multimapEntry(key1), multimapEntry(key2))),
        30);

    multimapState.remove(key1);

    Iterable<byte[]> keys = keysResult.read();
    assertEquals(1, Iterables.size(keys));
    assertThat(keys, Matchers.containsInAnyOrder(key2));

    Windmill.WorkItemCommitRequest.Builder commitBuilder =
        Windmill.WorkItemCommitRequest.newBuilder();
    underTest.persist(commitBuilder);

    assertEquals(1, commitBuilder.getMultimapUpdatesCount());
    Windmill.TagMultimapUpdateRequest.Builder builder =
        Iterables.getOnlyElement(commitBuilder.getMultimapUpdatesBuilderList());
    assertEquals(1, builder.getUpdatesCount());
    assertFalse(builder.getDeleteAll());
    Windmill.TagMultimapEntry entryUpdate = Iterables.getOnlyElement(builder.getUpdatesList());
    byte[] decodedKey =
        ByteArrayCoder.of().decode(entryUpdate.getEntryName().newInput(), Context.OUTER);
    assertArrayEquals(key1, decodedKey);
    assertTrue(entryUpdate.getDeleteAll());
  }

  @Test
  public void testMultimapFuzzTest() {
    final String tag = "multimap";
    StateTag<MultimapState<String, Integer>> addr =
        StateTags.multimap(tag, StringUtf8Coder.of(), VarIntCoder.of());
    MultimapState<String, Integer> multimapState = underTest.state(NAMESPACE, addr);
    Random rand = new Random();
    final int ROUNDS = 100;

    SettableFuture<Iterable<Map.Entry<ByteString, Iterable<Integer>>>> entriesFuture =
        SettableFuture.create();
    when(mockReader.multimapFetchAllFuture(
            false, key(NAMESPACE, tag), STATE_FAMILY, VarIntCoder.of()))
        .thenReturn(entriesFuture);

    // to set up the multimap as initially empty and cache complete
    waitAndSet(entriesFuture, Collections.emptyList(), 30);
    multimapState.entries().read();

    // mirror mimics all operations on multimapState and is used to verify the correctness.
    Multimap<String, Integer> mirror = ArrayListMultimap.create();

    final BiConsumer<Multimap<String, Integer>, MultimapState<String, Integer>> operateFn =
        (Multimap<String, Integer> expected, MultimapState<String, Integer> actual) -> {
          final int OPS_PER_ROUND = 2000;
          final int NUM_KEY = 20;
          for (int j = 0; j < OPS_PER_ROUND; j++) {
            int op = rand.nextInt(100);
            String key = "key" + rand.nextInt(NUM_KEY);
            if (op < 50) {
              // 50% put operation
              Integer value = rand.nextInt();
              actual.put(key, value);
              expected.put(key, value);
            } else if (op < 95) {
              // 45% remove key operation
              actual.remove(key);
              expected.removeAll(key);
            } else {
              // 5% clear operation
              actual.clear();
              expected.clear();
            }
          }
        };

    final BiConsumer<Multimap<String, Integer>, MultimapState<String, Integer>> validateFn =
        (Multimap<String, Integer> expected, MultimapState<String, Integer> actual) -> {
          Iterable<String> read = actual.keys().read();
          Set<String> bytes = expected.keySet();
          assertThat(read, Matchers.containsInAnyOrder(bytes.toArray()));
          for (String key : actual.keys().read()) {
            assertThat(
                actual.get(key).read(), Matchers.containsInAnyOrder(expected.get(key).toArray()));
          }
        };

    for (int i = 0; i < ROUNDS; i++) {
      operateFn.accept(mirror, multimapState);
      validateFn.accept(mirror, multimapState);
    }

    // clear cache and recreate multimapState
    cache
        .forComputation("comp")
        .invalidate(ByteString.copyFrom("dummyKey", StandardCharsets.UTF_8), 123);
    resetUnderTest();
    multimapState = underTest.state(NAMESPACE, addr);

    // create corresponding GetData response based on mirror
    entriesFuture = SettableFuture.create();
    when(mockReader.multimapFetchAllFuture(
            false, key(NAMESPACE, tag), STATE_FAMILY, VarIntCoder.of()))
        .thenReturn(entriesFuture);
    SettableFuture<Iterable<Map.Entry<ByteString, Iterable<Integer>>>> keysFuture =
        SettableFuture.create();
    when(mockReader.multimapFetchAllFuture(
            true, key(NAMESPACE, tag), STATE_FAMILY, VarIntCoder.of()))
        .thenReturn(keysFuture);

    List<Map.Entry<ByteString, Iterable<Integer>>> entriesFutureContent = new ArrayList<>();
    List<Map.Entry<ByteString, Iterable<Integer>>> keysFutureContent = new ArrayList<>();

    // multimapState is not cache complete, state backend should return content in mirror.
    for (Map.Entry<String, Collection<Integer>> entry : mirror.asMap().entrySet()) {
      entriesFutureContent.add(
          new AbstractMap.SimpleEntry<>(
              encodeWithCoder(entry.getKey(), StringUtf8Coder.of()), entry.getValue()));
      keysFutureContent.add(
          new AbstractMap.SimpleEntry<>(
              encodeWithCoder(entry.getKey(), StringUtf8Coder.of()), Collections.emptyList()));
      SettableFuture<Iterable<Integer>> getKeyFuture = SettableFuture.create();
      when(mockReader.multimapFetchSingleEntryFuture(
              encodeWithCoder(entry.getKey(), StringUtf8Coder.of()),
              key(NAMESPACE, tag),
              STATE_FAMILY,
              VarIntCoder.of()))
          .thenReturn(getKeyFuture);
      waitAndSet(getKeyFuture, entry.getValue(), 20);
    }
    waitAndSet(entriesFuture, entriesFutureContent, 20);
    waitAndSet(keysFuture, keysFutureContent, 20);

    validateFn.accept(mirror, multimapState);

    // merge cache and new changes
    for (int i = 0; i < ROUNDS; i++) {
      operateFn.accept(mirror, multimapState);
      validateFn.accept(mirror, multimapState);
    }
    Windmill.WorkItemCommitRequest.Builder commitBuilder =
        Windmill.WorkItemCommitRequest.newBuilder();
    underTest.persist(commitBuilder);
  }

  @Test
  public void testOrderedListAddBeforeRead() throws Exception {
    StateTag<OrderedListState<String>> addr =
        StateTags.orderedList("orderedList", StringUtf8Coder.of());
    OrderedListState<String> orderedList = underTest.state(NAMESPACE, addr);

    SettableFuture<Iterable<TimestampedValue<String>>> future = SettableFuture.create();
    when(mockReader.orderedListFuture(
            FULL_ORDERED_LIST_RANGE,
            key(NAMESPACE, "orderedList"),
            STATE_FAMILY,
            StringUtf8Coder.of()))
        .thenReturn(future);

    orderedList.readLater();

    final TimestampedValue<String> helloValue =
        TimestampedValue.of("hello", Instant.ofEpochMilli(100));
    final TimestampedValue<String> worldValue =
        TimestampedValue.of("world", Instant.ofEpochMilli(75));
    final TimestampedValue<String> goodbyeValue =
        TimestampedValue.of("goodbye", Instant.ofEpochMilli(50));

    orderedList.add(helloValue);
    waitAndSet(future, Collections.singletonList(worldValue), 200);
    assertThat(orderedList.read(), Matchers.contains(worldValue, helloValue));

    orderedList.add(goodbyeValue);
    assertThat(orderedList.read(), Matchers.contains(goodbyeValue, worldValue, helloValue));
  }

  @Test
  public void testOrderedListAddBeforeRangeRead() throws Exception {
    StateTag<OrderedListState<String>> addr =
        StateTags.orderedList("orderedList", StringUtf8Coder.of());
    OrderedListState<String> orderedList = underTest.state(NAMESPACE, addr);

    SettableFuture<Iterable<TimestampedValue<String>>> future = SettableFuture.create();
    Range<Long> readSubrange = Range.closedOpen(70 * 1000L, 100 * 1000L);
    when(mockReader.orderedListFuture(
            readSubrange, key(NAMESPACE, "orderedList"), STATE_FAMILY, StringUtf8Coder.of()))
        .thenReturn(future);

    orderedList.readRangeLater(Instant.ofEpochMilli(70), Instant.ofEpochMilli(100));

    final TimestampedValue<String> helloValue =
        TimestampedValue.of("hello", Instant.ofEpochMilli(100));
    final TimestampedValue<String> worldValue =
        TimestampedValue.of("world", Instant.ofEpochMilli(75));
    final TimestampedValue<String> goodbyeValue =
        TimestampedValue.of("goodbye", Instant.ofEpochMilli(50));

    orderedList.add(helloValue);
    waitAndSet(future, Collections.singletonList(worldValue), 200);
    orderedList.add(goodbyeValue);

    assertThat(
        orderedList.readRange(Instant.ofEpochMilli(70), Instant.ofEpochMilli(100)),
        Matchers.contains(worldValue));
  }

  @Test
  public void testOrderedListClearBeforeRead() throws Exception {
    StateTag<OrderedListState<String>> addr =
        StateTags.orderedList("orderedList", StringUtf8Coder.of());
    OrderedListState<String> orderedListState = underTest.state(NAMESPACE, addr);

    final TimestampedValue<String> helloElement =
        TimestampedValue.of("hello", Instant.ofEpochSecond(1));
    orderedListState.clear();
    orderedListState.add(helloElement);
    assertThat(orderedListState.read(), Matchers.containsInAnyOrder(helloElement));
    // Shouldn't need to read from windmill for this.
    Mockito.verifyZeroInteractions(mockReader);

    assertThat(
        orderedListState.readRange(Instant.ofEpochSecond(1), Instant.ofEpochSecond(2)),
        Matchers.containsInAnyOrder(helloElement));
    // Shouldn't need to read from windmill for this.
    Mockito.verifyZeroInteractions(mockReader);

    // Shouldn't need to read from windmill for this.
    assertThat(
        orderedListState.readRange(Instant.ofEpochSecond(100), Instant.ofEpochSecond(200)),
        Matchers.emptyIterable());
    assertThat(
        orderedListState.readRange(Instant.EPOCH, Instant.ofEpochSecond(1)),
        Matchers.emptyIterable());
    Mockito.verifyZeroInteractions(mockReader);
  }

  @Test
  public void testOrderedListIsEmptyFalse() throws Exception {
    StateTag<OrderedListState<String>> addr =
        StateTags.orderedList("orderedList", StringUtf8Coder.of());
    OrderedListState<String> orderedList = underTest.state(NAMESPACE, addr);

    SettableFuture<Iterable<TimestampedValue<String>>> future = SettableFuture.create();
    when(mockReader.orderedListFuture(
            FULL_ORDERED_LIST_RANGE,
            key(NAMESPACE, "orderedList"),
            STATE_FAMILY,
            StringUtf8Coder.of()))
        .thenReturn(future);
    ReadableState<Boolean> result = orderedList.isEmpty().readLater();
    Mockito.verify(mockReader)
        .orderedListFuture(
            FULL_ORDERED_LIST_RANGE,
            key(NAMESPACE, "orderedList"),
            STATE_FAMILY,
            StringUtf8Coder.of());

    waitAndSet(future, Collections.singletonList(TimestampedValue.of("world", Instant.EPOCH)), 200);
    assertThat(result.read(), Matchers.is(false));
  }

  @Test
  public void testOrderedListIsEmptyTrue() throws Exception {
    StateTag<OrderedListState<String>> addr =
        StateTags.orderedList("orderedList", StringUtf8Coder.of());
    OrderedListState<String> orderedList = underTest.state(NAMESPACE, addr);

    SettableFuture<Iterable<TimestampedValue<String>>> future = SettableFuture.create();
    when(mockReader.orderedListFuture(
            FULL_ORDERED_LIST_RANGE,
            key(NAMESPACE, "orderedList"),
            STATE_FAMILY,
            StringUtf8Coder.of()))
        .thenReturn(future);
    ReadableState<Boolean> result = orderedList.isEmpty().readLater();
    Mockito.verify(mockReader)
        .orderedListFuture(
            FULL_ORDERED_LIST_RANGE,
            key(NAMESPACE, "orderedList"),
            STATE_FAMILY,
            StringUtf8Coder.of());

    waitAndSet(future, Collections.emptyList(), 200);
    assertThat(result.read(), Matchers.is(true));
  }

  @Test
  public void testOrderedListIsEmptyAfterClear() throws Exception {
    StateTag<OrderedListState<String>> addr =
        StateTags.orderedList("orderedList", StringUtf8Coder.of());
    OrderedListState<String> orderedList = underTest.state(NAMESPACE, addr);

    orderedList.clear();
    ReadableState<Boolean> result = orderedList.isEmpty();
    Mockito.verify(mockReader, never())
        .orderedListFuture(
            FULL_ORDERED_LIST_RANGE,
            key(NAMESPACE, "orderedList"),
            STATE_FAMILY,
            StringUtf8Coder.of());
    assertThat(result.read(), Matchers.is(true));

    orderedList.add(TimestampedValue.of("hello", Instant.EPOCH));
    assertThat(result.read(), Matchers.is(false));
  }

  @Test
  public void testOrderedListAddPersist() throws Exception {
    StateTag<OrderedListState<String>> addr =
        StateTags.orderedList("orderedList", StringUtf8Coder.of());
    OrderedListState<String> orderedList = underTest.state(NAMESPACE, addr);

    SettableFuture<Map<Range<Instant>, RangeSet<Long>>> orderedListFuture = SettableFuture.create();
    orderedListFuture.set(null);
    SettableFuture<Map<Range<Instant>, RangeSet<Instant>>> deletionsFuture =
        SettableFuture.create();
    deletionsFuture.set(null);
    when(mockReader.valueFuture(
            systemKey(NAMESPACE, "orderedList" + IdTracker.IDS_AVAILABLE_STR),
            STATE_FAMILY,
            IdTracker.IDS_AVAILABLE_CODER))
        .thenReturn(orderedListFuture);
    when(mockReader.valueFuture(
            systemKey(NAMESPACE, "orderedList" + IdTracker.DELETIONS_STR),
            STATE_FAMILY,
            IdTracker.SUBRANGE_DELETIONS_CODER))
        .thenReturn(deletionsFuture);

    orderedList.add(TimestampedValue.of("hello", Instant.ofEpochMilli(1)));

    Windmill.WorkItemCommitRequest.Builder commitBuilder =
        Windmill.WorkItemCommitRequest.newBuilder();
    underTest.persist(commitBuilder);

    assertEquals(1, commitBuilder.getSortedListUpdatesCount());
    TagSortedListUpdateRequest updates = commitBuilder.getSortedListUpdates(0);
    assertEquals(key(NAMESPACE, "orderedList"), updates.getTag());
    assertEquals(1, updates.getInsertsCount());
    assertEquals(1, updates.getInserts(0).getEntriesCount());

    assertEquals("hello", updates.getInserts(0).getEntries(0).getValue().toStringUtf8());
    assertEquals(1000, updates.getInserts(0).getEntries(0).getSortKey());
    assertEquals(IdTracker.NEW_RANGE_MIN_ID, updates.getInserts(0).getEntries(0).getId());
  }

  @Test
  public void testOrderedListClearPersist() throws Exception {
    StateTag<OrderedListState<String>> addr =
        StateTags.orderedList("orderedList", StringUtf8Coder.of());
    OrderedListState<String> orderedListState = underTest.state(NAMESPACE, addr);

    orderedListState.add(TimestampedValue.of("hello", Instant.ofEpochMilli(1)));
    orderedListState.clear();
    orderedListState.add(TimestampedValue.of("world", Instant.ofEpochMilli(2)));
    orderedListState.add(TimestampedValue.of("world", Instant.ofEpochMilli(2)));

    Windmill.WorkItemCommitRequest.Builder commitBuilder =
        Windmill.WorkItemCommitRequest.newBuilder();
    underTest.persist(commitBuilder);

    assertEquals(1, commitBuilder.getSortedListUpdatesCount());
    TagSortedListUpdateRequest updates = commitBuilder.getSortedListUpdates(0);
    assertEquals(STATE_FAMILY, updates.getStateFamily());
    assertEquals(key(NAMESPACE, "orderedList"), updates.getTag());
    assertEquals(1, updates.getInsertsCount());
    assertEquals(2, updates.getInserts(0).getEntriesCount());

    assertEquals("world", updates.getInserts(0).getEntries(0).getValue().toStringUtf8());
    assertEquals("world", updates.getInserts(0).getEntries(1).getValue().toStringUtf8());
    assertEquals(2000, updates.getInserts(0).getEntries(0).getSortKey());
    assertEquals(2000, updates.getInserts(0).getEntries(1).getSortKey());
    assertEquals(IdTracker.NEW_RANGE_MIN_ID, updates.getInserts(0).getEntries(0).getId());
    assertEquals(IdTracker.NEW_RANGE_MIN_ID + 1, updates.getInserts(0).getEntries(1).getId());
    Mockito.verifyNoMoreInteractions(mockReader);
  }

  @Test
  public void testOrderedListDeleteRangePersist() {
    SettableFuture<Map<Range<Instant>, RangeSet<Long>>> orderedListFuture = SettableFuture.create();
    orderedListFuture.set(null);
    SettableFuture<Map<Range<Instant>, RangeSet<Instant>>> deletionsFuture =
        SettableFuture.create();
    deletionsFuture.set(null);
    when(mockReader.valueFuture(
            systemKey(NAMESPACE, "orderedList" + IdTracker.IDS_AVAILABLE_STR),
            STATE_FAMILY,
            IdTracker.IDS_AVAILABLE_CODER))
        .thenReturn(orderedListFuture);
    when(mockReader.valueFuture(
            systemKey(NAMESPACE, "orderedList" + IdTracker.DELETIONS_STR),
            STATE_FAMILY,
            IdTracker.SUBRANGE_DELETIONS_CODER))
        .thenReturn(deletionsFuture);

    StateTag<OrderedListState<String>> addr =
        StateTags.orderedList("orderedList", StringUtf8Coder.of());
    OrderedListState<String> orderedListState = underTest.state(NAMESPACE, addr);

    orderedListState.add(TimestampedValue.of("hello", Instant.ofEpochMilli(1)));
    orderedListState.add(TimestampedValue.of("hello", Instant.ofEpochMilli(2)));
    orderedListState.add(TimestampedValue.of("hello", Instant.ofEpochMilli(2)));
    orderedListState.add(TimestampedValue.of("world", Instant.ofEpochMilli(3)));
    orderedListState.add(TimestampedValue.of("world", Instant.ofEpochMilli(4)));
    orderedListState.clearRange(Instant.ofEpochMilli(2), Instant.ofEpochMilli(4));
    Windmill.WorkItemCommitRequest.Builder commitBuilder =
        Windmill.WorkItemCommitRequest.newBuilder();
    underTest.persist(commitBuilder);

    assertEquals(1, commitBuilder.getSortedListUpdatesCount());
    TagSortedListUpdateRequest updates = commitBuilder.getSortedListUpdates(0);
    assertEquals(STATE_FAMILY, updates.getStateFamily());
    assertEquals(key(NAMESPACE, "orderedList"), updates.getTag());
    assertEquals(1, updates.getInsertsCount());
    assertEquals(2, updates.getInserts(0).getEntriesCount());

    assertEquals("hello", updates.getInserts(0).getEntries(0).getValue().toStringUtf8());
    assertEquals("world", updates.getInserts(0).getEntries(1).getValue().toStringUtf8());
    assertEquals(1000, updates.getInserts(0).getEntries(0).getSortKey());
    assertEquals(4000, updates.getInserts(0).getEntries(1).getSortKey());
    assertEquals(IdTracker.NEW_RANGE_MIN_ID, updates.getInserts(0).getEntries(0).getId());
    assertEquals(IdTracker.NEW_RANGE_MIN_ID + 1, updates.getInserts(0).getEntries(1).getId());
  }

  @Test
  public void testOrderedListMergePendingAdds() {
    SettableFuture<Map<Range<Instant>, RangeSet<Long>>> orderedListFuture = SettableFuture.create();
    orderedListFuture.set(null);
    SettableFuture<Map<Range<Instant>, RangeSet<Instant>>> deletionsFuture =
        SettableFuture.create();
    deletionsFuture.set(null);
    when(mockReader.valueFuture(
            systemKey(NAMESPACE, "orderedList" + IdTracker.IDS_AVAILABLE_STR),
            STATE_FAMILY,
            IdTracker.IDS_AVAILABLE_CODER))
        .thenReturn(orderedListFuture);
    when(mockReader.valueFuture(
            systemKey(NAMESPACE, "orderedList" + IdTracker.DELETIONS_STR),
            STATE_FAMILY,
            IdTracker.SUBRANGE_DELETIONS_CODER))
        .thenReturn(deletionsFuture);

    SettableFuture<Iterable<TimestampedValue<String>>> fromStorage = SettableFuture.create();
    when(mockReader.orderedListFuture(
            FULL_ORDERED_LIST_RANGE,
            key(NAMESPACE, "orderedList"),
            STATE_FAMILY,
            StringUtf8Coder.of()))
        .thenReturn(fromStorage);

    StateTag<OrderedListState<String>> addr =
        StateTags.orderedList("orderedList", StringUtf8Coder.of());
    OrderedListState<String> orderedListState = underTest.state(NAMESPACE, addr);

    orderedListState.add(TimestampedValue.of("second", Instant.ofEpochMilli(1)));
    orderedListState.add(TimestampedValue.of("third", Instant.ofEpochMilli(2)));
    orderedListState.add(TimestampedValue.of("fourth", Instant.ofEpochMilli(2)));
    orderedListState.add(TimestampedValue.of("eighth", Instant.ofEpochMilli(10)));
    orderedListState.add(TimestampedValue.of("ninth", Instant.ofEpochMilli(15)));

    fromStorage.set(
        ImmutableList.of(
            TimestampedValue.of("first", Instant.ofEpochMilli(-1)),
            TimestampedValue.of("fifth", Instant.ofEpochMilli(5)),
            TimestampedValue.of("sixth", Instant.ofEpochMilli(5)),
            TimestampedValue.of("seventh", Instant.ofEpochMilli(5)),
            TimestampedValue.of("tenth", Instant.ofEpochMilli(20))));

    TimestampedValue[] expected =
        Iterables.toArray(
            ImmutableList.of(
                TimestampedValue.of("first", Instant.ofEpochMilli(-1)),
                TimestampedValue.of("second", Instant.ofEpochMilli(1)),
                TimestampedValue.of("third", Instant.ofEpochMilli(2)),
                TimestampedValue.of("fourth", Instant.ofEpochMilli(2)),
                TimestampedValue.of("fifth", Instant.ofEpochMilli(5)),
                TimestampedValue.of("sixth", Instant.ofEpochMilli(5)),
                TimestampedValue.of("seventh", Instant.ofEpochMilli(5)),
                TimestampedValue.of("eighth", Instant.ofEpochMilli(10)),
                TimestampedValue.of("ninth", Instant.ofEpochMilli(15)),
                TimestampedValue.of("tenth", Instant.ofEpochMilli(20))),
            TimestampedValue.class);

    TimestampedValue[] read = Iterables.toArray(orderedListState.read(), TimestampedValue.class);
    assertArrayEquals(expected, read);
  }

  @Test
  public void testOrderedListMergePendingAddsAndDeletes() {
    SettableFuture<Map<Range<Instant>, RangeSet<Long>>> orderedListFuture = SettableFuture.create();
    orderedListFuture.set(null);
    SettableFuture<Map<Range<Instant>, RangeSet<Instant>>> deletionsFuture =
        SettableFuture.create();
    deletionsFuture.set(null);
    when(mockReader.valueFuture(
            systemKey(NAMESPACE, "orderedList" + IdTracker.IDS_AVAILABLE_STR),
            STATE_FAMILY,
            IdTracker.IDS_AVAILABLE_CODER))
        .thenReturn(orderedListFuture);
    when(mockReader.valueFuture(
            systemKey(NAMESPACE, "orderedList" + IdTracker.DELETIONS_STR),
            STATE_FAMILY,
            IdTracker.SUBRANGE_DELETIONS_CODER))
        .thenReturn(deletionsFuture);

    SettableFuture<Iterable<TimestampedValue<String>>> fromStorage = SettableFuture.create();
    when(mockReader.orderedListFuture(
            FULL_ORDERED_LIST_RANGE,
            key(NAMESPACE, "orderedList"),
            STATE_FAMILY,
            StringUtf8Coder.of()))
        .thenReturn(fromStorage);

    StateTag<OrderedListState<String>> addr =
        StateTags.orderedList("orderedList", StringUtf8Coder.of());
    OrderedListState<String> orderedListState = underTest.state(NAMESPACE, addr);

    orderedListState.add(TimestampedValue.of("second", Instant.ofEpochMilli(1)));
    orderedListState.add(TimestampedValue.of("third", Instant.ofEpochMilli(2)));
    orderedListState.add(TimestampedValue.of("fourth", Instant.ofEpochMilli(2)));
    orderedListState.add(TimestampedValue.of("eighth", Instant.ofEpochMilli(10)));
    orderedListState.add(TimestampedValue.of("ninth", Instant.ofEpochMilli(15)));

    orderedListState.clearRange(Instant.ofEpochMilli(2), Instant.ofEpochMilli(5));
    orderedListState.add(TimestampedValue.of("fourth", Instant.ofEpochMilli(4)));

    fromStorage.set(
        ImmutableList.of(
            TimestampedValue.of("first", Instant.ofEpochMilli(-1)),
            TimestampedValue.of("fifth", Instant.ofEpochMilli(5)),
            TimestampedValue.of("sixth", Instant.ofEpochMilli(5)),
            TimestampedValue.of("seventh", Instant.ofEpochMilli(5)),
            TimestampedValue.of("tenth", Instant.ofEpochMilli(20))));

    TimestampedValue[] expected =
        Iterables.toArray(
            ImmutableList.of(
                TimestampedValue.of("first", Instant.ofEpochMilli(-1)),
                TimestampedValue.of("second", Instant.ofEpochMilli(1)),
                TimestampedValue.of("fourth", Instant.ofEpochMilli(4)),
                TimestampedValue.of("fifth", Instant.ofEpochMilli(5)),
                TimestampedValue.of("sixth", Instant.ofEpochMilli(5)),
                TimestampedValue.of("seventh", Instant.ofEpochMilli(5)),
                TimestampedValue.of("eighth", Instant.ofEpochMilli(10)),
                TimestampedValue.of("ninth", Instant.ofEpochMilli(15)),
                TimestampedValue.of("tenth", Instant.ofEpochMilli(20))),
            TimestampedValue.class);

    TimestampedValue[] read = Iterables.toArray(orderedListState.read(), TimestampedValue.class);
    assertArrayEquals(expected, read);
  }

  @Test
  public void testOrderedListInterleavedLocalAddClearReadRange() {
    Future<Map<Range<Instant>, RangeSet<Long>>> orderedListFuture = Futures.immediateFuture(null);
    Future<Map<Range<Instant>, RangeSet<Instant>>> deletionsFuture = Futures.immediateFuture(null);
    when(mockReader.valueFuture(
            systemKey(NAMESPACE, "orderedList" + IdTracker.IDS_AVAILABLE_STR),
            STATE_FAMILY,
            IdTracker.IDS_AVAILABLE_CODER))
        .thenReturn(orderedListFuture);
    when(mockReader.valueFuture(
            systemKey(NAMESPACE, "orderedList" + IdTracker.DELETIONS_STR),
            STATE_FAMILY,
            IdTracker.SUBRANGE_DELETIONS_CODER))
        .thenReturn(deletionsFuture);

    SettableFuture<Iterable<TimestampedValue<String>>> fromStorage = SettableFuture.create();

    Range<Long> readSubrange = Range.closedOpen(1 * 1000000L, 8 * 1000000L);
    when(mockReader.orderedListFuture(
            readSubrange, key(NAMESPACE, "orderedList"), STATE_FAMILY, StringUtf8Coder.of()))
        .thenReturn(fromStorage);

    StateTag<OrderedListState<String>> addr =
        StateTags.orderedList("orderedList", StringUtf8Coder.of());
    OrderedListState<String> orderedListState = underTest.state(NAMESPACE, addr);

    orderedListState.add(TimestampedValue.of("1", Instant.ofEpochSecond(1)));
    orderedListState.add(TimestampedValue.of("2", Instant.ofEpochSecond(2)));
    orderedListState.add(TimestampedValue.of("3", Instant.ofEpochSecond(3)));
    orderedListState.add(TimestampedValue.of("4", Instant.ofEpochSecond(4)));

    orderedListState.clearRange(Instant.ofEpochSecond(1), Instant.ofEpochSecond(4));

    orderedListState.add(TimestampedValue.of("5", Instant.ofEpochSecond(5)));
    orderedListState.add(TimestampedValue.of("6", Instant.ofEpochSecond(6)));

    orderedListState.add(TimestampedValue.of("3_again", Instant.ofEpochSecond(3)));

    orderedListState.add(TimestampedValue.of("7", Instant.ofEpochSecond(7)));
    orderedListState.add(TimestampedValue.of("8", Instant.ofEpochSecond(8)));

    fromStorage.set(ImmutableList.<TimestampedValue<String>>of());

    TimestampedValue[] expected =
        Iterables.toArray(
            ImmutableList.of(
                TimestampedValue.of("3_again", Instant.ofEpochSecond(3)),
                TimestampedValue.of("4", Instant.ofEpochSecond(4)),
                TimestampedValue.of("5", Instant.ofEpochSecond(5)),
                TimestampedValue.of("6", Instant.ofEpochSecond(6)),
                TimestampedValue.of("7", Instant.ofEpochSecond(7))),
            TimestampedValue.class);

    TimestampedValue[] read =
        Iterables.toArray(
            orderedListState.readRange(Instant.ofEpochSecond(1), Instant.ofEpochSecond(8)),
            TimestampedValue.class);
    assertArrayEquals(expected, read);
  }

  @Test
  public void testOrderedListPersistEmpty() throws Exception {
    StateTag<OrderedListState<String>> addr =
        StateTags.orderedList("orderedList", StringUtf8Coder.of());
    OrderedListState<String> orderedListState = underTest.state(NAMESPACE, addr);

    orderedListState.clear();

    Windmill.WorkItemCommitRequest.Builder commitBuilder =
        Windmill.WorkItemCommitRequest.newBuilder();
    underTest.persist(commitBuilder);

    // 1 bag update = the clear
    assertEquals(1, commitBuilder.getSortedListUpdatesCount());
    TagSortedListUpdateRequest updates = commitBuilder.getSortedListUpdates(0);
    assertEquals(1, updates.getDeletesCount());
    assertEquals(WindmillOrderedList.MIN_TS_MICROS, updates.getDeletes(0).getRange().getStart());
    assertEquals(WindmillOrderedList.MAX_TS_MICROS, updates.getDeletes(0).getRange().getLimit());
  }

  @Test
  public void testNewOrderedListNoFetch() throws Exception {
    StateTag<OrderedListState<String>> addr =
        StateTags.orderedList("orderedList", StringUtf8Coder.of());
    OrderedListState<String> orderedList = underTestNewKey.state(NAMESPACE, addr);

    assertThat(orderedList.read(), Matchers.emptyIterable());

    // Shouldn't need to read from windmill for this.
    Mockito.verifyZeroInteractions(mockReader);
  }

  @Test
  public void testBagAddBeforeRead() throws Exception {
    StateTag<BagState<String>> addr = StateTags.bag("bag", StringUtf8Coder.of());
    BagState<String> bag = underTest.state(NAMESPACE, addr);

    SettableFuture<Iterable<String>> future = SettableFuture.create();
    when(mockReader.bagFuture(key(NAMESPACE, "bag"), STATE_FAMILY, StringUtf8Coder.of()))
        .thenReturn(future);

    bag.readLater();

    bag.add("hello");
    waitAndSet(future, Collections.singletonList("world"), 200);
    assertThat(bag.read(), Matchers.containsInAnyOrder("hello", "world"));

    bag.add("goodbye");
    assertThat(bag.read(), Matchers.containsInAnyOrder("hello", "world", "goodbye"));
  }

  // test ordered list cleared before read
  // test fetch + add + read
  // test ids

  @Test
  public void testBagClearBeforeRead() throws Exception {
    StateTag<BagState<String>> addr = StateTags.bag("bag", StringUtf8Coder.of());
    BagState<String> bag = underTest.state(NAMESPACE, addr);

    bag.clear();
    bag.add("hello");
    assertThat(bag.read(), Matchers.containsInAnyOrder("hello"));

    // Shouldn't need to read from windmill for this.
    Mockito.verifyZeroInteractions(mockReader);
  }

  @Test
  public void testBagIsEmptyFalse() throws Exception {
    StateTag<BagState<String>> addr = StateTags.bag("bag", StringUtf8Coder.of());
    BagState<String> bag = underTest.state(NAMESPACE, addr);

    SettableFuture<Iterable<String>> future = SettableFuture.create();
    when(mockReader.bagFuture(key(NAMESPACE, "bag"), STATE_FAMILY, StringUtf8Coder.of()))
        .thenReturn(future);
    ReadableState<Boolean> result = bag.isEmpty().readLater();
    Mockito.verify(mockReader).bagFuture(key(NAMESPACE, "bag"), STATE_FAMILY, StringUtf8Coder.of());

    waitAndSet(future, Collections.singletonList("world"), 200);
    assertThat(result.read(), Matchers.is(false));
  }

  @Test
  public void testBagIsEmptyTrue() throws Exception {
    StateTag<BagState<String>> addr = StateTags.bag("bag", StringUtf8Coder.of());
    BagState<String> bag = underTest.state(NAMESPACE, addr);

    SettableFuture<Iterable<String>> future = SettableFuture.create();
    when(mockReader.bagFuture(key(NAMESPACE, "bag"), STATE_FAMILY, StringUtf8Coder.of()))
        .thenReturn(future);
    ReadableState<Boolean> result = bag.isEmpty().readLater();
    Mockito.verify(mockReader).bagFuture(key(NAMESPACE, "bag"), STATE_FAMILY, StringUtf8Coder.of());

    waitAndSet(future, Collections.emptyList(), 200);
    assertThat(result.read(), Matchers.is(true));
  }

  @Test
  public void testBagIsEmptyAfterClear() throws Exception {
    StateTag<BagState<String>> addr = StateTags.bag("bag", StringUtf8Coder.of());
    BagState<String> bag = underTest.state(NAMESPACE, addr);

    bag.clear();
    ReadableState<Boolean> result = bag.isEmpty();
    Mockito.verify(mockReader, never())
        .bagFuture(key(NAMESPACE, "bag"), STATE_FAMILY, StringUtf8Coder.of());
    assertThat(result.read(), Matchers.is(true));

    bag.add("hello");
    assertThat(result.read(), Matchers.is(false));
  }

  @Test
  public void testBagAddPersist() throws Exception {
    StateTag<BagState<String>> addr = StateTags.bag("bag", StringUtf8Coder.of());
    BagState<String> bag = underTest.state(NAMESPACE, addr);

    bag.add("hello");

    Windmill.WorkItemCommitRequest.Builder commitBuilder =
        Windmill.WorkItemCommitRequest.newBuilder();
    underTest.persist(commitBuilder);

    assertEquals(1, commitBuilder.getBagUpdatesCount());

    TagBag bagUpdates = commitBuilder.getBagUpdates(0);
    assertEquals(key(NAMESPACE, "bag"), bagUpdates.getTag());
    assertEquals(1, bagUpdates.getValuesCount());
    assertEquals("hello", bagUpdates.getValues(0).toStringUtf8());

    Mockito.verifyNoMoreInteractions(mockReader);
  }

  @Test
  public void testBagClearPersist() throws Exception {
    StateTag<BagState<String>> addr = StateTags.bag("bag", StringUtf8Coder.of());
    BagState<String> bag = underTest.state(NAMESPACE, addr);

    bag.add("hello");
    bag.clear();
    bag.add("world");

    Windmill.WorkItemCommitRequest.Builder commitBuilder =
        Windmill.WorkItemCommitRequest.newBuilder();
    underTest.persist(commitBuilder);

    assertEquals(1, commitBuilder.getBagUpdatesCount());

    TagBag tagBag = commitBuilder.getBagUpdates(0);
    assertEquals(key(NAMESPACE, "bag"), tagBag.getTag());
    assertEquals(STATE_FAMILY, tagBag.getStateFamily());
    assertTrue(tagBag.getDeleteAll());
    assertEquals(1, tagBag.getValuesCount());
    assertEquals("world", tagBag.getValues(0).toStringUtf8());

    Mockito.verifyNoMoreInteractions(mockReader);
  }

  @Test
  public void testBagPersistEmpty() throws Exception {
    StateTag<BagState<String>> addr = StateTags.bag("bag", StringUtf8Coder.of());
    BagState<String> bag = underTest.state(NAMESPACE, addr);

    bag.clear();

    Windmill.WorkItemCommitRequest.Builder commitBuilder =
        Windmill.WorkItemCommitRequest.newBuilder();
    underTest.persist(commitBuilder);

    // 1 bag update = the clear
    assertEquals(1, commitBuilder.getBagUpdatesCount());
  }

  @Test
  public void testNewBagNoFetch() throws Exception {
    StateTag<BagState<String>> addr = StateTags.bag("bag", StringUtf8Coder.of());
    BagState<String> bag = underTestNewKey.state(NAMESPACE, addr);

    assertThat(bag.read(), Matchers.emptyIterable());

    // Shouldn't need to read from windmill for this.
    Mockito.verifyZeroInteractions(mockReader);
  }

  @Test
  @SuppressWarnings("ArraysAsListPrimitiveArray")
  public void testCombiningAddBeforeRead() throws Exception {
    GroupingState<Integer, Integer> value = underTest.state(NAMESPACE, COMBINING_ADDR);

    SettableFuture<Iterable<int[]>> future = SettableFuture.create();
    when(mockReader.bagFuture(eq(COMBINING_KEY), eq(STATE_FAMILY), Mockito.<Coder<int[]>>any()))
        .thenReturn(future);

    value.readLater();

    value.add(5);
    value.add(6);
    waitAndSet(future, Arrays.asList(new int[] {8}, new int[] {10}), 200);
    assertThat(value.read(), Matchers.equalTo(29));

    // That get "compressed" the combiner. So, the underlying future should change:
    future.set(Collections.singletonList(new int[] {29}));

    value.add(2);
    assertThat(value.read(), Matchers.equalTo(31));
  }

  @Test
  public void testCombiningClearBeforeRead() throws Exception {
    GroupingState<Integer, Integer> value = underTest.state(NAMESPACE, COMBINING_ADDR);

    value.clear();
    value.readLater();

    value.add(5);
    value.add(6);
    assertThat(value.read(), Matchers.equalTo(11));

    value.add(2);
    assertThat(value.read(), Matchers.equalTo(13));

    // Shouldn't need to read from windmill for this because we immediately cleared..
    Mockito.verifyZeroInteractions(mockReader);
  }

  @Test
  @SuppressWarnings("ArraysAsListPrimitiveArray")
  public void testCombiningIsEmpty() throws Exception {
    GroupingState<Integer, Integer> value = underTest.state(NAMESPACE, COMBINING_ADDR);

    SettableFuture<Iterable<int[]>> future = SettableFuture.create();
    when(mockReader.bagFuture(eq(COMBINING_KEY), eq(STATE_FAMILY), Mockito.<Coder<int[]>>any()))
        .thenReturn(future);
    ReadableState<Boolean> result = value.isEmpty().readLater();
    ArgumentCaptor<ByteString> byteString = ArgumentCaptor.forClass(ByteString.class);

    // Note that we do expect the third argument - the coder - to be equal to accumCoder, but that
    // is possibly overspecified and currently trips an issue in the SDK where identical coders are
    // not #equals().
    //
    // What matters is that a future is created, hence a Windmill RPC sent.
    Mockito.verify(mockReader)
        .bagFuture(byteString.capture(), eq(STATE_FAMILY), Mockito.<Coder<int[]>>any());
    assertThat(byteString.getValue(), byteStringEq(COMBINING_KEY));

    waitAndSet(future, Collections.singletonList(new int[] {29}), 200);
    assertThat(result.read(), Matchers.is(false));
  }

  @Test
  public void testCombiningIsEmptyAfterClear() throws Exception {
    GroupingState<Integer, Integer> value = underTest.state(NAMESPACE, COMBINING_ADDR);

    value.clear();
    ReadableState<Boolean> result = value.isEmpty();
    Mockito.verify(mockReader, never()).bagFuture(COMBINING_KEY, STATE_FAMILY, accumCoder);
    assertThat(result.read(), Matchers.is(true));

    value.add(87);
    assertThat(result.read(), Matchers.is(false));
  }

  @Test
  public void testCombiningAddPersist() throws Exception {
    disableCompactOnWrite();

    GroupingState<Integer, Integer> value = underTest.state(NAMESPACE, COMBINING_ADDR);

    value.add(5);
    value.add(6);

    Windmill.WorkItemCommitRequest.Builder commitBuilder =
        Windmill.WorkItemCommitRequest.newBuilder();
    underTest.persist(commitBuilder);

    assertEquals(1, commitBuilder.getBagUpdatesCount());

    TagBag bagUpdates = commitBuilder.getBagUpdates(0);
    assertEquals(COMBINING_KEY, bagUpdates.getTag());
    assertEquals(1, bagUpdates.getValuesCount());
    assertEquals(
        11, CoderUtils.decodeFromByteArray(accumCoder, bagUpdates.getValues(0).toByteArray())[0]);

    Mockito.verifyNoMoreInteractions(mockReader);
  }

  @Test
  public void testCombiningAddPersistWithCompact() throws Exception {
    forceCompactOnWrite();

    Mockito.when(
            mockReader.bagFuture(
                org.mockito.Matchers.any(),
                org.mockito.Matchers.any(),
                org.mockito.Matchers.<Coder<int[]>>any()))
        .thenReturn(Futures.immediateFuture(ImmutableList.of(new int[] {40}, new int[] {60})));

    GroupingState<Integer, Integer> value = underTest.state(NAMESPACE, COMBINING_ADDR);

    value.add(5);
    value.add(6);

    Windmill.WorkItemCommitRequest.Builder commitBuilder =
        Windmill.WorkItemCommitRequest.newBuilder();
    underTest.persist(commitBuilder);

    assertEquals(1, commitBuilder.getBagUpdatesCount());
    TagBag bagUpdates = commitBuilder.getBagUpdates(0);
    assertEquals(COMBINING_KEY, bagUpdates.getTag());
    assertEquals(1, bagUpdates.getValuesCount());
    assertTrue(bagUpdates.getDeleteAll());
    assertEquals(
        111, CoderUtils.decodeFromByteArray(accumCoder, bagUpdates.getValues(0).toByteArray())[0]);
  }

  @Test
  public void testCombiningClearPersist() throws Exception {
    disableCompactOnWrite();

    GroupingState<Integer, Integer> value = underTest.state(NAMESPACE, COMBINING_ADDR);

    value.clear();
    value.add(5);
    value.add(6);

    Windmill.WorkItemCommitRequest.Builder commitBuilder =
        Windmill.WorkItemCommitRequest.newBuilder();
    underTest.persist(commitBuilder);

    assertEquals(1, commitBuilder.getBagUpdatesCount());

    TagBag tagBag = commitBuilder.getBagUpdates(0);
    assertEquals(COMBINING_KEY, tagBag.getTag());
    assertEquals(STATE_FAMILY, tagBag.getStateFamily());
    assertTrue(tagBag.getDeleteAll());
    assertEquals(1, tagBag.getValuesCount());
    assertEquals(
        11, CoderUtils.decodeFromByteArray(accumCoder, tagBag.getValues(0).toByteArray())[0]);

    Mockito.verifyNoMoreInteractions(mockReader);
  }

  @Test
  public void testNewCombiningNoFetch() throws Exception {
    GroupingState<Integer, Integer> value = underTestNewKey.state(NAMESPACE, COMBINING_ADDR);

    assertThat(value.isEmpty().read(), Matchers.is(true));
    assertThat(value.read(), Matchers.is(Sum.ofIntegers().identity()));
    assertThat(value.isEmpty().read(), Matchers.is(false));

    // Shouldn't need to read from windmill for this.
    Mockito.verifyZeroInteractions(mockReader);
  }

  @Test
  public void testWatermarkAddBeforeReadEarliest() throws Exception {
    StateTag<WatermarkHoldState> addr =
        StateTags.watermarkStateInternal("watermark", TimestampCombiner.EARLIEST);
    WatermarkHoldState bag = underTest.state(NAMESPACE, addr);

    SettableFuture<Instant> future = SettableFuture.create();
    when(mockReader.watermarkFuture(key(NAMESPACE, "watermark"), STATE_FAMILY)).thenReturn(future);

    bag.readLater();

    bag.add(new Instant(3000));
    waitAndSet(future, new Instant(2000), 200);
    assertThat(bag.read(), Matchers.equalTo(new Instant(2000)));

    Mockito.verify(mockReader, times(2)).watermarkFuture(key(NAMESPACE, "watermark"), STATE_FAMILY);
    Mockito.verifyNoMoreInteractions(mockReader);

    // Adding another value doesn't create another future, but does update the result.
    bag.add(new Instant(1000));
    assertThat(bag.read(), Matchers.equalTo(new Instant(1000)));
    Mockito.verifyNoMoreInteractions(mockReader);
  }

  @Test
  public void testWatermarkAddBeforeReadLatest() throws Exception {
    StateTag<WatermarkHoldState> addr =
        StateTags.watermarkStateInternal("watermark", TimestampCombiner.LATEST);
    WatermarkHoldState bag = underTest.state(NAMESPACE, addr);

    SettableFuture<Instant> future = SettableFuture.create();
    when(mockReader.watermarkFuture(key(NAMESPACE, "watermark"), STATE_FAMILY)).thenReturn(future);

    // Suggesting we will read it later should get a future from the underlying WindmillStateReader
    bag.readLater();

    // Actually reading it will request another future, and get the same one, from
    // WindmillStateReader
    bag.add(new Instant(3000));
    waitAndSet(future, new Instant(2000), 200);
    assertThat(bag.read(), Matchers.equalTo(new Instant(3000)));

    Mockito.verify(mockReader, times(2)).watermarkFuture(key(NAMESPACE, "watermark"), STATE_FAMILY);
    Mockito.verifyNoMoreInteractions(mockReader);

    // Adding another value doesn't create another future, but does update the result.
    bag.add(new Instant(3000));
    assertThat(bag.read(), Matchers.equalTo(new Instant(3000)));
    Mockito.verifyNoMoreInteractions(mockReader);
  }

  @Test
  public void testWatermarkAddBeforeReadEndOfWindow() throws Exception {
    StateTag<WatermarkHoldState> addr =
        StateTags.watermarkStateInternal("watermark", TimestampCombiner.END_OF_WINDOW);
    WatermarkHoldState bag = underTest.state(NAMESPACE, addr);

    SettableFuture<Instant> future = SettableFuture.create();
    when(mockReader.watermarkFuture(key(NAMESPACE, "watermark"), STATE_FAMILY)).thenReturn(future);

    // Requests a future once
    bag.readLater();

    bag.add(new Instant(3000));
    waitAndSet(future, new Instant(3000), 200);
    // read() requests a future again, receiving the same one
    assertThat(bag.read(), Matchers.equalTo(new Instant(3000)));

    Mockito.verify(mockReader, times(2)).watermarkFuture(key(NAMESPACE, "watermark"), STATE_FAMILY);
    Mockito.verifyNoMoreInteractions(mockReader);

    // Adding another value doesn't create another future, but does update the result.
    bag.add(new Instant(3000));
    assertThat(bag.read(), Matchers.equalTo(new Instant(3000)));
    Mockito.verifyNoMoreInteractions(mockReader);
  }

  @Test
  public void testWatermarkClearBeforeRead() throws Exception {
    StateTag<WatermarkHoldState> addr =
        StateTags.watermarkStateInternal("watermark", TimestampCombiner.EARLIEST);

    WatermarkHoldState bag = underTest.state(NAMESPACE, addr);

    bag.clear();
    assertThat(bag.read(), Matchers.nullValue());

    bag.add(new Instant(300));
    assertThat(bag.read(), Matchers.equalTo(new Instant(300)));

    // Shouldn't need to read from windmill because the value is already available.
    Mockito.verifyNoMoreInteractions(mockReader);
  }

  @Test
  public void testWatermarkPersistEarliest() throws Exception {
    StateTag<WatermarkHoldState> addr =
        StateTags.watermarkStateInternal("watermark", TimestampCombiner.EARLIEST);
    WatermarkHoldState bag = underTest.state(NAMESPACE, addr);

    bag.add(new Instant(1000));
    bag.add(new Instant(2000));

    Windmill.WorkItemCommitRequest.Builder commitBuilder =
        Windmill.WorkItemCommitRequest.newBuilder();
    underTest.persist(commitBuilder);

    assertEquals(1, commitBuilder.getWatermarkHoldsCount());

    Windmill.WatermarkHold watermarkHold = commitBuilder.getWatermarkHolds(0);
    assertEquals(key(NAMESPACE, "watermark"), watermarkHold.getTag());
    assertEquals(TimeUnit.MILLISECONDS.toMicros(1000), watermarkHold.getTimestamps(0));

    Mockito.verifyNoMoreInteractions(mockReader);
  }

  @Test
  public void testWatermarkPersistLatestEmpty() throws Exception {
    StateTag<WatermarkHoldState> addr =
        StateTags.watermarkStateInternal("watermark", TimestampCombiner.LATEST);
    WatermarkHoldState hold = underTest.state(NAMESPACE, addr);

    hold.add(new Instant(1000));
    hold.add(new Instant(2000));

    when(mockReader.watermarkFuture(key(NAMESPACE, "watermark"), STATE_FAMILY))
        .thenReturn(Futures.immediateFuture(null));

    Windmill.WorkItemCommitRequest.Builder commitBuilder =
        Windmill.WorkItemCommitRequest.newBuilder();
    underTest.persist(commitBuilder);

    assertEquals(1, commitBuilder.getWatermarkHoldsCount());

    Windmill.WatermarkHold watermarkHold = commitBuilder.getWatermarkHolds(0);
    assertEquals(key(NAMESPACE, "watermark"), watermarkHold.getTag());
    assertEquals(TimeUnit.MILLISECONDS.toMicros(2000), watermarkHold.getTimestamps(0));

    Mockito.verify(mockReader).watermarkFuture(key(NAMESPACE, "watermark"), STATE_FAMILY);
    Mockito.verifyNoMoreInteractions(mockReader);
  }

  @Test
  public void testWatermarkPersistLatestWindmillWins() throws Exception {
    StateTag<WatermarkHoldState> addr =
        StateTags.watermarkStateInternal("watermark", TimestampCombiner.LATEST);
    WatermarkHoldState hold = underTest.state(NAMESPACE, addr);

    hold.add(new Instant(1000));
    hold.add(new Instant(2000));

    when(mockReader.watermarkFuture(key(NAMESPACE, "watermark"), STATE_FAMILY))
        .thenReturn(Futures.immediateFuture(new Instant(4000)));

    Windmill.WorkItemCommitRequest.Builder commitBuilder =
        Windmill.WorkItemCommitRequest.newBuilder();
    underTest.persist(commitBuilder);

    assertEquals(1, commitBuilder.getWatermarkHoldsCount());

    Windmill.WatermarkHold watermarkHold = commitBuilder.getWatermarkHolds(0);
    assertEquals(key(NAMESPACE, "watermark"), watermarkHold.getTag());
    assertEquals(TimeUnit.MILLISECONDS.toMicros(4000), watermarkHold.getTimestamps(0));

    Mockito.verify(mockReader).watermarkFuture(key(NAMESPACE, "watermark"), STATE_FAMILY);
    Mockito.verifyNoMoreInteractions(mockReader);
  }

  @Test
  public void testWatermarkPersistLatestLocalAdditionsWin() throws Exception {
    StateTag<WatermarkHoldState> addr =
        StateTags.watermarkStateInternal("watermark", TimestampCombiner.LATEST);
    WatermarkHoldState hold = underTest.state(NAMESPACE, addr);

    hold.add(new Instant(1000));
    hold.add(new Instant(2000));

    when(mockReader.watermarkFuture(key(NAMESPACE, "watermark"), STATE_FAMILY))
        .thenReturn(Futures.immediateFuture(new Instant(500)));

    Windmill.WorkItemCommitRequest.Builder commitBuilder =
        Windmill.WorkItemCommitRequest.newBuilder();
    underTest.persist(commitBuilder);

    assertEquals(1, commitBuilder.getWatermarkHoldsCount());

    Windmill.WatermarkHold watermarkHold = commitBuilder.getWatermarkHolds(0);
    assertEquals(key(NAMESPACE, "watermark"), watermarkHold.getTag());
    assertEquals(TimeUnit.MILLISECONDS.toMicros(2000), watermarkHold.getTimestamps(0));

    Mockito.verify(mockReader).watermarkFuture(key(NAMESPACE, "watermark"), STATE_FAMILY);
    Mockito.verifyNoMoreInteractions(mockReader);
  }

  @Test
  public void testWatermarkPersistEndOfWindow() throws Exception {
    StateTag<WatermarkHoldState> addr =
        StateTags.watermarkStateInternal("watermark", TimestampCombiner.END_OF_WINDOW);
    WatermarkHoldState hold = underTest.state(NAMESPACE, addr);

    hold.add(new Instant(2000));
    hold.add(new Instant(2000));

    Windmill.WorkItemCommitRequest.Builder commitBuilder =
        Windmill.WorkItemCommitRequest.newBuilder();
    underTest.persist(commitBuilder);

    assertEquals(1, commitBuilder.getWatermarkHoldsCount());

    Windmill.WatermarkHold watermarkHold = commitBuilder.getWatermarkHolds(0);
    assertEquals(key(NAMESPACE, "watermark"), watermarkHold.getTag());
    assertEquals(TimeUnit.MILLISECONDS.toMicros(2000), watermarkHold.getTimestamps(0));

    // Blind adds should not need to read the future.
    Mockito.verifyNoMoreInteractions(mockReader);
  }

  @Test
  public void testWatermarkClearPersist() throws Exception {
    StateTag<WatermarkHoldState> addr =
        StateTags.watermarkStateInternal("watermark", TimestampCombiner.EARLIEST);
    WatermarkHoldState hold = underTest.state(NAMESPACE, addr);

    hold.add(new Instant(500));
    hold.clear();
    hold.add(new Instant(1000));
    hold.add(new Instant(2000));

    Windmill.WorkItemCommitRequest.Builder commitBuilder =
        Windmill.WorkItemCommitRequest.newBuilder();
    underTest.persist(commitBuilder);

    assertEquals(1, commitBuilder.getWatermarkHoldsCount());

    Windmill.WatermarkHold clearAndUpdate = commitBuilder.getWatermarkHolds(0);
    assertEquals(key(NAMESPACE, "watermark"), clearAndUpdate.getTag());
    assertEquals(1, clearAndUpdate.getTimestampsCount());
    assertEquals(TimeUnit.MILLISECONDS.toMicros(1000), clearAndUpdate.getTimestamps(0));

    Mockito.verifyNoMoreInteractions(mockReader);
  }

  @Test
  public void testWatermarkPersistEmpty() throws Exception {
    StateTag<WatermarkHoldState> addr =
        StateTags.watermarkStateInternal("watermark", TimestampCombiner.EARLIEST);
    WatermarkHoldState bag = underTest.state(NAMESPACE, addr);

    bag.add(new Instant(500));
    bag.clear();

    Windmill.WorkItemCommitRequest.Builder commitBuilder =
        Windmill.WorkItemCommitRequest.newBuilder();
    underTest.persist(commitBuilder);

    // 1 bag update corresponds to deletion. There shouldn't be a bag update adding items.
    assertEquals(1, commitBuilder.getWatermarkHoldsCount());
  }

  @Test
  public void testNewWatermarkNoFetch() throws Exception {
    StateTag<WatermarkHoldState> addr =
        StateTags.watermarkStateInternal("watermark", TimestampCombiner.EARLIEST);

    WatermarkHoldState bag = underTestNewKey.state(NAMESPACE, addr);
    assertThat(bag.read(), Matchers.nullValue());

    // Shouldn't need to read from windmill for this.
    Mockito.verifyZeroInteractions(mockReader);
  }

  @Test
  public void testValueSetBeforeRead() throws Exception {
    StateTag<ValueState<String>> addr = StateTags.value("value", StringUtf8Coder.of());
    ValueState<String> value = underTest.state(NAMESPACE, addr);

    value.write("Hello");

    assertEquals("Hello", value.read());
    Mockito.verifyNoMoreInteractions(mockReader);
  }

  @Test
  public void testValueClearBeforeRead() throws Exception {
    StateTag<ValueState<String>> addr = StateTags.value("value", StringUtf8Coder.of());
    ValueState<String> value = underTest.state(NAMESPACE, addr);

    value.clear();

    assertNull(value.read());
    Mockito.verifyNoMoreInteractions(mockReader);
  }

  @Test
  public void testValueRead() throws Exception {
    StateTag<ValueState<String>> addr = StateTags.value("value", StringUtf8Coder.of());
    ValueState<String> value = underTest.state(NAMESPACE, addr);

    SettableFuture<String> future = SettableFuture.create();
    when(mockReader.valueFuture(key(NAMESPACE, "value"), STATE_FAMILY, StringUtf8Coder.of()))
        .thenReturn(future);
    waitAndSet(future, "World", 200);

    assertEquals("World", value.read());
  }

  @Test
  public void testValueSetPersist() throws Exception {
    StateTag<ValueState<String>> addr = StateTags.value("value", StringUtf8Coder.of());
    ValueState<String> value = underTest.state(NAMESPACE, addr);

    value.write("Hi");

    Windmill.WorkItemCommitRequest.Builder commitBuilder =
        Windmill.WorkItemCommitRequest.newBuilder();
    underTest.persist(commitBuilder);

    assertEquals(1, commitBuilder.getValueUpdatesCount());
    TagValue valueUpdate = commitBuilder.getValueUpdates(0);
    assertEquals(key(NAMESPACE, "value"), valueUpdate.getTag());
    assertEquals("Hi", valueUpdate.getValue().getData().toStringUtf8());
    assertTrue(valueUpdate.isInitialized());

    Mockito.verifyNoMoreInteractions(mockReader);
  }

  @Test
  public void testValueClearPersist() throws Exception {
    StateTag<ValueState<String>> addr = StateTags.value("value", StringUtf8Coder.of());
    ValueState<String> value = underTest.state(NAMESPACE, addr);

    value.write("Hi");
    value.clear();

    Windmill.WorkItemCommitRequest.Builder commitBuilder =
        Windmill.WorkItemCommitRequest.newBuilder();
    underTest.persist(commitBuilder);

    assertEquals(1, commitBuilder.getValueUpdatesCount());
    TagValue valueUpdate = commitBuilder.getValueUpdates(0);
    assertEquals(key(NAMESPACE, "value"), valueUpdate.getTag());
    assertEquals(0, valueUpdate.getValue().getData().size());

    Mockito.verifyNoMoreInteractions(mockReader);
  }

  @Test
  public void testValueNoChangePersist() throws Exception {
    StateTag<ValueState<String>> addr = StateTags.value("value", StringUtf8Coder.of());
    underTest.state(NAMESPACE, addr);

    Windmill.WorkItemCommitRequest.Builder commitBuilder =
        Windmill.WorkItemCommitRequest.newBuilder();
    underTest.persist(commitBuilder);

    assertEquals(0, commitBuilder.getValueUpdatesCount());

    Mockito.verifyNoMoreInteractions(mockReader);
  }

  @Test
  public void testNewValueNoFetch() throws Exception {
    StateTag<ValueState<String>> addr = StateTags.value("value", StringUtf8Coder.of());
    ValueState<String> value = underTestNewKey.state(NAMESPACE, addr);

    assertNull(value.read());

    // Shouldn't need to read from windmill for this.
    Mockito.verifyZeroInteractions(mockReader);
  }

  @Test
  public void testCachedValue() throws Exception {
    StateTag<ValueState<String>> addr = StateTags.value("value", StringUtf8Coder.of());
    ValueState<String> value = underTest.state(NAMESPACE, addr);

    assertEquals(0, cache.getWeight());

    value.write("Hi");
    underTest.persist(Windmill.WorkItemCommitRequest.newBuilder());

    assertEquals(221, cache.getWeight());

    resetUnderTest();
    value = underTest.state(NAMESPACE, addr);
    assertEquals("Hi", value.read());
    value.clear();
    underTest.persist(Windmill.WorkItemCommitRequest.newBuilder());

    assertEquals(219, cache.getWeight());

    resetUnderTest();
    value = underTest.state(NAMESPACE, addr);
    assertNull(value.read());
    underTest.persist(Windmill.WorkItemCommitRequest.newBuilder());

    Mockito.verifyNoMoreInteractions(mockReader);
  }

  @Test
  public void testCachedBag() throws Exception {
    StateTag<BagState<String>> addr = StateTags.bag("bag", StringUtf8Coder.of());
    BagState<String> bag = underTest.state(NAMESPACE, addr);

    assertEquals(0, cache.getWeight());

    SettableFuture<Iterable<String>> future = SettableFuture.create();
    when(mockReader.bagFuture(key(NAMESPACE, "bag"), STATE_FAMILY, StringUtf8Coder.of()))
        .thenReturn(future);

    bag.readLater();

    assertEquals(0, cache.getWeight());

    bag.add("hello");
    waitAndSet(future, weightedList("world"), 200);
    Iterable<String> readResult1 = bag.read();
    assertThat(readResult1, Matchers.containsInAnyOrder("hello", "world"));

    underTest.persist(Windmill.WorkItemCommitRequest.newBuilder());

    assertEquals(227, cache.getWeight());

    resetUnderTest();
    bag = underTest.state(NAMESPACE, addr);
    bag.add("goodbye");

    // Make sure that cached iterables have not changed after persist+add.
    assertThat(readResult1, Matchers.containsInAnyOrder("hello", "world"));

    Iterable<String> readResult2 = bag.read();
    assertThat(readResult2, Matchers.containsInAnyOrder("hello", "world", "goodbye"));
    bag.clear();
    // Make sure that cached iterables have not changed after clear.
    assertThat(readResult2, Matchers.containsInAnyOrder("hello", "world", "goodbye"));
    bag.add("new");
    // Make sure that cached iterables have not changed after clear+add.
    assertThat(readResult2, Matchers.containsInAnyOrder("hello", "world", "goodbye"));

    underTest.persist(Windmill.WorkItemCommitRequest.newBuilder());

    assertEquals(220, cache.getWeight());

    resetUnderTest();
    bag = underTest.state(NAMESPACE, addr);
    bag.add("new2");
    assertThat(bag.read(), Matchers.containsInAnyOrder("new", "new2"));
    bag.clear();
    bag.add("new3");

    underTest.persist(Windmill.WorkItemCommitRequest.newBuilder());

    assertEquals(221, cache.getWeight());

    resetUnderTest();
    bag = underTest.state(NAMESPACE, addr);
    assertThat(bag.read(), Matchers.containsInAnyOrder("new3"));
    underTest.persist(Windmill.WorkItemCommitRequest.newBuilder());

    Mockito.verify(mockReader, times(2))
        .bagFuture(key(NAMESPACE, "bag"), STATE_FAMILY, StringUtf8Coder.of());
    Mockito.verifyNoMoreInteractions(mockReader);
  }

  @Test
  public void testCachedWatermarkHold() throws Exception {
    StateTag<WatermarkHoldState> addr =
        StateTags.watermarkStateInternal("watermark", TimestampCombiner.EARLIEST);
    WatermarkHoldState hold = underTest.state(NAMESPACE, addr);

    SettableFuture<Instant> future = SettableFuture.create();
    when(mockReader.watermarkFuture(key(NAMESPACE, "watermark"), STATE_FAMILY)).thenReturn(future);

    assertEquals(0, cache.getWeight());

    hold.readLater();

    hold.add(new Instant(3000));
    waitAndSet(future, new Instant(2000), 200);
    assertThat(hold.read(), Matchers.equalTo(new Instant(2000)));

    underTest.persist(Windmill.WorkItemCommitRequest.newBuilder());

    assertEquals(231, cache.getWeight());

    resetUnderTest();
    hold = underTest.state(NAMESPACE, addr);
    assertThat(hold.read(), Matchers.equalTo(new Instant(2000)));
    hold.clear();

    underTest.persist(Windmill.WorkItemCommitRequest.newBuilder());

    assertEquals(231, cache.getWeight());

    resetUnderTest();
    hold = underTest.state(NAMESPACE, addr);
    assertNull(hold.read());
    underTest.persist(Windmill.WorkItemCommitRequest.newBuilder());

    Mockito.verify(mockReader, times(2)).watermarkFuture(key(NAMESPACE, "watermark"), STATE_FAMILY);
    Mockito.verifyNoMoreInteractions(mockReader);
  }

  @Test
  @SuppressWarnings("ArraysAsListPrimitiveArray")
  public void testCachedCombining() throws Exception {
    GroupingState<Integer, Integer> value = underTest.state(NAMESPACE, COMBINING_ADDR);

    SettableFuture<Iterable<int[]>> future = SettableFuture.create();
    when(mockReader.bagFuture(
            eq(key(NAMESPACE, "combining")), eq(STATE_FAMILY), Mockito.<Coder<int[]>>any()))
        .thenReturn(future);

    assertEquals(0, cache.getWeight());

    value.readLater();

    value.add(1);
    waitAndSet(future, Collections.singletonList(new int[] {2}), 200);
    assertThat(value.read(), Matchers.equalTo(3));

    underTest.persist(Windmill.WorkItemCommitRequest.newBuilder());

    assertEquals(224, cache.getWeight());

    resetUnderTest();
    value = underTest.state(NAMESPACE, COMBINING_ADDR);
    assertThat(value.read(), Matchers.equalTo(3));
    value.add(3);
    assertThat(value.read(), Matchers.equalTo(6));
    value.clear();

    underTest.persist(Windmill.WorkItemCommitRequest.newBuilder());

    assertEquals(223, cache.getWeight());

    resetUnderTest();
    value = underTest.state(NAMESPACE, COMBINING_ADDR);
    assertThat(value.read(), Matchers.equalTo(0));
    underTest.persist(Windmill.WorkItemCommitRequest.newBuilder());

    // Note that we do expect the third argument - the coder - to be equal to accumCoder, but that
    // is possibly overspecified and currently trips an issue in the SDK where identical coders are
    // not #equals().
    //
    // What matters is the number of futures created, hence Windmill RPCs.
    Mockito.verify(mockReader, times(2))
        .bagFuture(eq(key(NAMESPACE, "combining")), eq(STATE_FAMILY), Mockito.<Coder<int[]>>any());
    Mockito.verifyNoMoreInteractions(mockReader);
  }

  private void disableCompactOnWrite() {
    WindmillStateInternals.COMPACT_NOW.set(() -> false);
  }

  private void forceCompactOnWrite() {
    WindmillStateInternals.COMPACT_NOW.set(() -> true);
  }

  private static class MultimapEntryUpdate {
    String key;
    Iterable<Integer> values;
    boolean deleteAll;

    public MultimapEntryUpdate(String key, Iterable<Integer> values, boolean deleteAll) {
      this.key = key;
      this.values = values;
      this.deleteAll = deleteAll;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (!(o instanceof MultimapEntryUpdate)) return false;
      MultimapEntryUpdate that = (MultimapEntryUpdate) o;
      return deleteAll == that.deleteAll
          && Objects.equals(key, that.key)
          && Objects.equals(values, that.values);
    }

    @Override
    public int hashCode() {
      return Objects.hash(key, values, deleteAll);
    }
  }
}
