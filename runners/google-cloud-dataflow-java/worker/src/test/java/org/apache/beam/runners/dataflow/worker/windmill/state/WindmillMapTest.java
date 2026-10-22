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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

import java.io.Closeable;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.annotation.Nullable;
import org.apache.beam.runners.core.StateNamespace;
import org.apache.beam.runners.core.StateNamespaceForTest;
import org.apache.beam.runners.core.StateTag;
import org.apache.beam.runners.core.StateTags;
import org.apache.beam.runners.dataflow.options.DataflowWorkerHarnessOptions;
import org.apache.beam.runners.dataflow.worker.WindmillComputationKey;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill;
import org.apache.beam.sdk.coders.ByteArrayCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.Coder.Context;
import org.apache.beam.sdk.coders.NullableCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.state.MapState;
import org.apache.beam.sdk.state.ReadableState;
import org.apache.beam.sdk.util.ByteStringOutputStream;
import org.apache.beam.vendor.grpc.v1p69p0.com.google.protobuf.ByteString;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Supplier;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.util.concurrent.Futures;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.util.concurrent.SettableFuture;
import org.hamcrest.Matchers;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

/** Unit tests demonstrating correctness issues and bugs in {@link WindmillMap}. */
@RunWith(JUnit4.class)
public class WindmillMapTest {
  @Rule public transient Timeout globalTimeout = Timeout.seconds(600);

  private static final String STATE_FAMILY = "family";
  private static final StateNamespace NAMESPACE = new StateNamespaceForTest("ns");

  @Mock private WindmillStateReader mockReader;
  @Mock private Supplier<Closeable> readStateSupplier;

  private WindmillStateCache cache;
  private WindmillStateInternals<String> underTest;

  private static ByteString key(StateNamespace namespace, String addrId) {
    return ByteString.copyFromUtf8(namespace.stringKey() + "+u" + addrId);
  }

  private <K> ByteString protoKey(String tagId, @Nullable K tag, Coder<K> keyCoder)
      throws IOException {
    ByteStringOutputStream keyStream = new ByteStringOutputStream();
    key(NAMESPACE, tagId).writeTo(keyStream);
    if (tag != null) {
      keyCoder.encode(tag, keyStream, Context.OUTER);
    }
    return keyStream.toByteString();
  }

  private <K> ByteString protoKey(@Nullable K tag, Coder<K> keyCoder) throws IOException {
    return protoKey("map", tag, keyCoder);
  }

  private static <T> ByteString encodeValue(T value, Coder<T> coder) throws IOException {
    ByteStringOutputStream out = new ByteStringOutputStream();
    coder.encode(value, out, Context.OUTER);
    return out.toByteString();
  }

  private static byte[] dup(byte[] key) {
    byte[] res = new byte[key.length];
    System.arraycopy(key, 0, res, 0, key.length);
    return res;
  }

  @SafeVarargs
  private static <T> List<T> weightedList(T... entries) {
    WeightedList<T> list = new WeightedList<>(new ArrayList<>());
    for (T entry : entries) {
      list.addWeighted(entry, 1);
    }
    return list;
  }

  @Before
  public void setUp() {
    MockitoAnnotations.initMocks(this);
    DataflowWorkerHarnessOptions options =
        PipelineOptionsFactory.as(DataflowWorkerHarnessOptions.class);
    cache = WindmillStateCache.builder().setSizeMb(options.getWorkerCacheMb()).build();

    WindmillTagEncoding tagEncoding = WindmillTagEncodingV1.instance();
    underTest =
        new WindmillStateInternals<>(
            "key",
            STATE_FAMILY,
            mockReader,
            false,
            cache
                .forComputation("comp")
                .forKey(
                    WindmillComputationKey.create(
                        "comp", ByteString.copyFrom("key", StandardCharsets.UTF_8), 123),
                    17L,
                    1L)
                .forFamily(STATE_FAMILY),
            tagEncoding,
            readStateSupplier);

    when(mockReader.valuePrefixFuture(any(), any(), any()))
        .thenReturn(Futures.immediateFuture(Collections.emptyList()));
  }

  /**
   * Tests that {@link WindmillMap#computeIfAbsent} correctly computes and inserts when the key was
   * previously read as absent (and therefore placed in {@code negativeCache}).
   */
  @Test
  public void testComputeIfAbsentInsertsWhenNegativelyCached() throws Exception {
    StateTag<MapState<String, Integer>> addr =
        StateTags.map("map", StringUtf8Coder.of(), VarIntCoder.of());
    MapState<String, Integer> mapState = underTest.state(NAMESPACE, addr);

    final String key = "tag1";
    SettableFuture<Integer> future = SettableFuture.create();
    when(mockReader.valueFuture(
            protoKey(key, StringUtf8Coder.of()), STATE_FAMILY, VarIntCoder.of()))
        .thenReturn(future);
    future.set(null);

    // First, read the key -> returns null and populates negativeCache
    assertNull(mapState.get(key).read());

    // Now, call computeIfAbsent on the absent key
    AtomicBoolean mappingFunctionCalled = new AtomicBoolean(false);
    ReadableState<Integer> result =
        mapState.computeIfAbsent(
            key,
            k -> {
              mappingFunctionCalled.set(true);
              return 42;
            });

    assertNull("computeIfAbsent returns null on new insertion", result.read());
    assertTrue(
        "computeIfAbsent must invoke mappingFunction when key was in negativeCache",
        mappingFunctionCalled.get());
    assertEquals(Integer.valueOf(42), mapState.get(key).read());
  }

  /**
   * Tests that {@link WindmillMap#computeIfAbsent} correctly computes and inserts when the key was
   * previously removed in the same work item (and therefore placed in {@code localRemovals}).
   */
  @Test
  public void testComputeIfAbsentInsertsWhenLocallyRemoved() {
    StateTag<MapState<String, Integer>> addr =
        StateTags.map("map", StringUtf8Coder.of(), VarIntCoder.of());
    MapState<String, Integer> mapState = underTest.state(NAMESPACE, addr);

    final String key = "tag1";
    mapState.put(key, 10);
    mapState.remove(key);

    AtomicBoolean mappingFunctionCalled = new AtomicBoolean(false);
    ReadableState<Integer> result =
        mapState.computeIfAbsent(
            key,
            k -> {
              mappingFunctionCalled.set(true);
              return 42;
            });

    assertNull("computeIfAbsent returns null on new insertion", result.read());
    assertTrue(
        "computeIfAbsent must invoke mappingFunction when key was in localRemovals",
        mappingFunctionCalled.get());
    assertEquals(Integer.valueOf(42), mapState.get(key).read());
  }

  /**
   * Tests that {@link WindmillMap#computeIfAbsent} correctly computes and inserts when {@code
   * complete == true} (e.g. after {@link MapState#clear}).
   */
  @Test
  public void testComputeIfAbsentInsertsWhenComplete() {
    StateTag<MapState<String, Integer>> addr =
        StateTags.map("map", StringUtf8Coder.of(), VarIntCoder.of());
    MapState<String, Integer> mapState = underTest.state(NAMESPACE, addr);

    mapState.clear();

    AtomicBoolean mappingFunctionCalled = new AtomicBoolean(false);
    ReadableState<Integer> result =
        mapState.computeIfAbsent(
            "newKey",
            k -> {
              mappingFunctionCalled.set(true);
              return 42;
            });

    assertNull("computeIfAbsent returns null on new insertion", result.read());
    assertTrue(
        "computeIfAbsent must invoke mappingFunction when complete == true",
        mappingFunctionCalled.get());
    assertEquals(Integer.valueOf(42), mapState.get("newKey").read());
  }

  /**
   * Tests that {@link WindmillMap#getOrDefault} returns {@code defaultValue} on repeated lookups.
   */
  @Test
  public void testGetOrDefaultReturnsDefaultValueOnRepeatedRead() throws Exception {
    StateTag<MapState<String, Integer>> addr =
        StateTags.map("map", StringUtf8Coder.of(), VarIntCoder.of());
    MapState<String, Integer> mapState = underTest.state(NAMESPACE, addr);

    final String key1 = "key1";
    SettableFuture<Integer> future = SettableFuture.create();
    when(mockReader.valueFuture(
            protoKey(key1, StringUtf8Coder.of()), STATE_FAMILY, VarIntCoder.of()))
        .thenReturn(future);
    future.set(null);

    // 1st read: returns defaultValue 99
    assertEquals(Integer.valueOf(99), mapState.getOrDefault(key1, 99).read());

    // 2nd read: key is now in negativeCache, must still return defaultValue 99
    assertEquals(Integer.valueOf(99), mapState.getOrDefault(key1, 99).read());
  }

  /**
   * Tests that {@link WindmillMap#getOrDefault} returns {@code defaultValue} after remove and
   * clear.
   */
  @Test
  public void testGetOrDefaultReturnsDefaultValueAfterRemoveAndClear() {
    StateTag<MapState<String, Integer>> addr =
        StateTags.map("map", StringUtf8Coder.of(), VarIntCoder.of());
    MapState<String, Integer> mapState = underTest.state(NAMESPACE, addr);

    // After remove:
    mapState.put("key2", 1);
    mapState.remove("key2");
    assertEquals(Integer.valueOf(99), mapState.getOrDefault("key2", 99).read());

    // After clear:
    mapState.clear();
    assertEquals(Integer.valueOf(99), mapState.getOrDefault("key3", 99).read());
  }

  /**
   * Tests that {@link WindmillMap#entries} does not resurrect locally deleted entries when the
   * response from Windmill storage is {@link org.apache.beam.sdk.util.Weighted}.
   */
  @Test
  public void testEntriesDoesNotResurrectDeletedEntryWhenWeighted() throws Exception {
    StateTag<MapState<String, Integer>> addr =
        StateTags.map("map", StringUtf8Coder.of(), VarIntCoder.of());
    MapState<String, Integer> mapState = underTest.state(NAMESPACE, addr);

    final String tag1 = "tag1";
    final String tag2 = "tag2";
    SettableFuture<Iterable<Map.Entry<ByteString, Integer>>> prefixFuture = SettableFuture.create();
    when(mockReader.valuePrefixFuture(
            protoKey(null, StringUtf8Coder.of()), STATE_FAMILY, VarIntCoder.of()))
        .thenReturn(prefixFuture);

    // Delete tag1 locally before reading entries
    mapState.remove(tag1);

    // Windmill storage returns weighted list containing tag1 and tag2
    prefixFuture.set(
        weightedList(
            new AbstractMap.SimpleEntry<>(protoKey(tag1, StringUtf8Coder.of()), 1),
            new AbstractMap.SimpleEntry<>(protoKey(tag2, StringUtf8Coder.of()), 2)));

    Iterable<Map.Entry<String, Integer>> entries = mapState.entries().read();

    assertThat(entries, Matchers.not(Matchers.hasItem(new AbstractMap.SimpleEntry<>(tag1, 1))));
    assertThat(entries, Matchers.hasItem(new AbstractMap.SimpleEntry<>(tag2, 2)));
  }

  /**
   * Tests that {@link WindmillMap#persistDirectly} succeeds and commits deletion of a removed key
   * without exception.
   */
  @Test
  public void testPersistSucceedsAfterRemovalAndWeightedEntriesRead() throws Exception {
    StateTag<MapState<String, Integer>> addr =
        StateTags.map("map", StringUtf8Coder.of(), VarIntCoder.of());
    MapState<String, Integer> mapState = underTest.state(NAMESPACE, addr);

    final String tag1 = "tag1";
    SettableFuture<Iterable<Map.Entry<ByteString, Integer>>> prefixFuture = SettableFuture.create();
    when(mockReader.valuePrefixFuture(
            protoKey(null, StringUtf8Coder.of()), STATE_FAMILY, VarIntCoder.of()))
        .thenReturn(prefixFuture);

    // 1. Remove tag1 locally (adds to localRemovals)
    mapState.remove(tag1);

    // 2. Read entries (weighted) -> does not resurrect tag1 into cachedValues
    prefixFuture.set(
        weightedList(new AbstractMap.SimpleEntry<>(protoKey(tag1, StringUtf8Coder.of()), 1)));
    mapState.entries().read();

    // 3. Persist state: persistDirectly commits tag1 deletion successfully
    Windmill.WorkItemCommitRequest.Builder commitBuilder =
        Windmill.WorkItemCommitRequest.newBuilder();
    underTest.persist(commitBuilder);

    assertEquals(1, commitBuilder.getValueUpdatesCount());
    assertEquals(protoKey(tag1, StringUtf8Coder.of()), commitBuilder.getValueUpdates(0).getTag());
    assertTrue(commitBuilder.getValueUpdates(0).getValue().getData().isEmpty());
  }

  /**
   * Tests that {@link WindmillMap} matches {@code byte[]} keys using {@link Coder#structuralValue}.
   */
  @Test
  public void testByteArrayKeysMatchCorrectly() throws Exception {
    StateTag<MapState<byte[], Integer>> addr =
        StateTags.map("map", ByteArrayCoder.of(), VarIntCoder.of());
    MapState<byte[], Integer> mapState = underTest.state(NAMESPACE, addr);

    byte[] key = new byte[] {1, 2, 3};
    mapState.put(key, 42);

    // Lookup with duplicate byte array (same bytes, distinct object instance)
    byte[] lookupKey = dup(key);

    assertEquals(Integer.valueOf(42), mapState.get(lookupKey).read());
    Mockito.verifyNoInteractions(mockReader);
  }

  /** Tests that {@link WindmillMap#put} supports null values when using {@link NullableCoder}. */
  @Test
  public void testPutNullValueSupported() {
    StateTag<MapState<String, String>> addr =
        StateTags.map("map", StringUtf8Coder.of(), NullableCoder.of(StringUtf8Coder.of()));
    MapState<String, String> mapState = underTest.state(NAMESPACE, addr);

    mapState.put("key", null);
    assertNull(mapState.get("key").read());
  }

  /** Tests {@link WindmillMap#isEmpty()}. */
  @Test
  public void testIsEmpty() {
    StateTag<MapState<String, Integer>> addr =
        StateTags.map("map", StringUtf8Coder.of(), VarIntCoder.of());
    MapState<String, Integer> mapState = underTest.state(NAMESPACE, addr);

    mapState.put("key1", 10);
    assertFalse(mapState.isEmpty().read());

    mapState.clear();
    assertTrue(mapState.isEmpty().read());
  }

  /** Tests that persistDirectly with no modifications produces no updates. */
  @Test
  public void testPersistDirectlyNoChangesReturnsEmpty() {
    StateTag<MapState<String, Integer>> addr =
        StateTags.map("map", StringUtf8Coder.of(), VarIntCoder.of());
    underTest.state(NAMESPACE, addr);

    Windmill.WorkItemCommitRequest.Builder commitBuilder =
        Windmill.WorkItemCommitRequest.newBuilder();
    underTest.persist(commitBuilder);

    assertEquals(0, commitBuilder.getValueUpdatesCount());
    assertEquals(0, commitBuilder.getTagValuePrefixDeletesCount());
  }

  /** Tests that persistDirectly commits local additions correctly and clears localAdditions. */
  @Test
  public void testPersistDirectlyLocalAdditions() throws Exception {
    StateTag<MapState<String, Integer>> addr =
        StateTags.map("map", StringUtf8Coder.of(), VarIntCoder.of());
    MapState<String, Integer> mapState = underTest.state(NAMESPACE, addr);

    mapState.put("k1", 100);
    mapState.put("k2", 200);

    Windmill.WorkItemCommitRequest.Builder commitBuilder =
        Windmill.WorkItemCommitRequest.newBuilder();
    underTest.persist(commitBuilder);

    assertEquals(2, commitBuilder.getValueUpdatesCount());
    ByteString k1Key = protoKey("k1", StringUtf8Coder.of());
    ByteString k2Key = protoKey("k2", StringUtf8Coder.of());
    ByteString k1Val = encodeValue(100, VarIntCoder.of());
    ByteString k2Val = encodeValue(200, VarIntCoder.of());

    Map<ByteString, ByteString> updates = new java.util.HashMap<>();
    for (Windmill.TagValue valUpdate : commitBuilder.getValueUpdatesList()) {
      assertEquals(STATE_FAMILY, valUpdate.getStateFamily());
      assertEquals(Long.MAX_VALUE, valUpdate.getValue().getTimestamp());
      updates.put(valUpdate.getTag(), valUpdate.getValue().getData());
    }
    assertEquals(k1Val, updates.get(k1Key));
    assertEquals(k2Val, updates.get(k2Key));

    // A second persist should produce no new changes since localAdditions was cleared
    Windmill.WorkItemCommitRequest.Builder secondCommit =
        Windmill.WorkItemCommitRequest.newBuilder();
    underTest.persist(secondCommit);
    assertEquals(0, secondCommit.getValueUpdatesCount());
  }

  /** Tests that persistDirectly commits local removals with empty data (tag delete). */
  @Test
  public void testPersistDirectlyLocalRemovals() throws Exception {
    StateTag<MapState<String, Integer>> addr =
        StateTags.map("map", StringUtf8Coder.of(), VarIntCoder.of());
    MapState<String, Integer> mapState = underTest.state(NAMESPACE, addr);

    mapState.remove("k1");

    Windmill.WorkItemCommitRequest.Builder commitBuilder =
        Windmill.WorkItemCommitRequest.newBuilder();
    underTest.persist(commitBuilder);

    assertEquals(1, commitBuilder.getValueUpdatesCount());
    Windmill.TagValue valUpdate = commitBuilder.getValueUpdates(0);
    assertEquals(protoKey("k1", StringUtf8Coder.of()), valUpdate.getTag());
    assertEquals(STATE_FAMILY, valUpdate.getStateFamily());
    assertTrue("Empty data indicates deletion", valUpdate.getValue().getData().isEmpty());
  }

  /** Tests that persistDirectly with clear() commits tag value prefix deletes and additions. */
  @Test
  public void testPersistDirectlyClearedAndLocalAdditions() throws Exception {
    StateTag<MapState<String, Integer>> addr =
        StateTags.map("map", StringUtf8Coder.of(), VarIntCoder.of());
    MapState<String, Integer> mapState = underTest.state(NAMESPACE, addr);

    mapState.clear();
    mapState.put("k1", 42);

    Windmill.WorkItemCommitRequest.Builder commitBuilder =
        Windmill.WorkItemCommitRequest.newBuilder();
    underTest.persist(commitBuilder);

    assertEquals(1, commitBuilder.getTagValuePrefixDeletesCount());
    assertEquals(STATE_FAMILY, commitBuilder.getTagValuePrefixDeletes(0).getStateFamily());
    assertEquals(key(NAMESPACE, "map"), commitBuilder.getTagValuePrefixDeletes(0).getTagPrefix());

    assertEquals(1, commitBuilder.getValueUpdatesCount());
    Windmill.TagValue valUpdate = commitBuilder.getValueUpdates(0);
    assertEquals(protoKey("k1", StringUtf8Coder.of()), valUpdate.getTag());
    assertEquals(encodeValue(42, VarIntCoder.of()), valUpdate.getValue().getData());
  }

  /** Tests basic put, get, overwrite, and remove on {@link WindmillMap}. */
  @Test
  public void testBasicPutGetOverwriteAndRemove() {
    StateTag<MapState<String, Integer>> addr =
        StateTags.map("map", StringUtf8Coder.of(), VarIntCoder.of());
    MapState<String, Integer> mapState = underTest.state(NAMESPACE, addr);

    mapState.put("a", 1);
    assertEquals(Integer.valueOf(1), mapState.get("a").read());

    mapState.put("a", 2);
    assertEquals(Integer.valueOf(2), mapState.get("a").read());

    mapState.remove("a");
    assertNull(mapState.get("a").read());
  }

  /** Tests keys, values, and entries on {@link WindmillMap}. */
  @Test
  public void testKeysValuesAndEntries() throws Exception {
    StateTag<MapState<String, Integer>> addr =
        StateTags.map("map", StringUtf8Coder.of(), VarIntCoder.of());
    MapState<String, Integer> mapState = underTest.state(NAMESPACE, addr);

    when(mockReader.valuePrefixFuture(
            protoKey(null, StringUtf8Coder.of()), STATE_FAMILY, VarIntCoder.of()))
        .thenReturn(Futures.immediateFuture(Collections.emptyList()));

    mapState.put("k1", 10);
    mapState.put("k2", 20);

    assertThat(mapState.keys().read(), Matchers.containsInAnyOrder("k1", "k2"));
    assertThat(mapState.values().read(), Matchers.containsInAnyOrder(10, 20));
    assertThat(
        mapState.entries().read(),
        Matchers.containsInAnyOrder(
            new AbstractMap.SimpleEntry<>("k1", 10), new AbstractMap.SimpleEntry<>("k2", 20)));
  }
}
