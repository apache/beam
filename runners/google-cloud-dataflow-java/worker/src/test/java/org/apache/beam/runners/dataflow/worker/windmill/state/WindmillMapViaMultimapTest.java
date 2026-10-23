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
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.when;

import java.io.Closeable;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.AbstractMap;
import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.beam.runners.core.StateNamespace;
import org.apache.beam.runners.core.StateNamespaceForTest;
import org.apache.beam.runners.core.StateTag;
import org.apache.beam.runners.core.StateTags;
import org.apache.beam.runners.dataflow.options.DataflowWorkerHarnessOptions;
import org.apache.beam.runners.dataflow.worker.WindmillComputationKey;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill;
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
import org.mockito.MockitoAnnotations;

/**
 * Unit tests demonstrating correctness issues and contract violations in {@link
 * WindmillMapViaMultimap}.
 */
@RunWith(JUnit4.class)
public class WindmillMapViaMultimapTest {
  @Rule public transient Timeout globalTimeout = Timeout.seconds(600);

  private static final String STATE_FAMILY = "family";
  private static final StateNamespace NAMESPACE = new StateNamespaceForTest("ns");

  @Mock private WindmillStateReader mockReader;
  @Mock private Supplier<Closeable> readStateSupplier;

  private WindmillStateCache cacheViaMultimap;
  private WindmillStateInternals<String> underTestMapViaMultimap;

  private static ByteString key(StateNamespace namespace, String addrId) {
    return ByteString.copyFromUtf8(namespace.stringKey() + "+u" + addrId);
  }

  private static <T> ByteString encodeWithCoder(T value, Coder<T> coder) {
    ByteStringOutputStream out = new ByteStringOutputStream();
    try {
      coder.encode(value, out, Context.OUTER);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return out.toByteString();
  }

  @Before
  public void setUp() {
    MockitoAnnotations.initMocks(this);
    DataflowWorkerHarnessOptions options =
        PipelineOptionsFactory.as(DataflowWorkerHarnessOptions.class);
    cacheViaMultimap =
        WindmillStateCache.builder()
            .setSizeMb(options.getWorkerCacheMb())
            .setSupportMapViaMultimap(true)
            .build();

    WindmillTagEncoding tagEncoding = WindmillTagEncodingV1.instance();
    underTestMapViaMultimap =
        new WindmillStateInternals<>(
            "key",
            STATE_FAMILY,
            mockReader,
            false,
            cacheViaMultimap
                .forComputation("comp")
                .forKey(
                    WindmillComputationKey.create(
                        "comp", ByteString.copyFrom("key", StandardCharsets.UTF_8), 123),
                    17L,
                    1L)
                .forFamily(STATE_FAMILY),
            tagEncoding,
            readStateSupplier);

    when(mockReader.multimapFetchAllFuture(anyBoolean(), any(), any(), any()))
        .thenReturn(Futures.immediateFuture(Collections.emptyList()));
    when(mockReader.multimapFetchSingleEntryFuture(any(), any(), any(), any()))
        .thenReturn(Futures.immediateFuture(Collections.emptyList()));
  }

  /**
   * Tests that {@link WindmillMapViaMultimap#computeIfAbsent} returns {@code null} on insertion,
   * conforming to the {@link MapState#computeIfAbsent} specification.
   */
  @Test
  public void testComputeIfAbsentReturnsNullOnInsert() {
    StateTag<MapState<String, Integer>> addr =
        StateTags.map("map", StringUtf8Coder.of(), VarIntCoder.of());
    MapState<String, Integer> mapState = underTestMapViaMultimap.state(NAMESPACE, addr);

    final String key = "tag1";
    SettableFuture<Iterable<Integer>> future = SettableFuture.create();
    when(mockReader.multimapFetchSingleEntryFuture(
            encodeWithCoder(key, StringUtf8Coder.of()),
            key(NAMESPACE, "map"),
            STATE_FAMILY,
            VarIntCoder.of()))
        .thenReturn(future);
    future.set(Collections.emptyList());

    ReadableState<Integer> result = mapState.computeIfAbsent(key, k -> 42);
    assertNull(
        "computeIfAbsent returns null when key was absent and computed value is inserted",
        result.read());
    assertEquals(Integer.valueOf(42), mapState.get(key).read());
  }

  /**
   * Tests that {@link WindmillMapViaMultimap} throws {@link IllegalArgumentException} if multiple
   * values exist for a key, enforcing at-most-one-value map invariants.
   */
  @Test(expected = IllegalArgumentException.class)
  public void testGetThrowsOnMultiValuedKey() {
    StateTag<MapState<String, Integer>> addr =
        StateTags.map("map", StringUtf8Coder.of(), VarIntCoder.of());
    MapState<String, Integer> mapState = underTestMapViaMultimap.state(NAMESPACE, addr);

    final String key = "tag1";
    SettableFuture<Iterable<Integer>> future = SettableFuture.create();
    when(mockReader.multimapFetchSingleEntryFuture(
            encodeWithCoder(key, StringUtf8Coder.of()),
            key(NAMESPACE, "map"),
            STATE_FAMILY,
            VarIntCoder.of()))
        .thenReturn(future);
    // Storage returns multiple values for key
    future.set(Arrays.asList(10, 20));

    mapState.get(key).read();
  }

  /** Tests basic put, get, overwrite, and remove on {@link WindmillMapViaMultimap}. */
  @Test
  public void testBasicPutGetOverwriteAndRemove() {
    StateTag<MapState<String, Integer>> addr =
        StateTags.map("map", StringUtf8Coder.of(), VarIntCoder.of());
    MapState<String, Integer> mapState = underTestMapViaMultimap.state(NAMESPACE, addr);

    mapState.put("a", 1);
    assertEquals(Integer.valueOf(1), mapState.get("a").read());

    mapState.put("a", 2);
    assertEquals(Integer.valueOf(2), mapState.get("a").read());

    mapState.remove("a");
    assertNull(mapState.get("a").read());
  }

  /** Tests keys, values, and entries on {@link WindmillMapViaMultimap}. */
  @Test
  public void testKeysValuesAndEntries() {
    StateTag<MapState<String, Integer>> addr =
        StateTags.map("map", StringUtf8Coder.of(), VarIntCoder.of());
    MapState<String, Integer> mapState = underTestMapViaMultimap.state(NAMESPACE, addr);

    mapState.put("k1", 10);
    mapState.put("k2", 20);

    assertThat(mapState.keys().read(), Matchers.containsInAnyOrder("k1", "k2"));
    assertThat(mapState.values().read(), Matchers.containsInAnyOrder(10, 20));
    assertThat(
        mapState.entries().read(),
        Matchers.containsInAnyOrder(
            new AbstractMap.SimpleEntry<>("k1", 10), new AbstractMap.SimpleEntry<>("k2", 20)));
  }

  /** Tests clear() and isEmpty() on {@link WindmillMapViaMultimap}. */
  @Test
  public void testClearAndIsEmpty() {
    StateTag<MapState<String, Integer>> addr =
        StateTags.map("map", StringUtf8Coder.of(), VarIntCoder.of());
    MapState<String, Integer> mapState = underTestMapViaMultimap.state(NAMESPACE, addr);

    assertTrue(mapState.isEmpty().read());

    mapState.put("key1", 10);
    assertFalse(mapState.isEmpty().read());

    mapState.clear();
    assertTrue(mapState.isEmpty().read());
  }

  /** Tests that computeIfAbsent on an existing key returns the existing value. */
  @Test
  public void testComputeIfAbsentExistingKeyReturnsExistingValue() {
    StateTag<MapState<String, Integer>> addr =
        StateTags.map("map", StringUtf8Coder.of(), VarIntCoder.of());
    MapState<String, Integer> mapState = underTestMapViaMultimap.state(NAMESPACE, addr);

    mapState.put("key1", 10);
    AtomicBoolean called = new AtomicBoolean(false);
    ReadableState<Integer> result =
        mapState.computeIfAbsent(
            "key1",
            k -> {
              called.set(true);
              return 99;
            });

    assertEquals(Integer.valueOf(10), result.read());
    assertFalse("Mapping function should not be called for existing key", called.get());
  }

  /**
   * Tests that computeIfAbsent computes and inserts a new value when the key is currently mapped to
   * null.
   */
  @Test
  public void testComputeIfAbsentWhenMappedToNull() {
    StateTag<MapState<String, String>> addr =
        StateTags.map("map", StringUtf8Coder.of(), NullableCoder.of(StringUtf8Coder.of()));
    MapState<String, String> mapState = underTestMapViaMultimap.state(NAMESPACE, addr);

    mapState.put("key1", null);
    AtomicBoolean called = new AtomicBoolean(false);
    ReadableState<String> result =
        mapState.computeIfAbsent(
            "key1",
            k -> {
              called.set(true);
              return "computed";
            });

    assertNull(result.read());
    assertTrue("Mapping function should be called when key is mapped to null", called.get());
    assertEquals("computed", mapState.get("key1").read());
  }
}
