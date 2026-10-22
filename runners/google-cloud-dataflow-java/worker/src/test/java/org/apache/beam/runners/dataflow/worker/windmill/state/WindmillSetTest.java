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
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

import java.io.Closeable;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import javax.annotation.Nullable;
import org.apache.beam.runners.core.StateNamespace;
import org.apache.beam.runners.core.StateNamespaceForTest;
import org.apache.beam.runners.core.StateTag;
import org.apache.beam.runners.core.StateTags;
import org.apache.beam.runners.dataflow.options.DataflowWorkerHarnessOptions;
import org.apache.beam.runners.dataflow.worker.WindmillComputationKey;
import org.apache.beam.sdk.coders.BooleanCoder;
import org.apache.beam.sdk.coders.ByteArrayCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.Coder.Context;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.state.ReadableState;
import org.apache.beam.sdk.state.SetState;
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

/** Unit tests demonstrating correctness issues and contract violations in {@link WindmillSet}. */
@RunWith(JUnit4.class)
public class WindmillSetTest {
  @Rule public transient Timeout globalTimeout = Timeout.seconds(600);

  private static final String STATE_FAMILY = "family";
  private static final StateNamespace NAMESPACE = new StateNamespaceForTest("ns");

  @Mock private WindmillStateReader mockReader;
  @Mock private Supplier<Closeable> readStateSupplier;

  private WindmillStateCache cache;
  private WindmillStateCache cacheViaMultimap;
  private WindmillStateInternals<String> underTest;
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

  private <K> ByteString protoKey(String tagId, @Nullable K tag, Coder<K> keyCoder)
      throws IOException {
    ByteStringOutputStream keyStream = new ByteStringOutputStream();
    key(NAMESPACE, tagId).writeTo(keyStream);
    if (tag != null) {
      keyCoder.encode(tag, keyStream, Context.OUTER);
    }
    return keyStream.toByteString();
  }

  private static byte[] dup(byte[] key) {
    byte[] res = new byte[key.length];
    System.arraycopy(key, 0, res, 0, key.length);
    return res;
  }

  @Before
  public void setUp() {
    MockitoAnnotations.initMocks(this);
    DataflowWorkerHarnessOptions options =
        PipelineOptionsFactory.as(DataflowWorkerHarnessOptions.class);
    cache = WindmillStateCache.builder().setSizeMb(options.getWorkerCacheMb()).build();
    cacheViaMultimap =
        WindmillStateCache.builder()
            .setSizeMb(options.getWorkerCacheMb())
            .setSupportMapViaMultimap(true)
            .build();

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

    when(mockReader.valuePrefixFuture(any(), any(), any()))
        .thenReturn(Futures.immediateFuture(Collections.emptyList()));
  }

  /**
   * Tests that {@link WindmillSet#addIfAbsent} returns {@code true} when the element was absent
   * (and newly added), and returns {@code false} when the element was already present.
   */
  @Test
  public void testAddIfAbsentReturnWithWindmillMap() throws Exception {
    StateTag<SetState<String>> addr = StateTags.set("set", StringUtf8Coder.of());
    SetState<String> setState = underTest.state(NAMESPACE, addr);

    final String elem = "elem1";
    SettableFuture<Boolean> future = SettableFuture.create();
    when(mockReader.valueFuture(
            protoKey("set", elem, StringUtf8Coder.of()), STATE_FAMILY, BooleanCoder.of()))
        .thenReturn(future);
    future.set(null); // null indicates not in Windmill storage

    // 1st add: element is absent from storage. It should be added and return true.
    ReadableState<Boolean> add1 = setState.addIfAbsent(elem);
    Boolean result1 = add1.read();
    assertTrue("addIfAbsent should return true when element was absent", result1);

    // 2nd add: element is now already present in the set. It should return false.
    ReadableState<Boolean> add2 = setState.addIfAbsent(elem);
    Boolean result2 = add2.read();
    assertFalse("addIfAbsent should return false when element was already present", result2);
  }

  /**
   * Tests that when backed by {@link WindmillMapViaMultimap}, {@link WindmillSet#addIfAbsent}
   * returns {@code true} on first add and {@code false} when already present.
   */
  @Test
  public void testAddIfAbsentWithMapViaMultimap() throws Exception {
    StateTag<SetState<String>> addr = StateTags.set("set", StringUtf8Coder.of());
    SetState<String> setState = underTestMapViaMultimap.state(NAMESPACE, addr);

    final String elem = "elem1";
    SettableFuture<Iterable<Boolean>> future = SettableFuture.create();
    when(mockReader.multimapFetchSingleEntryFuture(
            encodeWithCoder(elem, StringUtf8Coder.of()),
            key(NAMESPACE, "set"),
            STATE_FAMILY,
            BooleanCoder.of()))
        .thenReturn(future);
    future.set(Collections.emptyList());

    // 1st add: returns true
    assertTrue(setState.addIfAbsent(elem).read());

    // 2nd add: element is already present, returns false
    assertFalse(setState.addIfAbsent(elem).read());
  }

  /**
   * Tests that {@link WindmillSet#contains} returns {@code false} (not {@code null}) on repeated
   * lookups.
   */
  @Test
  public void testContainsReturnsFalseOnRepeatedRead() throws Exception {
    StateTag<SetState<String>> addr = StateTags.set("set", StringUtf8Coder.of());
    SetState<String> setState = underTest.state(NAMESPACE, addr);

    final String elem = "elem1";
    SettableFuture<Boolean> future = SettableFuture.create();
    when(mockReader.valueFuture(
            protoKey("set", elem, StringUtf8Coder.of()), STATE_FAMILY, BooleanCoder.of()))
        .thenReturn(future);
    future.set(null);

    // 1st call returns Boolean.FALSE (and adds to negativeCache)
    assertEquals(Boolean.FALSE, setState.contains(elem).read());

    // 2nd call: key is in negativeCache, must return Boolean.FALSE
    assertEquals(Boolean.FALSE, setState.contains(elem).read());
  }

  /** Tests that {@link WindmillSet#contains} returns {@code false} after an element is removed. */
  @Test
  public void testContainsReturnsFalseAfterRemove() {
    StateTag<SetState<String>> addr = StateTags.set("set", StringUtf8Coder.of());
    SetState<String> setState = underTest.state(NAMESPACE, addr);

    setState.add("elem1");
    setState.remove("elem1");

    assertEquals(Boolean.FALSE, setState.contains("elem1").read());
  }

  /** Tests that {@link WindmillSet#contains} returns {@code false} after {@link SetState#clear}. */
  @Test
  public void testContainsReturnsFalseAfterClear() {
    StateTag<SetState<String>> addr = StateTags.set("set", StringUtf8Coder.of());
    SetState<String> setState = underTest.state(NAMESPACE, addr);

    setState.add("elem1");
    setState.clear();

    assertEquals(Boolean.FALSE, setState.contains("elem1").read());
  }

  /**
   * Tests that {@link WindmillSet} correctly matches {@code byte[]} elements using {@link
   * Coder#structuralValue}.
   */
  @Test
  public void testByteArrayKeyEquality() throws Exception {
    StateTag<SetState<byte[]>> addr = StateTags.set("set", ByteArrayCoder.of());
    SetState<byte[]> setState = underTest.state(NAMESPACE, addr);

    byte[] elem = new byte[] {1, 2, 3};
    setState.add(elem);

    byte[] lookupElem = dup(elem);
    assertTrue(
        "WindmillSet matches byte[] elements via structuralValue",
        setState.contains(lookupElem).read());
  }

  /** Tests basic add and contains on {@link WindmillSet}. */
  @Test
  public void testBasicAddAndContains() throws Exception {
    StateTag<SetState<String>> addr = StateTags.set("set", StringUtf8Coder.of());
    SetState<String> setState = underTest.state(NAMESPACE, addr);

    when(mockReader.valueFuture(
            protoKey("set", "elem1", StringUtf8Coder.of()), STATE_FAMILY, BooleanCoder.of()))
        .thenReturn(Futures.immediateFuture(null));

    assertFalse(setState.contains("elem1").read());
    setState.add("elem1");
    assertTrue(setState.contains("elem1").read());
  }

  /** Tests read() iteration on {@link WindmillSet}. */
  @Test
  public void testReadIteration() throws Exception {
    StateTag<SetState<String>> addr = StateTags.set("set", StringUtf8Coder.of());
    SetState<String> setState = underTest.state(NAMESPACE, addr);

    when(mockReader.valuePrefixFuture(
            protoKey("set", null, StringUtf8Coder.of()), STATE_FAMILY, BooleanCoder.of()))
        .thenReturn(Futures.immediateFuture(Collections.emptyList()));

    setState.add("a");
    setState.add("b");

    assertThat(setState.read(), Matchers.containsInAnyOrder("a", "b"));
  }

  /** Tests isEmpty on {@link WindmillSet}. */
  @Test
  public void testIsEmpty() {
    StateTag<SetState<String>> addr = StateTags.set("set", StringUtf8Coder.of());
    SetState<String> setState = underTest.state(NAMESPACE, addr);

    setState.add("elem1");
    assertFalse(setState.isEmpty().read());

    setState.clear();
    assertTrue(setState.isEmpty().read());
  }
}
