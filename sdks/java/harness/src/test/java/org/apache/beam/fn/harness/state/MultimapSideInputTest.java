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
package org.apache.beam.fn.harness.state;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.beam.fn.harness.Cache;
import org.apache.beam.fn.harness.Caches;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.StateKey;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.StateResponse;
import org.apache.beam.sdk.coders.ByteArrayCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CustomCoder;
import org.apache.beam.sdk.coders.IterableCoder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.NullableCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.SdkHarnessOptions;
import org.apache.beam.sdk.util.ByteStringOutputStream;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.vendor.grpc.v1p69p0.com.google.protobuf.ByteString;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Iterables;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Tests for {@link MultimapSideInput}.
 *
 * <p>It is important to use a key type where its coder is not {@link Coder#consistentWithEquals()}
 * to ensure that comparisons are performed using structural values instead of object equality
 * during testing.
 */
@RunWith(JUnit4.class)
public class MultimapSideInputTest {
  private static final byte[] A = "A".getBytes(StandardCharsets.UTF_8);
  private static final byte[] B = "B".getBytes(StandardCharsets.UTF_8);
  private static final byte[] UNKNOWN = "UNKNOWN".getBytes(StandardCharsets.UTF_8);

  @Test
  public void testGetWithBulkRead() throws Exception {
    FakeBeamFnStateClient fakeBeamFnStateClient =
        new FakeBeamFnStateClient(
            ImmutableMap.of(
                keysValuesStateKey(),
                KV.of(
                    KvCoder.of(ByteArrayCoder.of(), IterableCoder.of(StringUtf8Coder.of())),
                    asList(KV.of(A, asList("A1", "A2", "A3")), KV.of(B, asList("B1", "B2"))))));

    MultimapSideInput<byte[], String> multimapSideInput =
        new MultimapSideInput<>(
            Caches.noop(),
            fakeBeamFnStateClient,
            "instructionId",
            keysStateKey(),
            ByteArrayCoder.of(),
            StringUtf8Coder.of(),
            true);
    assertArrayEquals(
        new String[] {"A1", "A2", "A3"},
        Iterables.toArray(multimapSideInput.get(Arrays.copyOf(A, A.length)), String.class));
    assertArrayEquals(
        new String[] {"B1", "B2"}, Iterables.toArray(multimapSideInput.get(B), String.class));
    assertArrayEquals(
        new String[] {}, Iterables.toArray(multimapSideInput.get(UNKNOWN), String.class));
  }

  @Test
  public void testGetWithBulkReadCached() throws Exception {
    FakeBeamFnStateClient fakeBeamFnStateClient =
        new FakeBeamFnStateClient(
            ImmutableMap.of(
                keysValuesStateKey(),
                KV.of(
                    KvCoder.of(ByteArrayCoder.of(), IterableCoder.of(StringUtf8Coder.of())),
                    asList(KV.of(A, asList("A1", "A2", "A3")), KV.of(B, asList("B1", "B2"))))));

    Cache<?, ?> cache = Caches.eternal();
    {
      // The first side input populates the bulk-read cache.
      MultimapSideInput<byte[], String> multimapSideInput =
          new MultimapSideInput<>(
              cache,
              fakeBeamFnStateClient,
              "instructionId",
              keysStateKey(),
              ByteArrayCoder.of(),
              StringUtf8Coder.of(),
              true);
      Iterable<String> aValues = multimapSideInput.get(A);
      assertArrayEquals(new String[] {"A1", "A2", "A3"}, Iterables.toArray(aValues, String.class));
      // Cached bulk-read iterables must be unmodifiable so callers cannot corrupt shared entries.
      if (aValues instanceof List) {
        assertThrows(UnsupportedOperationException.class, () -> ((List<String>) aValues).clear());
      }
      Iterator<String> it = aValues.iterator();
      it.next();
      assertThrows(UnsupportedOperationException.class, it::remove);
      assertArrayEquals(
          new String[] {"B1", "B2"}, Iterables.toArray(multimapSideInput.get(B), String.class));
      assertArrayEquals(
          new String[] {}, Iterables.toArray(multimapSideInput.get(UNKNOWN), String.class));
    }

    {
      // The second side input must serve all bulk-read lookups purely from the cache.
      MultimapSideInput<byte[], String> multimapSideInput =
          new MultimapSideInput<>(
              cache,
              requestBuilder -> {
                throw new IllegalStateException("Unexpected call for test.");
              },
              "instructionId",
              keysStateKey(),
              ByteArrayCoder.of(),
              StringUtf8Coder.of(),
              true);
      assertArrayEquals(
          new String[] {"A1", "A2", "A3"},
          Iterables.toArray(multimapSideInput.get(Arrays.copyOf(A, A.length)), String.class));
      assertArrayEquals(
          new String[] {"B1", "B2"}, Iterables.toArray(multimapSideInput.get(B), String.class));
      assertArrayEquals(
          new String[] {}, Iterables.toArray(multimapSideInput.get(UNKNOWN), String.class));
    }
  }

  @Test
  public void testGetWithBulkReadMoreThan100KeysCached() throws Exception {
    List<KV<byte[], List<String>>> entries = new ArrayList<>();
    Map<StateKey, KV<Coder<?>, List<?>>> initialData = new HashMap<>();
    for (int i = 0; i < 120; i++) {
      byte[] k = ("K" + i).getBytes(StandardCharsets.UTF_8);
      List<String> vals = Collections.singletonList("V" + i);
      entries.add(KV.of(k, vals));
      initialData.put(key(k), KV.of(StringUtf8Coder.of(), vals));
    }
    initialData.put(
        keysValuesStateKey(),
        KV.of(KvCoder.of(ByteArrayCoder.of(), IterableCoder.of(StringUtf8Coder.of())), entries));
    FakeBeamFnStateClient fakeClient = new FakeBeamFnStateClient(initialData, 1024 * 1024);

    Cache<?, ?> cache = Caches.eternal();
    byte[] key0 = "K0".getBytes(StandardCharsets.UTF_8);
    byte[] key99 = "K99".getBytes(StandardCharsets.UTF_8);
    byte[] key105 = "K105".getBytes(StandardCharsets.UTF_8);
    {
      MultimapSideInput<byte[], String> first =
          new MultimapSideInput<>(
              cache,
              fakeClient,
              "instructionId",
              keysStateKey(),
              ByteArrayCoder.of(),
              StringUtf8Coder.of(),
              true);
      assertArrayEquals(new String[] {"V0"}, Iterables.toArray(first.get(key0), String.class));
      assertArrayEquals(new String[] {"V99"}, Iterables.toArray(first.get(key99), String.class));
      assertArrayEquals(new String[] {"V105"}, Iterables.toArray(first.get(key105), String.class));
      assertArrayEquals(new String[] {}, Iterables.toArray(first.get(UNKNOWN), String.class));
    }
    {
      // Both the cached 100-key bulk read and previously fetched overflow/unknown keys must hit
      // the cache with zero RPCs.
      MultimapSideInput<byte[], String> second =
          new MultimapSideInput<>(
              cache,
              requestBuilder -> {
                throw new IllegalStateException("Unexpected call for test.");
              },
              "instructionId",
              keysStateKey(),
              ByteArrayCoder.of(),
              StringUtf8Coder.of(),
              true);
      assertArrayEquals(new String[] {"V0"}, Iterables.toArray(second.get(key0), String.class));
      assertArrayEquals(new String[] {"V99"}, Iterables.toArray(second.get(key99), String.class));
      assertArrayEquals(new String[] {"V105"}, Iterables.toArray(second.get(key105), String.class));
      assertArrayEquals(new String[] {}, Iterables.toArray(second.get(UNKNOWN), String.class));
    }
  }

  @Test
  public void testGetWithBulkReadEmptyCached() throws Exception {
    BeamFnStateClient emptyBulkClient =
        requestBuilder -> {
          if (requestBuilder.getStateKey().hasMultimapKeysValuesSideInput()) {
            StateResponse.Builder resp = StateResponse.newBuilder();
            resp.getGetBuilder().setData(ByteString.EMPTY);
            return CompletableFuture.completedFuture(resp.build());
          }
          throw new IllegalStateException("Unexpected non-bulk call");
        };
    Cache<?, ?> cache = Caches.eternal();
    {
      MultimapSideInput<byte[], String> first =
          new MultimapSideInput<>(
              cache,
              emptyBulkClient,
              "instructionId",
              keysStateKey(),
              ByteArrayCoder.of(),
              StringUtf8Coder.of(),
              true);
      assertArrayEquals(new String[] {}, Iterables.toArray(first.get(UNKNOWN), String.class));
    }
    {
      MultimapSideInput<byte[], String> second =
          new MultimapSideInput<>(
              cache,
              requestBuilder -> {
                throw new IllegalStateException("Unexpected call for test.");
              },
              "instructionId",
              keysStateKey(),
              ByteArrayCoder.of(),
              StringUtf8Coder.of(),
              true);
      assertArrayEquals(new String[] {}, Iterables.toArray(second.get(A), String.class));
    }
  }

  @Test
  public void testGetWithBulkReadDisabled() throws Exception {
    FakeBeamFnStateClient fakeClient =
        new FakeBeamFnStateClient(
            ImmutableMap.of(
                key(A), KV.of(StringUtf8Coder.of(), asList("A1", "A2")),
                key(B), KV.of(StringUtf8Coder.of(), asList("B1"))));
    AtomicInteger bulkCalls = new AtomicInteger();
    BeamFnStateClient countingClient =
        requestBuilder -> {
          if (requestBuilder.getStateKey().hasMultimapKeysValuesSideInput()) {
            bulkCalls.incrementAndGet();
          }
          return fakeClient.handle(requestBuilder);
        };
    MultimapSideInput<byte[], String> sideInput =
        new MultimapSideInput<>(
            Caches.eternal(),
            countingClient,
            "instructionId",
            keysStateKey(),
            ByteArrayCoder.of(),
            StringUtf8Coder.of(),
            false);
    assertArrayEquals(new String[] {"A1", "A2"}, Iterables.toArray(sideInput.get(A), String.class));
    assertArrayEquals(new String[] {"B1"}, Iterables.toArray(sideInput.get(B), String.class));
    assertEquals("Expected no bulk-read RPC when useBulkRead=false", 0, bulkCalls.get());
  }

  @Test
  public void testGetWithBulkReadStopsDecodingAtLimitAndHandlesSplitChunks() throws Exception {
    int totalKeys = 150;
    Map<StateKey, KV<Coder<?>, List<?>>> state = new HashMap<>();
    List<KV<String, Iterable<String>>> allEntries = new ArrayList<>(totalKeys);
    for (int i = 0; i < totalKeys; i++) {
      String k = "key-" + i;
      String v = "value-" + i;
      allEntries.add(KV.of(k, Collections.singletonList(v)));
      ByteStringOutputStream keyOut = new ByteStringOutputStream();
      StringUtf8Coder.of().encode(k, keyOut);
      StateKey pointKey =
          StateKey.newBuilder()
              .setMultimapSideInput(
                  StateKey.MultimapSideInput.newBuilder()
                      .setTransformId("ptransformId")
                      .setSideInputId("sideInputId")
                      .setWindow(ByteString.copyFromUtf8("encodedWindow"))
                      .setKey(keyOut.toByteStringAndReset()))
              .build();
      state.put(pointKey, KV.of(StringUtf8Coder.of(), Collections.singletonList(v)));
    }
    state.put(
        keysValuesStateKey(),
        KV.of(
            KvCoder.of(StringUtf8Coder.of(), IterableCoder.of(StringUtf8Coder.of())), allEntries));

    // Single oversized chunk: must stop decoding at BULK_READ_SIZE (100) keys instead of 150.
    FakeBeamFnStateClient singleChunkClient = new FakeBeamFnStateClient(state, 1024 * 1024);
    AtomicInteger decodedKeyCount = new AtomicInteger();
    Coder<String> countingKeyCoder =
        new CustomCoder<String>() {
          @Override
          public void encode(String value, OutputStream outStream) throws IOException {
            StringUtf8Coder.of().encode(value, outStream);
          }

          @Override
          public String decode(InputStream inStream) throws IOException {
            decodedKeyCount.incrementAndGet();
            return StringUtf8Coder.of().decode(inStream);
          }
        };

    MultimapSideInput<String, String> singleChunkInput =
        new MultimapSideInput<>(
            Caches.eternal(),
            singleChunkClient,
            "instructionId",
            keysStateKey(),
            countingKeyCoder,
            StringUtf8Coder.of(),
            true);
    assertArrayEquals(
        new String[] {"value-0"}, Iterables.toArray(singleChunkInput.get("key-0"), String.class));
    assertEquals(100, decodedKeyCount.get());
    assertArrayEquals(
        new String[] {"value-120"},
        Iterables.toArray(singleChunkInput.get("key-120"), String.class));

    // Tiny 6-byte chunks: individual KV entries straddle chunk boundaries cleanly.
    FakeBeamFnStateClient splitChunkClient = new FakeBeamFnStateClient(state, 6);
    AtomicInteger splitDecodedKeyCount = new AtomicInteger();
    Coder<String> splitCountingKeyCoder =
        new CustomCoder<String>() {
          @Override
          public void encode(String value, OutputStream outStream) throws IOException {
            StringUtf8Coder.of().encode(value, outStream);
          }

          @Override
          public String decode(InputStream inStream) throws IOException {
            splitDecodedKeyCount.incrementAndGet();
            return StringUtf8Coder.of().decode(inStream);
          }
        };
    MultimapSideInput<String, String> splitChunkInput =
        new MultimapSideInput<>(
            Caches.eternal(),
            splitChunkClient,
            "instructionId",
            keysStateKey(),
            splitCountingKeyCoder,
            StringUtf8Coder.of(),
            true);
    assertArrayEquals(
        new String[] {"value-99"}, Iterables.toArray(splitChunkInput.get("key-99"), String.class));
    assertEquals(100, splitDecodedKeyCount.get());
    assertArrayEquals(
        new String[] {"value-149"},
        Iterables.toArray(splitChunkInput.get("key-149"), String.class));
  }

  @Test
  public void testGetWithBulkReadNullableKeyCoder() throws Exception {
    Coder<String> nullableKeyCoder = NullableCoder.of(StringUtf8Coder.of());
    FakeBeamFnStateClient client =
        new FakeBeamFnStateClient(
            ImmutableMap.of(
                keysValuesStateKey(),
                KV.of(
                    KvCoder.of(nullableKeyCoder, IterableCoder.of(StringUtf8Coder.of())),
                    asList(KV.of(null, asList("null1", "null2")), KV.of("k", asList("v1"))))));
    Cache<?, ?> sharedCache = Caches.eternal();
    MultimapSideInput<String, String> first =
        new MultimapSideInput<>(
            sharedCache,
            client,
            "instructionId",
            keysStateKey(),
            nullableKeyCoder,
            StringUtf8Coder.of(),
            true);
    assertArrayEquals(
        new String[] {"null1", "null2"}, Iterables.toArray(first.get(null), String.class));
    assertArrayEquals(new String[] {"v1"}, Iterables.toArray(first.get("k"), String.class));
    assertArrayEquals(new String[] {}, Iterables.toArray(first.get("missing"), String.class));

    // Second instance must serve null key strictly from the shared bulk-read cache.
    BeamFnStateClient throwingClient =
        req -> {
          throw new AssertionError("Unexpected RPC for cached nullable key");
        };
    MultimapSideInput<String, String> second =
        new MultimapSideInput<>(
            sharedCache,
            throwingClient,
            "instructionId2",
            keysStateKey(),
            nullableKeyCoder,
            StringUtf8Coder.of(),
            true);
    assertArrayEquals(
        new String[] {"null1", "null2"}, Iterables.toArray(second.get(null), String.class));
    assertArrayEquals(new String[] {}, Iterables.toArray(second.get("missing"), String.class));
  }

  @Test
  public void testGetNullKeyThrowsIllegalStateForNonNullableCoders() throws Exception {
    FakeBeamFnStateClient byteClient =
        new FakeBeamFnStateClient(
            ImmutableMap.of(
                keysValuesStateKey(),
                KV.of(
                    KvCoder.of(ByteArrayCoder.of(), IterableCoder.of(StringUtf8Coder.of())),
                    Collections.singletonList(KV.of(A, asList("A1"))))));
    for (boolean useBulkRead : new boolean[] {false, true}) {
      MultimapSideInput<byte[], String> byteInput =
          new MultimapSideInput<>(
              Caches.eternal(),
              byteClient,
              "instructionId",
              keysStateKey(),
              ByteArrayCoder.of(),
              StringUtf8Coder.of(),
              useBulkRead);
      assertThrows(IllegalStateException.class, () -> byteInput.get(null));

      MultimapSideInput<String, String> strInput =
          new MultimapSideInput<>(
              Caches.eternal(),
              new FakeBeamFnStateClient(
                  ImmutableMap.of(
                      keysValuesStateKey(),
                      KV.of(
                          KvCoder.of(StringUtf8Coder.of(), IterableCoder.of(StringUtf8Coder.of())),
                          Collections.singletonList(KV.of("a", asList("1")))))),
              "instructionId",
              keysStateKey(),
              StringUtf8Coder.of(),
              StringUtf8Coder.of(),
              useBulkRead);
      assertThrows(IllegalStateException.class, () -> strInput.get(null));
    }
  }

  @Test
  public void testGetWithBulkReadMidStreamFailureNotCached() throws Exception {
    KvCoder<byte[], Iterable<String>> entryCoder =
        KvCoder.of(ByteArrayCoder.of(), IterableCoder.of(StringUtf8Coder.of()));
    ByteStringOutputStream out = new ByteStringOutputStream();
    entryCoder.encode(KV.of(A, asList("A1", "A2")), out);
    ByteString page1 = out.toByteStringAndReset();
    AtomicInteger bulkRequests = new AtomicInteger();

    BeamFnStateClient flakyClient =
        requestBuilder -> {
          StateKey sk = requestBuilder.getStateKey();
          if (sk.hasMultimapKeysValuesSideInput()) {
            bulkRequests.incrementAndGet();
            if (requestBuilder.getGet().getContinuationToken().isEmpty()) {
              StateResponse.Builder resp = StateResponse.newBuilder();
              resp.getGetBuilder()
                  .setData(page1)
                  .setContinuationToken(ByteString.copyFromUtf8("p2"));
              return CompletableFuture.completedFuture(resp.build());
            }
            CompletableFuture<StateResponse> failed = new CompletableFuture<>();
            failed.completeExceptionally(new IllegalStateException("Transient page 2 error"));
            return failed;
          }
          // Single-key fallback for key B
          ByteStringOutputStream bOut = new ByteStringOutputStream();
          try {
            StringUtf8Coder.of().encode("B1", bOut);
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
          StateResponse.Builder resp = StateResponse.newBuilder();
          resp.getGetBuilder().setData(bOut.toByteStringAndReset());
          return CompletableFuture.completedFuture(resp.build());
        };

    Cache<?, ?> cache = Caches.eternal();
    MultimapSideInput<byte[], String> first =
        new MultimapSideInput<>(
            cache,
            flakyClient,
            "instructionId",
            keysStateKey(),
            ByteArrayCoder.of(),
            StringUtf8Coder.of(),
            true);
    // Page 1 entry A succeeded inside this instance; key B fell back to point lookup.
    assertArrayEquals(new String[] {"A1", "A2"}, Iterables.toArray(first.get(A), String.class));
    assertArrayEquals(new String[] {"B1"}, Iterables.toArray(first.get(B), String.class));
    assertTrue(bulkRequests.get() >= 2);

    // Next bundle must retry bulk-read instead of using a poisoned incomplete cache entry.
    int before = bulkRequests.get();
    MultimapSideInput<byte[], String> second =
        new MultimapSideInput<>(
            cache,
            flakyClient,
            "instructionId",
            keysStateKey(),
            ByteArrayCoder.of(),
            StringUtf8Coder.of(),
            true);
    assertArrayEquals(new String[] {"A1", "A2"}, Iterables.toArray(second.get(A), String.class));
    assertTrue(bulkRequests.get() > before);
  }

  @Test
  public void testGetWithBulkReadWeighsNestedValues() throws Exception {
    // 1 key with 20,000 ~100-byte strings (~2 MB actual heap). A 1 MB cache must reject/evict it.
    List<String> bigValues = new ArrayList<>(20_000);
    StringBuilder payload = new StringBuilder(100);
    for (int i = 0; i < 100; i++) {
      payload.append('x');
    }
    String val = payload.toString();
    for (int i = 0; i < 20_000; i++) {
      bigValues.add(val);
    }
    FakeBeamFnStateClient fakeClient =
        new FakeBeamFnStateClient(
            ImmutableMap.of(
                keysValuesStateKey(),
                KV.of(
                    KvCoder.of(ByteArrayCoder.of(), IterableCoder.of(StringUtf8Coder.of())),
                    Collections.singletonList(KV.of(A, bigValues)))),
            10 * 1024 * 1024);

    PipelineOptions opts = PipelineOptionsFactory.create();
    opts.as(SdkHarnessOptions.class).setMaxCacheMemoryUsageMb(1);
    Cache<?, ?> smallCache = Caches.fromOptions(opts);
    MultimapSideInput<byte[], String> first =
        new MultimapSideInput<>(
            smallCache,
            fakeClient,
            "instructionId",
            keysStateKey(),
            ByteArrayCoder.of(),
            StringUtf8Coder.of(),
            true);
    assertEquals(20_000, Iterables.size(first.get(A)));
    assertTrue(smallCache.describeStats().contains("evictions 1"));
  }

  @Test
  public void testGet() throws Exception {
    FakeBeamFnStateClient fakeBeamFnStateClient =
        new FakeBeamFnStateClient(
            ImmutableMap.of(
                keysStateKey(), KV.of(ByteArrayCoder.of(), asList(A, B)),
                key(A), KV.of(StringUtf8Coder.of(), asList("A1", "A2", "A3")),
                key(B), KV.of(StringUtf8Coder.of(), asList("B1", "B2"))));

    MultimapSideInput<byte[], String> multimapSideInput =
        new MultimapSideInput<>(
            Caches.noop(),
            fakeBeamFnStateClient,
            "instructionId",
            keysStateKey(),
            ByteArrayCoder.of(),
            StringUtf8Coder.of(),
            true);
    assertArrayEquals(
        new String[] {"A1", "A2", "A3"}, Iterables.toArray(multimapSideInput.get(A), String.class));
    assertArrayEquals(
        new String[] {"B1", "B2"}, Iterables.toArray(multimapSideInput.get(B), String.class));
    assertArrayEquals(
        new String[] {}, Iterables.toArray(multimapSideInput.get(UNKNOWN), String.class));
    assertArrayEquals(
        new byte[][] {A, B}, Iterables.toArray(multimapSideInput.get(), byte[].class));
  }

  @Test
  public void testGetCached() throws Exception {
    FakeBeamFnStateClient fakeBeamFnStateClient =
        new FakeBeamFnStateClient(
            ImmutableMap.of(
                keysStateKey(), KV.of(ByteArrayCoder.of(), asList(A, B)),
                key(A), KV.of(StringUtf8Coder.of(), asList("A1", "A2", "A3")),
                key(B), KV.of(StringUtf8Coder.of(), asList("B1", "B2"))));

    Cache<?, ?> cache = Caches.eternal();
    {
      // The first side input will populate the cache.
      MultimapSideInput<byte[], String> multimapSideInput =
          new MultimapSideInput<>(
              cache,
              fakeBeamFnStateClient,
              "instructionId",
              keysStateKey(),
              ByteArrayCoder.of(),
              StringUtf8Coder.of(),
              true);
      assertArrayEquals(
          new String[] {"A1", "A2", "A3"},
          Iterables.toArray(multimapSideInput.get(A), String.class));
      assertArrayEquals(
          new String[] {"B1", "B2"}, Iterables.toArray(multimapSideInput.get(B), String.class));
      assertArrayEquals(
          new String[] {}, Iterables.toArray(multimapSideInput.get(UNKNOWN), String.class));
      assertArrayEquals(
          new byte[][] {A, B}, Iterables.toArray(multimapSideInput.get(), byte[].class));
    }

    {
      // The next side input will load all of its contents from the cache.
      MultimapSideInput<byte[], String> multimapSideInput =
          new MultimapSideInput<>(
              cache,
              requestBuilder -> {
                throw new IllegalStateException("Unexpected call for test.");
              },
              "instructionId",
              keysStateKey(),
              ByteArrayCoder.of(),
              StringUtf8Coder.of(),
              true);
      assertArrayEquals(
          new String[] {"A1", "A2", "A3"},
          Iterables.toArray(multimapSideInput.get(A), String.class));
      assertArrayEquals(
          new String[] {"B1", "B2"}, Iterables.toArray(multimapSideInput.get(B), String.class));
      assertArrayEquals(
          new String[] {}, Iterables.toArray(multimapSideInput.get(UNKNOWN), String.class));
      assertArrayEquals(
          new byte[][] {A, B}, Iterables.toArray(multimapSideInput.get(), byte[].class));
    }
  }

  private StateKey keysStateKey() throws IOException {
    return StateKey.newBuilder()
        .setMultimapKeysSideInput(
            StateKey.MultimapKeysSideInput.newBuilder()
                .setTransformId("ptransformId")
                .setSideInputId("sideInputId")
                .setWindow(ByteString.copyFromUtf8("encodedWindow")))
        .build();
  }

  private StateKey keysValuesStateKey() throws IOException {
    return StateKey.newBuilder()
        .setMultimapKeysValuesSideInput(
            StateKey.MultimapKeysValuesSideInput.newBuilder()
                .setTransformId("ptransformId")
                .setSideInputId("sideInputId")
                .setWindow(ByteString.copyFromUtf8("encodedWindow")))
        .build();
  }

  private StateKey key(byte[] key) throws IOException {
    ByteStringOutputStream out = new ByteStringOutputStream();
    ByteArrayCoder.of().encode(key, out);
    return StateKey.newBuilder()
        .setMultimapSideInput(
            StateKey.MultimapSideInput.newBuilder()
                .setTransformId("ptransformId")
                .setSideInputId("sideInputId")
                .setWindow(ByteString.copyFromUtf8("encodedWindow"))
                .setKey(out.toByteString()))
        .build();
  }
}
