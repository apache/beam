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
package org.apache.beam.fn.harness.jmh;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.beam.fn.harness.Cache;
import org.apache.beam.fn.harness.Caches;
import org.apache.beam.fn.harness.Caches.ClearableCache;
import org.apache.beam.fn.harness.state.BagUserState;
import org.apache.beam.fn.harness.state.BeamFnStateClient;
import org.apache.beam.fn.harness.state.MultimapSideInput;
import org.apache.beam.fn.harness.state.MultimapUserState;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.StateKey;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.StateResponse;
import org.apache.beam.sdk.coders.IterableCoder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.SdkHarnessOptions;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.vendor.grpc.v1p69p0.com.google.protobuf.ByteString;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.infra.Blackhole;

/**
 * Microbenchmarks for Java SDK Harness {@link Caches}, {@link ClearableCache}, {@link
 * BagUserState}, {@link MultimapUserState}, and {@link MultimapSideInput}.
 */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
public class CachesAndStateBenchmark {

  @State(Scope.Thread)
  public static class HarnessCacheState {
    final String sampleString = "ValuesForKey-transform-12345";
    final ByteString sampleByteString =
        ByteString.copyFromUtf8("encoded-user-key-0123456789-abcdef");
    final byte[] sampleByteArray = new byte[128];
    final Long sampleLong = 123456789L;

    Cache<Object, Object> processWideCache;
    Cache<Object, Object> sideInputCache;
    Cache<Object, Object> clearableParentCache;
    StateKey bagStateKey;
    StateKey multimapStateKey;
    StateKey multimapSideInputKey;
    BeamFnStateClient noopStateClient;
    BeamFnStateClient sideInputStateClient;
    ByteString encodedBulkSideInput;
    final AtomicLong bundleCounter = new AtomicLong();
    List<KV<String, String>> kvBatch;

    @Setup
    public void setUp() throws IOException {
      PipelineOptions opts64 = PipelineOptionsFactory.create();
      opts64.as(SdkHarnessOptions.class).setMaxCacheMemoryUsageMb(64);
      processWideCache = Caches.fromOptions(opts64);
      sideInputCache = Caches.fromOptions(opts64);

      PipelineOptions opts4 = PipelineOptionsFactory.create();
      opts4.as(SdkHarnessOptions.class).setMaxCacheMemoryUsageMb(4);
      clearableParentCache = Caches.fromOptions(opts4);

      // Pre-populate a lookup target in sideInputCache
      Caches.subCache(sideInputCache, "ValuesForKey", sampleByteString).put("k", "v");

      bagStateKey =
          StateKey.newBuilder()
              .setBagUserState(
                  StateKey.BagUserState.newBuilder()
                      .setTransformId("t1")
                      .setUserStateId("bag")
                      .setWindow(ByteString.EMPTY)
                      .setKey(sampleByteString))
              .build();

      multimapStateKey =
          StateKey.newBuilder()
              .setMultimapKeysUserState(
                  StateKey.MultimapKeysUserState.newBuilder()
                      .setTransformId("t1")
                      .setUserStateId("mmap")
                      .setWindow(ByteString.EMPTY)
                      .setKey(sampleByteString))
              .build();

      multimapSideInputKey =
          StateKey.newBuilder()
              .setMultimapKeysSideInput(
                  StateKey.MultimapKeysSideInput.newBuilder()
                      .setTransformId("t1")
                      .setSideInputId("si")
                      .setWindow(ByteString.EMPTY))
              .build();

      noopStateClient =
          requestBuilder -> CompletableFuture.completedFuture(StateResponse.getDefaultInstance());

      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      KvCoder<String, Iterable<String>> entryCoder =
          KvCoder.of(StringUtf8Coder.of(), IterableCoder.of(StringUtf8Coder.of()));
      for (int i = 0; i < 20; i++) {
        entryCoder.encode(KV.of("key-" + i, Collections.singletonList("val-" + i)), baos);
      }
      encodedBulkSideInput = ByteString.copyFrom(baos.toByteArray());

      sideInputStateClient =
          requestBuilder -> {
            StateResponse.Builder resp = StateResponse.newBuilder();
            resp.getGetBuilder().setData(encodedBulkSideInput);
            return CompletableFuture.completedFuture(resp.build());
          };

      kvBatch = new ArrayList<>(16);
      for (int i = 0; i < 16; i++) {
        kvBatch.add(KV.of("k-" + i, "payload-value-for-bag-state-" + i));
      }
    }
  }

  @Benchmark
  public long weighString(HarnessCacheState s) {
    return Caches.weigh(s.sampleString);
  }

  @Benchmark
  public long weighByteString(HarnessCacheState s) {
    return Caches.weigh(s.sampleByteString);
  }

  @Benchmark
  public long weighByteArray(HarnessCacheState s) {
    return Caches.weigh(s.sampleByteArray);
  }

  @Benchmark
  public long weighLong(HarnessCacheState s) {
    return Caches.weigh(s.sampleLong);
  }

  @Benchmark
  public Object subCacheLookup(HarnessCacheState s) {
    return Caches.subCache(s.sideInputCache, "ValuesForKey", s.sampleByteString).peek("k");
  }

  @Benchmark
  public void clearableNestedSubCacheLifecycle(HarnessCacheState s, Blackhole bh) {
    long id = s.bundleCounter.incrementAndGet();
    ClearableCache<Object, Object> bundleCache =
        new ClearableCache<>(Caches.subCache(s.clearableParentCache, "Bundle", "inst-" + id));
    Cache<Object, Object> stateSubCache = Caches.subCache(bundleCache, s.bagStateKey);
    stateSubCache.put("entry", s.sampleByteArray);
    bh.consume(stateSubCache.peek("entry"));
    bundleCache.clear();
  }

  @Benchmark
  public void bagUserStateClearAppendClose(HarnessCacheState s) throws Exception {
    Cache<Object, Object> userCache = Caches.subCache(s.processWideCache, "BagBench");
    BagUserState<KV<String, String>> bag =
        new BagUserState<>(
            userCache,
            s.noopStateClient,
            "inst-1",
            s.bagStateKey,
            KvCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of()));
    bag.clear();
    for (KV<String, String> item : s.kvBatch) {
      bag.append(item);
    }
    bag.asyncClose();
  }

  @Benchmark
  public void multimapUserStateClearPutClose(HarnessCacheState s) throws Exception {
    Cache<Object, Object> userCache = Caches.subCache(s.processWideCache, "MmapBench");
    MultimapUserState<String, String> mmap =
        new MultimapUserState<>(
            userCache,
            s.noopStateClient,
            "inst-1",
            s.multimapStateKey,
            StringUtf8Coder.of(),
            StringUtf8Coder.of());
    mmap.clear();
    for (KV<String, String> item : s.kvBatch) {
      mmap.put(item.getKey(), item.getValue());
    }
    mmap.asyncClose();
  }

  @Benchmark
  public void multimapSideInputBulkRead(HarnessCacheState s, Blackhole bh) {
    Cache<Object, Object> siCache = Caches.subCache(s.sideInputCache, "BulkSiBench");
    MultimapSideInput<String, String> si =
        new MultimapSideInput<>(
            siCache,
            s.sideInputStateClient,
            "inst-1",
            s.multimapSideInputKey,
            StringUtf8Coder.of(),
            StringUtf8Coder.of(),
            true);
    bh.consume(si.get("key-5"));
  }
}
