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

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import org.apache.beam.fn.harness.Cache;
import org.apache.beam.fn.harness.Caches;
import org.apache.beam.fn.harness.Caches.ClearableCache;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.StateKey;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.SdkHarnessOptions;
import org.apache.beam.sdk.util.ByteStringOutputStream;
import org.apache.beam.sdk.util.WeightedValue;
import org.apache.beam.vendor.grpc.v1p69p0.com.google.protobuf.ByteString;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Threads;

/** Microbenchmarks for Java SDK Harness {@link Caches} and {@link ClearableCache}. */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
public class CachesBenchmark {

  private static final int MULTI_KEY_COUNT = 1024;
  private static final int EVICTION_KEY_COUNT = 64;

  /** Thread-local benchmark fixtures. */
  @State(Scope.Thread)
  public static class HarnessCacheState {
    // Non-final Object references prevent HotSpot C2 from folding Caches.weigh() based on
    // concrete static field types.
    Object sampleString;
    Object sampleLargeString;
    Object sampleUtf16String;
    Object sampleByteString;
    Object sampleBoundedByteString;
    Object sampleByteArray;
    Object sampleLong;
    Object[] mixedWeighObjects;
    ByteString samplePrefixByteString;
    String[] multiKeys;
    byte[] churnPayload;

    Cache<Object, Object> sideInputCache;
    Cache<String, String> prebuiltMultiKeySubCache;
    Cache<Object, Object> clearableParentCache;
    Cache<Object, Object> tinyEvictionParentCache;
    StateKey bagStateKey;

    long bundleCounter;
    int lookupIdx;
    int weighIdx;

    /** Initializes caches, keys, and sample objects for weighing and sub-cache benchmarks. */
    @Setup(Level.Trial)
    public void setUp() throws IOException {
      sampleString = "ValuesForKey-transform-12345";
      sampleLargeString = "x".repeat(16 * 1024);
      sampleUtf16String = "ValuesForKey-\u4e2d\u6587-12345";
      samplePrefixByteString = ByteString.copyFromUtf8("encoded-user-key-0123456789-abcdef");
      sampleByteString = samplePrefixByteString;

      try (ByteStringOutputStream bsos = new ByteStringOutputStream()) {
        bsos.write(new byte[] {1, 2, 3, 4});
        sampleBoundedByteString = bsos.toByteString();
      }

      sampleByteArray = new byte[128];
      sampleLong = 123456789L;
      churnPayload = new byte[32 * 1024];

      bagStateKey =
          StateKey.newBuilder()
              .setBagUserState(
                  StateKey.BagUserState.newBuilder()
                      .setTransformId("t1")
                      .setUserStateId("bag")
                      .setWindow(ByteString.EMPTY)
                      .setKey(samplePrefixByteString))
              .build();

      // 16-entry power-of-two mixed type array for polymorphic weigh() profiling.
      mixedWeighObjects =
          new Object[] {
            sampleString,
            sampleByteString,
            sampleByteArray,
            sampleLong,
            42,
            1.5d,
            sampleUtf16String,
            WeightedValue.of("w", 64L),
            TimeUnit.SECONDS,
            null,
            sampleBoundedByteString,
            bagStateKey,
            Boolean.TRUE,
            "short",
            new byte[32],
            ByteString.copyFromUtf8("key")
          };

      PipelineOptions opts64 = PipelineOptionsFactory.create();
      opts64.as(SdkHarnessOptions.class).setMaxCacheMemoryUsageMb(64);
      sideInputCache = Caches.fromOptions(opts64);

      PipelineOptions opts4 = PipelineOptionsFactory.create();
      opts4.as(SdkHarnessOptions.class).setMaxCacheMemoryUsageMb(4);
      clearableParentCache = Caches.fromOptions(opts4);

      PipelineOptions opts1 = PipelineOptionsFactory.create();
      opts1.as(SdkHarnessOptions.class).setMaxCacheMemoryUsageMb(1);
      tinyEvictionParentCache = Caches.fromOptions(opts1);

      prebuiltMultiKeySubCache =
          Caches.subCache(sideInputCache, "ValuesForKey", samplePrefixByteString);
      multiKeys = new String[MULTI_KEY_COUNT];
      for (int i = 0; i < MULTI_KEY_COUNT; i++) {
        String key = "key-" + i;
        multiKeys[i] = key;
        prebuiltMultiKeySubCache.put(key, "val-" + i);
      }
    }
  }

  /** Shared cache fixture for multi-threaded contention benchmarks. */
  @State(Scope.Benchmark)
  public static class SharedCacheState {
    Cache<Object, Object> rootCache;
    Cache<String, String> sharedSubCache;
    String[] keys;
    byte[] payload;

    /** Initializes a shared 64 MB cache populated with 1024 entries. */
    @Setup(Level.Trial)
    public void setUp() {
      PipelineOptions opts = PipelineOptionsFactory.create();
      opts.as(SdkHarnessOptions.class).setMaxCacheMemoryUsageMb(64);
      rootCache = Caches.fromOptions(opts);
      sharedSubCache = Caches.subCache(rootCache, "SharedValues", "t1");
      keys = new String[MULTI_KEY_COUNT];
      for (int i = 0; i < MULTI_KEY_COUNT; i++) {
        keys[i] = "shared-key-" + i;
        sharedSubCache.put(keys[i], "shared-val-" + i);
      }
      payload = new byte[128];
    }
  }

  /** Thread-local index cursor for shared benchmark states. */
  @State(Scope.Thread)
  public static class CursorState {
    String threadPrefix;
    int index;
    long bundleCounter;

    /** Captures a distinct per-thread namespace prefix for contended cache writes. */
    @Setup(Level.Trial)
    public void setUp() {
      threadPrefix = Thread.currentThread().getName();
    }
  }

  @Benchmark
  public long weighString(HarnessCacheState s) {
    return Caches.weigh(s.sampleString);
  }

  @Benchmark
  public long weighLargeString(HarnessCacheState s) {
    return Caches.weigh(s.sampleLargeString);
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
  public long weighMixed(HarnessCacheState s) {
    int idx = s.weighIdx++ & 15;
    return Caches.weigh(s.mixedWeighObjects[idx]);
  }

  @Benchmark
  public Object subCacheLookupMultiKey(HarnessCacheState s) {
    String key = s.multiKeys[s.lookupIdx++ & (MULTI_KEY_COUNT - 1)];
    return s.prebuiltMultiKeySubCache.peek(key);
  }

  @Benchmark
  public Object subCacheCreateAndLookup(HarnessCacheState s) {
    String key = s.multiKeys[s.lookupIdx++ & (MULTI_KEY_COUNT - 1)];
    return Caches.subCache(s.sideInputCache, "ValuesForKey", s.samplePrefixByteString).peek(key);
  }

  @Benchmark
  @Threads(4)
  public Object sharedSubCacheLookupContended(SharedCacheState shared, CursorState cursor) {
    String key = shared.keys[cursor.index++ & (MULTI_KEY_COUNT - 1)];
    return shared.sharedSubCache.peek(key);
  }

  @Benchmark
  @Threads(4)
  public Object sharedClearablePutClearContended(SharedCacheState shared, CursorState cursor) {
    long bundleId = cursor.bundleCounter++;
    ClearableCache<Object, Object> bundleCache =
        new ClearableCache<>(
            Caches.subCache(shared.rootCache, "Contended", cursor.threadPrefix, bundleId));
    Object last = null;
    for (int i = 0; i < 4; i++) {
      String key = shared.keys[i];
      bundleCache.put(key, shared.payload);
      last = bundleCache.peek(key);
    }
    bundleCache.clear();
    return last;
  }

  @Benchmark
  public Object clearableNestedSubCacheLifecycle(HarnessCacheState s) {
    long id = s.bundleCounter++;
    ClearableCache<Object, Object> bundleCache =
        new ClearableCache<>(Caches.subCache(s.clearableParentCache, "Bundle", id));
    Cache<Object, Object> stateSubCache = Caches.subCache(bundleCache, s.bagStateKey);
    Object last = null;
    for (int i = 0; i < 8; i++) {
      String key = s.multiKeys[i];
      stateSubCache.put(key, s.sampleByteArray);
      last = stateSubCache.peek(key);
    }
    bundleCache.clear();
    return last;
  }

  @Benchmark
  public int clearableEvictionChurn(HarnessCacheState s) {
    long id = s.bundleCounter++;
    ClearableCache<Object, Object> bundleCache =
        new ClearableCache<>(Caches.subCache(s.tinyEvictionParentCache, "Evict", id));
    Cache<Object, Object> child = Caches.subCache(bundleCache, "Child");
    // 64 * 32 KB (2 MB) inserts into a 1 MB parent force evictions before clear().
    for (int i = 0; i < EVICTION_KEY_COUNT; i++) {
      child.put(s.multiKeys[i], s.churnPayload);
    }
    bundleCache.clear();
    return EVICTION_KEY_COUNT;
  }
}
