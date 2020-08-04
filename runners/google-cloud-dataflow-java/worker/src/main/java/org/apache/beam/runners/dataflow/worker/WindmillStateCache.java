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
package org.apache.beam.runners.dataflow.worker;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.apache.beam.runners.core.StateNamespace;
import org.apache.beam.runners.core.StateTag;
import org.apache.beam.runners.core.StateTags;
import org.apache.beam.runners.dataflow.worker.status.BaseStatusServlet;
import org.apache.beam.runners.dataflow.worker.status.StatusDataProvider;
import org.apache.beam.sdk.state.State;
import org.apache.beam.sdk.util.Weighted;
import org.apache.beam.vendor.grpc.v1p26p0.com.google.protobuf.ByteString;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Equivalence;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.cache.Cache;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.cache.CacheBuilder;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.cache.RemovalCause;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.cache.Weigher;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.HashMultimap;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * Process-wide cache of per-key state.
 *
 * <p>This is backed by Guava {@link Cache} which is thread-safe. The entries are accessed often
 * from multiple threads. Logical consistency of each entry requires accessing each key (computation
 * * processing key * state_family * namespace) by a single thread at a time. {@link
 * StreamingDataflowWorker} ensures that a single computation * processing key is executing on one
 * thread at a time, so this is safe.
 */
public class WindmillStateCache implements StatusDataProvider {
  // Convert Megabytes to bytes
  private static final long MEGABYTES = 1024 * 1024;
  // Estimate of overhead per StateId.
  private static final int PER_STATE_ID_OVERHEAD = 20;
  // Initial size of hash tables per entry.
  private static final int INITIAL_HASH_MAP_CAPACITY = 4;
  // Overhead of each hash map entry.
  private static final int HASH_MAP_ENTRY_OVERHEAD = 16;
  // Overhead of each cache entry.  Three longs, plus a hash table.
  private static final int PER_CACHE_ENTRY_OVERHEAD =
      24 + HASH_MAP_ENTRY_OVERHEAD * INITIAL_HASH_MAP_CAPACITY;

  private Cache<StateId, StateCacheEntry> stateCache;
  private HashMultimap<ComputationKey, StateId> keyIndex =
      HashMultimap.<ComputationKey, StateId>create();
  private int displayedWeight = 0; // Only used for status pages and unit tests.
  private long workerCacheBytes; // Copy workerCacheMb and convert to bytes.

  public WindmillStateCache(Integer workerCacheMb) {
    final Weigher<Weighted, Weighted> weigher = Weighers.weightedKeysAndValues();
    workerCacheBytes = workerCacheMb * MEGABYTES;
    stateCache =
        CacheBuilder.newBuilder()
            .maximumWeight(workerCacheBytes)
            .recordStats()
            .weigher(weigher)
            .removalListener(
                removal -> {
                  if (removal.getCause() != RemovalCause.REPLACED) {
                    synchronized (this) {
                      StateId id = (StateId) removal.getKey();
                      if (removal.getCause() != RemovalCause.EXPLICIT) {
                        // When we invalidate a key explicitly, we'll also update the keyIndex, so
                        // no need to do it here.
                        keyIndex.remove(id.getComputationKey(), id);
                      }
                      displayedWeight -= weigher.weigh(id, removal.getValue());
                    }
                  }
                })
            .build();
  }

  public long getWeight() {
    return displayedWeight;
  }

  public long getMaxWeight() {
    return workerCacheBytes;
  }

  /** Per-computation view of the state cache. */
  public class ForComputation {

    private final String computation;

    private ForComputation(String computation) {
      this.computation = computation;
    }

    /**
     * Invalidate all cache entries for this computation and {@code processingKey}.
     */
    public void invalidate(ByteString processingKey, long shardingKey) {
      synchronized (this) {
        ComputationKey key = new ComputationKey(computation, processingKey, shardingKey);
        for (StateId id : keyIndex.removeAll(key)) {
          stateCache.invalidate(id);
        }
      }
    }

    /**
     * Returns a per-computation, per-key view of the state cache.
     */
    public ForKey forKey(ByteString key, long shardingKey, String stateFamily, long cacheToken,
        long workToken) {
      return new ForKey(computation, key, shardingKey, stateFamily, cacheToken, workToken);
    }
  }

  /**
   * Per-computation, per-key view of the state cache.
   */
  public class ForKey {

    private final String computation;
    private final ByteString key;
    private long shardingKey;
    private final String stateFamily;
    // Cache token must be consistent for the key for the cache to be valid.
    private final long cacheToken;

    // The work token for processing must be greater than the last work token.  As work items are
    // increasing for a key, a less-than or equal to work token indicates that the current token is
    // for stale processing. We don't use the cache so that fetches performed will fail with a no
    // longer valid work token.
    private final long workToken;

    private ForKey(
        String computation, ByteString key, long shardingKey, String stateFamily, long cacheToken,
        long workToken) {
      this.computation = computation;
      this.key = key;
      this.shardingKey = shardingKey;
      this.stateFamily = stateFamily;
      this.cacheToken = cacheToken;
      this.workToken = workToken;
    }

    public <T extends State> T get(StateNamespace namespace, StateTag<T> address) {
      return WindmillStateCache.this.get(
          computation, key, shardingKey, stateFamily, cacheToken, workToken, namespace, address);
    }

    // Note that once a value has been put for a given workToken, get calls with that same workToken
    // will fail. This is ok as we only put entries when we are building the commit and will no
    // longer be performing gets for the same work token.
    public <T extends State> void put(
        StateNamespace namespace, StateTag<T> address, T value, long weight) {
      WindmillStateCache.this.put(
          computation, key, shardingKey, stateFamily, cacheToken, workToken, namespace, address, value, weight);
    }
  }

  /**
   * Returns a per-computation view of the state cache.
   */
  public ForComputation forComputation(String computation) {
    return new ForComputation(computation);
  }

  private <T extends State> T get(
      String computation,
      ByteString processingKey,
      long shardingKey,
      String stateFamily,
      long cacheToken,
      long workToken,
      StateNamespace namespace,
      StateTag<T> address) {
    StateId id = new StateId(computation, processingKey, shardingKey, stateFamily, namespace);
    StateCacheEntry entry = stateCache.getIfPresent(id);
    if (entry == null) {
      return null;
    }
    if (entry.getCacheToken() != cacheToken) {
      stateCache.invalidate(id);
      return null;
    }
    if (workToken <= entry.getLastWorkToken()) {
      // We don't used the cached item but we don't invalidate it.
      return null;
    }
    return entry.get(namespace, address);
  }

  private <T extends State> void put(
      String computation,
      ByteString processingKey,
      long shardingKey,
      String stateFamily,
      long cacheToken,
      long workToken,
      StateNamespace namespace,
      StateTag<T> address,
      T value,
      long weight) {
    StateId id = new StateId(computation, processingKey, shardingKey, stateFamily, namespace);
    StateCacheEntry entry = stateCache.getIfPresent(id);
    if (entry == null) {
      synchronized (this) {
        keyIndex.put(id.getComputationKey(), id);
      }
    }
    if (entry == null || entry.getCacheToken() != cacheToken) {
      entry = new StateCacheEntry(cacheToken);
      this.displayedWeight += (int) id.getWeight();
      this.displayedWeight += (int) entry.getWeight();
    }
    entry.setLastWorkToken(workToken);
    this.displayedWeight += (int) entry.put(namespace, address, value, weight);
    // Always add back to the cache to update the weight.
    stateCache.put(id, entry);
  }

  private static class ComputationKey {

    private final String computation;
    private final ByteString key;
    private final long shardingKey;

    public ComputationKey(String computation, ByteString key, long shardingKey) {
      this.computation = computation;
      this.key = key;
      this.shardingKey = shardingKey;
    }

    public ByteString getKey() {
      return key;
    }

    @Override
    public boolean equals(@Nullable Object that) {
      if (that instanceof ComputationKey) {
        ComputationKey other = (ComputationKey) that;
        return computation.equals(other.computation) && key.equals(other.key)
            && shardingKey == other.shardingKey;
      }
      return false;
    }

    @Override
    public int hashCode() {
      return Objects.hash(computation, key, shardingKey);
    }
  }

  /**
   * Struct identifying a cache entry that contains all data for a key and namespace.
   */
  private static class StateId implements Weighted {

    private final ComputationKey computationKey;
    private final String stateFamily;
    private final Object namespaceKey;

    public StateId(
        String computation,
        ByteString processingKey,
        long shardingKey,
        String stateFamily,
        StateNamespace namespace) {
      this.computationKey = new ComputationKey(computation, processingKey, shardingKey);
      this.stateFamily = stateFamily;
      this.namespaceKey = namespace.getCacheKey();
    }

    @Override
    public boolean equals(@Nullable Object other) {
      if (other instanceof StateId) {
        StateId otherId = (StateId) other;
        return computationKey.equals(otherId.computationKey)
            && stateFamily.equals(otherId.stateFamily)
            && namespaceKey.equals(otherId.namespaceKey);
      }
      return false;
    }

    public ComputationKey getComputationKey() {
      return computationKey;
    }

    @Override
    public int hashCode() {
      return Objects.hash(computationKey, namespaceKey);
    }

    @Override
    public long getWeight() {
      return (long) computationKey.getKey().size() + PER_STATE_ID_OVERHEAD;
    }
  }

  /**
   * Entry in the state cache that stores a map of values, a cache token representing the validity
   * of the values, and a work token that is increasing to ensure sequential processing.
   */
  private static class StateCacheEntry implements Weighted {

    private final long cacheToken;
    private long lastWorkToken;
    private final Map<NamespacedTag<?>, WeightedValue<?>> values;
    private long weight;

    public StateCacheEntry(long cacheToken) {
      this.values = new HashMap<>(INITIAL_HASH_MAP_CAPACITY);
      this.cacheToken = cacheToken;
      this.lastWorkToken = Long.MIN_VALUE;
      this.weight = 0;
    }

    public void setLastWorkToken(long workToken) {
      this.lastWorkToken = workToken;
    }

    @SuppressWarnings("unchecked")
    public <T extends State> T get(StateNamespace namespace, StateTag<T> tag) {
      WeightedValue<T> weightedValue =
          (WeightedValue<T>) values.get(new NamespacedTag<>(namespace, tag));
      return weightedValue == null ? null : weightedValue.value;
    }

    public <T extends State> long put(
        StateNamespace namespace, StateTag<T> tag, T value, long weight) {
      @SuppressWarnings("unchecked")
      WeightedValue<T> weightedValue =
          (WeightedValue<T>) values.get(new NamespacedTag<>(namespace, tag));
      long weightDelta = 0;
      if (weightedValue == null) {
        weightedValue = new WeightedValue<>();
        weightDelta += HASH_MAP_ENTRY_OVERHEAD;
      } else {
        weightDelta -= weightedValue.weight;
      }
      weightedValue.value = value;
      weightedValue.weight = weight;
      weightDelta += weight;
      this.weight += weightDelta;
      values.put(new NamespacedTag<>(namespace, tag), weightedValue);
      return weightDelta;
    }

    @Override
    public long getWeight() {
      return weight + PER_CACHE_ENTRY_OVERHEAD;
    }

    public long getCacheToken() {
      return cacheToken;
    }

    public long getLastWorkToken() {
      return lastWorkToken;
    }

    private static class NamespacedTag<T extends State> {

      private final StateNamespace namespace;
      private final Equivalence.Wrapper<StateTag> tag;

      NamespacedTag(StateNamespace namespace, StateTag<T> tag) {
        this.namespace = namespace;
        this.tag = StateTags.ID_EQUIVALENCE.wrap((StateTag) tag);
      }

      @Override
      public boolean equals(@Nullable Object other) {
        if (!(other instanceof NamespacedTag)) {
          return false;
        }
        NamespacedTag<?> that = (NamespacedTag<?>) other;
        return namespace.equals(that.namespace) && tag.equals(that.tag);
      }

      @Override
      public int hashCode() {
        return Objects.hash(namespace, tag);
      }
    }

    private static class WeightedValue<T> {

      public long weight = 0;
      public T value = null;
    }
  }

  /**
   * Print summary statistics of the cache to the given {@link PrintWriter}.
   */
  @Override
  public void appendSummaryHtml(PrintWriter response) {
    response.println("Cache Stats: <br><table border=0>");
    response.println(
        "<tr><th>Hit Ratio</th><th>Evictions</th><th>Size</th><th>Weight</th></tr><tr>");
    response.println("<th>" + stateCache.stats().hitRate() + "</th>");
    response.println("<th>" + stateCache.stats().evictionCount() + "</th>");
    response.println("<th>" + stateCache.size() + "</th>");
    response.println("<th>" + getWeight() + "</th>");
    response.println("</tr></table><br>");
  }

  public BaseStatusServlet statusServlet() {
    return new BaseStatusServlet("/cachez") {
      @Override
      protected void doGet(HttpServletRequest request, HttpServletResponse response)
          throws IOException, ServletException {

        PrintWriter writer = response.getWriter();
        writer.println("<h1>Cache Information</h1>");
        appendSummaryHtml(writer);
      }
    };
  }
}
