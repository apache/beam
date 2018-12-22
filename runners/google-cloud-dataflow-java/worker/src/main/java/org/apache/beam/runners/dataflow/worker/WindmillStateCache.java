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

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalCause;
import com.google.common.cache.Weigher;
import com.google.common.collect.HashMultimap;
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
import org.apache.beam.runners.dataflow.worker.status.BaseStatusServlet;
import org.apache.beam.runners.dataflow.worker.status.StatusDataProvider;
import org.apache.beam.sdk.state.State;
import org.apache.beam.sdk.util.Weighted;
import org.apache.beam.vendor.grpc.v1p13p1.com.google.protobuf.ByteString;

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
  // Estimate of overhead per StateId.
  private static final int PER_STATE_ID_OVERHEAD = 20;
  // Initial size of hash tables per entry.
  private static final int INITIAL_HASH_MAP_CAPACITY = 4;
  // Overhead of each hash map entry.
  private static final int HASH_MAP_ENTRY_OVERHEAD = 16;
  // Overhead of each cache entry.  Two longs, plus a hash table.
  private static final int PER_CACHE_ENTRY_OVERHEAD =
      16 + HASH_MAP_ENTRY_OVERHEAD * INITIAL_HASH_MAP_CAPACITY;

  private Cache<StateId, StateCacheEntry> stateCache;
  private HashMultimap<ComputationKey, StateId> keyIndex =
      HashMultimap.<ComputationKey, StateId>create();
  private int displayedWeight = 0; // Only used for status pages and unit tests.

  public WindmillStateCache() {
    final Weigher<Weighted, Weighted> weigher = Weighers.weightedKeysAndValues();

    stateCache =
        CacheBuilder.newBuilder()
            .maximumWeight(100000000 /* 100 MB */)
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

  /** Per-computation view of the state cache. */
  public class ForComputation {
    private final String computation;

    private ForComputation(String computation) {
      this.computation = computation;
    }

    /** Invalidate all cache entries for this computation and {@code processingKey}. */
    public void invalidate(ByteString processingKey) {
      synchronized (this) {
        ComputationKey key = new ComputationKey(computation, processingKey);
        for (StateId id : keyIndex.get(key)) {
          stateCache.invalidate(id);
        }
        keyIndex.removeAll(key);
      }
    }

    /** Returns a per-computation, per-key view of the state cache. */
    public ForKey forKey(ByteString key, String stateFamily, long cacheToken) {
      return new ForKey(computation, key, stateFamily, cacheToken);
    }
  }

  /** Per-computation, per-key view of the state cache. */
  public class ForKey {
    private final String computation;
    private final ByteString key;
    private final String stateFamily;
    private final long cacheToken;

    private ForKey(String computation, ByteString key, String stateFamily, long cacheToken) {
      this.computation = computation;
      this.key = key;
      this.stateFamily = stateFamily;
      this.cacheToken = cacheToken;
    }

    public <T extends State> T get(StateNamespace namespace, StateTag<T> address) {
      return WindmillStateCache.this.get(
          computation, key, stateFamily, cacheToken, namespace, address);
    }

    public <T extends State> void put(
        StateNamespace namespace, StateTag<T> address, T value, long weight) {
      WindmillStateCache.this.put(
          computation, key, stateFamily, cacheToken, namespace, address, value, weight);
    }
  }

  /** Returns a per-computation view of the state cache. */
  public ForComputation forComputation(String computation) {
    return new ForComputation(computation);
  }

  private <T extends State> T get(
      String computation,
      ByteString processingKey,
      String stateFamily,
      long token,
      StateNamespace namespace,
      StateTag<T> address) {
    StateId id = new StateId(computation, processingKey, stateFamily, namespace);
    StateCacheEntry entry = stateCache.getIfPresent(id);
    if (entry == null) {
      return null;
    }
    if (entry.getToken() != token) {
      stateCache.invalidate(id);
      return null;
    }
    return entry.get(namespace, address);
  }

  private <T extends State> void put(
      String computation,
      ByteString processingKey,
      String stateFamily,
      long token,
      StateNamespace namespace,
      StateTag<T> address,
      T value,
      long weight) {
    StateId id = new StateId(computation, processingKey, stateFamily, namespace);
    StateCacheEntry entry = stateCache.getIfPresent(id);
    if (entry == null) {
      synchronized (this) {
        keyIndex.put(id.getComputationKey(), id);
      }
    }
    if (entry == null || entry.getToken() != token) {
      entry = new StateCacheEntry(token);
      this.displayedWeight += (int) id.getWeight();
      this.displayedWeight += (int) entry.getWeight();
    }
    this.displayedWeight += (int) entry.put(namespace, address, value, weight);
    // Always add back to the cache to update the weight.
    stateCache.put(id, entry);
  }

  private static class ComputationKey {
    private final String computation;
    private final ByteString key;

    public ComputationKey(String computation, ByteString key) {
      this.computation = computation;
      this.key = key;
    }

    public ByteString getKey() {
      return key;
    }

    @Override
    public boolean equals(Object that) {
      if (that instanceof ComputationKey) {
        ComputationKey other = (ComputationKey) that;
        return computation.equals(other.computation) && key.equals(other.key);
      }
      return false;
    }

    @Override
    public int hashCode() {
      return Objects.hash(computation, key);
    }
  }

  /** Struct identifying a cache entry that contains all data for a key and namespace. */
  private static class StateId implements Weighted {
    private final ComputationKey computationKey;
    private final String stateFamily;
    private final Object namespaceKey;

    public StateId(
        String computation,
        ByteString processingKey,
        String stateFamily,
        StateNamespace namespace) {
      this.computationKey = new ComputationKey(computation, processingKey);
      this.stateFamily = stateFamily;
      this.namespaceKey = namespace.getCacheKey();
    }

    @Override
    public boolean equals(Object other) {
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
   * Entry in the state cache that stores a map of values and a token representing the validity of
   * the values.
   */
  private static class StateCacheEntry implements Weighted {
    private final long token;
    private final Map<NamespacedTag<?>, WeightedValue<?>> values;
    private long weight;

    public StateCacheEntry(long token) {
      this.values = new HashMap<>(INITIAL_HASH_MAP_CAPACITY);
      this.token = token;
      this.weight = 0;
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

    public long getToken() {
      return token;
    }

    private static class NamespacedTag<T extends State> {
      private final StateNamespace namespace;
      private final StateTag<T> tag;

      NamespacedTag(StateNamespace namespace, StateTag<T> tag) {
        this.namespace = namespace;
        this.tag = tag;
      }

      @Override
      public boolean equals(Object other) {
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

  /** Print summary statistics of the cache to the given {@link PrintWriter}. */
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
