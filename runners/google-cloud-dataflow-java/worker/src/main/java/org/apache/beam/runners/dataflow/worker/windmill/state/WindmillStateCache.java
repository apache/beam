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

import com.google.auto.value.AutoBuilder;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentMap;
import java.util.function.BiConsumer;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.apache.beam.runners.core.StateNamespace;
import org.apache.beam.runners.core.StateTag;
import org.apache.beam.runners.core.StateTags;
import org.apache.beam.runners.dataflow.worker.*;
import org.apache.beam.runners.dataflow.worker.status.BaseStatusServlet;
import org.apache.beam.runners.dataflow.worker.status.StatusDataProvider;
import org.apache.beam.runners.dataflow.worker.streaming.ShardedKey;
import org.apache.beam.sdk.state.State;
import org.apache.beam.sdk.util.Weighted;
import org.apache.beam.vendor.grpc.v1p60p1.com.google.protobuf.ByteString;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Equivalence;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.cache.Cache;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.cache.CacheBuilder;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.cache.CacheStats;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.MapMaker;
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
  private static final int STATE_CACHE_CONCURRENCY_LEVEL = 4;
  // Convert Megabytes to bytes
  private static final long MEGABYTES = 1024 * 1024;
  // Estimate of overhead per StateId.
  private static final long PER_STATE_ID_OVERHEAD = 28;
  // Initial size of hash tables per entry.
  private static final int INITIAL_HASH_MAP_CAPACITY = 4;
  // Overhead of each hash map entry.
  // https://appsintheopen.com/posts/52-the-memory-overhead-of-java-ojects
  private static final int HASH_MAP_ENTRY_OVERHEAD = 32;
  // Overhead of each StateCacheEntry.  One long, plus a hash table.
  private static final int PER_CACHE_ENTRY_OVERHEAD =
      8 + HASH_MAP_ENTRY_OVERHEAD * INITIAL_HASH_MAP_CAPACITY;

  private final Cache<StateId, StateCacheEntry> stateCache;
  // Contains the current valid ForKey object. Entries in the cache are keyed by ForKey with pointer
  // equality so entries may be invalidated by creating a new key object, rendering the previous
  // entries inaccessible. They will be evicted through normal cache operation.
  private final ConcurrentMap<WindmillComputationKey, ForKey> keyIndex;
  private final long workerCacheBytes; // Copy workerCacheMb and convert to bytes.
  private final boolean supportMapViaMultimap;

  WindmillStateCache(long sizeMb, boolean supportMapViaMultimap) {
    this.workerCacheBytes = sizeMb * MEGABYTES;
    this.stateCache =
        CacheBuilder.newBuilder()
            .maximumWeight(workerCacheBytes)
            .recordStats()
            .weigher(Weighers.weightedKeysAndValues())
            .concurrencyLevel(STATE_CACHE_CONCURRENCY_LEVEL)
            .build();
    this.keyIndex =
        new MapMaker().weakValues().concurrencyLevel(STATE_CACHE_CONCURRENCY_LEVEL).makeMap();
    this.supportMapViaMultimap = supportMapViaMultimap;
  }

  @AutoBuilder(ofClass = WindmillStateCache.class)
  public interface Builder {
    Builder setSizeMb(long sizeMb);

    Builder setSupportMapViaMultimap(boolean supportMapViaMultimap);

    WindmillStateCache build();
  }

  public static Builder builder() {
    return new AutoBuilder_WindmillStateCache_Builder().setSupportMapViaMultimap(false);
  }

  private EntryStats calculateEntryStats() {
    EntryStats stats = new EntryStats();
    BiConsumer<StateId, StateCacheEntry> consumer =
        (stateId, stateCacheEntry) -> {
          stats.entries++;
          stats.idWeight += stateId.getWeight();
          stats.entryWeight += stateCacheEntry.getWeight();
          stats.entryValues += stateCacheEntry.values.size();
          stats.maxEntryValues = Math.max(stats.maxEntryValues, stateCacheEntry.values.size());
        };
    stateCache.asMap().forEach(consumer);
    return stats;
  }

  public long getWeight() {
    EntryStats w = calculateEntryStats();
    return w.idWeight + w.entryWeight;
  }

  public long getMaxWeight() {
    return workerCacheBytes;
  }

  public CacheStats getCacheStats() {
    return stateCache.stats();
  }

  /** Returns a per-computation view of the state cache. */
  public ForComputation forComputation(String computation) {
    return new ForComputation(computation);
  }

  /** Print summary statistics of the cache to the given {@link PrintWriter}. */
  @Override
  public void appendSummaryHtml(PrintWriter response) {
    response.println("Cache Stats: <br><table>");
    response.println(
        "<tr><th>Hit Ratio</th><th>Evictions</th><th>Entries</th>"
            + "<th>Entry Values</th><th>Max Entry Values</th>"
            + "<th>Id Weight</th><th>Entry Weight</th><th>Max Weight</th><th>Keys</th>"
            + "</tr><tr>");
    CacheStats cacheStats = stateCache.stats();
    EntryStats entryStats = calculateEntryStats();
    response.println("<td>" + cacheStats.hitRate() + "</td>");
    response.println("<td>" + cacheStats.evictionCount() + "</td>");
    response.println("<td>" + entryStats.entries + "(" + stateCache.size() + " inc. weak) </td>");
    response.println("<td>" + entryStats.entryValues + "</td>");
    response.println("<td>" + entryStats.maxEntryValues + "</td>");
    response.println("<td>" + entryStats.idWeight / MEGABYTES + "MB</td>");
    response.println("<td>" + entryStats.entryWeight / MEGABYTES + "MB</td>");
    response.println("<td>" + getMaxWeight() / MEGABYTES + "MB</td>");
    response.println("<td>" + keyIndex.size() + "</td>");
    response.println("</tr></table><br>");
  }

  public BaseStatusServlet statusServlet() {
    return new BaseStatusServlet("/cachez") {
      @Override
      protected void doGet(HttpServletRequest request, HttpServletResponse response)
          throws IOException {
        PrintWriter writer = response.getWriter();
        writer.println("<h1>Cache Information</h1>");
        appendSummaryHtml(writer);
      }
    };
  }

  private static class EntryStats {
    long entries;
    long idWeight;
    long entryWeight;
    long entryValues;
    long maxEntryValues;
  }

  /**
   * Struct identifying a cache entry that contains all data for a ForKey instance and namespace.
   */
  private static class StateId implements Weighted {
    private final ForKey forKey;
    private final String stateFamily;
    private final Object namespaceKey;
    private final int hashCode;

    public StateId(ForKey forKey, String stateFamily, StateNamespace namespace) {
      this.forKey = forKey;
      this.stateFamily = stateFamily;
      this.namespaceKey = namespace.getCacheKey();
      this.hashCode = Objects.hash(forKey, stateFamily, namespaceKey);
    }

    @Override
    public boolean equals(@Nullable Object other) {
      if (this == other) {
        return true;
      }
      if (!(other instanceof StateId)) {
        return false;
      }
      StateId otherId = (StateId) other;
      return hashCode == otherId.hashCode
          && forKey == otherId.forKey
          && stateFamily.equals(otherId.stateFamily)
          && namespaceKey.equals(otherId.namespaceKey);
    }

    @Override
    public int hashCode() {
      return hashCode;
    }

    @Override
    public long getWeight() {
      return forKey.computationKey.key().size() + stateFamily.length() + PER_STATE_ID_OVERHEAD;
    }
  }

  /** Entry in the state cache that stores a map of values. */
  private static class StateCacheEntry implements Weighted {
    private final HashMap<NamespacedTag<?>, WeightedValue<?>> values;
    private long weight;

    public StateCacheEntry() {
      this.values = new HashMap<>(INITIAL_HASH_MAP_CAPACITY);
      this.weight = 0;
    }

    @SuppressWarnings("unchecked")
    public <T extends State> Optional<T> get(StateNamespace namespace, StateTag<T> tag) {
      return Optional.ofNullable((WeightedValue<T>) values.get(new NamespacedTag<>(namespace, tag)))
          .flatMap(WeightedValue::value);
    }

    public <T extends State> void put(
        StateNamespace namespace, StateTag<T> tag, T value, long weight) {
      values.compute(
          new NamespacedTag<>(namespace, tag),
          (t, v) -> {
            @SuppressWarnings("unchecked")
            WeightedValue<T> weightedValue = (WeightedValue<T>) v;
            if (weightedValue == null) {
              weightedValue = new WeightedValue<>();
              this.weight += HASH_MAP_ENTRY_OVERHEAD;
            } else {
              this.weight -= weightedValue.weight;
            }
            this.weight += weight;
            weightedValue.value = value;
            weightedValue.weight = weight;
            return weightedValue;
          });
    }

    @Override
    public long getWeight() {
      return weight + PER_CACHE_ENTRY_OVERHEAD;
    }

    // Even though we use the namespace at the higher cache level, we are only using the cacheKey.
    // That allows for grouped eviction of entries sharing a cacheKey but we require the full
    // namespace here to distinguish between grouped entries.
    private static class NamespacedTag<T extends State> {

      private final StateNamespace namespace;
      private final Equivalence.Wrapper<StateTag<T>> tag;

      NamespacedTag(StateNamespace namespace, StateTag<T> tag) {
        this.namespace = namespace;
        this.tag = StateTags.ID_EQUIVALENCE.wrap(tag);
      }

      @Override
      public boolean equals(@Nullable Object other) {
        if (other == this) {
          return true;
        }
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
      private long weight;
      private @Nullable T value;

      private Optional<T> value() {
        return Optional.ofNullable(this.value);
      }
    }
  }

  /** Per-computation view of the state cache. */
  public class ForComputation {

    private final String computation;

    private ForComputation(String computation) {
      this.computation = computation;
    }

    /** Returns the computation associated to this class. */
    public String getComputation() {
      return this.computation;
    }

    /** Invalidate all cache entries for this computation and {@code processingKey}. */
    public void invalidate(ByteString processingKey, long shardingKey) {
      WindmillComputationKey key =
          WindmillComputationKey.create(computation, processingKey, shardingKey);
      // By removing the ForKey object, all state for the key is orphaned in the cache and will
      // be removed by normal cache cleanup.
      keyIndex.remove(key);
    }

    public final void invalidate(ShardedKey shardedKey) {
      invalidate(shardedKey.key(), shardedKey.shardingKey());
    }

    /**
     * Returns a per-computation, per-key view of the state cache. Access to the cached data for
     * this key is not thread-safe. Callers should ensure that there is only a single ForKey object
     * in use at a time and that access to it is synchronized or single-threaded.
     */
    public ForKey forKey(WindmillComputationKey computationKey, long cacheToken, long workToken) {
      ForKey forKey = keyIndex.get(computationKey);
      if (forKey == null || !forKey.updateTokens(cacheToken, workToken)) {
        forKey = new ForKey(computationKey, cacheToken, workToken);
        // We prefer this implementation to using compute because that is implemented similarly for
        // ConcurrentHashMap with the downside of it performing inserts for unchanged existing
        // values as well.
        keyIndex.put(computationKey, forKey);
      }
      return forKey;
    }
  }

  /** Per-computation, per-key view of the state cache. */
  // Note that we utilize the default equality and hashCode for this class based upon the instance
  // (instead of the fields) to optimize cache invalidation.
  public class ForKey {
    private final WindmillComputationKey computationKey;
    // Cache token must be consistent for the key for the cache to be valid.
    private final long cacheToken;

    // The work token for processing must be greater than the last work token.  As work items are
    // increasing for a key, a less-than or equal to work token indicates that the current token is
    // for stale processing.
    private long workToken;

    private ForKey(WindmillComputationKey computationKey, long cacheToken, long workToken) {
      this.computationKey = computationKey;
      this.cacheToken = cacheToken;
      this.workToken = workToken;
    }

    /**
     * Returns a per-computation, per-key, per-family view of the state cache. Access to the cached
     * data for this key is not thread-safe. Callers should ensure that there is only a single
     * ForKeyAndFamily object in use at a time for a given computation, key, family tuple and that
     * access to it is synchronized or single-threaded.
     */
    public ForKeyAndFamily forFamily(String stateFamily) {
      return new ForKeyAndFamily(this, stateFamily);
    }

    private boolean updateTokens(long cacheToken, long workToken) {
      if (this.cacheToken != cacheToken || workToken <= this.workToken) {
        return false;
      }
      this.workToken = workToken;
      return true;
    }
  }

  /**
   * Per-computation, per-key, per-family view of the state cache. Modifications are cached locally
   * and must be flushed to the cache by calling persist. This class is not thread-safe.
   */
  public class ForKeyAndFamily {
    final ForKey forKey;
    final String stateFamily;
    private final HashMap<StateId, StateCacheEntry> localCache;

    private ForKeyAndFamily(ForKey forKey, String stateFamily) {
      this.forKey = forKey;
      this.stateFamily = stateFamily;
      localCache = new HashMap<>();
    }

    public String getStateFamily() {
      return stateFamily;
    }

    public boolean supportMapStateViaMultimapState() {
      return supportMapViaMultimap;
    }

    public <T extends State> Optional<T> get(StateNamespace namespace, StateTag<T> address) {
      @SuppressWarnings("nullness")
      // the mapping function for localCache.computeIfAbsent (i.e stateCache.getIfPresent) is
      // nullable.
      Optional<StateCacheEntry> stateCacheEntry =
          Optional.ofNullable(
              localCache.computeIfAbsent(
                  new StateId(forKey, stateFamily, namespace), stateCache::getIfPresent));

      return stateCacheEntry.flatMap(entry -> entry.get(namespace, address));
    }

    public <T extends State> void put(
        StateNamespace namespace, StateTag<T> address, T value, long weight) {
      StateId id = new StateId(forKey, stateFamily, namespace);
      @Nullable StateCacheEntry entry = localCache.get(id);
      if (entry == null) {
        entry = stateCache.getIfPresent(id);
        if (entry == null) {
          entry = new StateCacheEntry();
        }
        boolean hadValue = localCache.putIfAbsent(id, entry) != null;
        Preconditions.checkState(!hadValue);
      }
      entry.put(namespace, address, value, weight);
    }

    public void persist() {
      localCache.forEach(stateCache::put);
    }
  }

  @FunctionalInterface
  public interface PerComputationStateCacheFetcher {
    ForComputation forComputation(String computationId);
  }
}
