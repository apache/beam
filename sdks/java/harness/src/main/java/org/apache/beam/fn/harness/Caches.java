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
package org.apache.beam.fn.harness;

import java.util.Arrays;
import java.util.Collections;
import java.util.Objects;
import java.util.Set;
import java.util.WeakHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Function;
import org.apache.beam.fn.harness.Cache.Shrinkable;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.SdkHarnessOptions;
import org.apache.beam.sdk.util.Weighted;
import org.apache.beam.sdk.util.WeightedValue;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.cache.CacheBuilder;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.cache.CacheStats;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.cache.RemovalListener;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.cache.RemovalNotification;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.cache.Weigher;
import org.github.jamm.MemoryMeter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Utility methods used to instantiate and operate over cache instances. */
@SuppressWarnings("nullness")
public final class Caches {
  private static final Logger LOG = LoggerFactory.getLogger(Caches.class);

  /**
   * Object sizes will always be rounded up to the next multiple of {@code 2^WEIGHT_RATIO} when
   * stored in the cache. This allows us to work around the limit on the Guava cache method which
   * only allows int weights by scaling object sizes appropriately.
   */
  @VisibleForTesting static final int WEIGHT_RATIO = 6;

  /** All objects less than or equal to this size will account for 1. */
  private static final long MIN_OBJECT_SIZE = 1 << WEIGHT_RATIO;

  /**
   * Objects which change in this amount should always update the cache.
   *
   * <p>The limit of 2^16 is chosen to be small enough such that objects will be close enough if
   * they change frequently. Future work could scale these ratios based upon the configured cache
   * size.
   */
  private static final long CACHE_SIZE_CHANGE_LIMIT_BYTES = 1 << 16;

  private static final MemoryMeter MEMORY_METER = MemoryMeter.builder().build();

  /** The size of a reference. */
  public static final long REFERENCE_SIZE = 8;

  /** Returns the amount of memory in bytes the provided object consumes. */
  public static long weigh(Object o) {
    if (o == null) {
      return REFERENCE_SIZE;
    }
    try {
      return MEMORY_METER.measureDeep(o);
    } catch (RuntimeException e) {
      // Checking for RuntimeException since java.lang.reflect.InaccessibleObjectException is only
      // available starting Java 9
      LOG.warn("JVM prevents jamm from accessing subgraph - cache sizes may be underestimated", e);
      return MEMORY_METER.measure(o);
    }
  }

  /**
   * Returns whether the cache should be updated in the case where the objects size has changed.
   *
   * <p>Note that this should only be used in the case where the cache is being updated very often
   * in a tight loop and is not a good fit for cases where the object being cached is the result of
   * an expensive operation like a disk read or remote service call.
   */
  public static boolean shouldUpdateOnSizeChange(long oldSize, long newSize) {
    /*
    Our strategy is three fold:
    - tiny objects don't impact the cache accounting and count as a size of `1` in the cache.
    - large changes (>= CACHE_SIZE_CHANGE_LIMIT_BYTES) should always update the size
    - all others if the size changed by a factor of 2
    */
    return (oldSize > MIN_OBJECT_SIZE || newSize > MIN_OBJECT_SIZE)
        && ((newSize - oldSize >= CACHE_SIZE_CHANGE_LIMIT_BYTES)
            || Long.highestOneBit(oldSize) != Long.highestOneBit(newSize));
  }

  /** An eviction listener that reduces the size of entries that are {@link Shrinkable}. */
  @VisibleForTesting
  static class ShrinkOnEviction implements RemovalListener<CompositeKey, WeightedValue<Object>> {

    private final org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.cache.Cache<
            CompositeKey, WeightedValue<Object>>
        cache;
    private final LongAdder weightInBytes;

    ShrinkOnEviction(
        CacheBuilder<CompositeKey, WeightedValue<Object>> cacheBuilder, LongAdder weightInBytes) {
      this.cache = cacheBuilder.removalListener(this).build();
      this.weightInBytes = weightInBytes;
    }

    public org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.cache.Cache<
            CompositeKey, WeightedValue<Object>>
        getCache() {
      return cache;
    }

    @Override
    public void onRemoval(
        RemovalNotification<CompositeKey, WeightedValue<Object>> removalNotification) {
      weightInBytes.add(
          -(removalNotification.getKey().getWeight() + removalNotification.getValue().getWeight()));
      if (removalNotification.wasEvicted()) {
        if (!(removalNotification.getValue().getValue() instanceof Cache.Shrinkable)) {
          return;
        }
        Object updatedEntry = ((Shrinkable<?>) removalNotification.getValue().getValue()).shrink();
        if (updatedEntry != null) {
          cache.put(
              removalNotification.getKey(),
              addWeightedValue(removalNotification.getKey(), updatedEntry, weightInBytes));
        }
      }
    }
  }

  /** A cache that never stores any values. */
  public static <K, V> Cache<K, V> noop() {
    return forMaximumBytes(0L);
  }

  /** A cache that never evicts any values. */
  public static <K, V> Cache<K, V> eternal() {
    return forMaximumBytes(Long.MAX_VALUE);
  }

  /**
   * Uses the specified {@link PipelineOptions} to configure and return a cache instance based upon
   * parameters within {@link SdkHarnessOptions}.
   */
  public static <K, V> Cache<K, V> fromOptions(PipelineOptions options) {
    return forMaximumBytes(
        ((long) options.as(SdkHarnessOptions.class).getMaxCacheMemoryUsageMb()) << 20);
  }

  /**
   * Returns a view of a cache that operates on keys with a specified key prefix.
   *
   * <p>All lookups, insertions, and removals into the parent {@link Cache} will be prefixed by the
   * specified prefixes.
   */
  public static <K, V> Cache<K, V> subCache(
      Cache<?, ?> cache, Object keyPrefix, Object... additionalKeyPrefix) {
    if (cache instanceof SubCache) {
      return new SubCache<>(
          ((SubCache<?, ?>) cache).cache,
          ((SubCache<?, ?>) cache).keyPrefix.subKey(keyPrefix, additionalKeyPrefix),
          ((SubCache<?, ?>) cache).maxWeightInBytes,
          ((SubCache<?, ?>) cache).weightInBytes);
    }
    throw new IllegalArgumentException(
        String.format(
            "An unsupported type of cache was passed in. Received %s.",
            cache == null ? "null" : cache.getClass()));
  }

  @VisibleForTesting
  static <K, V> Cache<K, V> forMaximumBytes(long maximumBytes) {
    // We specifically use Guava cache since it allows for recursive computeIfAbsent calls
    // preventing deadlock from occurring when a loading function mutates the underlying cache
    LongAdder weightInBytes = new LongAdder();
    return new SubCache<>(
        new ShrinkOnEviction(
                CacheBuilder.newBuilder()
                    .maximumWeight(maximumBytes >> WEIGHT_RATIO)
                    .weigher(
                        new Weigher<CompositeKey, WeightedValue<Object>>() {

                          @Override
                          public int weigh(CompositeKey key, WeightedValue<Object> value) {
                            // Round up to the next closest multiple of WEIGHT_RATIO
                            long size =
                                ((key.getWeight() + value.getWeight() - 1) >> WEIGHT_RATIO) + 1;
                            if (size > Integer.MAX_VALUE) {
                              LOG.warn(
                                  "Entry with size {} MiBs inserted into the cache. This is larger than the maximum individual entry size of {} MiBs. The cache will under report its memory usage by the difference. This may lead to OutOfMemoryErrors.",
                                  ((size - 1) >> 20) + 1,
                                  2 << (WEIGHT_RATIO + 10));
                              return Integer.MAX_VALUE;
                            }
                            return (int) size;
                          }
                        })
                    // The maximum size of an entry in the cache is maxWeight / concurrencyLevel
                    // which is why we set the concurrency level to 1. See
                    // https://github.com/google/guava/issues/3462 for further details.
                    //
                    // The PrecombineGroupingTable showed contention here since it was working in
                    // a tight loop. We were able to resolve the contention by reducing the
                    // frequency of updates. Reconsider this value if we could solve the maximum
                    // entry size issue. Note that using Runtime.getRuntime().availableProcessors()
                    // is subject to docker CPU shares issues
                    // (https://bugs.openjdk.org/browse/JDK-8281181).
                    //
                    // We could revisit the caffeine cache library based upon reinvestigating
                    // recursive computeIfAbsent calls since it doesn't have this limit.
                    .concurrencyLevel(1)
                    .recordStats(),
                weightInBytes)
            .getCache(),
        CompositeKeyPrefix.ROOT,
        maximumBytes,
        weightInBytes);
  }

  private static long findWeight(Object o) {
    if (o instanceof WeightedValue) {
      return ((WeightedValue<Object>) o).getWeight();
    } else if (o instanceof Weighted) {
      return ((Weighted) o).getWeight();
    } else {
      return weigh(o);
    }
  }

  private static WeightedValue<Object> addWeightedValue(
      CompositeKey key, Object o, LongAdder weightInBytes) {
    WeightedValue<Object> rval;
    if (o instanceof WeightedValue) {
      rval = (WeightedValue<Object>) o;
    } else if (o instanceof Weighted) {
      rval = WeightedValue.of(o, ((Weighted) o).getWeight());
    } else {
      rval = WeightedValue.of(o, weigh(o));
    }
    weightInBytes.add(key.getWeight() + rval.getWeight());
    return rval;
  }

  /**
   * A view of a cache that operates on keys with a specified key prefix.
   *
   * <p>All lookups, insertions, and removals into the parent {@link Cache} will be prefixed by the
   * specified prefixes.
   */
  private static class SubCache<K, V> implements Cache<K, V> {
    private final org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.cache.Cache<
            CompositeKey, WeightedValue<Object>>
        cache;
    private final CompositeKeyPrefix keyPrefix;
    private final long maxWeightInBytes;
    private final LongAdder weightInBytes;

    SubCache(
        org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.cache.Cache<
                CompositeKey, WeightedValue<Object>>
            cache,
        CompositeKeyPrefix keyPrefix,
        long maxWeightInBytes,
        LongAdder weightInBytes) {
      this.cache = cache;
      this.keyPrefix = keyPrefix;
      this.maxWeightInBytes = maxWeightInBytes;
      this.weightInBytes = weightInBytes;
    }

    @Override
    public V peek(K key) {
      WeightedValue<Object> value = cache.getIfPresent(keyPrefix.valueKey(key));
      if (value == null) {
        return null;
      }
      return (V) value.getValue();
    }

    @Override
    public V computeIfAbsent(K key, Function<K, V> loadingFunction) {
      try {
        CompositeKey compositeKey = keyPrefix.valueKey(key);
        return (V)
            cache
                .get(
                    compositeKey,
                    () -> addWeightedValue(compositeKey, loadingFunction.apply(key), weightInBytes))
                .getValue();
      } catch (ExecutionException e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    public void put(K key, V value) {
      CompositeKey compositeKey = keyPrefix.valueKey(key);
      cache.put(compositeKey, addWeightedValue(compositeKey, value, weightInBytes));
    }

    @Override
    public void remove(K key) {
      cache.invalidate(keyPrefix.valueKey(key));
    }

    @Override
    public String describeStats() {
      CacheStats stats = cache.stats();
      return String.format(
          "used/max %d/%d MB, hit %.2f%%, lookups %d, avg load time %.0f ns, loads %d, evictions %d",
          weightInBytes.longValue() >> 20,
          maxWeightInBytes >> 20,
          stats.hitRate() * 100.,
          stats.requestCount(),
          stats.averageLoadPenalty(),
          stats.loadCount(),
          stats.evictionCount());
    }
  }

  /** A key prefix used to generate keys that are stored within a sub-cache. */
  static class CompositeKeyPrefix {
    public static final CompositeKeyPrefix ROOT = new CompositeKeyPrefix(new Object[0], 0);

    private final Object[] namespace;
    private final long weight;

    private CompositeKeyPrefix(Object[] namespace, long weight) {
      this.namespace = namespace;
      this.weight = weight;
    }

    CompositeKeyPrefix subKey(Object suffix, Object... additionalSuffixes) {
      Object[] subKey = new Object[namespace.length + 1 + additionalSuffixes.length];
      System.arraycopy(namespace, 0, subKey, 0, namespace.length);
      subKey[namespace.length] = suffix;
      System.arraycopy(
          additionalSuffixes, 0, subKey, namespace.length + 1, additionalSuffixes.length);
      long subKeyWeight = weight + findWeight(suffix);
      for (int i = 0; i < additionalSuffixes.length; ++i) {
        subKeyWeight += findWeight(additionalSuffixes[i]);
      }
      return new CompositeKeyPrefix(subKey, subKeyWeight);
    }

    <K> CompositeKey valueKey(K k) {
      return new CompositeKey(namespace, weight, k);
    }

    boolean isProperPrefixOf(CompositeKey otherKey) {
      if (namespace.length > otherKey.namespace.length) {
        return false;
      }
      // Do this in reverse order since the suffix is the part most likely to differ first
      for (int i = namespace.length - 1; i >= 0; --i) {
        if (!Objects.equals(namespace[i], otherKey.namespace[i])) {
          return false;
        }
      }
      return true;
    }

    boolean isEquivalentNamespace(CompositeKey otherKey) {
      if (namespace.length != otherKey.namespace.length) {
        return false;
      }
      // Do this in reverse order since the suffix is the part most likely to differ first
      for (int i = namespace.length - 1; i >= 0; --i) {
        if (!Objects.equals(namespace[i], otherKey.namespace[i])) {
          return false;
        }
      }
      return true;
    }
  }

  /** A tuple of key parts used to represent a key within a cache. */
  @VisibleForTesting
  static class CompositeKey implements Weighted {
    private final Object[] namespace;
    private final Object key;
    private final long weight;

    private CompositeKey(Object[] namespace, long namespaceWeight, Object key) {
      this.namespace = namespace;
      this.key = key;
      this.weight = namespaceWeight + findWeight(key);
    }

    @Override
    public String toString() {
      return "CompositeKey{namespace=" + Arrays.toString(namespace) + ", key=" + key + "}";
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof CompositeKey)) {
        return false;
      }
      CompositeKey that = (CompositeKey) o;
      return Arrays.equals(namespace, that.namespace) && Objects.equals(key, that.key);
    }

    @Override
    public int hashCode() {
      return Arrays.hashCode(namespace);
    }

    @Override
    public long getWeight() {
      return weight;
    }
  }

  /**
   * A cache that tracks keys that have been inserted into the cache and supports clearing them.
   *
   * <p>The set of keys that are tracked are only those provided to {@link #peek} and {@link
   * #computeIfAbsent}.
   */
  public static class ClearableCache<K, V> extends SubCache<K, V> {
    private final Set<K> weakHashSet;

    public ClearableCache(Cache<K, V> cache) {
      super(
          ((SubCache<K, V>) cache).cache,
          ((SubCache<CompositeKey, V>) cache).keyPrefix,
          ((SubCache<CompositeKey, V>) cache).maxWeightInBytes,
          ((SubCache<CompositeKey, V>) cache).weightInBytes);
      // We specifically use a weak hash map so that once the key is no longer referenced we don't
      // have to keep track of it anymore and the weak hash map will garbage collect it for us.
      this.weakHashSet = Collections.newSetFromMap(new WeakHashMap<>());
    }

    @Override
    public V computeIfAbsent(K key, Function<K, V> loadingFunction) {
      weakHashSet.add(key);
      return super.computeIfAbsent(key, loadingFunction);
    }

    @Override
    public void put(K key, V value) {
      weakHashSet.add(key);
      super.put(key, value);
    }

    @Override
    public void remove(K key) {
      weakHashSet.remove(key);
      super.remove(key);
    }

    /** Removes all tracked keys from the cache. */
    public void clear() {
      for (K key : weakHashSet) {
        super.remove(key);
      }
      weakHashSet.clear();
    }
  }
}
