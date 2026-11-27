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

import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkArgument;
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkNotNull;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Function;
import javax.annotation.Nullable;
import org.apache.beam.fn.harness.Cache.Shrinkable;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.SdkHarnessOptions;
import org.apache.beam.sdk.util.Weighted;
import org.apache.beam.sdk.util.WeightedValue;
import org.apache.beam.vendor.grpc.v1p69p0.com.google.protobuf.ByteString;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.cache.CacheBuilder;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.cache.CacheStats;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.cache.RemovalListener;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.cache.RemovalNotification;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.cache.Weigher;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.math.LongMath;
import org.github.jamm.MemoryMeter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Utility methods used to instantiate and operate over cache instances. */
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

  /**
   * Exact {@code LiteralByteString} class ({@link ByteString#EMPTY}). Restricting the fast path to
   * this concrete class ensures sliced {@code BoundedByteString}, {@code RopeByteString}, and
   * {@code NioByteString} instances fall back to deep measurement so retained backing buffers are
   * not under-counted.
   */
  private static final Class<?> LITERAL_BYTE_STRING_CLASS = ByteString.EMPTY.getClass();

  private static final AtomicBoolean LOGGED_JAMM_WARNING = new AtomicBoolean(false);

  /** The size of a reference. */
  public static final long REFERENCE_SIZE = 8;

  private static long alignObjectSize(long bytes) {
    return (bytes + 7L) & ~7L;
  }

  /**
   * Returns an estimate of the amount of memory in bytes the provided object consumes.
   *
   * <p>Uses heuristic fast paths (assuming 64-bit compressed OOPs, 8-byte alignment) for common
   * immutable/primitive types ({@link String}, {@code byte[]}, literal {@link ByteString}, boxed
   * primitives, primitive arrays, and {@link Enum}) and delegates other objects (including sliced
   * {@link ByteString} subclasses) to {@link MemoryMeter#measureDeep}. Objects implementing {@link
   * Weighted} return their reported weight plus reference/header overhead.
   */
  public static long weigh(@Nullable Object o) {
    if (o == null) {
      return REFERENCE_SIZE;
    }
    // Check common final/array types via exact class equality before the `instanceof Weighted`
    // interface check to avoid secondary-super table scans on non-Weighted objects.
    Class<?> cls = o.getClass();
    if (cls == String.class) {
      // Conservative O(1) 2 bytes/char estimate (40B String + backing array headers + payload)
      // avoids scanning or reflecting into String's internal array and never under-counts UTF-16
      // or -XX:-CompactStrings strings, at the cost of overestimating Latin-1 string cache usage
      // by up to ~2x under default -XX:+CompactStrings.
      return 40L + alignObjectSize(2L * ((String) o).length());
    }
    if (cls == byte[].class) {
      return 16L + alignObjectSize(((byte[]) o).length);
    }
    if (cls == LITERAL_BYTE_STRING_CLASS) {
      return 40L + alignObjectSize(((ByteString) o).size());
    }
    if (cls == Long.class || cls == Double.class) {
      return 24L;
    }
    if (cls == Integer.class) {
      return 16L;
    }
    if (o instanceof Weighted) {
      return LongMath.saturatedAdd(Math.max(0L, ((Weighted) o).getWeight()), REFERENCE_SIZE + 8);
    }
    return weighSlow(o, cls);
  }

  private static long weighSlow(Object o, Class<?> cls) {
    if (cls == Float.class
        || cls == Boolean.class
        || cls == Byte.class
        || cls == Short.class
        || cls == Character.class) {
      return 16L;
    }
    if (cls == int[].class) {
      return 16L + alignObjectSize(4L * ((int[]) o).length);
    }
    if (cls == long[].class) {
      return 16L + 8L * ((long[]) o).length;
    }
    if (o instanceof Enum<?>) {
      return REFERENCE_SIZE;
    }
    try {
      return MEMORY_METER.measureDeep(o);
    } catch (RuntimeException e) {
      // Checking for RuntimeException since java.lang.reflect.InaccessibleObjectException is only
      // available starting Java 9.
      if (LOGGED_JAMM_WARNING.compareAndSet(false, true)) {
        LOG.warn(
            "JVM prevents jamm from accessing subgraph - cache sizes may be underestimated", e);
      }
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

  /** Reference-counted set of live keys owned by a {@link ClearableCache}. */
  private static final class KeyTracker {
    private static final Integer ONE = 1;

    private final ConcurrentHashMap<CompositeKey, Integer> counts = new ConcurrentHashMap<>();
    private final @Nullable KeyTracker parent;
    private final AtomicLong clearEpoch = new AtomicLong();

    KeyTracker(@Nullable KeyTracker parent) {
      this.parent = parent;
    }

    long currentEpoch() {
      long epoch = 0L;
      for (@Nullable KeyTracker t = this; t != null; t = t.parent) {
        epoch += t.clearEpoch.get();
      }
      return epoch;
    }

    void increment(CompositeKey key) {
      @Nullable KeyTracker cur = this;
      try {
        while (cur != null) {
          cur.counts.merge(key, ONE, Integer::sum);
          cur = cur.parent;
        }
      } catch (Throwable t) {
        for (@Nullable KeyTracker undo = this; undo != null && undo != cur; undo = undo.parent) {
          try {
            decrementSingle(undo, key);
          } catch (Throwable suppressed) {
            t.addSuppressed(suppressed);
          }
        }
        throw t;
      }
    }

    void decrement(CompositeKey key) {
      for (@Nullable KeyTracker t = this; t != null; t = t.parent) {
        decrementSingle(t, key);
      }
    }

    @SuppressWarnings(
        "return") // ConcurrentHashMap.computeIfPresent removes entry when returning null.
    private static void decrementSingle(KeyTracker tracker, CompositeKey key) {
      tracker.counts.computeIfPresent(key, (unused, count) -> count > 1 ? count - 1 : null);
    }

    void clear(
        org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.cache.Cache<
                CompositeKey, CacheEntry>
            cache) {
      // Advance the epoch before reading counts.keySet() so ShrinkOnEviction.onRemoval can detect
      // concurrent clear() and refuse to resurrect a shrunk entry from an earlier epoch.
      clearEpoch.incrementAndGet();
      if (!counts.isEmpty()) {
        // Invalidating iterates the weakly consistent KeySetView; ShrinkOnEviction.onRemoval
        // decrements each explicitly removed key from counts, preserving any concurrent inserts
        // not yet reached by invalidateAll.
        cache.invalidateAll(counts.keySet());
      }
    }

    int size() {
      return counts.size();
    }
  }

  /**
   * Internal cache value holding the user value, reported weight, exact byte delta tracked in
   * {@code weightInBytes} (stored explicitly so key replacements where {@code k1.equals(k2)} with
   * different concrete weights such as literal vs sliced {@link ByteString} cancel out with zero
   * drift), and optional eviction tracker.
   */
  private static final class CacheEntry {
    private final @Nullable Object value;
    private final long weight;
    private final long trackedBytes;
    private final @Nullable KeyTracker tracker;
    private final long trackerEpoch;
    // Marked true inside Weigher.weigh once Guava inserts this entry into a cache segment, so
    // putEntry avoids double-decrementing if an eviction listener throws after insertion.
    private boolean weighed;

    CacheEntry(
        @Nullable Object value,
        long weight,
        long trackedBytes,
        @Nullable KeyTracker tracker,
        long trackerEpoch) {
      this.value = value;
      this.weight = weight;
      this.trackedBytes = trackedBytes;
      this.tracker = tracker;
      this.trackerEpoch = trackerEpoch;
    }
  }

  /** An eviction listener that reduces the size of entries that are {@link Shrinkable}. */
  @VisibleForTesting
  static final class ShrinkOnEviction implements RemovalListener<CompositeKey, CacheEntry> {

    private final org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.cache.Cache<
            CompositeKey, CacheEntry>
        cache;
    private final LongAdder weightInBytes;

    @SuppressWarnings("argument")
    ShrinkOnEviction(CacheBuilder<CompositeKey, CacheEntry> cacheBuilder, LongAdder weightInBytes) {
      this.cache = cacheBuilder.removalListener(this).build();
      this.weightInBytes = weightInBytes;
    }

    public org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.cache.Cache<
            CompositeKey, CacheEntry>
        getCache() {
      return cache;
    }

    @Override
    public void onRemoval(RemovalNotification<CompositeKey, CacheEntry> removalNotification) {
      CompositeKey key = checkNotNull(removalNotification.getKey());
      CacheEntry entry = checkNotNull(removalNotification.getValue());
      weightInBytes.add(-entry.trackedBytes);
      KeyTracker tracker = entry.tracker;
      try {
        if (!removalNotification.wasEvicted()) {
          return;
        }
        @Nullable Object v = entry.value;
        if (!(v instanceof Cache.Shrinkable)) {
          return;
        }
        if (tracker != null && tracker.currentEpoch() != entry.trackerEpoch) {
          return;
        }
        @Nullable Object updatedEntry = ((Shrinkable<?>) v).shrink();
        if (updatedEntry != null) {
          if (tracker != null && tracker.currentEpoch() != entry.trackerEpoch) {
            return;
          }
          @Nullable
          CacheEntry shrunk =
              putEntry(cache, key, updatedEntry, weightInBytes, tracker, entry.trackerEpoch, true);
          // Re-check after inserting (Dekker check with KeyTracker.clear): if a concurrent clear()
          // incremented clearEpoch and finished scanning counts before putIfAbsent published
          // shrunk, remove only this exact entry instance.
          if (shrunk != null && tracker != null && tracker.currentEpoch() != entry.trackerEpoch) {
            cache.asMap().remove(key, shrunk);
          }
        }
      } finally {
        if (tracker != null) {
          tracker.decrement(key);
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
   * Returns a view of a cache that operates on keys with a single key prefix.
   *
   * <p>All lookups, insertions, and removals into the parent {@link Cache} will be prefixed by the
   * specified prefix. If the parent is a {@link ClearableCache}, entries inserted through the
   * returned sub-cache are tracked by the parent so calling {@link ClearableCache#clear()} clears
   * them too; the returned view does not expose {@code clear()}.
   *
   * <p>Namespace prefix objects should have immutable {@link Object#hashCode()} and {@link
   * Object#equals(Object)} implementations.
   */
  public static <K, V> Cache<K, V> subCache(Cache<?, ?> cache, Object keyPrefix) {
    SubCache<?, ?> subCache = requireSubCache(cache);
    return new SubCache<>(
        subCache.cache,
        subCache.keyPrefix.subKey(keyPrefix),
        subCache.maxWeightInBytes,
        subCache.weightInBytes,
        subCache.tracker);
  }

  /**
   * Returns a view of a cache that operates on keys with a specified key prefix.
   *
   * <p>All lookups, insertions, and removals into the parent {@link Cache} will be prefixed by the
   * specified prefixes. If the parent is a {@link ClearableCache}, entries inserted through the
   * returned sub-cache are tracked by the parent so calling {@link ClearableCache#clear()} clears
   * them too; the returned view does not expose {@code clear()}.
   *
   * <p>Namespace prefix objects should have immutable {@link Object#hashCode()} and {@link
   * Object#equals(Object)} implementations.
   */
  public static <K, V> Cache<K, V> subCache(
      Cache<?, ?> cache, Object keyPrefix, Object... additionalKeyPrefix) {
    SubCache<?, ?> subCache = requireSubCache(cache);
    return new SubCache<>(
        subCache.cache,
        subCache.keyPrefix.subKey(keyPrefix, additionalKeyPrefix),
        subCache.maxWeightInBytes,
        subCache.weightInBytes,
        subCache.tracker);
  }

  private static SubCache<?, ?> requireSubCache(@Nullable Cache<?, ?> cache) {
    if (cache instanceof SubCache) {
      return (SubCache<?, ?>) cache;
    }
    throw new IllegalArgumentException(
        String.format(
            "An unsupported type of cache was passed in. Received %s.",
            cache == null ? "null" : cache.getClass()));
  }

  @VisibleForTesting
  static long estimatedWeightInBytes(Cache<?, ?> cache) {
    return requireSubCache(cache).weightInBytes.sum();
  }

  @VisibleForTesting
  static <K, V> Cache<K, V> forMaximumBytes(long maximumBytes) {
    checkArgument(maximumBytes >= 0, "maximumBytes must be non-negative, got %s", maximumBytes);
    // We specifically use Guava cache since it allows for recursive computeIfAbsent calls
    // preventing deadlock from occurring when a loading function mutates the underlying cache
    LongAdder weightInBytes = new LongAdder();
    return new SubCache<>(
        new ShrinkOnEviction(
                CacheBuilder.newBuilder()
                    .maximumWeight(maximumBytes >> WEIGHT_RATIO)
                    .weigher(
                        new Weigher<CompositeKey, CacheEntry>() {

                          @Override
                          public int weigh(CompositeKey key, CacheEntry value) {
                            // Since our weights are tracking bytes used, we need to account for the
                            // cache internal bytes.
                            long bytes =
                                LongMath.saturatedAdd(
                                    LongMath.saturatedAdd(
                                        key.getWeight(), Math.max(0L, value.weight)),
                                    REFERENCE_SIZE * 15);
                            // Round up to the next closest multiple of 2^WEIGHT_RATIO.
                            long weight = bytes <= 0 ? 1L : ((bytes - 1L) >>> WEIGHT_RATIO) + 1L;
                            if (weight > Integer.MAX_VALUE) {
                              LOG.warn(
                                  "Entry with size {} MiBs inserted into the cache. This is larger than the maximum individual entry size of {} MiBs. The cache will under report its memory usage by the difference. This may lead to OutOfMemoryErrors.",
                                  ((bytes - 1L) >>> 20) + 1L,
                                  2L << (WEIGHT_RATIO + 10));
                              value.weighed = true;
                              return Integer.MAX_VALUE;
                            }
                            value.weighed = true;
                            return (int) weight;
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
        weightInBytes,
        null);
  }

  private static CacheEntry addWeightedValue(
      CompositeKey key,
      Object o,
      LongAdder weightInBytes,
      @Nullable KeyTracker tracker,
      long trackerEpoch) {
    @Nullable Object actualValue;
    long valueWeight;
    long additionalBytes;
    if (o instanceof WeightedValue) {
      WeightedValue<?> wv = (WeightedValue<?>) o;
      actualValue = wv.getValue();
      valueWeight = Math.max(0L, wv.getWeight());
      additionalBytes = 0L;
    } else if (o instanceof Weighted) {
      actualValue = o;
      valueWeight = Math.max(0L, ((Weighted) o).getWeight());
      additionalBytes = REFERENCE_SIZE * 2;
    } else {
      actualValue = o;
      valueWeight = Math.max(0L, weigh(o));
      additionalBytes = REFERENCE_SIZE * 2;
    }
    long keyWeight = key.getWeight();
    long trackedBytes =
        LongMath.saturatedAdd(LongMath.saturatedAdd(keyWeight, valueWeight), additionalBytes);
    weightInBytes.add(trackedBytes);
    return new CacheEntry(actualValue, valueWeight, trackedBytes, tracker, trackerEpoch);
  }

  private static @Nullable CacheEntry putEntry(
      org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.cache.Cache<
              CompositeKey, CacheEntry>
          cache,
      CompositeKey key,
      Object value,
      LongAdder weightInBytes,
      @Nullable KeyTracker tracker,
      long trackerEpoch,
      boolean onlyIfAbsent) {
    CacheEntry entry = addWeightedValue(key, value, weightInBytes, tracker, trackerEpoch);
    boolean tracked = false;
    @Nullable CacheEntry existing;
    try {
      if (tracker != null) {
        tracker.increment(key);
        tracked = true;
      }
      if (onlyIfAbsent) {
        existing = cache.asMap().putIfAbsent(key, entry);
      } else {
        cache.put(key, entry);
        existing = null;
      }
    } catch (Throwable t) {
      // Only roll back if Guava failed before committing `entry` via Weigher.weigh (e.g., a
      // buggy key.equals() threw). Once weighed, ShrinkOnEviction.onRemoval owns decrementing.
      if (!entry.weighed) {
        weightInBytes.add(-entry.trackedBytes);
        if (tracked && tracker != null) {
          try {
            tracker.decrement(key);
          } catch (Throwable suppressed) {
            t.addSuppressed(suppressed);
          }
        }
      }
      throw t;
    }
    if (existing != null) {
      // A newer entry already occupies `key`; putIfAbsent neither replaced it nor invoked
      // Weigher.weigh, so undo weight/tracker accounting for the discarded entry.
      weightInBytes.add(-entry.trackedBytes);
      if (tracked && tracker != null) {
        tracker.decrement(key);
      }
      return null;
    }
    return entry;
  }

  /**
   * A view of a cache that operates on keys with a specified key prefix.
   *
   * <p>All lookups, insertions, and removals into the parent {@link Cache} will be prefixed by the
   * specified prefixes.
   */
  private static class SubCache<K, V> implements Cache<K, V> {
    private final org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.cache.Cache<
            CompositeKey, CacheEntry>
        cache;
    private final CompositeKeyPrefix keyPrefix;
    private final long maxWeightInBytes;
    private final LongAdder weightInBytes;
    private final @Nullable KeyTracker tracker;

    SubCache(
        org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.cache.Cache<
                CompositeKey, CacheEntry>
            cache,
        CompositeKeyPrefix keyPrefix,
        long maxWeightInBytes,
        LongAdder weightInBytes,
        @Nullable KeyTracker tracker) {
      this.cache = cache;
      this.keyPrefix = keyPrefix;
      this.maxWeightInBytes = maxWeightInBytes;
      this.weightInBytes = weightInBytes;
      this.tracker = tracker;
    }

    @Override
    @SuppressWarnings("unchecked")
    public @Nullable V peek(K key) {
      @Nullable CacheEntry entry = cache.getIfPresent(keyPrefix.valueKey(key));
      if (entry == null) {
        return null;
      }
      return (V) entry.value;
    }

    @Override
    @SuppressWarnings("unchecked")
    public V computeIfAbsent(K key, Function<K, V> loadingFunction) {
      CompositeKey compositeKey = keyPrefix.valueKey(key);
      try {
        return (V)
            cache.get(
                    compositeKey,
                    () -> {
                      Object loaded = checkNotNull(loadingFunction.apply(key));
                      long epoch = tracker == null ? 0L : tracker.currentEpoch();
                      CacheEntry entry =
                          addWeightedValue(compositeKey, loaded, weightInBytes, tracker, epoch);
                      if (tracker != null) {
                        try {
                          tracker.increment(compositeKey);
                        } catch (Throwable t) {
                          weightInBytes.add(-entry.trackedBytes);
                          throw t;
                        }
                      }
                      return entry;
                    })
                .value;
      } catch (ExecutionException e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    public void put(K key, V value) {
      Object nonNullValue = checkNotNull(value);
      CompositeKey compositeKey = keyPrefix.valueKey(key);
      long epoch = tracker == null ? 0L : tracker.currentEpoch();
      putEntry(cache, compositeKey, nonNullValue, weightInBytes, tracker, epoch, false);
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

  /**
   * A key prefix used to generate keys that are stored within a sub-cache.
   *
   * <p>Prefix objects must have immutable {@link Object#hashCode()} and {@link
   * Object#equals(Object)} implementations because {@code namespaceHashCode} is cached on creation.
   */
  static final class CompositeKeyPrefix {
    private static final long UNCOMPUTED_WEIGHT = 0L;

    public static final CompositeKeyPrefix ROOT = new CompositeKeyPrefix(null, new Object[0], 1);

    // Cleared once weight is cached to avoid pinning ancestor CompositeKeyPrefix objects. A benign
    // plain-write race clearing `parent = null` before another thread sees `weight != 0` is safe
    // because `final Object[] namespace` contains the complete prefix [0..length-1], so getWeight()
    // falls back to summing all elements directly with the exact same result.
    private @Nullable CompositeKeyPrefix parent;
    private final Object[] namespace;
    private final int namespaceHashCode;
    // Left unassigned in the constructor (JVM zero-initialized to UNCOMPUTED_WEIGHT = 0L) so
    // creating a sub-cache does not emit a volatile store barrier on the lookup path.
    private volatile long weight;

    private CompositeKeyPrefix(
        @Nullable CompositeKeyPrefix parent, Object[] namespace, int namespaceHashCode) {
      this.parent = parent;
      this.namespace = namespace;
      this.namespaceHashCode = namespaceHashCode;
    }

    CompositeKeyPrefix subKey(Object suffix) {
      int len = namespace.length;
      Object[] subKey = new Object[len + 1];
      System.arraycopy(namespace, 0, subKey, 0, len);
      subKey[len] = suffix;
      int hash = 31 * namespaceHashCode + Objects.hashCode(suffix);
      return new CompositeKeyPrefix(this, subKey, hash);
    }

    CompositeKeyPrefix subKey(Object suffix, Object... additionalSuffixes) {
      int len = namespace.length;
      int extra = additionalSuffixes.length;
      Object[] subKey = new Object[len + 1 + extra];
      System.arraycopy(namespace, 0, subKey, 0, len);
      subKey[len] = suffix;
      int hash = 31 * namespaceHashCode + Objects.hashCode(suffix);
      for (int i = 0; i < extra; i++) {
        Object part = additionalSuffixes[i];
        subKey[len + 1 + i] = part;
        hash = 31 * hash + Objects.hashCode(part);
      }
      return new CompositeKeyPrefix(this, subKey, hash);
    }

    long getWeight() {
      if (namespace.length == 0) {
        return 0L;
      }
      long w = this.weight;
      if (w != UNCOMPUTED_WEIGHT) {
        return w;
      }
      @Nullable CompositeKeyPrefix p = this.parent;
      if (p == null || p.namespace.length == 0) {
        long sum = 0L;
        for (int i = 0; i < namespace.length; i++) {
          sum = LongMath.saturatedAdd(sum, Math.max(0L, weigh(namespace[i])));
        }
        long stored = Math.max(1L, sum);
        this.weight = stored;
        this.parent = null;
        return stored;
      }
      long pw = p.weight;
      if (pw != UNCOMPUTED_WEIGHT) {
        long sum = pw;
        for (int i = p.namespace.length; i < namespace.length; i++) {
          sum = LongMath.saturatedAdd(sum, Math.max(0L, weigh(namespace[i])));
        }
        long stored = Math.max(1L, sum);
        this.weight = stored;
        this.parent = null;
        return stored;
      }
      return computeWeightIterative();
    }

    private long computeWeightIterative() {
      List<CompositeKeyPrefix> chain = new ArrayList<>();
      @Nullable CompositeKeyPrefix cur = this;
      long sum = 0L;
      int startIdx = 0;
      while (cur != null && cur.namespace.length > 0) {
        long cw = cur.weight;
        if (cw != UNCOMPUTED_WEIGHT) {
          sum = cw;
          startIdx = cur.namespace.length;
          break;
        }
        chain.add(cur);
        cur = cur.parent;
      }
      for (int c = chain.size() - 1; c >= 0; c--) {
        CompositeKeyPrefix node = chain.get(c);
        int nodeLen = node.namespace.length;
        for (int i = startIdx; i < nodeLen; i++) {
          sum = LongMath.saturatedAdd(sum, Math.max(0L, weigh(node.namespace[i])));
        }
        // Clamp non-root prefix weights to >= 1 so they stay distinct from UNCOMPUTED_WEIGHT (0L).
        sum = Math.max(1L, sum);
        node.weight = sum;
        node.parent = null;
        startIdx = nodeLen;
      }
      return sum;
    }

    boolean equalsNamespace(CompositeKeyPrefix other) {
      if (this == other) {
        return true;
      }
      if (this.namespaceHashCode != other.namespaceHashCode
          || this.namespace.length != other.namespace.length) {
        return false;
      }
      // Compare in reverse order because the suffix is most likely to differ first.
      for (int i = namespace.length - 1; i >= 0; --i) {
        if (!Objects.equals(namespace[i], other.namespace[i])) {
          return false;
        }
      }
      return true;
    }

    <K> CompositeKey valueKey(@Nullable K k) {
      return new CompositeKey(this, k);
    }
  }

  /** A tuple of key parts used to represent a key within a cache. */
  @VisibleForTesting
  static final class CompositeKey implements Weighted {
    private static final long UNCOMPUTED_WEIGHT = 0L;
    // Object header (12) + prefix ref (4) + key ref (4) + hashCode (4) + weight (8) = 32 B.
    private static final long COMPOSITE_KEY_OVERHEAD = 32L;

    private final CompositeKeyPrefix prefix;
    private final @Nullable Object key;
    private final int hashCode;
    // Left unassigned in the constructor (JVM zero-initialized to UNCOMPUTED_WEIGHT = 0L) so
    // creating lookup keys avoids emitting a volatile store barrier. Computed weights are always
    // >= COMPOSITE_KEY_OVERHEAD (32).
    private volatile long weight;

    private CompositeKey(CompositeKeyPrefix prefix, @Nullable Object key) {
      this.prefix = prefix;
      this.key = key;
      this.hashCode = 31 * prefix.namespaceHashCode + Objects.hashCode(key);
    }

    @Override
    public String toString() {
      return "CompositeKey{namespace="
          + Arrays.toString(prefix.namespace)
          + ", key="
          + String.valueOf(key)
          + "}";
    }

    @Override
    public boolean equals(@Nullable Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof CompositeKey)) {
        return false;
      }
      CompositeKey that = (CompositeKey) o;
      return hashCode == that.hashCode
          && Objects.equals(key, that.key)
          && (prefix == that.prefix || prefix.equalsNamespace(that.prefix));
    }

    @Override
    public int hashCode() {
      return hashCode;
    }

    @Override
    public long getWeight() {
      long w = this.weight;
      if (w == UNCOMPUTED_WEIGHT) {
        long namespaceRefsBytes =
            LongMath.saturatedMultiply(prefix.namespace.length, REFERENCE_SIZE);
        long computed =
            LongMath.saturatedAdd(
                LongMath.saturatedAdd(prefix.getWeight(), Math.max(0L, weigh(key))),
                LongMath.saturatedAdd(COMPOSITE_KEY_OVERHEAD, namespaceRefsBytes));
        this.weight = computed;
        return computed;
      }
      return w;
    }
  }

  /**
   * A cache that tracks keys that have been inserted into the cache and supports clearing them.
   *
   * <p>The set of keys that are tracked are those inserted via {@link #put} or loaded on a cache
   * miss via {@link #computeIfAbsent}, including entries inserted through {@link Caches#subCache}
   * views of this cache. Evicted, invalidated, or replaced entries are automatically untracked so
   * key tracking stays bounded by live cache entries.
   */
  @SuppressWarnings("nullness")
  public static class ClearableCache<K, V> extends SubCache<K, V> {
    public ClearableCache(Cache<K, V> cache) {
      this(requireSubCache(cache));
    }

    private ClearableCache(SubCache<?, ?> delegate) {
      super(
          delegate.cache,
          delegate.keyPrefix,
          delegate.maxWeightInBytes,
          delegate.weightInBytes,
          (delegate.maxWeightInBytes >> WEIGHT_RATIO) > 0
              ? new KeyTracker(delegate.tracker)
              : null);
    }

    /** Removes all live tracked keys from the cache. */
    public void clear() {
      KeyTracker tracker = super.tracker;
      if (tracker != null) {
        tracker.clear(super.cache);
      }
    }

    @VisibleForTesting
    int trackedKeyCount() {
      KeyTracker tracker = super.tracker;
      return tracker == null ? 0 : tracker.size();
    }
  }
}
