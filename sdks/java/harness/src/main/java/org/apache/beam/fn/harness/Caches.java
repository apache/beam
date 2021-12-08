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
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.SdkHarnessOptions;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Iterables;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Sets;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.util.concurrent.MoreExecutors;
import org.cache2k.Cache2kBuilder;
import org.cache2k.operation.Weigher;
import org.github.jamm.MemoryMeter;

/** Utility methods used to instantiate and operate over cache instances. */
@SuppressWarnings("nullness")
public final class Caches {

  /** A cache that never stores any values. */
  public static <K, V> Cache<K, V> noop() {
    // We specifically use cache2k since it allows for recursive computeIfAbsent calls
    // preventing deadlock from occurring when a loading function mutates the underlying cache
    org.cache2k.Cache<CompositeKey, Object> cache =
        Cache2kBuilder.of(CompositeKey.class, Object.class)
            .entryCapacity(1)
            .storeByReference(true)
            .expireAfterWrite(0, TimeUnit.NANOSECONDS)
            .sharpExpiry(true)
            .executor(MoreExecutors.directExecutor())
            .build();

    return (Cache<K, V>) forCache(cache);
  }

  /** A cache that never evicts any values. */
  public static <K, V> Cache<K, V> eternal() {
    // We specifically use cache2k since it allows for recursive computeIfAbsent calls
    // preventing deadlock from occurring when a loading function mutates the underlying cache
    org.cache2k.Cache<CompositeKey, Object> cache =
        Cache2kBuilder.of(CompositeKey.class, Object.class)
            .entryCapacity(Long.MAX_VALUE)
            .storeByReference(true)
            .executor(MoreExecutors.directExecutor())
            .build();
    return (Cache<K, V>) forCache(cache);
  }

  /**
   * Uses the specified {@link PipelineOptions} to configure and return a cache instance based upon
   * parameters within {@link SdkHarnessOptions}.
   */
  public static <K, V> Cache<K, V> fromOptions(PipelineOptions options) {
    // We specifically use cache2k since it allows for recursive computeIfAbsent calls
    // preventing deadlock from occurring when a loading function mutates the underlying cache
    org.cache2k.Cache<CompositeKey, Object> cache =
        Cache2kBuilder.of(CompositeKey.class, Object.class)
            .maximumWeight(
                options.as(SdkHarnessOptions.class).getMaxCacheMemoryUsageMb() * 1024L * 1024L)
            .weigher(
                new Weigher<CompositeKey, Object>() {
                  private final MemoryMeter memoryMeter = MemoryMeter.builder().build();

                  @Override
                  public int weigh(CompositeKey key, Object value) {
                    long size = memoryMeter.measureDeep(key) + memoryMeter.measureDeep(value);
                    return size > Integer.MAX_VALUE ? Integer.MAX_VALUE : (int) size;
                  }
                })
            .storeByReference(true)
            .executor(MoreExecutors.directExecutor())
            .build();

    return (Cache<K, V>) forCache(cache);
  }

  /**
   * Returns a view of a cache that operates on keys with a specified key prefix.
   *
   * <p>All lookups, insertions, and removals into the parent {@link Cache} will be prefixed by the
   * specified prefixes.
   *
   * <p>Operations which operate over the entire caches contents such as {@link Cache#clear} only
   * operate over keys with the specified prefixes.
   */
  public static <K, V> Cache<K, V> subCache(
      Cache<?, ?> cache, Object keyPrefix, Object... additionalKeyPrefix) {
    if (cache instanceof SubCache) {
      return new SubCache<>(
          ((SubCache<?, ?>) cache).cache,
          ((SubCache<?, ?>) cache).keyPrefix.subKey(keyPrefix, additionalKeyPrefix));
    }
    throw new IllegalArgumentException(
        String.format(
            "An unsupported type of cache was passed in. Received %s.",
            cache == null ? "null" : cache.getClass()));
  }

  private static Cache<Object, Object> forCache(org.cache2k.Cache<CompositeKey, Object> cache) {
    return new SubCache<>(cache, CompositeKeyPrefix.ROOT);
  }

  /**
   * A view of a cache that operates on keys with a specified key prefix.
   *
   * <p>All lookups, insertions, and removals into the parent {@link Cache} will be prefixed by the
   * specified prefixes.
   *
   * <p>Operations which operate over the entire caches contents such as {@link Cache#clear} only
   * operate over keys with the specified prefixes.
   */
  private static class SubCache<K, V> implements Cache<K, V> {
    private final org.cache2k.Cache<CompositeKey, Object> cache;
    private final CompositeKeyPrefix keyPrefix;

    SubCache(org.cache2k.Cache<CompositeKey, Object> cache, CompositeKeyPrefix keyPrefix) {
      this.cache = cache;
      this.keyPrefix = keyPrefix;
    }

    @Override
    public V peek(K key) {
      return (V) cache.peek(keyPrefix.valueKey(key));
    }

    @Override
    public V computeIfAbsent(K key, Function<K, V> loadingFunction) {
      return (V)
          cache.computeIfAbsent(keyPrefix.valueKey(key), o -> loadingFunction.apply((K) o.key));
    }

    @Override
    public void put(K key, V value) {
      cache.put(keyPrefix.valueKey(key), value);
    }

    @Override
    public void clear() {
      for (CompositeKey key : Sets.filter(cache.keys(), keyPrefix::isProperPrefixOf)) {
        cache.remove(key);
      }
    }

    @Override
    public Iterable<K> keys() {
      return Iterables.transform(
          Sets.filter(cache.keys(), keyPrefix::isEquivalentNamespace),
          input -> (K) Preconditions.checkNotNull(input.key));
    }

    @Override
    public void remove(K key) {
      cache.remove(keyPrefix.valueKey(key));
    }
  }

  /** A key prefix used to generate keys that are stored within a sub-cache. */
  static class CompositeKeyPrefix {
    public static final CompositeKeyPrefix ROOT = new CompositeKeyPrefix(new Object[0]);

    private final Object[] namespace;

    private CompositeKeyPrefix(Object[] namespace) {
      this.namespace = namespace;
    }

    CompositeKeyPrefix subKey(Object suffix, Object... additionalSuffixes) {
      Object[] subKey = new Object[namespace.length + 1 + additionalSuffixes.length];
      System.arraycopy(namespace, 0, subKey, 0, namespace.length);
      subKey[namespace.length] = suffix;
      System.arraycopy(
          additionalSuffixes, 0, subKey, namespace.length + 1, additionalSuffixes.length);
      return new CompositeKeyPrefix(subKey);
    }

    <K> CompositeKey valueKey(K k) {
      return new CompositeKey(namespace, k);
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
  static class CompositeKey {
    private final Object[] namespace;
    private final Object key;

    private CompositeKey(Object[] namespace, Object key) {
      this.namespace = namespace;
      this.key = key;
    }

    @Override
    public String toString() {
      return "CompositeKey{" + "namespace=" + Arrays.toString(namespace) + ", key=" + key + "}";
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
  }
}
