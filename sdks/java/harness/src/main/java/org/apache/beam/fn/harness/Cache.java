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

import java.util.function.Function;
import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;

/**
 * A cache allows for the storage and retrieval of values which are associated with keys.
 *
 * <p>The cache allows for concurrent access and modification to its content and automatically
 * controls the amount of entries in the cache to stay within configured resource limits.
 */
@ThreadSafe
public interface Cache<K, V> {
  /**
   * An interface that marks an object that can be reduced in size instead of being evicted
   * completely.
   *
   * <p>Types should consider implementing {@link org.apache.beam.sdk.util.Weighted} to not invoke
   * the overhead of using the {@link Caches#weigh default weigher} multiple times.
   *
   * <p>This interface may be invoked from any other thread that manipulates the cache causing this
   * value to be shrunk. Implementers must ensure thread safety with respect to any side effects
   * caused.
   */
  @ThreadSafe
  @FunctionalInterface
  interface Shrinkable<V> {
    /**
     * Returns a new object that is smaller than the object being evicted.
     *
     * <p>It is recommended to return an object that is at most half as large as the one being
     * evicted. If {@code null} is returned then the object will be evicted.
     */
    @Nullable
    V shrink();
  }

  /** Looks up the specified key returning {@code null} if the value is not within the cache. */
  @Nullable
  V peek(K key);

  /**
   * Looks up the specified key and returns the associated value.
   *
   * <p>If the key is not present in the cache, the specified function will be used to load and
   * populate the cache.
   */
  V computeIfAbsent(K key, Function<K, V> loadingFunction);

  /**
   * Inserts a new value associated with the given key or updates an existing association of the
   * same key with the new value.
   */
  void put(K key, V value);

  /** Removes the mapping for a key from the cache if it is present. */
  void remove(K key);

  /** Returns a string containing caching statistics. */
  String describeStats();
}
