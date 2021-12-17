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
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.annotations.Experimental.Kind;

/**
 * A cache allows for the storage and retrieval of values which are associated with keys.
 *
 * <p>The cache allows for concurrent access and modification to its content and automatically
 * controls the amount of entries in the cache to stay within configured resource limits.
 */
@Experimental(Kind.PORTABILITY)
@ThreadSafe
public interface Cache<K, V> {
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

  /** Clears all keys and values in the cache. */
  void clear();

  /**
   * A view of all keys in the cache. The view is guaranteed to contain all keys present in the
   * cache at the time of calling the method, and may or may not reflect concurrent inserts or
   * removals.
   */
  Iterable<K> keys();
}
