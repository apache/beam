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
package org.apache.beam.sdk.state;

import java.util.Map;

/**
 * A {@link ReadableState} cell mapping keys to bags of values. Keys are considered equivalent if
 * their structural values are equivalent, see {@link
 * org.apache.beam.sdk.coders.Coder#structuralValue} for additional details.
 *
 * <p>Implementations of this form of state are expected to implement multimap operations
 * efficiently as supported by some associated backing key-value store.
 *
 * @param <K> the type of keys maintained by this multimap
 * @param <V> the type of mapped values
 */
public interface MultimapState<K, V> extends State {

  /**
   * Associates the specified value with the specified key in this multimap. Existing values
   * associated with the same key will not be removed.
   *
   * <p>Changes will not be reflected in the results returned by previous calls to {@link
   * ReadableState#read} on the results any of the reading methods({@link #get}, {@link #keys},
   * {@link #entries}).
   */
  void put(K key, V value);

  /**
   * A deferred lookup, returns an empty iterable if the item is not found.
   *
   * <p>A user is encouraged to call {@code get} for all relevant keys and call {@code readLater()}
   * on the results.
   *
   * <p>When {@code read} is called, a particular state implementation is encouraged to perform all
   * pending reads in a single batch.
   */
  ReadableState<Iterable<V>> get(K key);

  /**
   * Removes all values associated with the key from this multimap. This is a no-op if the key is
   * not contained within the multimap.
   *
   * <p>Changes will not be reflected in the results returned by previous calls to {@link
   * ReadableState#read} on the results of any of the reading methods({@link #get}, {@link #keys},
   * {@link #entries}).
   */
  void remove(K key);

  /** Returns an {@link Iterable} over the keys contained in this multimap. */
  ReadableState<Iterable<K>> keys();

  /** Returns an {@link Iterable} over all key-value pairs contained in this multimap. */
  ReadableState<Iterable<Map.Entry<K, V>>> entries();

  /**
   * Returns a {@link ReadableState} whose {@link ReadableState#read} method will return true if
   * this multimap contains the specified key at the point when that {@link ReadableState#read} call
   * returns.
   */
  ReadableState<Boolean> containsKey(K key);

  /**
   * Returns a {@link ReadableState} whose {@link ReadableState#read} method will return true if
   * this state is empty at the point when that {@link ReadableState#read} call returns.
   */
  ReadableState<Boolean> isEmpty();
}
