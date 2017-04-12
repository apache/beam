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
package org.apache.beam.sdk.util.state;

import java.util.Map;

/**
 * An object that maps keys to values.
 * A map cannot contain duplicate keys;
 * each key can map to at most one value.
 *
 * @param <K> the type of keys maintained by this map
 * @param <V> the type of mapped values
 */
public interface MapState<K, V> extends State {

  /**
   * Associates the specified value with the specified key in this state.
   */
  void put(K key, V value);

  /**
   * A deferred read-followed-by-write.
   *
   * <p>When {@code read()} is called on the result or state is committed, it forces a read of the
   * map and reconciliation with any pending modifications.
   *
   * <p>If the specified key is not already associated with a value (or is mapped to {@code null})
   * associates it with the given value and returns {@code null}, else returns the current value.
   */
  ReadableState<V> putIfAbsent(K key, V value);

  /**
   * Removes the mapping for a key from this map if it is present.
   */
  void remove(K key);

  /**
   * A deferred lookup.
   *
   * <p>A user is encouraged to call {@code get} for all relevant keys and call {@code readLater()}
   * on the results.
   *
   * <p>When {@code read()} is called, a particular state implementation is encouraged to perform
   * all pending reads in a single batch.
   */
  ReadableState<V> get(K key);

  /**
   * Returns a iterable view of the keys contained in this map.
   */
  ReadableState<Iterable<K>> keys();

  /**
   * Returns a iterable view of the values contained in this map.
   */
  ReadableState<Iterable<V>> values();

  /**
   * Returns a iterable view of all key-values.
   */
  ReadableState<Iterable<Map.Entry<K, V>>> entries();
}

