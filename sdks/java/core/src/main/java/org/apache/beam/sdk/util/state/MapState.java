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
   * Returns the value to which the specified key is mapped in the state.
   */
  V get(K key);

  /**
   * Associates the specified value with the specified key in this state.
   */
  void put(K key, V value);

  /**
   * If the specified key is not already associated with a value (or is mapped
   * to {@code null}) associates it with the given value and returns
   * {@code null}, else returns the current value.
   */
  V putIfAbsent(K key, V value);

  /**
   * Removes the mapping for a key from this map if it is present.
   */
  void remove(K key);

  /**
   * A bulk get.
   * @param keys the keys to search for
   * @return a iterable view of values, maybe some values is null.
   * The order of values corresponds to the order of the keys.
   */
  Iterable<V> get(Iterable<K> keys);

  /**
   * Indicate that specified key will be read later.
   */
  MapState<K, V> getLater(K k);

  /**
   * Indicate that specified batch keys will be read later.
   */
  MapState<K, V> getLater(Iterable<K> keys);

  /**
   * Returns a iterable view of the keys contained in this map.
   */
  Iterable<K> keys();

  /**
   * Returns a iterable view of the values contained in this map.
   */
  Iterable<V> values();

  /**
   * Indicate that all key-values will be read later.
   */
  MapState<K, V> iterateLater();

  /**
   * Returns a iterable view of all key-values.
   */
  Iterable<Map.Entry<K, V>> iterate();

}

