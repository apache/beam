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
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.annotations.Experimental.Kind;

/**
 * A {@link ReadableState} cell mapping keys to values.
 *
 * <p>Implementations of this form of state are expected to implement map operations efficiently as
 * supported by some associated backing key-value store.
 *
 * @param <K> the type of keys maintained by this map
 * @param <V> the type of mapped values
 */
@Experimental(Kind.STATE)
public interface MapState<K, V> extends State {

  /**
   * Associates the specified value with the specified key in this state.
   *
   * <p>Changes will not be reflected in the results returned by previous calls to {@link
   * ReadableState#read} on the results any of the reading methods ({@link #get}, {@link #keys},
   * {@link #values}, and {@link #entries}).
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
   *
   * <p>Changes will not be reflected in the results returned by previous calls to {@link
   * ReadableState#read} on the results any of the reading methods ({@link #get}, {@link #keys},
   * {@link #values}, and {@link #entries}).
   */
  ReadableState<V> putIfAbsent(K key, V value);

  /**
   * Remove the mapping for a key from this map if it is present.
   *
   * <p>Changes will not be reflected in the results returned by previous calls to {@link
   * ReadableState#read} on the results any of the reading methods ({@link #get}, {@link #keys},
   * {@link #values}, and {@link #entries}).
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

  /** Returns an {@link Iterable} over the keys contained in this map. */
  ReadableState<Iterable<K>> keys();

  /** Returns an {@link Iterable} over the values contained in this map. */
  ReadableState<Iterable<V>> values();

  /** Returns an {@link Iterable} over the key-value pairs contained in this map. */
  ReadableState<Iterable<Map.Entry<K, V>>> entries();
}
