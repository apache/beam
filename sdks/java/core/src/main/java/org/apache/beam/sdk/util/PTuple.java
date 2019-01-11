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
package org.apache.beam.sdk.util;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import org.apache.beam.sdk.values.TupleTag;

/**
 * A {@code PTuple} is an immutable tuple of
 * heterogeneously-typed values, "keyed" by {@link TupleTag}s.
 *
 * <p>PTuples can be created and accessed like follows:
 * <pre> {@code
 * String v1 = ...;
 * Integer v2 = ...;
 * Iterable<String> v3 = ...;
 *
 * // Create TupleTags for each of the values to put in the
 * // PTuple (the type of the TupleTag enables tracking the
 * // static type of each of the values in the PTuple):
 * TupleTag<String> tag1 = new TupleTag<>();
 * TupleTag<Integer> tag2 = new TupleTag<>();
 * TupleTag<Iterable<String>> tag3 = new TupleTag<>();
 *
 * // Create a PTuple with three values:
 * PTuple povs =
 *     PTuple.of(tag1, v1)
 *         .and(tag2, v2)
 *         .and(tag3, v3);
 *
 * // Create an empty PTuple:
 * Pipeline p = ...;
 * PTuple povs2 = PTuple.empty(p);
 *
 * // Get values out of a PTuple, using the same tags
 * // that were used to put them in:
 * Integer vX = povs.get(tag2);
 * String vY = povs.get(tag1);
 * Iterable<String> vZ = povs.get(tag3);
 *
 * // Get a map of all values in a PTuple:
 * Map<TupleTag<?>, ?> allVs = povs.getAll();
 * } </pre>
 */
public class PTuple {
  /**
   * Returns an empty PTuple.
   *
   * <p>Longer PTuples can be created by calling
   * {@link #and} on the result.
   */
  public static PTuple empty() {
    return new PTuple();
  }

  /**
   * Returns a singleton PTuple containing the given
   * value keyed by the given TupleTag.
   *
   * <p>Longer PTuples can be created by calling
   * {@link #and} on the result.
   */
  public static <V> PTuple of(TupleTag<V> tag, V value) {
    return empty().and(tag, value);
  }

  /**
   * Returns a new PTuple that has all the values and
   * tags of this PTuple plus the given value and tag.
   *
   * <p>The given TupleTag should not already be mapped to a
   * value in this PTuple.
   */
  public <V> PTuple and(TupleTag<V> tag, V value) {
    Map<TupleTag<?>, Object> newMap = new LinkedHashMap<TupleTag<?>, Object>();
    newMap.putAll(valueMap);
    newMap.put(tag, value);
    return new PTuple(newMap);
  }

  /**
   * Returns whether this PTuple contains a value with
   * the given tag.
   */
  public <V> boolean has(TupleTag<V> tag) {
    return valueMap.containsKey(tag);
  }

  /**
   * Returns true if this {@code PTuple} is empty.
   */
  public boolean isEmpty() {
    return valueMap.isEmpty();
  }

  /**
   * Returns the value with the given tag in this
   * PTuple.  Throws IllegalArgumentException if there is no
   * such value, i.e., {@code !has(tag)}.
   */
  public <V> V get(TupleTag<V> tag) {
    if (!has(tag)) {
      throw new IllegalArgumentException(
          "TupleTag not found in this PTuple");
    }
    @SuppressWarnings("unchecked")
    V value = (V) valueMap.get(tag);
    return value;
  }

  /**
   * Returns an immutable Map from TupleTag to corresponding
   * value, for all the members of this PTuple.
   */
  public Map<TupleTag<?>, ?> getAll() {
    return valueMap;
  }


  /////////////////////////////////////////////////////////////////////////////
  // Internal details below here.

  private final Map<TupleTag<?>, ?> valueMap;

  @SuppressWarnings("rawtypes")
  private PTuple() {
    this(new LinkedHashMap());
  }

  private PTuple(Map<TupleTag<?>, ?> valueMap) {
    this.valueMap = Collections.unmodifiableMap(valueMap);
  }

  /**
   * Returns a PTuple with each of the given tags mapping
   * to the corresponding value.
   *
   * <p>For internal use only.
   */
  public static PTuple ofInternal(Map<TupleTag<?>, ?> valueMap) {
    return new PTuple(valueMap);
  }
}
