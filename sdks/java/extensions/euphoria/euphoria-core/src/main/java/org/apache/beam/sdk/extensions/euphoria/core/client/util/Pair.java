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
package org.apache.beam.sdk.extensions.euphoria.core.client.util;

import java.util.Comparator;
import java.util.Objects;
import org.apache.beam.sdk.extensions.euphoria.core.annotation.audience.Audience;

/**
 * A pair, i.e. a tuple of two elements.
 *
 * @param <K> the type of the first element of the pair
 * @param <V> the type of the second element of the pair
 */
@Audience(Audience.Type.CLIENT)
public final class Pair<K, V> {

  private static final Comparator<Pair> CMP_BY_FIRST =
      (o1, o2) -> doCompare(o1.getFirst(), o2.getFirst());

  private static final Comparator<Pair> CMP_BY_SECOND =
      (o1, o2) -> doCompare(o1.getSecond(), o2.getSecond());
  final K first;
  final V second;

  private Pair(K first, V second) {
    this.first = first;
    this.second = second;
  }

  // ~ -----------------------------------------------------------------------------

  @SuppressWarnings("unchecked")
  private static int doCompare(Object a, Object b) {
    Comparable ca = (Comparable) a;
    Comparable cb = (Comparable) b;
    // ~ ensure nulls are produced last
    if (ca == cb) {
      return 0;
    }
    if (ca == null) {
      return 1;
    }
    if (cb == null) {
      return -1;
    }
    return ca.compareTo(cb);
  }

  /**
   * Retrieves a comparator which compares pairs by their {@link #getFirst()} member.
   *
   * @param <K> the type of key of the pairs to compare
   * @param <V> type of the value of the pairs - can be anything since the returned comparator does
   *     not used it at all
   * @return a comparator based on the {@link #getFirst()} property of pairs
   */
  @SuppressWarnings({"unchecked", "UnusedDeclaration"})
  public static <K extends Comparable<K>, V> Comparator<Pair<K, V>> compareByFirst() {
    return (Comparator) CMP_BY_FIRST;
  }

  /**
   * Retrieves a comparator which compares pairs by their {@link #getSecond()} member.
   *
   * @param <K> the type of key of the pairs - can be anything since the returned comparator does
   *     not used it at all
   * @param <V> type of the value of the pairs to compare
   * @return a comparator based on the {@link #getSecond()} ()} property of pairs
   */
  @SuppressWarnings({"unchecked", "UnusedDeclaration"})
  public static <K, V extends Comparable<V>> Comparator<Pair<K, V>> compareBySecond() {
    return (Comparator) CMP_BY_SECOND;
  }

  /**
   * Construct the pair.
   *
   * @param <K> type of the key of the pair
   * @param <V> type of the value of the pair
   * @param first the first element of the pair
   * @param second the second element of the pair
   * @return a newly constructed pair
   */
  public static <K, V> Pair<K, V> of(K first, V second) {
    return new Pair<>(first, second);
  }

  public K getFirst() {
    return first;
  }

  public V getSecond() {
    return second;
  }

  @Override
  public String toString() {
    return "Pair{first='" + first + "', second='" + second + "'}";
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == this) {
      return true;
    }
    if (obj instanceof Pair) {
      Pair other = (Pair) obj;
      return Objects.equals(first, other.first) && Objects.equals(second, other.second);
    }
    return false;
  }

  @Override
  public int hashCode() {
    if (getFirst() != null && getSecond() != null) {
      int h = getFirst().hashCode();
      int shift = Integer.SIZE >> 1;
      return ((h >> shift) | (h << shift)) ^ getSecond().hashCode();
    }
    if (getFirst() != null) {
      return getFirst().hashCode();
    }
    if (getSecond() != null) {
      return getSecond().hashCode();
    }
    return 0;
  }
}
