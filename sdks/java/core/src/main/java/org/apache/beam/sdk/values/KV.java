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
package org.apache.beam.sdk.values;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Objects;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.SerializableComparator;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.MoreObjects;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * An immutable key/value pair.
 *
 * <p>Various {@link PTransform PTransforms} like {@link GroupByKey} and {@link Combine#perKey}
 * operate on {@link PCollection PCollections} of {@link KV KVs}.
 *
 * @param <K> the type of the key
 * @param <V> the type of the value
 */
public class KV<K extends @Nullable Object, V extends @Nullable Object> implements Serializable {
  /** Returns a {@link KV} with the given key and value. */
  public static <K extends @Nullable Object, V extends @Nullable Object> KV<K, V> of(
      K key, V value) {
    return new KV<>(key, value);
  }

  /** Returns the key of this {@link KV}. */
  public K getKey() {
    return key;
  }

  /** Returns the value of this {@link KV}. */
  public V getValue() {
    return value;
  }

  /////////////////////////////////////////////////////////////////////////////

  final K key;
  final V value;

  private KV(K key, V value) {
    this.key = key;
    this.value = value;
  }

  @Override
  public boolean equals(@Nullable Object other) {
    if (this == other) {
      return true;
    }
    if (!(other instanceof KV)) {
      return false;
    }
    KV<?, ?> otherKv = (KV<?, ?>) other;
    // Arrays are very common as values and keys, so deepEquals is mandatory
    return Objects.deepEquals(this.key, otherKv.key)
        && Objects.deepEquals(this.value, otherKv.value);
  }

  /**
   * A {@link Comparator} that orders {@link KV KVs} by the natural ordering of their keys.
   *
   * <p>A {@code null} key is less than any non-{@code null} key.
   */
  public static class OrderByKey<K extends Comparable<? super K>, V>
      implements SerializableComparator<KV<K, V>> {
    @Override
    public int compare(KV<K, V> a, KV<K, V> b) {
      if (a.key == null) {
        return b.key == null ? 0 : -1;
      } else if (b.key == null) {
        return 1;
      } else {
        return a.key.compareTo(b.key);
      }
    }
  }

  /**
   * A {@link Comparator} that orders {@link KV KVs} by the natural ordering of their values.
   *
   * <p>A {@code null} value is less than any non-{@code null} value.
   */
  public static class OrderByValue<K, V extends Comparable<? super V>>
      implements SerializableComparator<KV<K, V>> {
    @Override
    public int compare(KV<K, V> a, KV<K, V> b) {
      if (a.value == null) {
        return b.value == null ? 0 : -1;
      } else if (b.value == null) {
        return 1;
      } else {
        return a.value.compareTo(b.value);
      }
    }
  }

  @Override
  public int hashCode() {
    // Objects.deepEquals requires Arrays.deepHashCode for correctness
    return Arrays.deepHashCode(new Object[] {key, value});
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this).addValue(key).addValue(value).toString();
  }
}
