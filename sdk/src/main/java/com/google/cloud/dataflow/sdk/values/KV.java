/*
 * Copyright (C) 2014 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.google.cloud.dataflow.sdk.values;

import java.io.Serializable;
import java.util.Comparator;

/**
 * An immutable key/value pair.
 *
 * <p> Various
 * {@link com.google.cloud.dataflow.sdk.transforms.PTransform}s like
 * {@link com.google.cloud.dataflow.sdk.transforms.GroupByKey} and
 * {@link com.google.cloud.dataflow.sdk.transforms.Combine#perKey}
 * work on {@link PCollection}s of KVs.
 *
 * @param <K> the type of the key
 * @param <V> the type of the value
 */
public class KV<K, V> implements Serializable {
  private static final long serialVersionUID = 0;

  /** Returns a KV with the given key and value. */
  public static <K, V> KV<K, V> of(K key, V value) {
    return new KV<>(key, value);
  }

  /** Returns the key of this KV. */
  public K getKey() {
    return key;
  }

  /** Returns the value of this KV. */
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
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o instanceof KV) {
      KV<?, ?> that = (KV<?, ?>) o;
      return (this.key == null ? that.key == null
              : this.key.equals(that.key))
          && (this.value == null ? that.value == null
              : this.value.equals(that.value));
    }
    return false;
  }

  /** Orders the KV by the key. A null key is less than any non-null key. */
  @SuppressWarnings("serial")
  public static class OrderByKey<K extends Comparable<? super K>, V> implements
      Comparator<KV<K, V>>, Serializable {
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

  /** Orders the KV by the value. A null value is less than any non-null value. */
  @SuppressWarnings("serial")
  public static class OrderByValue<K, V extends Comparable<? super V>>
      implements Comparator<KV<K, V>>, Serializable {
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
    return getClass().hashCode()
        + (key == null ? 0 : key.hashCode())
        + (value == null ? 0 : value.hashCode());
  }

  @Override
  public String toString() {
    return "KV(" + key + ", " + value + ")";
  }
}
