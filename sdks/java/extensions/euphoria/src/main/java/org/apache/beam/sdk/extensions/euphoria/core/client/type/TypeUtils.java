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
package org.apache.beam.sdk.extensions.euphoria.core.client.type;

import java.util.Objects;
import org.apache.beam.sdk.extensions.euphoria.core.client.util.Triple;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.TypeParameter;
import org.checkerframework.checker.nullness.qual.Nullable;

/** A collections of {@link TypeDescriptor} construction methods. */
public class TypeUtils {

  /**
   * Creates composite {@link TypeDescriptor} of {@code <KV<K,V>}. Provided that both given
   * parameters are non null.
   *
   * @param key key type descriptor
   * @param value value type descriptor
   * @param <K> key tye parameter
   * @param <V> value type parameter
   * @return {@link TypeDescriptor} of {@code <KV<K,V>} when {@code key} and {@code value} are not
   *     null, null otherwise
   */
  public static @Nullable <K, V> TypeDescriptor<KV<K, V>> keyValues(
      TypeDescriptor<K> key, TypeDescriptor<V> value) {

    if (Objects.isNull(key) || Objects.isNull(value)) {
      return null;
    }

    return new TypeDescriptor<KV<K, V>>() {}.where(new TypeParameter<K>() {}, key)
        .where(new TypeParameter<V>() {}, value);
  }

  /**
   * Creates composite {@link TypeDescriptor} of {@code <KV<K,V>}. Provided that both given
   * parameters are non null.
   *
   * @param key key type descriptor
   * @param value value type descriptor
   * @param <K> key type parameter
   * @param <V> value type parameter
   * @return {@link TypeDescriptor} of {@code <KV<K,V>} when {@code key} and {@code value} are not
   *     null, null otherwise
   */
  public static @Nullable <K, V> TypeDescriptor<KV<K, V>> keyValues(Class<K> key, Class<V> value) {

    if (Objects.isNull(key) || Objects.isNull(value)) {
      return null;
    }

    return keyValues(TypeDescriptor.of(key), TypeDescriptor.of(value));
  }

  /**
   * Creates composite {@link TypeDescriptor} of {@code <Triple<K,V, ScoreT>}. Provided that all
   * given parameters are non null.
   *
   * @param key key type descriptor
   * @param value value type descriptor
   * @param score score type descriptor
   * @param <K> key type parameter
   * @param <V> value type parameter
   * @param <ScoreT> score type parameter
   * @return {@link TypeDescriptor} of {@code <Triple<K,V, ScoreT>} or {@code null} when any given
   *     parameter is {@code null}
   */
  public static <K, V, ScoreT> TypeDescriptor<Triple<K, V, ScoreT>> triplets(
      TypeDescriptor<K> key, TypeDescriptor<V> value, TypeDescriptor<ScoreT> score) {

    if (Objects.isNull(key) || Objects.isNull(value) || Objects.isNull(score)) {
      return null;
    }

    return new TypeDescriptor<Triple<K, V, ScoreT>>() {}.where(new TypeParameter<K>() {}, key)
        .where(new TypeParameter<V>() {}, value)
        .where(new TypeParameter<ScoreT>() {}, score);
  }
}
