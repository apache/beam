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
package org.apache.beam.runners.core;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.transforms.Materializations;
import org.apache.beam.sdk.transforms.Materializations.MultimapView;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.FluentIterable;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Iterables;
import org.checkerframework.checker.nullness.qual.Nullable;

/** An in-memory representation of {@link MultimapView}. */
public class InMemoryMultimapSideInputView<K, V> implements Materializations.MultimapView<K, V> {
  /** An empty {@link MultimapView}. */
  private static final MultimapView EMPTY =
      new MultimapView() {
        @Override
        public Iterable get() {
          return Collections.emptyList();
        }

        @Override
        public Iterable get(@Nullable Object k) {
          return Collections.emptyList();
        }
      };

  /**
   * Creates a {@link MultimapView} from the provided values. The provided {@link Coder} is used to
   * guarantee structural equality for keys instead of assuming Java object equality.
   */
  public static <K, V> MultimapView<K, V> fromIterable(
      Coder<K> keyCoder, Iterable<KV<K, V>> values) {
    // We specifically use a hash map to allow for null keys
    Map<Object, KV<K, List<V>>> data = new HashMap<>();

    for (KV<K, V> value : values) {
      KV<K, List<V>> keyedValues =
          data.computeIfAbsent(
              keyCoder.structuralValue(value.getKey()),
              o -> KV.of(value.getKey(), new ArrayList<>()));
      keyedValues.getValue().add(value.getValue());
    }
    return new InMemoryMultimapSideInputView(keyCoder, data);
  }

  /** Returns an empty {@link MultimapView}. */
  public static <K, V> MultimapView<K, V> empty() {
    return EMPTY;
  }

  private final Coder<K> keyCoder;
  private final Map<Object, KV<K, List<V>>> structuralKeyToValuesMap;

  private InMemoryMultimapSideInputView(Coder<K> keyCoder, Map<Object, KV<K, List<V>>> data) {
    this.keyCoder = keyCoder;
    this.structuralKeyToValuesMap = data;
  }

  @Override
  public Iterable<K> get() {
    return Iterables.unmodifiableIterable(
        FluentIterable.from(structuralKeyToValuesMap.values())
            .transform(kListKV -> kListKV.getKey()));
  }

  @Override
  public Iterable<V> get(K k) {
    KV<K, List<V>> records = structuralKeyToValuesMap.get(keyCoder.structuralValue(k));
    if (records == null) {
      return Collections.emptyList();
    }
    return records.getValue();
  }
}
