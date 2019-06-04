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

import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.transforms.Materializations;
import org.apache.beam.sdk.transforms.Materializations.MultimapView;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.vendor.guava.v20_0.com.google.common.collect.ArrayListMultimap;
import org.apache.beam.vendor.guava.v20_0.com.google.common.collect.Multimap;
import org.apache.beam.vendor.guava.v20_0.com.google.common.collect.Multimaps;

/** An in-memory representation of {@link MultimapView}. */
public class InMemoryMultimapSideInputView<K, V> implements Materializations.MultimapView<K, V> {

  /**
   * Creates a {@link MultimapView} from the provided values. The provided {@link Coder} is used to
   * guarantee structural equality for keys instead of assuming Java object equality.
   */
  public static <K, V> MultimapView<K, V> fromIterable(
      Coder<K> keyCoder, Iterable<KV<K, V>> values) {
    // We specifically use an array list multimap to allow for:
    //  * null keys
    //  * null values
    //  * duplicate values
    Multimap<Object, Object> multimap = ArrayListMultimap.create();
    for (KV<K, V> value : values) {
      multimap.put(keyCoder.structuralValue(value.getKey()), value.getValue());
    }
    return new InMemoryMultimapSideInputView(keyCoder, Multimaps.unmodifiableMultimap(multimap));
  }

  private final Coder<K> keyCoder;
  private final Multimap<Object, V> structuralKeyToValuesMap;

  private InMemoryMultimapSideInputView(Coder<K> keyCoder, Multimap<Object, V> data) {
    this.keyCoder = keyCoder;
    this.structuralKeyToValuesMap = data;
  }

  @Override
  public Iterable<V> get(K k) {
    return structuralKeyToValuesMap.get(keyCoder.structuralValue(k));
  }
}
