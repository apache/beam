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
package org.apache.beam.sdk.transforms.join;

import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;

/** A util library for join implementations. */
public class JoinUtils {

  /**
   * Returns the value coder for the given PCollection. Assumes that the value coder is an instance
   * of {@code KvCoder<K, V>}.
   */
  public static <K, V> Coder<V> getValueCoder(PCollection<KV<K, V>> pCollection) {
    // Assumes that the PCollection uses a KvCoder.
    Coder<?> entryCoder = pCollection.getCoder();
    if (!(entryCoder instanceof KvCoder<?, ?>)) {
      throw new IllegalArgumentException("PCollection does not use a KvCoder");
    }
    @SuppressWarnings("unchecked")
    KvCoder<K, V> coder = (KvCoder<K, V>) entryCoder;
    return coder.getValueCoder();
  }

  /**
   * Returns the key coder for the given PCollection. Assumes that the value coder is an instance of
   * {@code KvCoder<K, V>}.
   */
  public static <K, V> Coder<K> getKeyCoder(PCollection<KV<K, V>> pCollection) {
    // Assumes that the PCollection uses a KvCoder.
    Coder<?> entryCoder = pCollection.getCoder();
    if (!(entryCoder instanceof KvCoder<?, ?>)) {
      throw new IllegalArgumentException("PCollection does not use a KvCoder");
    }
    @SuppressWarnings("unchecked")
    KvCoder<K, V> coder = (KvCoder<K, V>) entryCoder;
    return coder.getKeyCoder();
  }

  /**
   * Returns a UnionTable for the given input PCollection, using the given union index and the given
   * unionTableEncoder.
   */
  public static <K, V> PCollection<KV<K, RawUnionValue>> makeUnionTable(
      final int index,
      PCollection<KV<K, V>> pCollection,
      KvCoder<K, RawUnionValue> unionTableEncoder) {
    return pCollection
        .apply(
            "MakeUnionTable" + index,
            MapElements.into(unionTableEncoder.getEncodedTypeDescriptor())
                .via(kv -> KV.of(kv.getKey(), new RawUnionValue(index, kv.getValue()))))
        .setCoder(unionTableEncoder);
  }
}
