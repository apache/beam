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
package org.apache.beam.runners.flink.translation.wrappers.streaming;

import org.apache.beam.runners.flink.translation.types.CoderTypeInformation;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.KV;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;

/**
 * {@link KeySelector} for keying a {@link org.apache.beam.sdk.values.PCollection}
 * of {@code KV<K, V>}.
 */
public class KVKeySelector<K, V>
    implements KeySelector<WindowedValue<KV<K, V>>, K>, ResultTypeQueryable<K> {

  private final TypeInformation<K> keyTypeInfo;

  private KVKeySelector(TypeInformation<K> keyTypeInfo) {
    this.keyTypeInfo = keyTypeInfo;
  }

  @Override
  @SuppressWarnings("unchecked")
  public K getKey(WindowedValue<KV<K, V>> value) throws Exception {
    K key = value.getValue().getKey();
    // hack, because Flink does not allow null keys
    return key != null ? key : (K) new Integer(0);
  }

  @Override
  public TypeInformation<K> getProducedType() {
    return keyTypeInfo;
  }

  public static <K, V> KeyedStream<WindowedValue<KV<K, V>>, K> keyBy(
      DataStream<WindowedValue<KV<K, V>>> inputDataStream,
      KvCoder<K, V> inputKvCoder) {

    final Coder<K> keyCoder = inputKvCoder.getKeyCoder();
    final TypeInformation<K> keyTypeInfo = new CoderTypeInformation<>(keyCoder);

    return inputDataStream.keyBy(new KVKeySelector<K, V>(keyTypeInfo));
  }

}
