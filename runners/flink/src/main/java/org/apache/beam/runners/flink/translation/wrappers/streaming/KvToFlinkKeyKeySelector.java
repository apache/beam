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

import org.apache.beam.runners.flink.adapter.FlinkKey;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.KV;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.api.java.typeutils.ValueTypeInfo;

/**
 * {@link KeySelector} that retrieves a key from a {@link KV}. This will return the key as encoded
 * by the provided {@link Coder} in a {@link FlinkKey}. This ensures that all key
 * comparisons/hashing happen on the encoded form.
 */
public class KvToFlinkKeyKeySelector<K, V>
    implements KeySelector<WindowedValue<KV<K, V>>, FlinkKey>, ResultTypeQueryable<FlinkKey> {

  private final Coder<K> keyCoder;

  public KvToFlinkKeyKeySelector(Coder<K> keyCoder) {
    this.keyCoder = keyCoder;
  }

  @Override
  public FlinkKey getKey(WindowedValue<KV<K, V>> value) {
    K key = value.getValue().getKey();
    return FlinkKey.of(key, keyCoder);
  }

  @Override
  public TypeInformation<FlinkKey> getProducedType() {
    return ValueTypeInfo.of(FlinkKey.class);
  }
}
