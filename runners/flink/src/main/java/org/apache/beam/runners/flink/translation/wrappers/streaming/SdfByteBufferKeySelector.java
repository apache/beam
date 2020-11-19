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

import java.nio.ByteBuffer;
import org.apache.beam.runners.core.construction.SerializablePipelineOptions;
import org.apache.beam.runners.flink.translation.types.CoderTypeInformation;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.KV;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;

/**
 * {@link KeySelector} that retrieves a key from a {@code KV<KV<element, KV<restriction,
 * watermarkState>>, size>}. This will return the element as encoded by the provided {@link Coder}
 * in a {@link ByteBuffer}. This ensures that all key comparisons/hashing happen on the encoded
 * form. Note that the reason we don't use the whole {@code KV<KV<element, KV<restriction,
 * watermarkState>>, Double>} as the key is when checkpoint happens, we will get different
 * restriction/watermarkState/size, which Flink treats as a new key. Using new key to set state and
 * timer may cause defined behavior.
 */
public class SdfByteBufferKeySelector<K, V>
    implements KeySelector<WindowedValue<KV<KV<K, V>, Double>>, ByteBuffer>,
        ResultTypeQueryable<ByteBuffer> {

  private final Coder<K> keyCoder;
  private final SerializablePipelineOptions pipelineOptions;

  public SdfByteBufferKeySelector(Coder<K> keyCoder, SerializablePipelineOptions pipelineOptions) {
    this.keyCoder = keyCoder;
    this.pipelineOptions = pipelineOptions;
  }

  @Override
  public ByteBuffer getKey(WindowedValue<KV<KV<K, V>, Double>> value) {
    K key = value.getValue().getKey().getKey();
    return FlinkKeyUtils.encodeKey(key, keyCoder);
  }

  @Override
  public TypeInformation<ByteBuffer> getProducedType() {
    return new CoderTypeInformation<>(FlinkKeyUtils.ByteBufferCoder.of(), pipelineOptions.get());
  }
}
