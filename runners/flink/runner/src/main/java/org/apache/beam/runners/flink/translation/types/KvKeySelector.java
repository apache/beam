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
package org.apache.beam.runners.flink.translation.types;

import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.util.CoderUtils;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.KV;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;

/**
 * {@link KeySelector} that extracts the key from a {@link KV} and returns
 * it in encoded form as a {@code byte} array.
 */
public class KvKeySelector<InputT, K>
    implements KeySelector<WindowedValue<KV<K, InputT>>, byte[]>, ResultTypeQueryable<byte[]> {

  private final Coder<K> keyCoder;

  public KvKeySelector(Coder<K> keyCoder) {
    this.keyCoder = keyCoder;
  }

  @Override
  public byte[] getKey(WindowedValue<KV<K, InputT>> value) throws Exception {
    return CoderUtils.encodeToByteArray(keyCoder, value.getValue().getKey());
  }

  @Override
  public TypeInformation<byte[]> getProducedType() {
    return new EncodedValueTypeInformation();
  }
}
