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
package org.apache.beam.runners.spark.structuredstreaming.translation.helpers;

import static org.apache.beam.runners.spark.structuredstreaming.translation.helpers.EncoderHelpers.kvEncoder;

import java.util.function.Function;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.values.KV;
import org.apache.spark.sql.Encoder;

@Internal
public interface EncoderProvider {
  interface Factory<T> extends Function<Coder<T>, Encoder<T>> {
    Factory<?> INSTANCE = EncoderHelpers::encoderFor;
  }

  <T> Encoder<T> encoderOf(Coder<T> coder, Factory<T> factory);

  default <T> Encoder<T> encoderOf(Coder<T> coder) {
    return coder instanceof KvCoder
        ? (Encoder<T>) kvEncoderOf((KvCoder) coder)
        : encoderOf(coder, encoderFactory());
  }

  default <K, V> Encoder<KV<K, V>> kvEncoderOf(KvCoder<K, V> coder) {
    return encoderOf(coder, c -> kvEncoder(keyEncoderOf(coder), valueEncoderOf(coder)));
  }

  default <K, V> Encoder<K> keyEncoderOf(KvCoder<K, V> coder) {
    return encoderOf(coder.getKeyCoder(), encoderFactory());
  }

  default <K, V> Encoder<V> valueEncoderOf(KvCoder<K, V> coder) {
    return encoderOf(coder.getValueCoder(), encoderFactory());
  }

  default <T> Factory<T> encoderFactory() {
    return (Factory<T>) Factory.INSTANCE;
  }
}
