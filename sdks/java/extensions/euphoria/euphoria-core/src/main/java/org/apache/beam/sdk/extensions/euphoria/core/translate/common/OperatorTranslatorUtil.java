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
package org.apache.beam.sdk.extensions.euphoria.core.translate.common;

import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.extensions.euphoria.core.client.functional.UnaryFunction;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;

/** Shared utility methods among operator translators. */
public class OperatorTranslatorUtil {

  /** Transform input to KV elements. */
  public static <K, ValueT> PCollection<KV<K, ValueT>> getKVInputCollection(
      PCollection<ValueT> inputPCollection,
      UnaryFunction<ValueT, K> keyExtractor,
      Coder<K> keyCoder,
      Coder<ValueT> valueCoder,
      String transformName) {

    @SuppressWarnings("unchecked")
    PCollection<ValueT> typedInput = inputPCollection;
    typedInput.setCoder(valueCoder);

    PCollection<KV<K, ValueT>> kvInput =
        typedInput.apply(transformName, ParDo.of(new InputToKvDoFn<>(keyExtractor)));
    kvInput.setCoder(KvCoder.of(keyCoder, valueCoder));

    return kvInput;
  }
}
