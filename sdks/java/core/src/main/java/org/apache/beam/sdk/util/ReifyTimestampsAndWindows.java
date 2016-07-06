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
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.beam.sdk.util;

import static com.google.common.base.Preconditions.checkArgument;

import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.util.WindowedValue.FullWindowedValueCoder;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;

/**
 * Helper transform that makes timestamps and window assignments explicit in the value part of
 * each key/value pair.
 */
public class ReifyTimestampsAndWindows<K, V>
    extends PTransform<PCollection<KV<K, V>>, PCollection<KV<K, WindowedValue<V>>>> {

  @Override
  public PCollection<KV<K, WindowedValue<V>>> apply(PCollection<KV<K, V>> input) {

    // The requirement to use a KvCoder *is* actually a model-level requirement, not specific
    // to this implementation of GBK. All runners need a way to get the key.
    checkArgument(
        input.getCoder() instanceof KvCoder,
        "%s requires its input to use a %s",
        GroupByKey.class.getSimpleName(),
        KvCoder.class.getSimpleName());

    @SuppressWarnings("unchecked")
    KvCoder<K, V> inputKvCoder = (KvCoder<K, V>) input.getCoder();
    Coder<K> keyCoder = inputKvCoder.getKeyCoder();
    Coder<V> inputValueCoder = inputKvCoder.getValueCoder();
    Coder<WindowedValue<V>> outputValueCoder =
        FullWindowedValueCoder.of(
            inputValueCoder, input.getWindowingStrategy().getWindowFn().windowCoder());
    Coder<KV<K, WindowedValue<V>>> outputKvCoder = KvCoder.of(keyCoder, outputValueCoder);
    return input
        .apply(ParDo.of(new ReifyTimestampAndWindowsDoFn<K, V>()))
        .setCoder(outputKvCoder);
  }
}
