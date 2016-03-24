/*
 * Copyright (C) 2015 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.google.cloud.dataflow.sdk.util;

import com.google.cloud.dataflow.sdk.coders.Coder;
import com.google.cloud.dataflow.sdk.coders.KvCoder;
import com.google.cloud.dataflow.sdk.transforms.PTransform;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.util.WindowedValue.FullWindowedValueCoder;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.PCollection;

/**
 * Helper transform that makes timestamps and window assignments
 * explicit in the value part of each key/value pair.
 */
public class ReifyTimestampsAndWindows<K, V>
    extends PTransform<PCollection<KV<K, V>>,
                       PCollection<KV<K, WindowedValue<V>>>> {
  @Override
  public PCollection<KV<K, WindowedValue<V>>> apply(
      PCollection<KV<K, V>> input) {
    @SuppressWarnings("unchecked")
    KvCoder<K, V> inputKvCoder = (KvCoder<K, V>) input.getCoder();
    Coder<K> keyCoder = inputKvCoder.getKeyCoder();
    Coder<V> inputValueCoder = inputKvCoder.getValueCoder();
    Coder<WindowedValue<V>> outputValueCoder = FullWindowedValueCoder.of(
        inputValueCoder, input.getWindowingStrategy().getWindowFn().windowCoder());
    Coder<KV<K, WindowedValue<V>>> outputKvCoder =
        KvCoder.of(keyCoder, outputValueCoder);
    return input.apply(ParDo.of(new ReifyTimestampAndWindowsDoFn<K, V>()))
        .setCoder(outputKvCoder);
  }
}
