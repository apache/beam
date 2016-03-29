/*******************************************************************************
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
 ******************************************************************************/

package com.google.cloud.dataflow.sdk.util;

import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.values.KV;

/**
 * DoFn that makes timestamps and window assignments explicit in the value part of each key/value
 * pair.
 *
 * @param <K> the type of the keys of the input and output {@code PCollection}s
 * @param <V> the type of the values of the input {@code PCollection}
 */
@SystemDoFnInternal
public class ReifyTimestampAndWindowsDoFn<K, V>
    extends DoFn<KV<K, V>, KV<K, WindowedValue<V>>> {
  @Override
  public void processElement(ProcessContext c)
      throws Exception {
    KV<K, V> kv = c.element();
    K key = kv.getKey();
    V value = kv.getValue();
    c.output(KV.of(
        key,
        WindowedValue.of(
            value,
            c.timestamp(),
            c.windowingInternals().windows(),
            c.pane())));
  }
}
