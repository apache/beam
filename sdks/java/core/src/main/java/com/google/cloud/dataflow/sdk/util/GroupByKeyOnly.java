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
import com.google.cloud.dataflow.sdk.transforms.GroupByKey;
import com.google.cloud.dataflow.sdk.transforms.PTransform;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.PCollection;

/**
 * Runner-specific primitive that groups by key only, ignoring any window assignments.
 */
public class GroupByKeyOnly<K, V>
    extends PTransform<PCollection<KV<K, V>>,
                       PCollection<KV<K, Iterable<V>>>> {

  @SuppressWarnings({"rawtypes", "unchecked"})
  @Override
  public PCollection<KV<K, Iterable<V>>> apply(PCollection<KV<K, V>> input) {
    return PCollection.<KV<K, Iterable<V>>>createPrimitiveOutputInternal(
        input.getPipeline(), input.getWindowingStrategy(), input.isBounded());
  }

  @Override
  public Coder<KV<K, Iterable<V>>> getDefaultOutputCoder(PCollection<KV<K, V>> input) {
    return GroupByKey.getOutputKvCoder(input.getCoder());
  }
}
