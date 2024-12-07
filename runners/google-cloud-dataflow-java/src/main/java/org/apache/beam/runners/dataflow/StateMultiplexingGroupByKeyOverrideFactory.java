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
package org.apache.beam.runners.dataflow;

import org.apache.beam.runners.dataflow.internal.StateMultiplexingGroupByKey;
import org.apache.beam.sdk.runners.AppliedPTransform;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.util.construction.PTransformReplacements;
import org.apache.beam.sdk.util.construction.SingleInputOutputOverrideFactory;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;

class StateMultiplexingGroupByKeyOverrideFactory<K, V>
    extends SingleInputOutputOverrideFactory<
        PCollection<KV<K, V>>, PCollection<KV<K, Iterable<V>>>, GroupByKey<K, V>> {

  @Override
  public PTransformReplacement<PCollection<KV<K, V>>, PCollection<KV<K, Iterable<V>>>>
      getReplacementTransform(
          AppliedPTransform<
                  PCollection<KV<K, V>>, PCollection<KV<K, Iterable<V>>>, GroupByKey<K, V>>
              transform) {
    return PTransformReplacement.of(
        PTransformReplacements.getSingletonMainInput(transform),
        StateMultiplexingGroupByKey.create(transform.getTransform().fewKeys()));
  }
}
