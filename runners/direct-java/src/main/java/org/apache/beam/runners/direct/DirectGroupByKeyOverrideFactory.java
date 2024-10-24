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
package org.apache.beam.runners.direct;

import org.apache.beam.sdk.runners.AppliedPTransform;
import org.apache.beam.sdk.runners.PTransformOverrideFactory;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.util.construction.PTransformReplacements;
import org.apache.beam.sdk.util.construction.SingleInputOutputOverrideFactory;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Iterables;

/** A {@link PTransformOverrideFactory} for {@link GroupByKey} PTransforms. */
final class DirectGroupByKeyOverrideFactory<K, V>
    extends SingleInputOutputOverrideFactory<
        PCollection<KV<K, V>>,
        PCollection<KV<K, Iterable<V>>>,
        PTransform<PCollection<KV<K, V>>, PCollection<KV<K, Iterable<V>>>>> {
  @Override
  public PTransformReplacement<PCollection<KV<K, V>>, PCollection<KV<K, Iterable<V>>>>
      getReplacementTransform(
          AppliedPTransform<
                  PCollection<KV<K, V>>,
                  PCollection<KV<K, Iterable<V>>>,
                  PTransform<PCollection<KV<K, V>>, PCollection<KV<K, Iterable<V>>>>>
              transform) {

    PCollection<KV<K, Iterable<V>>> output =
        (PCollection<KV<K, Iterable<V>>>) Iterables.getOnlyElement(transform.getOutputs().values());

    return PTransformReplacement.of(
        PTransformReplacements.getSingletonMainInput(transform),
        new DirectGroupByKey<>(transform.getTransform(), output.getWindowingStrategy()));
  }
}
