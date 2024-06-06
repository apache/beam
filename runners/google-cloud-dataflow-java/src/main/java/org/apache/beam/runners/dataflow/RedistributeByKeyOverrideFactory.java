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

import org.apache.beam.sdk.runners.AppliedPTransform;
import org.apache.beam.sdk.runners.PTransformOverrideFactory;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.Redistribute.RedistributeByKey;
import org.apache.beam.sdk.util.construction.PTransformReplacements;
import org.apache.beam.sdk.util.construction.SingleInputOutputOverrideFactory;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;

class RedistributeByKeyOverrideFactory<K, V>
    extends SingleInputOutputOverrideFactory<
        PCollection<KV<K, V>>, PCollection<KV<K, V>>, RedistributeByKey<K, V>> {

  private final DataflowRunner runner;

  RedistributeByKeyOverrideFactory(DataflowRunner runner) {
    this.runner = runner;
  }

  @Override
  public PTransformReplacement<PCollection<KV<K, V>>, PCollection<KV<K, V>>>
      getReplacementTransform(
          AppliedPTransform<PCollection<KV<K, V>>, PCollection<KV<K, V>>, RedistributeByKey<K, V>>
              transform) {
    return PTransformOverrideFactory.PTransformReplacement.of(
        PTransformReplacements.getSingletonMainInput(transform),
        new DataflowRedistributeByKey<>(
            runner,
            transform.getTransform(),
            PTransformReplacements.getSingletonMainOutput(transform)));
  }

  /** Specialized implementation of {@link RedistributeByKey} for Dataflow pipelines. */
  private static class DataflowRedistributeByKey<K, V>
      extends PTransform<PCollection<KV<K, V>>, PCollection<KV<K, V>>> {

    private final transient DataflowRunner runner;
    private final RedistributeByKey<K, V> originalTransform;
    private final transient PCollection<KV<K, V>> originalOutput;

    private DataflowRedistributeByKey(
        DataflowRunner runner,
        RedistributeByKey<K, V> originalTransform,
        PCollection<KV<K, V>> originalOutput) {
      this.runner = runner;
      this.originalTransform = originalTransform;
      this.originalOutput = originalOutput;
    }

    @Override
    public PCollection<KV<K, V>> expand(PCollection<KV<K, V>> input) {
      if (originalTransform.getAllowDuplicates()) {
        runner.maybeRecordPCollectionAllowDuplicates(originalOutput);
      }
      return input.apply(originalTransform);
    }
  }
}
