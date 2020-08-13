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

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.apache.beam.runners.core.construction.PTransformReplacements;
import org.apache.beam.runners.core.construction.ReplacementOutputs;
import org.apache.beam.sdk.runners.AppliedPTransform;
import org.apache.beam.sdk.runners.PTransformOverrideFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.GroupIntoBatches;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PValue;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Iterators;

public class GroupIntoBatchesOverride {

  static class BatchGroupIntoBatchesOverrideFactory<K, V>
      implements PTransformOverrideFactory<
          PCollection<KV<K, V>>, PCollection<KV<K, Iterable<V>>>, GroupIntoBatches<K, V>> {

    @Override
    public PTransformReplacement<PCollection<KV<K, V>>, PCollection<KV<K, Iterable<V>>>>
        getReplacementTransform(
            AppliedPTransform<
                    PCollection<KV<K, V>>, PCollection<KV<K, Iterable<V>>>, GroupIntoBatches<K, V>>
                transform) {
      return PTransformReplacement.of(
          PTransformReplacements.getSingletonMainInput(transform),
          new BatchGroupIntoBatches(transform.getTransform().getBatchSize()));
    }

    @Override
    public Map<PValue, ReplacementOutput> mapOutputs(
        Map<TupleTag<?>, PValue> outputs, PCollection<KV<K, Iterable<V>>> newOutput) {
      return ReplacementOutputs.singleton(outputs, newOutput);
    }
  }

  /** Specialized implementation of {@link GroupIntoBatches} for bounded Dataflow pipelines. */
  static class BatchGroupIntoBatches<K, V>
      extends PTransform<PCollection<KV<K, V>>, PCollection<KV<K, Iterable<V>>>> {

    private final long batchSize;

    private BatchGroupIntoBatches(long batchSize) {
      this.batchSize = batchSize;
    }

    @Override
    public PCollection<KV<K, Iterable<V>>> expand(PCollection<KV<K, V>> input) {
      return input
          .apply("GroupAll", GroupByKey.create())
          .apply(
              "SplitIntoBatches",
              ParDo.of(
                  new DoFn<KV<K, Iterable<V>>, KV<K, Iterable<V>>>() {
                    @ProcessElement
                    public void process(ProcessContext c) {
                      // Iterators.partition lazily creates the partitions as they are accessed
                      // allowing it to partition very large iterators.
                      Iterator<List<V>> iterator =
                          Iterators.partition(c.element().getValue().iterator(), (int) batchSize);

                      // Note that GroupIntoBatches only outputs when the batch is non-empty.
                      while (iterator.hasNext()) {
                        c.output(KV.of(c.element().getKey(), iterator.next()));
                      }
                    }
                  }));
    }
  }

  static class StreamingGroupIntoBatchesOverrideFactory<K, V>
      implements PTransformOverrideFactory<
          PCollection<KV<K, V>>, PCollection<KV<K, Iterable<V>>>, GroupIntoBatches<K, V>> {

    private final DataflowRunner runner;

    StreamingGroupIntoBatchesOverrideFactory(DataflowRunner runner) {
      this.runner = runner;
    }

    @Override
    public PTransformReplacement<PCollection<KV<K, V>>, PCollection<KV<K, Iterable<V>>>>
        getReplacementTransform(
            AppliedPTransform<
                    PCollection<KV<K, V>>, PCollection<KV<K, Iterable<V>>>, GroupIntoBatches<K, V>>
                transform) {
      return PTransformReplacement.of(
          PTransformReplacements.getSingletonMainInput(transform),
          new StreamingGroupIntoBatches(runner, transform.getTransform()));
    }

    @Override
    public Map<PValue, ReplacementOutput> mapOutputs(
        Map<TupleTag<?>, PValue> outputs, PCollection<KV<K, Iterable<V>>> newOutput) {
      return ReplacementOutputs.singleton(outputs, newOutput);
    }
  }

  /**
   * Specialized implementation of {@link GroupIntoBatches} for unbounded Dataflow pipelines. The
   * override does the same thing as the original transform but additionally record the input to add
   * corresponding properties during the graph translation.
   */
  static class StreamingGroupIntoBatches<K, V>
      extends PTransform<PCollection<KV<K, V>>, PCollection<KV<K, Iterable<V>>>> {

    private final transient DataflowRunner runner;
    private final GroupIntoBatches<K, V> original;

    public StreamingGroupIntoBatches(DataflowRunner runner, GroupIntoBatches<K, V> original) {
      this.runner = runner;
      this.original = original;
    }

    @Override
    public PCollection<KV<K, Iterable<V>>> expand(PCollection<KV<K, V>> input) {
      runner.maybeRecordPCollectionWithAutoSharding(input);
      return input.apply(original);
    }
  }
}
