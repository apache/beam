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

import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.beam.runners.core.construction.PTransformReplacements;
import org.apache.beam.runners.core.construction.ReplacementOutputs;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.runners.AppliedPTransform;
import org.apache.beam.sdk.runners.PTransformOverrideFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.GroupIntoBatches;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.util.ShardedKey;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Iterators;

@SuppressWarnings({
  "rawtypes" // TODO(https://issues.apache.org/jira/browse/BEAM-10556)
})
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
          new BatchGroupIntoBatches<>(transform.getTransform().getBatchingParams().getBatchSize()));
    }

    @Override
    public Map<PCollection<?>, ReplacementOutput> mapOutputs(
        Map<TupleTag<?>, PCollection<?>> outputs, PCollection<KV<K, Iterable<V>>> newOutput) {
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

  static class BatchGroupIntoBatchesWithShardedKeyOverrideFactory<K, V>
      implements PTransformOverrideFactory<
          PCollection<KV<K, V>>,
          PCollection<KV<ShardedKey<K>, Iterable<V>>>,
          GroupIntoBatches<K, V>.WithShardedKey> {

    @Override
    public PTransformReplacement<PCollection<KV<K, V>>, PCollection<KV<ShardedKey<K>, Iterable<V>>>>
        getReplacementTransform(
            AppliedPTransform<
                    PCollection<KV<K, V>>,
                    PCollection<KV<ShardedKey<K>, Iterable<V>>>,
                    GroupIntoBatches<K, V>.WithShardedKey>
                transform) {
      return PTransformReplacement.of(
          PTransformReplacements.getSingletonMainInput(transform),
          new BatchGroupIntoBatchesWithShardedKey<>(
              transform.getTransform().getBatchingParams().getBatchSize()));
    }

    @Override
    public Map<PCollection<?>, ReplacementOutput> mapOutputs(
        Map<TupleTag<?>, PCollection<?>> outputs,
        PCollection<KV<ShardedKey<K>, Iterable<V>>> newOutput) {
      return ReplacementOutputs.singleton(outputs, newOutput);
    }
  }

  /**
   * Specialized implementation of {@link GroupIntoBatches.WithShardedKey} for bounded Dataflow
   * pipelines.
   */
  static class BatchGroupIntoBatchesWithShardedKey<K, V>
      extends PTransform<PCollection<KV<K, V>>, PCollection<KV<ShardedKey<K>, Iterable<V>>>> {

    private final long batchSize;

    private BatchGroupIntoBatchesWithShardedKey(long batchSize) {
      this.batchSize = batchSize;
    }

    @Override
    public PCollection<KV<ShardedKey<K>, Iterable<V>>> expand(PCollection<KV<K, V>> input) {
      return shardKeys(input).apply(new BatchGroupIntoBatches<>(batchSize));
    }
  }

  static class StreamingGroupIntoBatchesWithShardedKeyOverrideFactory<K, V>
      implements PTransformOverrideFactory<
          PCollection<KV<K, V>>,
          PCollection<KV<ShardedKey<K>, Iterable<V>>>,
          GroupIntoBatches<K, V>.WithShardedKey> {

    private final DataflowRunner runner;

    StreamingGroupIntoBatchesWithShardedKeyOverrideFactory(DataflowRunner runner) {
      this.runner = runner;
    }

    @Override
    public PTransformReplacement<PCollection<KV<K, V>>, PCollection<KV<ShardedKey<K>, Iterable<V>>>>
        getReplacementTransform(
            AppliedPTransform<
                    PCollection<KV<K, V>>,
                    PCollection<KV<ShardedKey<K>, Iterable<V>>>,
                    GroupIntoBatches<K, V>.WithShardedKey>
                transform) {
      return PTransformReplacement.of(
          PTransformReplacements.getSingletonMainInput(transform),
          new StreamingGroupIntoBatchesWithShardedKey<>(
              runner,
              transform.getTransform(),
              PTransformReplacements.getSingletonMainOutput(transform)));
    }

    @Override
    public Map<PCollection<?>, ReplacementOutput> mapOutputs(
        Map<TupleTag<?>, PCollection<?>> outputs,
        PCollection<KV<ShardedKey<K>, Iterable<V>>> newOutput) {
      return ReplacementOutputs.singleton(outputs, newOutput);
    }
  }

  /**
   * Specialized implementation of {@link GroupIntoBatches.WithShardedKey} for unbounded Dataflow
   * pipelines. The override does the same thing as the original transform but additionally records
   * the output in order to append required step properties during the graph translation.
   */
  static class StreamingGroupIntoBatchesWithShardedKey<K, V>
      extends PTransform<PCollection<KV<K, V>>, PCollection<KV<ShardedKey<K>, Iterable<V>>>> {

    private final transient DataflowRunner runner;
    private final GroupIntoBatches<K, V>.WithShardedKey originalTransform;
    private final transient PCollection<KV<ShardedKey<K>, Iterable<V>>> originalOutput;

    public StreamingGroupIntoBatchesWithShardedKey(
        DataflowRunner runner,
        GroupIntoBatches<K, V>.WithShardedKey original,
        PCollection<KV<ShardedKey<K>, Iterable<V>>> output) {
      this.runner = runner;
      this.originalTransform = original;
      this.originalOutput = output;
    }

    @Override
    public PCollection<KV<ShardedKey<K>, Iterable<V>>> expand(PCollection<KV<K, V>> input) {
      // Record the output PCollection of the original transform since the new output will be
      // replaced by the original one when the replacement transform is wired to other nodes in the
      // graph, although the old and the new outputs are effectively the same.
      runner.maybeRecordPCollectionWithAutoSharding(originalOutput);
      return input.apply(originalTransform);
    }
  }

  private static final UUID workerUuid = UUID.randomUUID();

  private static <K, V> PCollection<KV<ShardedKey<K>, V>> shardKeys(PCollection<KV<K, V>> input) {
    KvCoder<K, V> inputCoder = (KvCoder<K, V>) input.getCoder();
    org.apache.beam.sdk.coders.Coder<K> keyCoder =
        (org.apache.beam.sdk.coders.Coder<K>) inputCoder.getCoderArguments().get(0);
    org.apache.beam.sdk.coders.Coder<V> valueCoder =
        (org.apache.beam.sdk.coders.Coder<V>) inputCoder.getCoderArguments().get(1);
    return input
        .apply(
            "Shard Keys",
            MapElements.via(
                new SimpleFunction<KV<K, V>, KV<ShardedKey<K>, V>>() {
                  @Override
                  public KV<ShardedKey<K>, V> apply(KV<K, V> input) {
                    long tid = Thread.currentThread().getId();
                    ByteBuffer buffer = ByteBuffer.allocate(3 * Long.BYTES);
                    buffer.putLong(workerUuid.getMostSignificantBits());
                    buffer.putLong(workerUuid.getLeastSignificantBits());
                    buffer.putLong(tid);
                    return KV.of(ShardedKey.of(input.getKey(), buffer.array()), input.getValue());
                  }
                }))
        .setCoder(KvCoder.of(ShardedKey.Coder.of(keyCoder), valueCoder));
  }
}
