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
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.runners.AppliedPTransform;
import org.apache.beam.sdk.runners.PTransformOverrideFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.GroupIntoBatches;
import org.apache.beam.sdk.transforms.GroupIntoBatches.BatchingParams;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.util.ShardedKey;
import org.apache.beam.sdk.util.construction.PTransformReplacements;
import org.apache.beam.sdk.util.construction.ReplacementOutputs;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Lists;

@SuppressWarnings({
  "rawtypes" // TODO(https://github.com/apache/beam/issues/20447)
})
public class GroupIntoBatchesOverride {

  static class BatchGroupIntoBatchesOverrideFactory<K, V>
      implements PTransformOverrideFactory<
          PCollection<KV<K, V>>, PCollection<KV<K, Iterable<V>>>, GroupIntoBatches<K, V>> {
    private final DataflowRunner runner;

    BatchGroupIntoBatchesOverrideFactory(DataflowRunner runner) {
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
          new BatchGroupIntoBatches<>(
              transform.getTransform().getBatchingParams(),
              runner,
              PTransformReplacements.getSingletonMainOutput(transform)));
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
    private final BatchingParams<V> batchingParams;
    private final transient DataflowRunner runner;
    private final transient PCollection<KV<K, Iterable<V>>> originalOutput;

    private BatchGroupIntoBatches(
        BatchingParams<V> batchingParams,
        DataflowRunner runner,
        PCollection<KV<K, Iterable<V>>> originalOutput) {
      this.batchingParams = batchingParams;
      this.runner = runner;
      this.originalOutput = originalOutput;
    }

    @Override
    public PCollection<KV<K, Iterable<V>>> expand(PCollection<KV<K, V>> input) {
      // Record the output PCollection of the original transform since the new output will be
      // replaced by the original one when the replacement transform is wired to other nodes in the
      // graph, although the old and the new outputs are effectively the same.
      runner.maybeRecordPCollectionPreservedKeys(originalOutput);

      KvCoder<K, V> inputCoder = (KvCoder<K, V>) input.getCoder();
      final Coder<V> valueCoder = (Coder<V>) inputCoder.getCoderArguments().get(1);
      final SerializableFunction<V, Long> weigher = batchingParams.getWeigher(valueCoder);
      long maxBatchSizeElements = batchingParams.getBatchSize();
      long maxBatchSizeBytes = batchingParams.getBatchSizeBytes();

      return input
          .apply("GroupAll", GroupByKey.create())
          .apply(
              "SplitIntoBatches",
              ParDo.of(
                  new DoFn<KV<K, Iterable<V>>, KV<K, Iterable<V>>>() {
                    @ProcessElement
                    public void process(ProcessContext c) {
                      List<V> currentBatch = Lists.newArrayList();
                      long batchSizeBytes = 0;
                      for (V element : c.element().getValue()) {
                        long currentSizeBytes = 0;
                        if (weigher != null) {
                          currentSizeBytes += weigher.apply(element);
                          // Ensure that the batch is smaller than the byte size limit
                          if (currentSizeBytes + batchSizeBytes > maxBatchSizeBytes
                              && !currentBatch.isEmpty()) {
                            c.output(KV.of(c.element().getKey(), currentBatch));
                            currentBatch = Lists.newArrayList();
                            batchSizeBytes = 0;
                          }
                        }
                        currentBatch.add(element);
                        batchSizeBytes += currentSizeBytes;
                        if (currentBatch.size() == maxBatchSizeElements
                            || (maxBatchSizeBytes != Long.MAX_VALUE
                                && batchSizeBytes >= maxBatchSizeBytes)) {
                          c.output(KV.of(c.element().getKey(), currentBatch));
                          currentBatch = Lists.newArrayList();
                          batchSizeBytes = 0;
                        }
                      }
                      if (!currentBatch.isEmpty()) {
                        c.output(KV.of(c.element().getKey(), currentBatch));
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
    private final DataflowRunner runner;

    BatchGroupIntoBatchesWithShardedKeyOverrideFactory(DataflowRunner runner) {
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
          new BatchGroupIntoBatchesWithShardedKey<>(
              transform.getTransform().getBatchingParams(),
              runner,
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
   * Specialized implementation of {@link GroupIntoBatches.WithShardedKey} for bounded Dataflow
   * pipelines.
   */
  static class BatchGroupIntoBatchesWithShardedKey<K, V>
      extends PTransform<PCollection<KV<K, V>>, PCollection<KV<ShardedKey<K>, Iterable<V>>>> {

    private final BatchingParams<V> batchingParams;
    private final transient DataflowRunner runner;
    private final transient PCollection<KV<ShardedKey<K>, Iterable<V>>> originalOutput;

    private BatchGroupIntoBatchesWithShardedKey(
        BatchingParams<V> batchingParams,
        DataflowRunner runner,
        PCollection<KV<ShardedKey<K>, Iterable<V>>> originalOutput) {
      this.batchingParams = batchingParams;
      this.runner = runner;
      this.originalOutput = originalOutput;
    }

    @Override
    public PCollection<KV<ShardedKey<K>, Iterable<V>>> expand(PCollection<KV<K, V>> input) {
      return shardKeys(input)
          .apply(new BatchGroupIntoBatches<>(batchingParams, runner, originalOutput));
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
          new StreamingGroupIntoBatches<>(
              runner,
              transform.getTransform(),
              PTransformReplacements.getSingletonMainOutput(transform)));
    }

    @Override
    public Map<PCollection<?>, ReplacementOutput> mapOutputs(
        Map<TupleTag<?>, PCollection<?>> outputs, PCollection<KV<K, Iterable<V>>> newOutput) {
      return ReplacementOutputs.singleton(outputs, newOutput);
    }
  }

  /**
   * Specialized implementation of {@link GroupIntoBatches} for unbounded Dataflow pipelines. The
   * override does the same thing as the original transform but additionally records the output in
   * order to append required step properties during the graph translation.
   */
  static class StreamingGroupIntoBatches<K, V>
      extends PTransform<PCollection<KV<K, V>>, PCollection<KV<K, Iterable<V>>>> {

    private final transient DataflowRunner runner;
    private final GroupIntoBatches<K, V> originalTransform;
    private final transient PCollection<KV<K, Iterable<V>>> originalOutput;

    public StreamingGroupIntoBatches(
        DataflowRunner runner,
        GroupIntoBatches<K, V> original,
        PCollection<KV<K, Iterable<V>>> output) {
      this.runner = runner;
      this.originalTransform = original;
      this.originalOutput = output;
    }

    @Override
    public PCollection<KV<K, Iterable<V>>> expand(PCollection<KV<K, V>> input) {
      // Record the output PCollection of the original transform since the new output will be
      // replaced by the original one when the replacement transform is wired to other nodes in the
      // graph, although the old and the new outputs are effectively the same.
      runner.maybeRecordPCollectionPreservedKeys(originalOutput);
      return input.apply(originalTransform);
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
