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

import org.apache.beam.runners.core.construction.PTransformReplacements;
import org.apache.beam.runners.core.construction.SingleInputOutputOverrideFactory;
import org.apache.beam.sdk.runners.AppliedPTransform;
import org.apache.beam.sdk.runners.PTransformOverrideFactory;
import org.apache.beam.sdk.transforms.AutoshardedKeyReshuffle;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Reshuffle;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.ReshuffleTrigger;
import org.apache.beam.sdk.transforms.windowing.TimestampCombiner;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.util.IdentityWindowFn;
import org.apache.beam.sdk.util.ShardedKey;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.joda.time.Duration;
import java.util.Map;
import org.apache.beam.sdk.values.TupleTag;

/**
 * A {@link PTransformOverrideFactory} that overrides {@link Reshuffle} with a version that does not
 * reify timestamps. Dataflow has special handling of the {@link ReshuffleTrigger} which never
 * buffers elements and outputs elements with their original timestamp.
 */
public class AutoshardedKeyReshuffleOverride{


    static class StreamingAutoshardedKeyReshuffleOverrideFactory<K, V>
    implements PTransformOverrideFactory<
        PCollection<KV<K, V>>, PCollection<KV<ShardedKey<K>, V>>, AutoshardedKeyReshuffle<K, V>.ViaRandomKey<K, V>> {
  private final DataflowRunner runner;

  StreamingAutoshardedKeyReshuffleOverrideFactory(DataflowRunner runner) {
    this.runner = runner;
  }

  @Override
  public PTransformReplacement<PCollection<KV<K, V>>, PCollection<KV<ShardedKey<K>, V>>>
      getReplacementTransform(
          AppliedPTransform<
          PCollection<KV<K, V>>, PCollection<KV<ShardedKey<K>, V>>, AutoshardedKeyReshuffle<K, V>.ViaRandomKey<K, V>>
              transform) {
    return PTransformReplacement.of(
        PTransformReplacements.getSingletonMainInput(transform),
        new StreamingAutoshardedKeyReshuffleOverride<>(
            runner,
            transform.getTransform(),
            PTransformReplacements.getSingletonMainOutput(transform)));
  }

  @Override
  public Map<PCollection<?>, ReplacementOutput> mapOutputs(
      Map<TupleTag<?>, PCollection<?>> outputs, PCollection<KV<ShardedKey<K>, V>> newOutput) {
    return ReplacementOutputs.singleton(outputs, newOutput);
  }

    /**
   * Specialized implementation of {@link GroupIntoBatches.WithShardedKey} for unbounded Dataflow
   * pipelines. The override does the same thing as the original transform but additionally records
   * the output in order to append required step properties during the graph translation.
   */
  static class StreamingAutoshardedKeyReshuffleOverride<K, V>
      extends PTransform<PCollection<KV<K, V>>, PCollection<KV<ShardedKey<K>, V>>> {

    private final transient DataflowRunner runner;
    private final AutoshardedKeyReshuffle<K, V>.ViaRandomKey originalTransform;
    private final transient PCollection<KV<ShardedKey<K>, V>> originalOutput;

    public StreamingAutoshardedKeyReshuffleOverride(
        DataflowRunner runner,
        AutoshardedKeyReshuffle<K, V>.ViaRandomKey original,
        PCollection<KV<ShardedKey<K>, V>> output) {
      this.runner = runner;
      this.originalTransform = original;
      this.originalOutput = output;
    }

    @Override
    public PCollection<KV<ShardedKey<K>, V>> expand(PCollection<KV<K, V>> input) {
      // Record the output PCollection of the original transform since the new output will be
      // replaced by the original one when the replacement transform is wired to other nodes in the
      // graph, although the old and the new outputs are effectively the same.
      runner.maybeRecordPCollectionWithAutoSharding(originalOutput);
      return input.apply(originalTransform);
    }
  }
}
}

 
