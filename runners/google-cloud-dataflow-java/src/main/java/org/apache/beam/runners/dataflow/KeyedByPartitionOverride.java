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

import java.util.Map;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.io.kafka.KafkaRecord;
import org.apache.beam.sdk.runners.AppliedPTransform;
import org.apache.beam.sdk.runners.PTransformOverrideFactory;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.util.construction.PTransformReplacements;
import org.apache.beam.sdk.util.construction.ReplacementOutputs;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TupleTag;

@SuppressWarnings({
  "rawtypes" // TODO(https://github.com/apache/beam/issues/20447)
})
public class KeyedByPartitionOverride {

  static class StreamingKeyedByPartitionOverrideFactory<K, V>
      implements PTransformOverrideFactory<
          PCollection<KafkaRecord<K, V>>,
          PCollection<KV<Integer, V>>,
          KafkaIO.Read.KeyedByPartition<K, V>> {

    private final DataflowRunner runner;

    StreamingKeyedByPartitionOverrideFactory(DataflowRunner runner) {
      this.runner = runner;
    }

    @Override
    public PTransformReplacement<PCollection<KafkaRecord<K, V>>, PCollection<KV<Integer, V>>>
        getReplacementTransform(
            AppliedPTransform<
                    PCollection<KafkaRecord<K, V>>,
                    PCollection<KV<Integer, V>>,
                    KafkaIO.Read.KeyedByPartition<K, V>>
                transform) {
      return PTransformReplacement.of(
          PTransformReplacements.getSingletonMainInput(transform),
          new StreamingKeyedByPartition<>(
              runner,
              transform.getTransform(),
              PTransformReplacements.getSingletonMainOutput(transform)));
    }

    @Override
    public Map<PCollection<?>, ReplacementOutput> mapOutputs(
        Map<TupleTag<?>, PCollection<?>> outputs, PCollection<KV<Integer, V>> newOutput) {
      return ReplacementOutputs.singleton(outputs, newOutput);
    }
  }

  static class StreamingKeyedByPartition<K, V>
      extends PTransform<PCollection<KafkaRecord<K, V>>, PCollection<KV<Integer, V>>> {

    private final transient DataflowRunner runner;
    private final KafkaIO.Read.KeyedByPartition<K, V> originalTransform;
    private final transient PCollection<KV<Integer, V>> originalOutput;

    public StreamingKeyedByPartition(
        DataflowRunner runner,
        KafkaIO.Read.KeyedByPartition<K, V> original,
        PCollection<KV<Integer, V>> output) {
      this.runner = runner;
      this.originalTransform = original;
      this.originalOutput = output;
    }

    @Override
    public PCollection<KV<Integer, V>> expand(PCollection<KafkaRecord<K, V>> input) {
      // Record the output PCollection of the original transform since the new output will be
      // replaced by the original one when the replacement transform is wired to other nodes in the
      // graph, although the old and the new outputs are effectively the same.
      runner.maybeRecordPCollectionPreservedKeys(originalOutput);
      System.out.println("StreamingKeyedByPartition override");
      return input.apply(originalTransform);
    }
  }
}
