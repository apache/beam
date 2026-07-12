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
package org.apache.beam.runners.kafka.streams.translation;

import java.util.ArrayList;
import java.util.List;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Iterables;
import org.apache.kafka.streams.Topology;

/**
 * Translates Beam's {@code Flatten} primitive ({@code beam:transform:flatten:v1}): the union of N
 * input PCollections into one output PCollection.
 *
 * <p>Wires a single {@link FlattenProcessor} node to the producer of every input PCollection (Kafka
 * Streams lets a processor have many parents), so the parents' data streams merge into it, and
 * registers it as the producer of the flattened output so downstream translators wire to it. The
 * processor forwards data through and owns its output watermark via a {@link WatermarkManager},
 * exactly like GroupByKey — see {@link FlattenProcessor}.
 *
 * <p>The producer id the {@link WatermarkManager} keys each branch on is stamped upstream, by the
 * branch's producing transform, because Kafka Streams does not tell a processor which parent
 * forwarded a record. Ids are assigned once per Flatten-input PCollection (see {@link
 * KafkaStreamsPipelineTranslator}), so an input shared by two Flattens reports one id and each
 * Flatten still waits only for its own branches.
 */
class FlattenTranslator implements PTransformTranslator {

  @Override
  public void translate(
      String transformId, RunnerApi.Pipeline pipeline, KafkaStreamsTranslationContext context) {
    RunnerApi.PTransform transform = pipeline.getComponents().getTransformsOrThrow(transformId);
    // Flatten produces exactly one output PCollection, fed by all of its input PCollections.
    String outputPCollectionId = Iterables.getOnlyElement(transform.getOutputsMap().values());

    List<String> parentProcessors = new ArrayList<>();
    for (String inputPCollectionId : transform.getInputsMap().values()) {
      parentProcessors.add(context.getProcessorNameForPCollection(inputPCollectionId));
    }

    // The Flatten holds its watermark until all of its input branches report. Inputs are distinct
    // (a self-flatten is rejected during producer-id assignment), so the input count is the number
    // of producer ids to wait for.
    int inputCount = transform.getInputsMap().size();
    Topology topology = context.getTopology();
    topology.addProcessor(
        transformId,
        () -> new FlattenProcessor(inputCount),
        parentProcessors.toArray(new String[0]));

    context.registerPCollectionProducer(outputPCollectionId, transformId);
  }
}
