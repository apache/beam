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
 * <p>The {@code (i of N)} branch identity the {@link WatermarkManager} needs is stamped upstream,
 * by the parent transform that produces each branch, because Kafka Streams does not tell a
 * processor which parent forwarded a record.
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

    Topology topology = context.getTopology();
    topology.addProcessor(
        transformId, FlattenProcessor::new, parentProcessors.toArray(new String[0]));

    context.registerPCollectionProducer(outputPCollectionId, transformId);
  }
}
