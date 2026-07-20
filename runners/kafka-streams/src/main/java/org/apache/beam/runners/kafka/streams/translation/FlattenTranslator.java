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
import java.util.HashSet;
import java.util.List;
import java.util.Set;
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
 * processor forwards data through and owns its output watermark via a {@link WatermarkAggregator},
 * which is handed the producers of the input PCollections — the upstream transform ids whose
 * watermark reports the Flatten must hear from. Producers stamp their own transform id on the
 * reports they emit, without regard to who consumes them, so an input shared with another Flatten
 * needs no special handling.
 *
 * <p>A user-written self-flatten never reaches this translator: the fuser folds the Flatten into
 * the consuming SDK-harness stage, which performs the duplication itself. The duplicate-input check
 * below is defensive — if a runner-executed Flatten ever did receive the same PCollection twice,
 * Kafka Streams could not wire the same parent to a child twice and the duplicate copy would be
 * silently dropped, so failing fast is safer.
 */
class FlattenTranslator implements PTransformTranslator {

  @Override
  public void translate(
      String transformId, RunnerApi.Pipeline pipeline, KafkaStreamsTranslationContext context) {
    RunnerApi.PTransform transform = pipeline.getComponents().getTransformsOrThrow(transformId);
    // Flatten produces exactly one output PCollection, fed by all of its input PCollections.
    String outputPCollectionId = Iterables.getOnlyElement(transform.getOutputsMap().values());

    Topology topology = context.getTopology();
    Set<String> seenInputs = new HashSet<>();
    List<String> parentProcessors = new ArrayList<>();
    Set<String> upstreamTransformIds = new HashSet<>();
    for (String inputPCollectionId : transform.getInputsMap().values()) {
      if (!seenInputs.add(inputPCollectionId)) {
        throw new UnsupportedOperationException(
            "Flatten "
                + transform.getUniqueName()
                + " has PCollection "
                + inputPCollectionId
                + " as an input more than once; a self-flatten is not yet supported by the Kafka"
                + " Streams runner.");
      }
      String parentProcessor = context.getProcessorNameForPCollection(inputPCollectionId);
      parentProcessors.add(parentProcessor);
      upstreamTransformIds.add(parentProcessor);
    }

    topology.addProcessor(
        transformId,
        () -> new FlattenProcessor(transformId, upstreamTransformIds),
        parentProcessors.toArray(new String[0]));

    context.registerPCollectionProducer(outputPCollectionId, transformId);
  }
}
