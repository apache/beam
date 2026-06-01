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

import java.io.IOException;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Iterables;
import org.apache.kafka.streams.Topology;

/**
 * Translates the {@code beam:runner:executable_stage:v1} URN.
 *
 * <p>Adds an {@link ExecutableStageProcessor} node to the topology, wired to the processor that
 * produces the stage's input PCollection (resolved through {@link
 * KafkaStreamsTranslationContext#getProcessorNameForPCollection}). The processor runs the fused
 * user code in the SDK harness; its single output PCollection is registered so downstream
 * translators can attach to this node.
 *
 * <p>Multi-output stages (additional outputs / side inputs / state / timers) are out of scope for
 * this first version and are rejected so the limitation fails fast rather than silently dropping
 * outputs.
 */
class ExecutableStageTranslator implements PTransformTranslator {

  @Override
  public void translate(
      String transformId, RunnerApi.Pipeline pipeline, KafkaStreamsTranslationContext context) {
    RunnerApi.PTransform transform = pipeline.getComponents().getTransformsOrThrow(transformId);

    RunnerApi.ExecutableStagePayload stagePayload;
    try {
      stagePayload = RunnerApi.ExecutableStagePayload.parseFrom(transform.getSpec().getPayload());
    } catch (IOException e) {
      throw new IllegalArgumentException(
          "Failed to parse ExecutableStagePayload for transform " + transformId, e);
    }

    String inputPCollectionId = Iterables.getOnlyElement(transform.getInputsMap().values());
    String parentProcessor = context.getProcessorNameForPCollection(inputPCollectionId);

    if (transform.getOutputsMap().size() > 1) {
      throw new UnsupportedOperationException(
          "ExecutableStage "
              + transformId
              + " has "
              + transform.getOutputsMap().size()
              + " outputs; multi-output stages are not yet supported by the Kafka Streams runner.");
    }

    Topology topology = context.getTopology();
    topology.addProcessor(
        transformId,
        () -> new ExecutableStageProcessor(stagePayload, context.getJobInfo()),
        parentProcessor);

    if (!transform.getOutputsMap().isEmpty()) {
      String outputPCollectionId = Iterables.getOnlyElement(transform.getOutputsMap().values());
      context.registerPCollectionProducer(outputPCollectionId, transformId);
    }
  }
}
