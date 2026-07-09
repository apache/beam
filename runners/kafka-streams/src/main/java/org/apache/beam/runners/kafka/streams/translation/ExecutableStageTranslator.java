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

    // Fail fast on stage features that are not yet supported, so users get a clear message rather
    // than a silent miss further down the harness/topology path.
    if (stagePayload.getSideInputsCount() > 0) {
      throw new UnsupportedOperationException(
          "ExecutableStage "
              + transformId
              + " has side inputs; side inputs are not yet supported by the Kafka Streams runner.");
    }
    if (stagePayload.getUserStatesCount() > 0 || stagePayload.getTimersCount() > 0) {
      throw new UnsupportedOperationException(
          "ExecutableStage "
              + transformId
              + " uses user state or timers; stateful ParDo is not yet supported by the Kafka"
              + " Streams runner.");
    }
    if (transform.getOutputsMap().size() > 1) {
      // Multi-output stages (DoFns with side outputs, etc.) are a planned follow-up — they need
      // an output-tag dispatch in the processor + per-output PCollection routing. The current
      // rejection just fails loudly until that's wired in.
      throw new UnsupportedOperationException(
          "ExecutableStage "
              + transformId
              + " has "
              + transform.getOutputsMap().size()
              + " outputs; multi-output stages are not yet supported by the Kafka Streams runner.");
    }

    // The payload distinguishes the main input from side inputs, so reading it from the payload
    // is unambiguous even before we add side-input support.
    String inputPCollectionId = stagePayload.getInput();
    String parentProcessor = context.getProcessorNameForPCollection(inputPCollectionId);

    // A stage has at most one output (multi-output rejected above). Stamp its watermark with that
    // output's Flatten branch identity if it feeds one, else the default single source (0 of 1).
    KafkaStreamsTranslationContext.SourceStamp stamp =
        transform.getOutputsMap().isEmpty()
            ? context.getSourceStamp("")
            : context.getSourceStamp(Iterables.getOnlyElement(transform.getOutputsMap().values()));

    Topology topology = context.getTopology();
    topology.addProcessor(
        transformId,
        () ->
            new ExecutableStageProcessor(
                stagePayload,
                context.getJobInfo(),
                stamp.getSourcePartition(),
                stamp.getTotalPartitions()),
        parentProcessor);

    if (!transform.getOutputsMap().isEmpty()) {
      String outputPCollectionId = Iterables.getOnlyElement(transform.getOutputsMap().values());
      context.registerPCollectionProducer(outputPCollectionId, transformId);
    }
  }
}
