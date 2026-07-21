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
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableSet;
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
    // The payload distinguishes the main input from side inputs, so reading it from the payload
    // is unambiguous even before we add side-input support.
    String inputPCollectionId = stagePayload.getInput();
    String parentProcessor = context.getProcessorNameForPCollection(inputPCollectionId);

    // A multi-output stage (a DoFn with side outputs, or a Read whose SDF wrapper produces several
    // outputs) needs each output routed to the right downstream. Since downstream transforms are
    // wired to a producer node by PCollection id, and that node must exist when the stage is
    // translated, give each output its own relay node (StageOutputProcessor) and route to it by
    // name. A single-output stage needs none of this — it forwards to its one downstream directly
    // and registers itself as that output's producer. Outputs are sorted so the routing is
    // deterministic across topology builds.
    List<String> outputPCollectionIds = new ArrayList<>(transform.getOutputsMap().values());
    Collections.sort(outputPCollectionIds);
    boolean multiOutput = outputPCollectionIds.size() > 1;
    Map<String, String> outputChildByPCollectionId = new LinkedHashMap<>();
    if (multiOutput) {
      for (int i = 0; i < outputPCollectionIds.size(); i++) {
        outputChildByPCollectionId.put(outputPCollectionIds.get(i), transformId + "-output-" + i);
      }
    }

    Topology topology = context.getTopology();
    // The stage stamps its own transform id on the watermarks it emits, and aggregates its input
    // watermark from the reports of its single upstream transform (the producer of its input
    // PCollection, whose node name is the upstream transform id). Harness-reported metrics land in
    // this stage's container of the job's metrics step map.
    topology.addProcessor(
        transformId,
        () ->
            new ExecutableStageProcessor(
                stagePayload,
                context.getJobInfo(),
                transformId,
                ImmutableSet.of(parentProcessor),
                context.getMetricsContainerStepMap().getContainer(transformId),
                outputChildByPCollectionId),
        parentProcessor);

    if (multiOutput) {
      // One relay per output; downstream transforms wire to the relay, which re-stamps the stage's
      // watermark with the relay's own id so their watermark aggregation stays consistent.
      outputChildByPCollectionId.forEach(
          (outputPCollectionId, relayName) -> {
            topology.addProcessor(
                relayName, () -> new StageOutputProcessor(relayName), transformId);
            context.registerPCollectionProducer(outputPCollectionId, relayName);
          });
    } else if (!outputPCollectionIds.isEmpty()) {
      context.registerPCollectionProducer(outputPCollectionIds.get(0), transformId);
    }
  }
}
