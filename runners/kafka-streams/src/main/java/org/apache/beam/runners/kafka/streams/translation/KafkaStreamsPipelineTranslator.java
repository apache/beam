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

import java.util.Map;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.runners.fnexecution.provisioning.JobInfo;
import org.apache.beam.runners.kafka.streams.KafkaStreamsPipelineOptions;
import org.apache.beam.sdk.util.construction.PTransformTranslation;
import org.apache.beam.sdk.util.construction.graph.ExecutableStage;
import org.apache.beam.sdk.util.construction.graph.GreedyPipelineFuser;
import org.apache.beam.sdk.util.construction.graph.PipelineNode;
import org.apache.beam.sdk.util.construction.graph.QueryablePipeline;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;

/**
 * Translates a portable Beam pipeline into a Kafka Streams {@link
 * org.apache.kafka.streams.Topology}.
 *
 * <p>Walks the pipeline in topological order via {@link QueryablePipeline} and dispatches each
 * transform to a {@link PTransformTranslator} keyed by URN. Transforms whose URN has no registered
 * translator fail fast with a clear {@link UnsupportedOperationException} so the failure points at
 * the exact transform that is not yet supported.
 */
public class KafkaStreamsPipelineTranslator {

  private final Map<String, PTransformTranslator> urnToTranslator;

  public KafkaStreamsPipelineTranslator() {
    this(
        ImmutableMap.<String, PTransformTranslator>builder()
            .put(PTransformTranslation.IMPULSE_TRANSFORM_URN, new ImpulseTranslator())
            .put(ExecutableStage.URN, new ExecutableStageTranslator())
            .build());
  }

  KafkaStreamsPipelineTranslator(Map<String, PTransformTranslator> urnToTranslator) {
    this.urnToTranslator = urnToTranslator;
  }

  public KafkaStreamsTranslationContext createTranslationContext(
      JobInfo jobInfo, KafkaStreamsPipelineOptions pipelineOptions) {
    return KafkaStreamsTranslationContext.create(jobInfo, pipelineOptions);
  }

  /**
   * Fuses the pipeline so that stateless user code is grouped into {@code ExecutableStage} nodes.
   *
   * <p>Runner-executed primitives that have their own translator (e.g. Impulse) are left intact;
   * everything else is fused. If the pipeline already contains {@code ExecutableStage} transforms
   * it is returned unchanged.
   */
  public RunnerApi.Pipeline prepareForTranslation(RunnerApi.Pipeline pipeline) {
    boolean alreadyFused =
        pipeline.getComponents().getTransformsMap().values().stream()
            .anyMatch(t -> ExecutableStage.URN.equals(t.getSpec().getUrn()));
    if (alreadyFused) {
      return pipeline;
    }
    return GreedyPipelineFuser.fuse(pipeline).toPipeline();
  }

  /**
   * Walks the pipeline in topological order and translates each transform whose URN is supported.
   * Throws {@link UnsupportedOperationException} on the first unsupported URN.
   */
  public void translate(KafkaStreamsTranslationContext context, RunnerApi.Pipeline pipeline) {
    QueryablePipeline queryable =
        QueryablePipeline.forTransforms(
            pipeline.getRootTransformIdsList(), pipeline.getComponents());
    for (PipelineNode.PTransformNode node : queryable.getTopologicallyOrderedTransforms()) {
      String urn = node.getTransform().getSpec().getUrn();
      PTransformTranslator translator = urnToTranslator.get(urn);
      if (translator == null) {
        throw new UnsupportedOperationException(
            "No translator registered for URN "
                + urn
                + " (transformId="
                + node.getId()
                + ", jobId="
                + context.getJobInfo().jobId()
                + ")");
      }
      translator.translate(node.getId(), pipeline, context);
    }
  }
}
