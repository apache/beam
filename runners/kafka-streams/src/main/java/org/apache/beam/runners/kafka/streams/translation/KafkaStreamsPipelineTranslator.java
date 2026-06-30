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

import com.google.auto.service.AutoService;
import java.util.Map;
import java.util.Set;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.runners.fnexecution.provisioning.JobInfo;
import org.apache.beam.runners.kafka.streams.KafkaStreamsPipelineOptions;
import org.apache.beam.sdk.util.construction.NativeTransforms;
import org.apache.beam.sdk.util.construction.PTransformTranslation;
import org.apache.beam.sdk.util.construction.graph.ExecutableStage;
import org.apache.beam.sdk.util.construction.graph.GreedyPipelineFuser;
import org.apache.beam.sdk.util.construction.graph.PipelineNode;
import org.apache.beam.sdk.util.construction.graph.QueryablePipeline;
import org.apache.beam.sdk.util.construction.graph.TrivialNativeTransformExpander;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableSet;

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
            .put(PTransformTranslation.REDISTRIBUTE_ARBITRARILY_URN, new RedistributeTranslator())
            .put(PTransformTranslation.GROUP_BY_KEY_TRANSFORM_URN, new GroupByKeyTranslator())
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
   * Returns the set of URNs this translator handles natively. {@link
   * TrivialNativeTransformExpander} uses this set to strip the sub-transforms of runner-native
   * composites (e.g. {@code Redistribute.arbitrarily}) before fusion, so they survive into
   * translation as leaves instead of being expanded into primitives the runner does not implement
   * yet (e.g. GroupByKey).
   */
  public Set<String> knownUrns() {
    return urnToTranslator.keySet();
  }

  /**
   * Prepares the pipeline for translation:
   *
   * <ol>
   *   <li>Trim sub-transforms of runner-native composites listed in {@link #knownUrns()} so the
   *       fuser leaves them as primitives.
   *   <li>Fuse remaining stateless user code into {@code ExecutableStage} nodes via {@link
   *       GreedyPipelineFuser}.
   * </ol>
   *
   * <p>If the pipeline already contains {@code ExecutableStage} transforms it is returned
   * unchanged.
   */
  public RunnerApi.Pipeline prepareForTranslation(RunnerApi.Pipeline pipeline) {
    boolean alreadyFused =
        pipeline.getComponents().getTransformsMap().values().stream()
            .anyMatch(t -> ExecutableStage.URN.equals(t.getSpec().getUrn()));
    if (alreadyFused) {
      return pipeline;
    }
    RunnerApi.Pipeline trimmed = TrivialNativeTransformExpander.forKnownUrns(pipeline, knownUrns());
    return GreedyPipelineFuser.fuse(trimmed).toPipeline();
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

  /**
   * Tells the SDK that URNs handled directly by the Kafka Streams runner should be treated as
   * primitives by {@link QueryablePipeline}. Mirrors Flink's {@code IsFlinkNativeTransform}
   * pattern. Without this, {@link TrivialNativeTransformExpander} strips a composite's
   * sub-transforms but {@link QueryablePipeline} still does not recognise the composite itself as a
   * producer of its outputs, and pipeline validation fails with "consumed but never produced".
   */
  @AutoService(NativeTransforms.IsNativeTransform.class)
  public static class IsKafkaStreamsNativeTransform implements NativeTransforms.IsNativeTransform {
    private static final Set<String> URNS =
        ImmutableSet.of(PTransformTranslation.REDISTRIBUTE_ARBITRARILY_URN);

    @Override
    public boolean test(RunnerApi.PTransform pTransform) {
      String urn = PTransformTranslation.urnForTransformOrNull(pTransform);
      return urn != null && URNS.contains(urn);
    }
  }
}
