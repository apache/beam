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
import org.apache.beam.sdk.util.SerializableUtils;
import org.apache.beam.sdk.util.construction.NativeTransforms;
import org.apache.beam.sdk.util.construction.PTransformTranslation;
import org.apache.beam.sdk.util.construction.graph.ExecutableStage;
import org.apache.beam.sdk.util.construction.graph.GreedyPipelineFuser;
import org.apache.beam.sdk.util.construction.graph.PipelineNode;
import org.apache.beam.sdk.util.construction.graph.QueryablePipeline;
import org.apache.beam.sdk.util.construction.graph.TrivialNativeTransformExpander;
import org.apache.beam.vendor.grpc.v1p69p0.com.google.protobuf.ByteString;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableSet;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Sets;

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
            .put(PTransformTranslation.READ_TRANSFORM_URN, new ReadTranslator())
            .put(PTransformTranslation.REDISTRIBUTE_ARBITRARILY_URN, new RedistributeTranslator())
            .put(PTransformTranslation.FLATTEN_TRANSFORM_URN, new FlattenTranslator())
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
    // Flatten is deliberately not exposed: the fuser has its own handling for Flatten (unzipping
    // it into producer stages and re-introducing a runner Flatten via the OutputDeduplicator), and
    // declaring it native makes TrivialNativeTransformExpander interact badly with multi-branch
    // flattens whose inputs come from composite expansions (their producing stages get dropped
    // from the fused pipeline, failing "consumed but never produced" validation). Mirrors the
    // Flink runner, which likewise keeps Read out of its known URNs for expander reasons.
    return Sets.difference(
        urnToTranslator.keySet(), ImmutableSet.of(PTransformTranslation.FLATTEN_TRANSFORM_URN));
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
    RunnerApi.Pipeline withoutEmptyFlattens = replaceEmptyFlattensWithEmptyReads(pipeline);
    RunnerApi.Pipeline trimmed =
        TrivialNativeTransformExpander.forKnownUrns(withoutEmptyFlattens, knownUrns());
    return GreedyPipelineFuser.fuse(trimmed).toPipeline();
  }

  /**
   * Rewrites every {@code Flatten} of zero PCollections into a primitive Read of an {@link
   * EmptyBoundedSource}. A zero-input Flatten produces an empty PCollection, but it is also a root
   * of the pipeline graph, and {@link GreedyPipelineFuser} only accepts Impulse or Read roots. The
   * Read of an empty source has exactly the right semantics — no elements, then the terminal
   * watermark — and reuses the existing {@link ReadTranslator} path.
   */
  private static RunnerApi.Pipeline replaceEmptyFlattensWithEmptyReads(
      RunnerApi.Pipeline pipeline) {
    RunnerApi.Pipeline.Builder pipelineBuilder = null;
    for (Map.Entry<String, RunnerApi.PTransform> entry :
        pipeline.getComponents().getTransformsMap().entrySet()) {
      RunnerApi.PTransform transform = entry.getValue();
      if (!PTransformTranslation.FLATTEN_TRANSFORM_URN.equals(transform.getSpec().getUrn())
          || !transform.getInputsMap().isEmpty()) {
        continue;
      }
      if (pipelineBuilder == null) {
        pipelineBuilder = pipeline.toBuilder();
      }
      RunnerApi.ReadPayload emptyReadPayload =
          RunnerApi.ReadPayload.newBuilder()
              .setIsBounded(RunnerApi.IsBounded.Enum.BOUNDED)
              .setSource(
                  RunnerApi.FunctionSpec.newBuilder()
                      .setUrn(JAVA_SERIALIZED_BOUNDED_SOURCE_URN)
                      .setPayload(
                          ByteString.copyFrom(
                              SerializableUtils.serializeToByteArray(new EmptyBoundedSource()))))
              .build();
      pipelineBuilder
          .getComponentsBuilder()
          .putTransforms(
              entry.getKey(),
              transform
                  .toBuilder()
                  .setSpec(
                      RunnerApi.FunctionSpec.newBuilder()
                          .setUrn(PTransformTranslation.READ_TRANSFORM_URN)
                          .setPayload(emptyReadPayload.toByteString()))
                  .build());
    }
    return pipelineBuilder == null ? pipeline : pipelineBuilder.build();
  }

  /** The URN {@code ReadTranslation} uses for a Java-serialized {@code BoundedSource} payload. */
  private static final String JAVA_SERIALIZED_BOUNDED_SOURCE_URN = "beam:java:boundedsource:v1";

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
