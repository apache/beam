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
package org.apache.beam.sdk.util.construction.graph;

import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkArgument;

import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import org.apache.beam.model.pipeline.v1.RunnerApi.Environment;
import org.apache.beam.model.pipeline.v1.RunnerApi.PCollection;
import org.apache.beam.model.pipeline.v1.RunnerApi.PTransform;
import org.apache.beam.model.pipeline.v1.RunnerApi.ParDoPayload;
import org.apache.beam.model.pipeline.v1.RunnerApi.Pipeline;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.util.construction.PTransformTranslation;
import org.apache.beam.vendor.grpc.v1p60p1.com.google.protobuf.InvalidProtocolBufferException;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A Fuser that constructs a fused pipeline by fusing as many PCollections into a stage as possible.
 */
@SuppressWarnings({
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
class GreedyPCollectionFusers {
  private static final Logger LOG = LoggerFactory.getLogger(GreedyPCollectionFusers.class);

  private static final Map<String, FusibilityChecker> URN_FUSIBILITY_CHECKERS =
      ImmutableMap.<String, FusibilityChecker>builder()
          .put(PTransformTranslation.PAR_DO_TRANSFORM_URN, GreedyPCollectionFusers::canFuseParDo)
          .put(
              PTransformTranslation.SPLITTABLE_PAIR_WITH_RESTRICTION_URN,
              GreedyPCollectionFusers::canFuseParDo)
          .put(
              PTransformTranslation.SPLITTABLE_PROCESS_KEYED_URN,
              GreedyPCollectionFusers::cannotFuse)
          .put(
              PTransformTranslation.SPLITTABLE_PROCESS_ELEMENTS_URN,
              GreedyPCollectionFusers::cannotFuse)
          .put(
              PTransformTranslation.SPLITTABLE_SPLIT_AND_SIZE_RESTRICTIONS_URN,
              GreedyPCollectionFusers::canFuseParDo)
          .put(
              PTransformTranslation.SPLITTABLE_PROCESS_SIZED_ELEMENTS_AND_RESTRICTIONS_URN,
              GreedyPCollectionFusers::cannotFuse)
          .put(
              PTransformTranslation.COMBINE_PER_KEY_PRECOMBINE_TRANSFORM_URN,
              GreedyPCollectionFusers::canFuseCompatibleEnvironment)
          .put(
              PTransformTranslation.COMBINE_PER_KEY_MERGE_ACCUMULATORS_TRANSFORM_URN,
              GreedyPCollectionFusers::canFuseCompatibleEnvironment)
          .put(
              PTransformTranslation.COMBINE_PER_KEY_EXTRACT_OUTPUTS_TRANSFORM_URN,
              GreedyPCollectionFusers::canFuseCompatibleEnvironment)
          .put(
              PTransformTranslation.ASSIGN_WINDOWS_TRANSFORM_URN,
              GreedyPCollectionFusers::canFuseCompatibleEnvironment)
          .put(PTransformTranslation.FLATTEN_TRANSFORM_URN, GreedyPCollectionFusers::canAlwaysFuse)
          .put(
              // GroupByKeys are runner-implemented only. PCollections consumed by a GroupByKey must
              // be materialized
              PTransformTranslation.GROUP_BY_KEY_TRANSFORM_URN, GreedyPCollectionFusers::cannotFuse)
          .put(PTransformTranslation.CREATE_VIEW_TRANSFORM_URN, GreedyPCollectionFusers::cannotFuse)
          .build();
  private static final FusibilityChecker DEFAULT_FUSIBILITY_CHECKER =
      GreedyPCollectionFusers::unknownTransformFusion;

  // TODO: Migrate
  private static final Map<String, CompatibilityChecker> URN_COMPATIBILITY_CHECKERS =
      ImmutableMap.<String, CompatibilityChecker>builder()
          .put(
              PTransformTranslation.PAR_DO_TRANSFORM_URN,
              GreedyPCollectionFusers::parDoCompatibility)
          .put(
              PTransformTranslation.SPLITTABLE_PAIR_WITH_RESTRICTION_URN,
              GreedyPCollectionFusers::parDoCompatibility)
          .put(
              PTransformTranslation.SPLITTABLE_SPLIT_AND_SIZE_RESTRICTIONS_URN,
              GreedyPCollectionFusers::parDoCompatibility)
          .put(
              PTransformTranslation.SPLITTABLE_PROCESS_SIZED_ELEMENTS_AND_RESTRICTIONS_URN,
              GreedyPCollectionFusers::parDoCompatibility)
          .put(
              PTransformTranslation.COMBINE_PER_KEY_PRECOMBINE_TRANSFORM_URN,
              GreedyPCollectionFusers::compatibleEnvironments)
          .put(
              PTransformTranslation.COMBINE_PER_KEY_MERGE_ACCUMULATORS_TRANSFORM_URN,
              GreedyPCollectionFusers::compatibleEnvironments)
          .put(
              PTransformTranslation.COMBINE_PER_KEY_EXTRACT_OUTPUTS_TRANSFORM_URN,
              GreedyPCollectionFusers::compatibleEnvironments)
          .put(
              PTransformTranslation.ASSIGN_WINDOWS_TRANSFORM_URN,
              GreedyPCollectionFusers::compatibleEnvironments)
          .put(
              PTransformTranslation.FLATTEN_TRANSFORM_URN, GreedyPCollectionFusers::noCompatibility)
          .put(
              PTransformTranslation.GROUP_BY_KEY_TRANSFORM_URN,
              GreedyPCollectionFusers::noCompatibility)
          .put(
              PTransformTranslation.CREATE_VIEW_TRANSFORM_URN,
              GreedyPCollectionFusers::noCompatibility)
          .build();
  private static final CompatibilityChecker DEFAULT_COMPATIBILITY_CHECKER =
      GreedyPCollectionFusers::unknownTransformCompatibility;

  /** Returns true if the PTransform node for the given input PCollection can be fused across. */
  public static boolean canFuse(
      PipelineNode.PTransformNode transformNode,
      Environment environment,
      PipelineNode.PCollectionNode candidate,
      Collection<PipelineNode.PCollectionNode> stagePCollections,
      QueryablePipeline pipeline) {
    return URN_FUSIBILITY_CHECKERS
        .getOrDefault(transformNode.getTransform().getSpec().getUrn(), DEFAULT_FUSIBILITY_CHECKER)
        .canFuse(transformNode, environment, candidate, stagePCollections, pipeline);
  }

  /**
   * Returns true if the two PTransforms are compatible such that they can be executed in the same
   * environment.
   */
  public static boolean isCompatible(
      PipelineNode.PTransformNode left,
      PipelineNode.PTransformNode right,
      QueryablePipeline pipeline) {
    CompatibilityChecker leftChecker =
        URN_COMPATIBILITY_CHECKERS.getOrDefault(
            left.getTransform().getSpec().getUrn(), DEFAULT_COMPATIBILITY_CHECKER);
    CompatibilityChecker rightChecker =
        URN_COMPATIBILITY_CHECKERS.getOrDefault(
            right.getTransform().getSpec().getUrn(), DEFAULT_COMPATIBILITY_CHECKER);
    // The nodes are mutually compatible
    return leftChecker.isCompatible(left, right, pipeline)
        && rightChecker.isCompatible(right, left, pipeline);
  }

  // For the following methods, these should be called if the ptransform is consumed by a
  // PCollection output by the ExecutableStage, to determine if it can be fused into that
  // Subgraph

  private interface FusibilityChecker {
    /**
     * Determine if a {@link PipelineNode.PTransformNode} can be fused into an existing {@link
     * ExecutableStage}.
     */
    boolean canFuse(
        PipelineNode.PTransformNode transformNode,
        Environment environment,
        @SuppressWarnings("unused") PipelineNode.PCollectionNode candidate,
        Collection<PipelineNode.PCollectionNode> stagePCollections,
        QueryablePipeline pipeline);
  }

  private interface CompatibilityChecker {
    /**
     * Determine if two {@link PipelineNode.PTransformNode PTransforms} can be fused into a new
     * stage. This determines sibling fusion for new {@link ExecutableStage stages}.
     */
    boolean isCompatible(
        PipelineNode.PTransformNode newNode,
        PipelineNode.PTransformNode otherNode,
        QueryablePipeline pipeline);
  }

  /**
   * A ParDo can be fused into a stage if it executes in the same Environment as that stage, and no
   * transform that are upstream of any of its side input are present in that stage.
   *
   * <p>A ParDo that consumes a side input cannot process an element until all of the side inputs
   * contain data for the side input window that contains the element.
   */
  private static boolean canFuseParDo(
      PipelineNode.PTransformNode parDo,
      Environment environment,
      PipelineNode.PCollectionNode candidate,
      Collection<PipelineNode.PCollectionNode> stagePCollections,
      QueryablePipeline pipeline) {
    Optional<Environment> env = pipeline.getEnvironment(parDo);
    checkArgument(
        env.isPresent(),
        "A %s must have an %s associated with it",
        ParDoPayload.class.getSimpleName(),
        Environment.class.getSimpleName());
    if (!env.get().equals(environment)) {
      // The PCollection's producer and this ParDo execute in different environments, so fusion
      // is never possible.
      return false;
    }
    try {
      ParDoPayload payload = ParDoPayload.parseFrom(parDo.getTransform().getSpec().getPayload());
      if (Maps.filterKeys(
              parDo.getTransform().getInputsMap(),
              s -> payload.getTimerFamilySpecsMap().containsKey(s))
          .values()
          .contains(candidate.getId())) {
        // Allow fusion across timer PCollections because they are a self loop.
        return true;
      } else if (payload.getStateSpecsCount() > 0 || payload.getTimerFamilySpecsCount() > 0) {
        // Inputs to a ParDo that uses State or Timers must be key-partitioned, and elements for
        // a key must execute serially. To avoid checking if the rest of the stage is
        // key-partitioned and preserves keys, these ParDos do not fuse into an existing stage.
        return false;
      } else if (!pipeline.getSideInputs(parDo).isEmpty()) {
        // At execution time, a Runner is required to only provide inputs to a PTransform that, at
        // the time the PTransform processes them, the associated window is ready in all side inputs
        // that the PTransform consumes. For an arbitrary stage, it is significantly complex for the
        // runner to determine this for each input. As a result, we break fusion to simplify this
        // inspection. In general, a ParDo which consumes side inputs cannot be fused into an
        // executable stage alongside any transforms which are upstream of any of its side inputs.
        return false;
      }
    } catch (InvalidProtocolBufferException e) {
      throw new IllegalArgumentException(e);
    }
    return true;
  }

  private static boolean parDoCompatibility(
      PipelineNode.PTransformNode parDo,
      PipelineNode.PTransformNode other,
      QueryablePipeline pipeline) {
    // Implicitly true if we are attempting to fuse against oneself. This case comes up for
    // PCollections representing timers since they create a self-loop in the graph.
    return parDo.equals(other)
        // This is a convenience rather than a strict requirement. In general, a ParDo that consumes
        // side inputs can be fused with other transforms in the same environment which are not
        // upstream of any of the side inputs.
        || (pipeline.getSideInputs(parDo).isEmpty()
            // We purposefully break fusion here to provide runners the opportunity to insert a
            // grouping operation to simplify implementing support for ParDo's that contain user
            // state.
            // We would not need to do this if we had the ability to mark upstream transforms as
            // key preserving or if runners could execute ParDos containing user state in a
            // distributed
            // fashion for a single key.
            && pipeline.getUserStates(parDo).isEmpty()
            // We purposefully break fusion here to provide runners the opportunity to insert a
            // grouping operation to simplify implementing support for ParDo's that contain timers.
            // We would not need to do this if we had the ability to mark upstream transforms as
            // key preserving or if runners could execute ParDos containing timers in a distributed
            // fashion for a single key.
            && pipeline.getTimers(parDo).isEmpty()
            && compatibleEnvironments(parDo, other, pipeline));
  }

  /**
   * A WindowInto can be fused into a stage if it executes in the same Environment as that stage.
   */
  private static boolean canFuseCompatibleEnvironment(
      PipelineNode.PTransformNode operation,
      Environment environment,
      @SuppressWarnings("unused") PipelineNode.PCollectionNode candidate,
      @SuppressWarnings("unused") Collection<PipelineNode.PCollectionNode> stagePCollections,
      QueryablePipeline pipeline) {
    // WindowInto transforms may not have an environment
    Optional<Environment> operationEnvironment = pipeline.getEnvironment(operation);
    return environment.equals(operationEnvironment.orElse(null));
  }

  private static boolean compatibleEnvironments(
      PipelineNode.PTransformNode left,
      PipelineNode.PTransformNode right,
      QueryablePipeline pipeline) {
    return pipeline.getEnvironment(left).equals(pipeline.getEnvironment(right));
  }

  /**
   * Flatten can be fused into any stage.
   *
   * <p>If the assumption that for each {@link PCollection}, an element is produced in that {@link
   * PCollection} via a single path through the {@link Pipeline} DAG, a Flatten can appear in each
   * stage that produces any of its input {@link PCollection PCollections}, as all of its inputs
   * will reach it via only one of those paths.
   *
   * <p>As flatten consumes multiple inputs and produces a single output, there are two cases that
   * must be considered for the inputs.
   *
   * <ul>
   *   <li>All of the producers of all inputs are within a single stage
   *   <li>The producers of the inputs are in two or more stages
   * </ul>
   *
   * <p>If all of the producers exist within a single stage, this is identical to any other
   * transform that consumes a single input - the output PCollection is materialized based only on
   * its consumers.
   *
   * <p>If the producers exist within separate stages, there are two other considerations:
   *
   * <ul>
   *   <li>The output PCollection must be materialized in all cases (has consumers which cannot be
   *       fused into at least one of the upstream stages).
   *   <li>All of the consumers of the output PCollection can be fused into at least one of the
   *       producing stages.
   * </ul>
   *
   * <p>For the former case, this again is identical to a transform that produces a materialized
   * {@link PCollection}; each path to the {@link Flatten} produces elements for the input {@link
   * PCollection}, and the output is materialized and consumed by downstream transforms identically
   * to any other materialized {@link PCollection}.
   *
   * <p>For the latter, where fusion is possible into at least one of the producer stages, Flatten
   * unzipping is performed. This consists of the following steps:
   *
   * <ol>
   *   <li>The flatten is fused into each upstream stage
   *   <li>Each stage which contains a producer that can be fused with the output {@link
   *       PCollection} fuses that {@link PCollection}. Elements produced by that stage for the
   *       output of the flatten are never materialized.
   *   <li>Each stage which cannot be fused with the output {@link PCollection} materializes the
   *       output of the {@link Flatten}. All of the downstream consumers exist in a stage which
   *       reads from the output of that {@link Flatten}, which contains all of the elements from
   *       the stages that could not fuse with those consumers.
   * </ol>
   */
  private static boolean canAlwaysFuse(
      @SuppressWarnings("unused") PipelineNode.PTransformNode flatten,
      @SuppressWarnings("unused") Environment environment,
      @SuppressWarnings("unused") PipelineNode.PCollectionNode candidate,
      @SuppressWarnings("unused") Collection<PipelineNode.PCollectionNode> stagePCollections,
      @SuppressWarnings("unused") QueryablePipeline pipeline) {
    return true;
  }

  private static boolean cannotFuse(
      @SuppressWarnings("unused") PipelineNode.PTransformNode cannotFuse,
      @SuppressWarnings("unused") Environment environment,
      @SuppressWarnings("unused") PipelineNode.PCollectionNode candidate,
      @SuppressWarnings("unused") Collection<PipelineNode.PCollectionNode> stagePCollections,
      @SuppressWarnings("unused") QueryablePipeline pipeline) {
    return false;
  }

  private static boolean noCompatibility(
      @SuppressWarnings("unused") PipelineNode.PTransformNode self,
      @SuppressWarnings("unused") PipelineNode.PTransformNode other,
      @SuppressWarnings("unused") QueryablePipeline pipeline) {
    // TODO: There is performance to be gained if the output of a flatten is fused into a stage
    // where its output is wholly consumed after a fusion break. This requires slightly more
    // lookahead.
    return false;
  }

  // Things with unknown URNs either execute within their own stage or are executed by the runner.
  // In either case, assume the system is capable of executing the expressed transform
  private static boolean unknownTransformFusion(
      PipelineNode.PTransformNode transform,
      @SuppressWarnings("unused") Environment environment,
      @SuppressWarnings("unused") PipelineNode.PCollectionNode candidate,
      @SuppressWarnings("unused") Collection<PipelineNode.PCollectionNode> stagePCollections,
      @SuppressWarnings("unused") QueryablePipeline pipeline) {
    LOG.debug(
        "Unknown {} {} will not fuse into an existing {}",
        PTransform.class.getSimpleName(),
        transform.getTransform(),
        ExecutableStage.class.getSimpleName());
    return false;
  }

  // Things with unknown URNs either execute within their own stage or are executed by the runner.
  // In either case, assume the system is capable of executing the expressed transform
  private static boolean unknownTransformCompatibility(
      PipelineNode.PTransformNode transform,
      @SuppressWarnings("unused") PipelineNode.PTransformNode other,
      @SuppressWarnings("unused") QueryablePipeline pipeline) {
    LOG.debug(
        "Unknown {} {} will not root a {} with other {}",
        PTransform.class.getSimpleName(),
        transform.getTransform(),
        ExecutableStage.class.getSimpleName(),
        PTransform.class.getSimpleName());
    return false;
  }
}
