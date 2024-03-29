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

import com.google.auto.value.AutoValue;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.model.pipeline.v1.RunnerApi.Components;
import org.apache.beam.model.pipeline.v1.RunnerApi.FunctionSpec;
import org.apache.beam.model.pipeline.v1.RunnerApi.PCollection;
import org.apache.beam.model.pipeline.v1.RunnerApi.PTransform;
import org.apache.beam.sdk.util.construction.PTransformTranslation;
import org.apache.beam.sdk.util.construction.SyntheticComponents;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.HashMultimap;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Multimap;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * Utilities to insert synthetic {@link PipelineNode.PCollectionNode PCollections} for {@link
 * PCollection PCollections} which are produced by multiple independently executable stages.
 */
@SuppressWarnings({
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
class OutputDeduplicator {

  /**
   * Ensure that no {@link PCollection} output by any of the {@code stages} or {@code
   * unfusedTransforms} is produced by more than one of those stages or transforms.
   *
   * <p>For each {@link PCollection} output by multiple stages and/or transforms, each producer is
   * rewritten to produce a partial {@link PCollection}, which are then flattened together via an
   * introduced Flatten node which produces the original output.
   */
  static DeduplicationResult ensureSingleProducer(
      QueryablePipeline pipeline,
      Collection<ExecutableStage> stages,
      Collection<PipelineNode.PTransformNode> unfusedTransforms) {
    RunnerApi.Components.Builder unzippedComponents = pipeline.getComponents().toBuilder();

    Multimap<PipelineNode.PCollectionNode, StageOrTransform> pcollectionProducers =
        getProducers(pipeline, stages, unfusedTransforms);
    Multimap<StageOrTransform, PipelineNode.PCollectionNode> requiresNewOutput =
        HashMultimap.create();
    // Create a synthetic PCollection for each of these nodes. The transforms in the runner
    // portion of the graph that creates them should be replaced in the result components. The
    // ExecutableStage must also be rewritten to have updated outputs and transforms.
    for (Map.Entry<PipelineNode.PCollectionNode, Collection<StageOrTransform>> collectionProducer :
        pcollectionProducers.asMap().entrySet()) {
      if (collectionProducer.getValue().size() > 1) {
        for (StageOrTransform producer : collectionProducer.getValue()) {
          requiresNewOutput.put(producer, collectionProducer.getKey());
        }
      }
    }

    Map<ExecutableStage, ExecutableStage> updatedStages = new LinkedHashMap<>();
    Map<String, PipelineNode.PTransformNode> updatedTransforms = new LinkedHashMap<>();
    Multimap<String, PipelineNode.PCollectionNode> originalToPartial = HashMultimap.create();
    for (Map.Entry<StageOrTransform, Collection<PipelineNode.PCollectionNode>>
        deduplicationTargets : requiresNewOutput.asMap().entrySet()) {
      if (deduplicationTargets.getKey().getStage() != null) {
        StageDeduplication deduplication =
            deduplicatePCollections(
                deduplicationTargets.getKey().getStage(),
                deduplicationTargets.getValue(),
                unzippedComponents::containsPcollections);
        for (Entry<String, PipelineNode.PCollectionNode> originalToPartialReplacement :
            deduplication.getOriginalToPartialPCollections().entrySet()) {
          originalToPartial.put(
              originalToPartialReplacement.getKey(), originalToPartialReplacement.getValue());
          unzippedComponents.putPcollections(
              originalToPartialReplacement.getValue().getId(),
              originalToPartialReplacement.getValue().getPCollection());
        }
        updatedStages.put(
            deduplicationTargets.getKey().getStage(), deduplication.getUpdatedStage());
      } else if (deduplicationTargets.getKey().getTransform() != null) {
        PTransformDeduplication deduplication =
            deduplicatePCollections(
                deduplicationTargets.getKey().getTransform(),
                deduplicationTargets.getValue(),
                unzippedComponents::containsPcollections);
        for (Entry<String, PipelineNode.PCollectionNode> originalToPartialReplacement :
            deduplication.getOriginalToPartialPCollections().entrySet()) {
          originalToPartial.put(
              originalToPartialReplacement.getKey(), originalToPartialReplacement.getValue());
          unzippedComponents.putPcollections(
              originalToPartialReplacement.getValue().getId(),
              originalToPartialReplacement.getValue().getPCollection());
        }
        updatedTransforms.put(
            deduplicationTargets.getKey().getTransform().getId(),
            deduplication.getUpdatedTransform());
      } else {
        throw new IllegalStateException(
            String.format(
                "%s with no %s or %s",
                StageOrTransform.class.getSimpleName(),
                ExecutableStage.class.getSimpleName(),
                PipelineNode.PTransformNode.class.getSimpleName()));
      }
    }

    Set<PipelineNode.PTransformNode> introducedFlattens = new LinkedHashSet<>();
    for (Map.Entry<String, Collection<PipelineNode.PCollectionNode>> partialFlattenTargets :
        originalToPartial.asMap().entrySet()) {
      String flattenId =
          SyntheticComponents.uniqueId("unzipped_flatten", unzippedComponents::containsTransforms);
      PTransform flattenPartialPCollections =
          createFlattenOfPartials(
              flattenId, partialFlattenTargets.getKey(), partialFlattenTargets.getValue());
      unzippedComponents.putTransforms(flattenId, flattenPartialPCollections);
      introducedFlattens.add(PipelineNode.pTransform(flattenId, flattenPartialPCollections));
    }

    Components components = unzippedComponents.build();
    return DeduplicationResult.of(components, introducedFlattens, updatedStages, updatedTransforms);
  }

  @AutoValue
  abstract static class DeduplicationResult {
    private static DeduplicationResult of(
        RunnerApi.Components components,
        Set<PipelineNode.PTransformNode> introducedTransforms,
        Map<ExecutableStage, ExecutableStage> stages,
        Map<String, PipelineNode.PTransformNode> unfused) {
      return new AutoValue_OutputDeduplicator_DeduplicationResult(
          components, introducedTransforms, stages, unfused);
    }

    abstract RunnerApi.Components getDeduplicatedComponents();

    abstract Set<PipelineNode.PTransformNode> getIntroducedTransforms();

    abstract Map<ExecutableStage, ExecutableStage> getDeduplicatedStages();

    abstract Map<String, PipelineNode.PTransformNode> getDeduplicatedTransforms();
  }

  private static PTransform createFlattenOfPartials(
      String transformId,
      String outputId,
      Collection<PipelineNode.PCollectionNode> generatedInputs) {
    PTransform.Builder newFlattenBuilder = PTransform.newBuilder();
    int i = 0;
    for (PipelineNode.PCollectionNode generatedInput : generatedInputs) {
      String localInputId = String.format("input_%s", i);
      i++;
      newFlattenBuilder.putInputs(localInputId, generatedInput.getId());
    }
    // Flatten all of the new partial nodes together.
    return newFlattenBuilder
        // Use transform ID as unique name.
        .setUniqueName(transformId)
        .putOutputs("output", outputId)
        .setSpec(FunctionSpec.newBuilder().setUrn(PTransformTranslation.FLATTEN_TRANSFORM_URN))
        .build();
  }

  /**
   * Returns the map from each {@link PipelineNode.PCollectionNode} produced by any of the {@link
   * ExecutableStage stages} or {@link PipelineNode.PTransformNode transforms} to all of the {@link
   * ExecutableStage stages} or {@link PipelineNode.PTransformNode transforms} that produce it.
   */
  private static Multimap<PipelineNode.PCollectionNode, StageOrTransform> getProducers(
      QueryablePipeline pipeline,
      Iterable<ExecutableStage> stages,
      Iterable<PipelineNode.PTransformNode> unfusedTransforms) {
    Multimap<PipelineNode.PCollectionNode, StageOrTransform> pcollectionProducers =
        HashMultimap.create();
    for (ExecutableStage stage : stages) {
      for (PipelineNode.PCollectionNode output : stage.getOutputPCollections()) {
        pcollectionProducers.put(output, StageOrTransform.stage(stage));
      }
    }
    for (PipelineNode.PTransformNode unfused : unfusedTransforms) {
      for (PipelineNode.PCollectionNode output : pipeline.getOutputPCollections(unfused)) {
        pcollectionProducers.put(output, StageOrTransform.transform(unfused));
      }
    }
    return pcollectionProducers;
  }

  private static PTransformDeduplication deduplicatePCollections(
      PipelineNode.PTransformNode transform,
      Collection<PipelineNode.PCollectionNode> duplicates,
      Predicate<String> existingPCollectionIds) {
    Map<String, PipelineNode.PCollectionNode> unzippedOutputs =
        createPartialPCollections(duplicates, existingPCollectionIds);
    PTransform pTransform = updateOutputs(transform.getTransform(), unzippedOutputs);
    return PTransformDeduplication.of(
        PipelineNode.pTransform(transform.getId(), pTransform), unzippedOutputs);
  }

  @AutoValue
  abstract static class PTransformDeduplication {
    public static PTransformDeduplication of(
        PipelineNode.PTransformNode updatedTransform,
        Map<String, PipelineNode.PCollectionNode> originalToPartial) {
      return new AutoValue_OutputDeduplicator_PTransformDeduplication(
          updatedTransform, originalToPartial);
    }

    abstract PipelineNode.PTransformNode getUpdatedTransform();

    abstract Map<String, PipelineNode.PCollectionNode> getOriginalToPartialPCollections();
  }

  private static StageDeduplication deduplicatePCollections(
      ExecutableStage stage,
      Collection<PipelineNode.PCollectionNode> duplicates,
      Predicate<String> existingPCollectionIds) {
    Map<String, PipelineNode.PCollectionNode> unzippedOutputs =
        createPartialPCollections(duplicates, existingPCollectionIds);
    ExecutableStage updatedStage = deduplicateStageOutput(stage, unzippedOutputs);
    return StageDeduplication.of(updatedStage, unzippedOutputs);
  }

  @AutoValue
  abstract static class StageDeduplication {
    public static StageDeduplication of(
        ExecutableStage updatedStage, Map<String, PipelineNode.PCollectionNode> originalToPartial) {
      return new AutoValue_OutputDeduplicator_StageDeduplication(updatedStage, originalToPartial);
    }

    abstract ExecutableStage getUpdatedStage();

    abstract Map<String, PipelineNode.PCollectionNode> getOriginalToPartialPCollections();
  }

  /**
   * Returns a {@link Map} from the ID of a {@link PipelineNode.PCollectionNode PCollection} to a
   * {@link PipelineNode.PCollectionNode} that contains part of that {@link
   * PipelineNode.PCollectionNode PCollection}.
   */
  private static Map<String, PipelineNode.PCollectionNode> createPartialPCollections(
      Collection<PipelineNode.PCollectionNode> duplicates,
      Predicate<String> existingPCollectionIds) {
    Map<String, PipelineNode.PCollectionNode> unzippedOutputs = new LinkedHashMap<>();
    Predicate<String> existingOrNewIds =
        existingPCollectionIds.or(
            id ->
                unzippedOutputs.values().stream()
                    .map(PipelineNode.PCollectionNode::getId)
                    .anyMatch(id::equals));
    for (PipelineNode.PCollectionNode duplicateOutput : duplicates) {
      String id = SyntheticComponents.uniqueId(duplicateOutput.getId(), existingOrNewIds);
      PCollection partial = duplicateOutput.getPCollection().toBuilder().setUniqueName(id).build();
      // Check to make sure there is only one duplicated output with the same id - which ensures we
      // only introduce one 'partial output' per producer of that output.
      PipelineNode.PCollectionNode alreadyDeduplicated =
          unzippedOutputs.put(duplicateOutput.getId(), PipelineNode.pCollection(id, partial));
      checkArgument(alreadyDeduplicated == null, "a duplicate should only appear once per stage");
    }
    return unzippedOutputs;
  }

  /**
   * Returns an {@link ExecutableStage} where all of the {@link PipelineNode.PCollectionNode
   * PCollections} matching the original are replaced with the introduced partial {@link
   * PCollection} in all references made within the {@link ExecutableStage}.
   */
  private static ExecutableStage deduplicateStageOutput(
      ExecutableStage stage, Map<String, PipelineNode.PCollectionNode> originalToPartial) {
    Collection<PipelineNode.PTransformNode> updatedTransforms = new ArrayList<>();
    for (PipelineNode.PTransformNode transform : stage.getTransforms()) {
      PTransform updatedTransform = updateOutputs(transform.getTransform(), originalToPartial);
      updatedTransforms.add(PipelineNode.pTransform(transform.getId(), updatedTransform));
    }
    Collection<PipelineNode.PCollectionNode> updatedOutputs = new ArrayList<>();
    for (PipelineNode.PCollectionNode output : stage.getOutputPCollections()) {
      updatedOutputs.add(originalToPartial.getOrDefault(output.getId(), output));
    }
    RunnerApi.Components updatedStageComponents =
        stage
            .getComponents()
            .toBuilder()
            .clearTransforms()
            .putAllTransforms(
                updatedTransforms.stream()
                    .collect(
                        Collectors.toMap(
                            PipelineNode.PTransformNode::getId,
                            PipelineNode.PTransformNode::getTransform)))
            .putAllPcollections(
                originalToPartial.values().stream()
                    .collect(
                        Collectors.toMap(
                            PipelineNode.PCollectionNode::getId,
                            PipelineNode.PCollectionNode::getPCollection)))
            .build();
    return ImmutableExecutableStage.of(
        updatedStageComponents,
        stage.getEnvironment(),
        stage.getInputPCollection(),
        stage.getSideInputs(),
        stage.getUserStates(),
        stage.getTimers(),
        updatedTransforms,
        updatedOutputs,
        stage.getWireCoderSettings());
  }

  /**
   * Returns a {@link PTransform} like the input {@link PTransform}, but with each output to {@code
   * originalPCollection} replaced with an output (with the same local name) to {@code
   * newPCollection}.
   */
  private static PTransform updateOutputs(
      PTransform transform, Map<String, PipelineNode.PCollectionNode> originalToPartial) {
    PTransform.Builder updatedTransformBuilder = transform.toBuilder();
    for (Map.Entry<String, String> output : transform.getOutputsMap().entrySet()) {
      if (originalToPartial.containsKey(output.getValue())) {
        updatedTransformBuilder.putOutputs(
            output.getKey(), originalToPartial.get(output.getValue()).getId());
      }
    }
    updatedTransformBuilder.setEnvironmentId(transform.getEnvironmentId());
    return updatedTransformBuilder.build();
  }

  @AutoValue
  abstract static class StageOrTransform {
    public static StageOrTransform stage(ExecutableStage stage) {
      return new AutoValue_OutputDeduplicator_StageOrTransform(stage, null);
    }

    public static StageOrTransform transform(PipelineNode.PTransformNode transform) {
      return new AutoValue_OutputDeduplicator_StageOrTransform(null, transform);
    }

    abstract @Nullable ExecutableStage getStage();

    abstract PipelineNode.@Nullable PTransformNode getTransform();
  }
}
