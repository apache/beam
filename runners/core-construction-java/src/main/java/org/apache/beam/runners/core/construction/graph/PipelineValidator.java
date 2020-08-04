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
package org.apache.beam.runners.core.construction.graph;

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkArgument;

import java.util.Map;
import java.util.Set;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.model.pipeline.v1.RunnerApi.CombinePayload;
import org.apache.beam.model.pipeline.v1.RunnerApi.Components;
import org.apache.beam.model.pipeline.v1.RunnerApi.ExecutableStagePayload;
import org.apache.beam.model.pipeline.v1.RunnerApi.PCollection;
import org.apache.beam.model.pipeline.v1.RunnerApi.PTransform;
import org.apache.beam.model.pipeline.v1.RunnerApi.ParDoPayload;
import org.apache.beam.model.pipeline.v1.RunnerApi.TestStreamPayload;
import org.apache.beam.model.pipeline.v1.RunnerApi.WindowIntoPayload;
import org.apache.beam.model.pipeline.v1.RunnerApi.WindowingStrategy;
import org.apache.beam.runners.core.construction.PTransformTranslation;
import org.apache.beam.runners.core.construction.ParDoTranslation;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableSet;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Maps;

/**
 * Validates well-formedness of a pipeline. It is recommended to use this class on any user-supplied
 * Pipeline protos, and after any transformations on the pipeline, to verify that the
 * transformations didn't break well-formedness.
 */
public class PipelineValidator {
  @FunctionalInterface
  private interface TransformValidator {
    void validate(
        String transformId, PTransform transform, Components components, Set<String> requirements)
        throws Exception;
  }

  private static final ImmutableMap<String, TransformValidator> VALIDATORS =
      ImmutableMap.<String, TransformValidator>builder()
          .put(PTransformTranslation.PAR_DO_TRANSFORM_URN, PipelineValidator::validateParDo)
          // Nothing to validate for FLATTEN, GROUP_BY_KEY, IMPULSE
          .put(
              PTransformTranslation.ASSIGN_WINDOWS_TRANSFORM_URN,
              PipelineValidator::validateAssignWindows)
          .put(
              PTransformTranslation.TEST_STREAM_TRANSFORM_URN,
              PipelineValidator::validateTestStream)
          // Nothing to validate for MAP_WINDOWS, READ, CREATE_VIEW.
          .put(
              PTransformTranslation.COMBINE_PER_KEY_TRANSFORM_URN,
              PipelineValidator::validateCombine)
          .put(
              PTransformTranslation.COMBINE_GLOBALLY_TRANSFORM_URN,
              PipelineValidator::validateCombine)
          // Nothing to validate for RESHUFFLE and WRITE_FILES
          .put(
              PTransformTranslation.COMBINE_PER_KEY_PRECOMBINE_TRANSFORM_URN,
              PipelineValidator::validateCombine)
          .put(
              PTransformTranslation.COMBINE_PER_KEY_MERGE_ACCUMULATORS_TRANSFORM_URN,
              PipelineValidator::validateCombine)
          .put(
              PTransformTranslation.COMBINE_PER_KEY_EXTRACT_OUTPUTS_TRANSFORM_URN,
              PipelineValidator::validateCombine)
          .put(
              PTransformTranslation.COMBINE_GROUPED_VALUES_TRANSFORM_URN,
              PipelineValidator::validateCombine)
          .put(
              PTransformTranslation.SPLITTABLE_PAIR_WITH_RESTRICTION_URN,
              PipelineValidator::validateParDo)
          .put(PTransformTranslation.SPLITTABLE_PROCESS_KEYED_URN, PipelineValidator::validateParDo)
          .put(ExecutableStage.URN, PipelineValidator::validateExecutableStage)
          .build();

  public static void validate(RunnerApi.Pipeline p) {
    Components components = p.getComponents();

    for (String transformId : p.getRootTransformIdsList()) {
      checkArgument(
          components.containsTransforms(transformId),
          "Root transform id %s is unknown",
          transformId);
    }

    validateComponents("pipeline", components, ImmutableSet.copyOf(p.getRequirementsList()));
  }

  private static void validateComponents(
      String context, Components components, Set<String> requirements) {
    {
      Map<String, String> uniqueNamesById = Maps.newHashMap();
      for (String transformId : components.getTransformsMap().keySet()) {
        PTransform transform = components.getTransformsOrThrow(transformId);
        String previousId = uniqueNamesById.put(transform.getUniqueName(), transformId);
        // A transform is allowed to not have unique_name set, but, obviously,
        // there can be only one such transform with an empty name.
        // It's allowed for the (only) root transform to have the empty unique_name.
        checkArgument(
            previousId == null,
            "%s: Transforms %s and %s both have unique_name \"%s\"",
            context,
            transformId,
            previousId,
            transform.getUniqueName());
        validateTransform(transformId, transform, components, requirements);
      }
    }
    {
      Map<String, String> uniqueNamesById = Maps.newHashMap();
      for (String pcollectionId : components.getPcollectionsMap().keySet()) {
        PCollection pc = components.getPcollectionsOrThrow(pcollectionId);
        checkArgument(
            !pc.getUniqueName().isEmpty(),
            "%s: PCollection %s does not have a unique_name set",
            context,
            pcollectionId);
        String previousId = uniqueNamesById.put(pc.getUniqueName(), pcollectionId);
        checkArgument(
            previousId == null,
            "%s: PCollections %s and %s both have unique_name \"%s\"",
            context,
            pcollectionId,
            previousId,
            pc.getUniqueName());
        checkArgument(
            components.containsCoders(pc.getCoderId()),
            "%s: PCollection %s uses unknown coder %s",
            context,
            pcollectionId,
            pc.getCoderId());
        checkArgument(
            components.containsWindowingStrategies(pc.getWindowingStrategyId()),
            "%s: PCollection %s uses unknown windowing strategy %s",
            context,
            pcollectionId,
            pc.getWindowingStrategyId());
      }
    }

    for (String strategyId : components.getWindowingStrategiesMap().keySet()) {
      WindowingStrategy strategy = components.getWindowingStrategiesOrThrow(strategyId);
      checkArgument(
          components.containsCoders(strategy.getWindowCoderId()),
          "%s: WindowingStrategy %s uses unknown coder %s",
          context,
          strategyId,
          strategy.getWindowCoderId());
    }

    for (String coderId : components.getCodersMap().keySet()) {
      for (String componentCoderId :
          components.getCodersOrThrow(coderId).getComponentCoderIdsList()) {
        checkArgument(
            components.containsCoders(componentCoderId),
            "%s: Coder %s uses unknown component coder %s",
            context,
            coderId,
            componentCoderId);
      }
    }
  }

  private static void validateTransform(
      String id, PTransform transform, Components components, Set<String> requirements) {
    for (String subtransformId : transform.getSubtransformsList()) {
      checkArgument(
          components.containsTransforms(subtransformId),
          "Transform %s references unknown subtransform %s",
          id,
          subtransformId);
    }

    for (String inputId : transform.getInputsMap().keySet()) {
      String pcollectionId = transform.getInputsOrThrow(inputId);
      checkArgument(
          components.containsPcollections(pcollectionId),
          "Transform %s input %s points to unknown PCollection %s",
          id,
          inputId,
          pcollectionId);
    }
    for (String outputId : transform.getOutputsMap().keySet()) {
      String pcollectionId = transform.getOutputsOrThrow(outputId);
      checkArgument(
          components.containsPcollections(pcollectionId),
          "Transform %s output %s points to unknown PCollection %s",
          id,
          outputId,
          pcollectionId);
    }

    String urn = transform.getSpec().getUrn();
    if (PTransformTranslation.RUNNER_IMPLEMENTED_TRANSFORMS.contains(urn)) {
      checkArgument(
          transform.getEnvironmentId().isEmpty(),
          "Transform %s references environment %s when no environment should be specified since it is a required runner implemented transform %s.",
          id,
          transform.getEnvironmentId(),
          urn);
    }

    if (VALIDATORS.containsKey(urn)) {
      try {
        VALIDATORS.get(urn).validate(id, transform, components, requirements);
      } catch (Exception e) {
        throw new RuntimeException(String.format("Failed to validate transform %s", id), e);
      }
    }
  }

  private static void validateParDo(
      String id, PTransform transform, Components components, Set<String> requirements)
      throws Exception {
    ParDoPayload payload = ParDoPayload.parseFrom(transform.getSpec().getPayload());
    // side_inputs
    for (String sideInputId : payload.getSideInputsMap().keySet()) {
      checkArgument(
          transform.containsInputs(sideInputId),
          "Transform %s side input %s is not listed in the transform's inputs",
          id,
          sideInputId);
    }
    if (payload.getStateSpecsCount() > 0 || payload.getTimerFamilySpecsCount() > 0) {
      checkArgument(requirements.contains(ParDoTranslation.REQUIRES_STATEFUL_PROCESSING_URN));
      // TODO: Validate state_specs and timer_specs
    }
    if (!payload.getRestrictionCoderId().isEmpty()) {
      checkArgument(components.containsCoders(payload.getRestrictionCoderId()));
      checkArgument(requirements.contains(ParDoTranslation.REQUIRES_SPLITTABLE_DOFN_URN));
    }
    if (payload.getRequestsFinalization()) {
      checkArgument(requirements.contains(ParDoTranslation.REQUIRES_BUNDLE_FINALIZATION_URN));
    }
    if (payload.getRequiresStableInput()) {
      checkArgument(requirements.contains(ParDoTranslation.REQUIRES_STABLE_INPUT_URN));
    }
    if (payload.getRequiresTimeSortedInput()) {
      checkArgument(requirements.contains(ParDoTranslation.REQUIRES_TIME_SORTED_INPUT_URN));
    }
  }

  private static void validateAssignWindows(
      String id, PTransform transform, Components components, Set<String> requirements)
      throws Exception {
    WindowIntoPayload.parseFrom(transform.getSpec().getPayload());
  }

  private static void validateTestStream(
      String id, PTransform transform, Components components, Set<String> requirements)
      throws Exception {
    TestStreamPayload.parseFrom(transform.getSpec().getPayload());
  }

  private static void validateCombine(
      String id, PTransform transform, Components components, Set<String> requirements)
      throws Exception {
    CombinePayload payload = CombinePayload.parseFrom(transform.getSpec().getPayload());
    checkArgument(
        components.containsCoders(payload.getAccumulatorCoderId()),
        "Transform %s uses unknown accumulator coder id %s",
        payload.getAccumulatorCoderId());
  }

  private static void validateExecutableStage(
      String id, PTransform transform, Components outerComponents, Set<String> requirements)
      throws Exception {
    ExecutableStagePayload payload =
        ExecutableStagePayload.parseFrom(transform.getSpec().getPayload());

    // Everything within an ExecutableStagePayload uses only the stage's components.
    Components components = payload.getComponents();

    checkArgument(
        transform.getInputsMap().values().contains(payload.getInput()),
        "ExecutableStage %s uses unknown input %s",
        id,
        payload.getInput());

    checkArgument(
        !payload.getTransformsList().isEmpty(), "ExecutableStage %s contains no transforms", id);

    for (String subtransformId : payload.getTransformsList()) {
      checkArgument(
          components.containsTransforms(subtransformId),
          "ExecutableStage %s uses unknown transform %s",
          id,
          subtransformId);
    }
    for (String outputId : payload.getOutputsList()) {
      checkArgument(
          components.containsPcollections(outputId),
          "ExecutableStage %s uses unknown output %s",
          id,
          outputId);
    }

    validateComponents("ExecutableStage " + id, components, requirements);

    // TODO: Also validate that side inputs of all transforms within components.getTransforms()
    // are contained within payload.getSideInputsList()
  }
}
