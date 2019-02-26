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

import static org.apache.beam.runners.core.construction.BeamUrns.getUrn;
import static org.apache.beam.vendor.guava.v20_0.com.google.common.base.Preconditions.checkArgument;

import java.util.Map;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.model.pipeline.v1.RunnerApi.CombinePayload;
import org.apache.beam.model.pipeline.v1.RunnerApi.Components;
import org.apache.beam.model.pipeline.v1.RunnerApi.ExecutableStagePayload;
import org.apache.beam.model.pipeline.v1.RunnerApi.PCollection;
import org.apache.beam.model.pipeline.v1.RunnerApi.PTransform;
import org.apache.beam.model.pipeline.v1.RunnerApi.ParDoPayload;
import org.apache.beam.model.pipeline.v1.RunnerApi.StandardPTransforms.CombineComponents;
import org.apache.beam.model.pipeline.v1.RunnerApi.StandardPTransforms.Composites;
import org.apache.beam.model.pipeline.v1.RunnerApi.StandardPTransforms.Primitives;
import org.apache.beam.model.pipeline.v1.RunnerApi.StandardPTransforms.SplittableParDoComponents;
import org.apache.beam.model.pipeline.v1.RunnerApi.TestStreamPayload;
import org.apache.beam.model.pipeline.v1.RunnerApi.WindowIntoPayload;
import org.apache.beam.model.pipeline.v1.RunnerApi.WindowingStrategy;
import org.apache.beam.vendor.guava.v20_0.com.google.common.collect.ImmutableMap;
import org.apache.beam.vendor.guava.v20_0.com.google.common.collect.Maps;

/**
 * Validates well-formedness of a pipeline. It is recommended to use this class on any user-supplied
 * Pipeline protos, and after any transformations on the pipeline, to verify that the
 * transformations didn't break well-formedness.
 */
public class PipelineValidator {
  @FunctionalInterface
  private interface TransformValidator {
    void validate(String transformId, PTransform transform, Components components) throws Exception;
  }

  private static final ImmutableMap<String, TransformValidator> VALIDATORS =
      ImmutableMap.<String, TransformValidator>builder()
          .put(getUrn(Primitives.PAR_DO), PipelineValidator::validateParDo)
          // Nothing to validate for FLATTEN, GROUP_BY_KEY, IMPULSE
          .put(getUrn(Primitives.ASSIGN_WINDOWS), PipelineValidator::validateAssignWindows)
          .put(getUrn(Primitives.TEST_STREAM), PipelineValidator::validateTestStream)
          // Nothing to validate for MAP_WINDOWS, READ, CREATE_VIEW.
          .put(getUrn(Composites.COMBINE_PER_KEY), PipelineValidator::validateCombine)
          .put(getUrn(Composites.COMBINE_GLOBALLY), PipelineValidator::validateCombine)
          // Nothing to validate for RESHUFFLE and WRITE_FILES
          .put(getUrn(CombineComponents.COMBINE_PGBKCV), PipelineValidator::validateCombine)
          .put(
              getUrn(CombineComponents.COMBINE_MERGE_ACCUMULATORS),
              PipelineValidator::validateCombine)
          .put(
              getUrn(CombineComponents.COMBINE_EXTRACT_OUTPUTS), PipelineValidator::validateCombine)
          .put(
              getUrn(CombineComponents.COMBINE_PER_KEY_PRECOMBINE),
              PipelineValidator::validateCombine)
          .put(
              getUrn(CombineComponents.COMBINE_PER_KEY_MERGE_ACCUMULATORS),
              PipelineValidator::validateCombine)
          .put(
              getUrn(CombineComponents.COMBINE_PER_KEY_EXTRACT_OUTPUTS),
              PipelineValidator::validateCombine)
          .put(getUrn(CombineComponents.COMBINE_GROUPED_VALUES), PipelineValidator::validateCombine)
          .put(
              getUrn(SplittableParDoComponents.PAIR_WITH_RESTRICTION),
              PipelineValidator::validateParDo)
          .put(
              getUrn(SplittableParDoComponents.SPLIT_RESTRICTION), PipelineValidator::validateParDo)
          .put(
              getUrn(SplittableParDoComponents.PROCESS_KEYED_ELEMENTS),
              PipelineValidator::validateParDo)
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

    validateComponents("pipeline", components);
  }

  private static void validateComponents(String context, Components components) {
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
        validateTransform(transformId, transform, components);
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

  private static void validateTransform(String id, PTransform transform, Components components) {
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
    if (VALIDATORS.containsKey(urn)) {
      try {
        VALIDATORS.get(urn).validate(id, transform, components);
      } catch (Exception e) {
        throw new RuntimeException(String.format("Failed to validate transform %s", id), e);
      }
    }
  }

  private static void validateParDo(String id, PTransform transform, Components components)
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
    // TODO: Validate state_specs and timer_specs
    if (!payload.getRestrictionCoderId().isEmpty()) {
      checkArgument(components.containsCoders(payload.getRestrictionCoderId()));
    }
  }

  private static void validateAssignWindows(String id, PTransform transform, Components components)
      throws Exception {
    WindowIntoPayload.parseFrom(transform.getSpec().getPayload());
  }

  private static void validateTestStream(String id, PTransform transform, Components components)
      throws Exception {
    TestStreamPayload.parseFrom(transform.getSpec().getPayload());
  }

  private static void validateCombine(String id, PTransform transform, Components components)
      throws Exception {
    CombinePayload payload = CombinePayload.parseFrom(transform.getSpec().getPayload());
    checkArgument(
        components.containsCoders(payload.getAccumulatorCoderId()),
        "Transform %s uses unknown accumulator coder id %s",
        payload.getAccumulatorCoderId());
  }

  private static void validateExecutableStage(
      String id, PTransform transform, Components outerComponents) throws Exception {
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

    validateComponents("ExecutableStage " + id, components);

    // TODO: Also validate that side inputs of all transforms within components.getTransforms()
    // are contained within payload.getSideInputsList()
  }
}
