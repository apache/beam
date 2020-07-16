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

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;
import java.util.function.Predicate;
import org.apache.beam.model.pipeline.v1.RunnerApi.Coder;
import org.apache.beam.model.pipeline.v1.RunnerApi.ComponentsOrBuilder;
import org.apache.beam.model.pipeline.v1.RunnerApi.FunctionSpec;
import org.apache.beam.model.pipeline.v1.RunnerApi.MessageWithComponents;
import org.apache.beam.model.pipeline.v1.RunnerApi.PCollection;
import org.apache.beam.model.pipeline.v1.RunnerApi.PTransform;
import org.apache.beam.model.pipeline.v1.RunnerApi.ParDoPayload;
import org.apache.beam.runners.core.construction.ModelCoders;
import org.apache.beam.runners.core.construction.PTransformTranslation;
import org.apache.beam.runners.core.construction.ParDoTranslation;
import org.apache.beam.runners.core.construction.graph.ProtoOverrides.TransformReplacement;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Maps;

/**
 * A set of transform replacements for expanding a splittable ParDo into various sub components.
 *
 * <p>Further details about the expansion can be found at <a
 * href="https://github.com/apache/beam/blob/cb15994d5228f729dda922419b08520c8be8804e/model/pipeline/src/main/proto/beam_runner_api.proto#L279"
 * />
 */
public class SplittableParDoExpander {

  /**
   * Returns a transform replacement which expands a splittable ParDo from:
   *
   * <pre>{@code
   * sideInputA ---------\
   * sideInputB ---------V
   * mainInput ---> SplittableParDo --> outputA
   *                                \-> outputB
   * }</pre>
   *
   * into:
   *
   * <pre>{@code
   * sideInputA ---------\---------------------\--------------------------\
   * sideInputB ---------V---------------------V--------------------------V
   * mainInput ---> PairWithRestricton --> SplitAndSize --> ProcessSizedElementsAndRestriction --> outputA
   *                                                                                           \-> outputB
   * }</pre>
   *
   * <p>Specifically this transform ensures that initial splitting is performed and that the sizing
   * information is available to the runner if it chooses to inspect it.
   */
  public static TransformReplacement createSizedReplacement() {
    return SizedReplacement.INSTANCE;
  }

  /**
   * Returns a transform replacement in drain mode which expands a splittable ParDo from:
   *
   * <pre>{@code
   * sideInputA ---------\
   * sideInputB ---------V
   * mainInput ---> SplittableParDo --> outputA
   *                                \-> outputB
   * }</pre>
   *
   * into:
   *
   * <pre>{@code
   * sideInputA ---------\---------------------\----------------------\--------------------------\
   * sideInputB ---------V---------------------V----------------------V--------------------------V
   * mainInput ---> PairWithRestriction --> SplitAndSize --> TruncateAndSize --> ProcessSizedElementsAndRestriction --> outputA
   *                                                                                                                \-> outputB
   * }</pre>
   */
  public static TransformReplacement createTruncateReplacement() {
    return TruncateReplacement.INSTANCE;
  }

  /** See {@link #createSizedReplacement()} for details. */
  private static class SizedReplacement implements TransformReplacement {

    private static final SizedReplacement INSTANCE = new SizedReplacement();

    @Override
    public MessageWithComponents getReplacement(
        String transformId, ComponentsOrBuilder existingComponents) {
      try {
        MessageWithComponents.Builder rval = MessageWithComponents.newBuilder();

        PTransform splittableParDo = existingComponents.getTransformsOrThrow(transformId);
        ParDoPayload payload = ParDoPayload.parseFrom(splittableParDo.getSpec().getPayload());
        // Only perform the expansion if this is a splittable DoFn.
        if (payload.getRestrictionCoderId() == null || payload.getRestrictionCoderId().isEmpty()) {
          return null;
        }

        String mainInputName = ParDoTranslation.getMainInputName(splittableParDo);
        String mainInputPCollectionId = splittableParDo.getInputsOrThrow(mainInputName);
        PCollection mainInputPCollection =
            existingComponents.getPcollectionsOrThrow(mainInputPCollectionId);
        Map<String, String> sideInputs =
            Maps.filterKeys(
                splittableParDo.getInputsMap(), input -> payload.containsSideInputs(input));

        String pairWithRestrictionOutCoderId =
            generateUniqueId(
                mainInputPCollection.getCoderId() + "/PairWithRestriction",
                existingComponents::containsCoders);
        rval.getComponentsBuilder()
            .putCoders(
                pairWithRestrictionOutCoderId,
                ModelCoders.kvCoder(
                    mainInputPCollection.getCoderId(), payload.getRestrictionCoderId()));

        String pairWithRestrictionOutId =
            generateUniqueId(
                mainInputPCollectionId + "/PairWithRestriction",
                existingComponents::containsPcollections);
        rval.getComponentsBuilder()
            .putPcollections(
                pairWithRestrictionOutId,
                PCollection.newBuilder()
                    .setCoderId(pairWithRestrictionOutCoderId)
                    .setIsBounded(mainInputPCollection.getIsBounded())
                    .setWindowingStrategyId(mainInputPCollection.getWindowingStrategyId())
                    .setUniqueName(
                        generateUniquePCollectonName(
                            mainInputPCollection.getUniqueName() + "/PairWithRestriction",
                            existingComponents))
                    .build());

        String splitAndSizeOutCoderId =
            generateUniqueId(
                mainInputPCollection.getCoderId() + "/SplitAndSize",
                existingComponents::containsCoders);
        rval.getComponentsBuilder()
            .putCoders(
                splitAndSizeOutCoderId,
                ModelCoders.kvCoder(
                    pairWithRestrictionOutCoderId, getOrAddDoubleCoder(existingComponents, rval)));

        String splitAndSizeOutId =
            generateUniqueId(
                mainInputPCollectionId + "/SplitAndSize", existingComponents::containsPcollections);
        rval.getComponentsBuilder()
            .putPcollections(
                splitAndSizeOutId,
                PCollection.newBuilder()
                    .setCoderId(splitAndSizeOutCoderId)
                    .setIsBounded(mainInputPCollection.getIsBounded())
                    .setWindowingStrategyId(mainInputPCollection.getWindowingStrategyId())
                    .setUniqueName(
                        generateUniquePCollectonName(
                            mainInputPCollection.getUniqueName() + "/SplitAndSize",
                            existingComponents))
                    .build());

        String pairWithRestrictionId =
            generateUniqueId(
                transformId + "/PairWithRestriction", existingComponents::containsTransforms);
        {
          PTransform.Builder pairWithRestriction = PTransform.newBuilder();
          pairWithRestriction.putAllInputs(splittableParDo.getInputsMap());
          pairWithRestriction.putOutputs("out", pairWithRestrictionOutId);
          pairWithRestriction.setUniqueName(
              generateUniquePCollectonName(
                  splittableParDo.getUniqueName() + "/PairWithRestriction", existingComponents));
          pairWithRestriction.setSpec(
              FunctionSpec.newBuilder()
                  .setUrn(PTransformTranslation.SPLITTABLE_PAIR_WITH_RESTRICTION_URN)
                  .setPayload(splittableParDo.getSpec().getPayload()));
          pairWithRestriction.setEnvironmentId(splittableParDo.getEnvironmentId());
          rval.getComponentsBuilder()
              .putTransforms(pairWithRestrictionId, pairWithRestriction.build());
        }

        String splitAndSizeId =
            generateUniqueId(transformId + "/SplitAndSize", existingComponents::containsTransforms);
        {
          PTransform.Builder splitAndSize = PTransform.newBuilder();
          splitAndSize.putInputs(mainInputName, pairWithRestrictionOutId);
          splitAndSize.putAllInputs(sideInputs);
          splitAndSize.putOutputs("out", splitAndSizeOutId);
          splitAndSize.setUniqueName(
              generateUniquePCollectonName(
                  splittableParDo.getUniqueName() + "/SplitAndSize", existingComponents));
          splitAndSize.setSpec(
              FunctionSpec.newBuilder()
                  .setUrn(PTransformTranslation.SPLITTABLE_SPLIT_AND_SIZE_RESTRICTIONS_URN)
                  .setPayload(splittableParDo.getSpec().getPayload()));
          splitAndSize.setEnvironmentId(splittableParDo.getEnvironmentId());
          rval.getComponentsBuilder().putTransforms(splitAndSizeId, splitAndSize.build());
        }

        String processSizedElementsAndRestrictionsId =
            generateUniqueId(
                transformId + "/ProcessSizedElementsAndRestrictions",
                existingComponents::containsTransforms);
        {
          PTransform.Builder processSizedElementsAndRestrictions = PTransform.newBuilder();
          processSizedElementsAndRestrictions.putInputs(mainInputName, splitAndSizeOutId);
          processSizedElementsAndRestrictions.putAllInputs(sideInputs);
          processSizedElementsAndRestrictions.putAllOutputs(splittableParDo.getOutputsMap());
          processSizedElementsAndRestrictions.setUniqueName(
              generateUniquePCollectonName(
                  splittableParDo.getUniqueName() + "/ProcessSizedElementsAndRestrictions",
                  existingComponents));
          processSizedElementsAndRestrictions.setSpec(
              FunctionSpec.newBuilder()
                  .setUrn(
                      PTransformTranslation.SPLITTABLE_PROCESS_SIZED_ELEMENTS_AND_RESTRICTIONS_URN)
                  .setPayload(splittableParDo.getSpec().getPayload()));
          processSizedElementsAndRestrictions.setEnvironmentId(splittableParDo.getEnvironmentId());
          rval.getComponentsBuilder()
              .putTransforms(
                  processSizedElementsAndRestrictionsId,
                  processSizedElementsAndRestrictions.build());
        }

        PTransform.Builder newCompositeRoot =
            splittableParDo
                .toBuilder()
                // Clear the original splittable ParDo spec and add all the new transforms as
                // children.
                .clearSpec()
                .addAllSubtransforms(
                    Arrays.asList(
                        pairWithRestrictionId,
                        splitAndSizeId,
                        processSizedElementsAndRestrictionsId));
        rval.setPtransform(newCompositeRoot);

        return rval.build();
      } catch (IOException e) {
        throw new RuntimeException("Unable to perform expansion for transform " + transformId, e);
      }
    }
  }

  private static String getOrAddDoubleCoder(
      ComponentsOrBuilder existingComponents, MessageWithComponents.Builder out) {
    for (Map.Entry<String, Coder> coder : existingComponents.getCodersMap().entrySet()) {
      if (ModelCoders.DOUBLE_CODER_URN.equals(coder.getValue().getSpec().getUrn())) {
        return coder.getKey();
      }
    }
    String doubleCoderId = generateUniqueId("DoubleCoder", existingComponents::containsCoders);
    out.getComponentsBuilder()
        .putCoders(
            doubleCoderId,
            Coder.newBuilder()
                .setSpec(FunctionSpec.newBuilder().setUrn(ModelCoders.DOUBLE_CODER_URN))
                .build());
    return doubleCoderId;
  }

  /**
   * Returns a PCollection name that uses the supplied prefix that does not exist in {@code
   * existingComponents}.
   */
  private static String generateUniquePCollectonName(
      String prefix, ComponentsOrBuilder existingComponents) {
    return generateUniqueId(
        prefix,
        input -> {
          for (PCollection pc : existingComponents.getPcollectionsMap().values()) {
            if (input.equals(pc.getUniqueName())) {
              return true;
            }
          }
          return false;
        });
  }

  /** Generates a unique id given a prefix and a predicate to compare if the id is already used. */
  private static String generateUniqueId(String prefix, Predicate<String> isExistingId) {
    int i = 0;
    while (isExistingId.test(prefix + i)) {
      i += 1;
    }
    return prefix + i;
  }

  /** See {@link #createTruncateReplacement()} ()} for details. */
  private static class TruncateReplacement extends SizedReplacement {
    private static final TruncateReplacement INSTANCE = new TruncateReplacement();

    @Override
    public MessageWithComponents getReplacement(
        String transformId, ComponentsOrBuilder existingComponents) {
      try {
        MessageWithComponents.Builder rval = MessageWithComponents.newBuilder();

        PTransform splittableParDo = existingComponents.getTransformsOrThrow(transformId);
        ParDoPayload payload = ParDoPayload.parseFrom(splittableParDo.getSpec().getPayload());
        // Only perform the expansion if this is a splittable DoFn.
        if (payload.getRestrictionCoderId() == null || payload.getRestrictionCoderId().isEmpty()) {
          return null;
        }

        String mainInputName = ParDoTranslation.getMainInputName(splittableParDo);
        String mainInputPCollectionId = splittableParDo.getInputsOrThrow(mainInputName);
        PCollection mainInputPCollection =
            existingComponents.getPcollectionsOrThrow(mainInputPCollectionId);
        Map<String, String> sideInputs =
            Maps.filterKeys(
                splittableParDo.getInputsMap(), input -> payload.containsSideInputs(input));

        String pairWithRestrictionOutCoderId =
            generateUniqueId(
                mainInputPCollection.getCoderId() + "/PairWithRestriction",
                existingComponents::containsCoders);
        rval.getComponentsBuilder()
            .putCoders(
                pairWithRestrictionOutCoderId,
                ModelCoders.kvCoder(
                    mainInputPCollection.getCoderId(), payload.getRestrictionCoderId()));

        String pairWithRestrictionOutId =
            generateUniqueId(
                mainInputPCollectionId + "/PairWithRestriction",
                existingComponents::containsPcollections);
        rval.getComponentsBuilder()
            .putPcollections(
                pairWithRestrictionOutId,
                PCollection.newBuilder()
                    .setCoderId(pairWithRestrictionOutCoderId)
                    .setIsBounded(mainInputPCollection.getIsBounded())
                    .setWindowingStrategyId(mainInputPCollection.getWindowingStrategyId())
                    .setUniqueName(
                        generateUniquePCollectonName(
                            mainInputPCollection.getUniqueName() + "/PairWithRestriction",
                            existingComponents))
                    .build());

        String splitAndSizeOutCoderId =
            generateUniqueId(
                mainInputPCollection.getCoderId() + "/SplitAndSize",
                existingComponents::containsCoders);
        rval.getComponentsBuilder()
            .putCoders(
                splitAndSizeOutCoderId,
                ModelCoders.kvCoder(
                    pairWithRestrictionOutCoderId, getOrAddDoubleCoder(existingComponents, rval)));

        String splitAndSizeOutId =
            generateUniqueId(
                mainInputPCollectionId + "/SplitAndSize", existingComponents::containsPcollections);
        rval.getComponentsBuilder()
            .putPcollections(
                splitAndSizeOutId,
                PCollection.newBuilder()
                    .setCoderId(splitAndSizeOutCoderId)
                    .setIsBounded(mainInputPCollection.getIsBounded())
                    .setWindowingStrategyId(mainInputPCollection.getWindowingStrategyId())
                    .setUniqueName(
                        generateUniquePCollectonName(
                            mainInputPCollection.getUniqueName() + "/SplitAndSize",
                            existingComponents))
                    .build());

        String truncateAndSizeCoderId =
            generateUniqueId(
                mainInputPCollection.getCoderId() + "/TruncateAndSize",
                existingComponents::containsCoders);
        rval.getComponentsBuilder()
            .putCoders(
                truncateAndSizeCoderId,
                ModelCoders.kvCoder(
                    splitAndSizeOutCoderId, getOrAddDoubleCoder(existingComponents, rval)));
        String truncateAndSizeOutId =
            generateUniqueId(
                mainInputPCollectionId + "/TruncateAndSize",
                existingComponents::containsPcollections);

        rval.getComponentsBuilder()
            .putPcollections(
                truncateAndSizeOutId,
                PCollection.newBuilder()
                    .setCoderId(truncateAndSizeCoderId)
                    .setIsBounded(mainInputPCollection.getIsBounded())
                    .setWindowingStrategyId(mainInputPCollection.getWindowingStrategyId())
                    .setUniqueName(
                        generateUniquePCollectonName(
                            mainInputPCollection.getUniqueName() + "/TruncateAndSize",
                            existingComponents))
                    .build());

        String pairWithRestrictionId =
            generateUniqueId(
                transformId + "/PairWithRestriction", existingComponents::containsTransforms);
        {
          PTransform.Builder pairWithRestriction = PTransform.newBuilder();
          pairWithRestriction.putAllInputs(splittableParDo.getInputsMap());
          pairWithRestriction.putOutputs("out", pairWithRestrictionOutId);
          pairWithRestriction.setUniqueName(
              generateUniquePCollectonName(
                  splittableParDo.getUniqueName() + "/PairWithRestriction", existingComponents));
          pairWithRestriction.setSpec(
              FunctionSpec.newBuilder()
                  .setUrn(PTransformTranslation.SPLITTABLE_PAIR_WITH_RESTRICTION_URN)
                  .setPayload(splittableParDo.getSpec().getPayload()));
          pairWithRestriction.setEnvironmentId(splittableParDo.getEnvironmentId());
          rval.getComponentsBuilder()
              .putTransforms(pairWithRestrictionId, pairWithRestriction.build());
        }

        String splitAndSizeId =
            generateUniqueId(transformId + "/SplitAndSize", existingComponents::containsTransforms);
        {
          PTransform.Builder splitAndSize = PTransform.newBuilder();
          splitAndSize.putInputs(mainInputName, pairWithRestrictionOutId);
          splitAndSize.putAllInputs(sideInputs);
          splitAndSize.putOutputs("out", splitAndSizeOutId);
          splitAndSize.setUniqueName(
              generateUniquePCollectonName(
                  splittableParDo.getUniqueName() + "/SplitAndSize", existingComponents));
          splitAndSize.setSpec(
              FunctionSpec.newBuilder()
                  .setUrn(PTransformTranslation.SPLITTABLE_SPLIT_AND_SIZE_RESTRICTIONS_URN)
                  .setPayload(splittableParDo.getSpec().getPayload()));
          splitAndSize.setEnvironmentId(splittableParDo.getEnvironmentId());
          rval.getComponentsBuilder().putTransforms(splitAndSizeId, splitAndSize.build());
        }

        String truncateAndSizeId =
            generateUniqueId(
                transformId + "/TruncateAndSize", existingComponents::containsTransforms);
        {
          PTransform.Builder truncateAndSize = PTransform.newBuilder();
          truncateAndSize.putInputs(mainInputName, splitAndSizeOutId);
          truncateAndSize.putAllInputs(sideInputs);
          truncateAndSize.putOutputs("out", truncateAndSizeOutId);
          truncateAndSize.setUniqueName(
              generateUniquePCollectonName(
                  splittableParDo.getUniqueName() + "/TruncateAndSize", existingComponents));
          truncateAndSize.setSpec(
              FunctionSpec.newBuilder()
                  .setUrn(PTransformTranslation.SPLITTABLE_TRUNCATE_SIZED_RESTRICTION_URN)
                  .setPayload(splittableParDo.getSpec().getPayload()));
          truncateAndSize.setEnvironmentId(splittableParDo.getEnvironmentId());
          rval.getComponentsBuilder().putTransforms(truncateAndSizeId, truncateAndSize.build());
        }

        String processSizedElementsAndRestrictionsId =
            generateUniqueId(
                transformId + "/ProcessSizedElementsAndRestrictions",
                existingComponents::containsTransforms);
        {
          PTransform.Builder processSizedElementsAndRestrictions = PTransform.newBuilder();
          processSizedElementsAndRestrictions.putInputs(mainInputName, truncateAndSizeOutId);
          processSizedElementsAndRestrictions.putAllInputs(sideInputs);
          processSizedElementsAndRestrictions.putAllOutputs(splittableParDo.getOutputsMap());
          processSizedElementsAndRestrictions.setUniqueName(
              generateUniquePCollectonName(
                  splittableParDo.getUniqueName() + "/ProcessSizedElementsAndRestrictions",
                  existingComponents));
          processSizedElementsAndRestrictions.setSpec(
              FunctionSpec.newBuilder()
                  .setUrn(
                      PTransformTranslation.SPLITTABLE_PROCESS_SIZED_ELEMENTS_AND_RESTRICTIONS_URN)
                  .setPayload(splittableParDo.getSpec().getPayload()));
          processSizedElementsAndRestrictions.setEnvironmentId(splittableParDo.getEnvironmentId());
          rval.getComponentsBuilder()
              .putTransforms(
                  processSizedElementsAndRestrictionsId,
                  processSizedElementsAndRestrictions.build());
        }

        PTransform.Builder newCompositeRoot =
            splittableParDo
                .toBuilder()
                // Clear the original splittable ParDo spec and add all the new transforms as
                // children.
                .clearSpec()
                .addAllSubtransforms(
                    Arrays.asList(
                        pairWithRestrictionId,
                        splitAndSizeId,
                        truncateAndSizeId,
                        processSizedElementsAndRestrictionsId));
        rval.setPtransform(newCompositeRoot);

        return rval.build();
      } catch (IOException e) {
        throw new RuntimeException("Unable to perform expansion for transform " + transformId, e);
      }
    }
  }
}
