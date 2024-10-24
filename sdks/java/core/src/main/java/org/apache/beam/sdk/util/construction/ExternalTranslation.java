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
package org.apache.beam.sdk.util.construction;

import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkArgument;
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkState;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.runners.AppliedPTransform;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.checkerframework.checker.nullness.qual.Nullable;

/** Translating External transforms to proto. */
@SuppressWarnings({
  "rawtypes" // TODO(https://github.com/apache/beam/issues/20447)
})
public class ExternalTranslation {
  public static final String EXTERNAL_TRANSFORM_URN = "beam:transform:external:v1";

  /** Translator for ExpandableTransform. */
  public static class ExternalTranslator
      implements PTransformTranslation.TransformTranslator<External.ExpandableTransform<?, ?>> {
    public static PTransformTranslation.TransformTranslator create() {
      return new ExternalTranslator();
    }

    @Override
    public @Nullable String getUrn(External.ExpandableTransform transform) {
      return EXTERNAL_TRANSFORM_URN;
    }

    @Override
    public boolean canTranslate(PTransform<?, ?> pTransform) {
      return pTransform instanceof External.ExpandableTransform;
    }

    @Override
    public RunnerApi.PTransform translate(
        AppliedPTransform<?, ?, ?> appliedPTransform,
        List<AppliedPTransform<?, ?, ?>> subtransforms,
        SdkComponents components)
        throws IOException {
      checkArgument(
          canTranslate(appliedPTransform.getTransform()), "can only translate ExpandableTransform");

      External.ExpandableTransform expandableTransform =
          (External.ExpandableTransform) appliedPTransform.getTransform();
      String nameSpace = expandableTransform.getNamespace();
      String impulsePrefix = expandableTransform.getImpulsePrefix();
      RunnerApi.PTransform expandedTransform = expandableTransform.getExpandedTransform();
      RunnerApi.Components expandedComponents = expandableTransform.getExpandedComponents();
      List<String> expandedRequirements = expandableTransform.getExpandedRequirements();

      for (String requirement : expandedRequirements) {
        components.addRequirement(requirement);
      }

      Map<PCollection, String> externalPCollectionIdMap =
          expandableTransform.getExternalPCollectionIdMap();
      Map<Coder, String> externalCoderIdMap = expandableTransform.getExternalCoderIdMap();

      ImmutableMap.Builder<String, String> pColRenameMapBuilder = ImmutableMap.builder();
      for (Map.Entry<PCollection, String> entry : externalPCollectionIdMap.entrySet()) {
        pColRenameMapBuilder.put(entry.getValue(), components.registerPCollection(entry.getKey()));
      }
      ImmutableMap<String, String> pColRenameMap = pColRenameMapBuilder.build();

      ImmutableMap.Builder<String, String> coderRenameMapBuilder = ImmutableMap.builder();
      for (Map.Entry<Coder, String> entry : externalCoderIdMap.entrySet()) {
        coderRenameMapBuilder.put(entry.getValue(), components.registerCoder(entry.getKey()));
      }
      ImmutableMap<String, String> coderRenameMap = coderRenameMapBuilder.build();

      RunnerApi.Components.Builder mergingComponentsBuilder = RunnerApi.Components.newBuilder();
      for (Map.Entry<String, RunnerApi.Coder> entry :
          expandedComponents.getCodersMap().entrySet()) {
        if (entry.getKey().startsWith(nameSpace)) {
          mergingComponentsBuilder.putCoders(entry.getKey(), entry.getValue());
        }
      }
      for (Map.Entry<String, RunnerApi.WindowingStrategy> entry :
          expandedComponents.getWindowingStrategiesMap().entrySet()) {
        if (entry.getKey().startsWith(nameSpace)) {
          mergingComponentsBuilder.putWindowingStrategies(entry.getKey(), entry.getValue());
        }
      }
      for (Map.Entry<String, RunnerApi.Environment> entry :
          expandedComponents.getEnvironmentsMap().entrySet()) {
        if (entry.getKey().startsWith(nameSpace)) {
          mergingComponentsBuilder.putEnvironments(entry.getKey(), entry.getValue());
        }
      }
      for (Map.Entry<String, RunnerApi.PCollection> entry :
          expandedComponents.getPcollectionsMap().entrySet()) {
        if (entry.getKey().startsWith(nameSpace)) {
          String coderId = entry.getValue().getCoderId();
          mergingComponentsBuilder.putPcollections(
              entry.getKey(),
              entry
                  .getValue()
                  .toBuilder()
                  .setCoderId(
                      Preconditions.checkNotNull(coderRenameMap.getOrDefault(coderId, coderId)))
                  .build());
        }
      }
      for (Map.Entry<String, RunnerApi.PTransform> entry :
          expandedComponents.getTransformsMap().entrySet()) {
        // ignore dummy Impulses we added for fake inputs
        if (entry.getKey().startsWith(impulsePrefix)) {
          continue;
        }
        checkState(entry.getKey().startsWith(nameSpace), "unknown transform found");
        RunnerApi.PTransform proto = entry.getValue();
        RunnerApi.PTransform.Builder transformBuilder = RunnerApi.PTransform.newBuilder();
        transformBuilder
            .setUniqueName(proto.getUniqueName())
            .setSpec(proto.getSpec())
            .setEnvironmentId(proto.getEnvironmentId())
            .addAllSubtransforms(proto.getSubtransformsList());
        for (Map.Entry<String, String> inputEntry : proto.getInputsMap().entrySet()) {
          transformBuilder.putInputs(
              inputEntry.getKey(),
              Preconditions.checkNotNull(
                  pColRenameMap.getOrDefault(inputEntry.getValue(), inputEntry.getValue())));
        }
        for (Map.Entry<String, String> outputEntry : proto.getOutputsMap().entrySet()) {
          transformBuilder.putOutputs(
              outputEntry.getKey(),
              Preconditions.checkNotNull(
                  pColRenameMap.getOrDefault(outputEntry.getValue(), outputEntry.getValue())));
        }
        mergingComponentsBuilder.putTransforms(entry.getKey(), transformBuilder.build());
      }

      RunnerApi.PTransform.Builder rootTransformBuilder = RunnerApi.PTransform.newBuilder();
      rootTransformBuilder
          .setUniqueName(expandedTransform.getUniqueName())
          .setSpec(expandedTransform.getSpec())
          .addAllSubtransforms(expandedTransform.getSubtransformsList())
          .setEnvironmentId(expandedTransform.getEnvironmentId())
          .putAllInputs(expandedTransform.getInputsMap());
      for (Map.Entry<String, String> outputEntry : expandedTransform.getOutputsMap().entrySet()) {
        rootTransformBuilder.putOutputs(
            outputEntry.getKey(),
            Preconditions.checkNotNull(
                pColRenameMap.getOrDefault(outputEntry.getValue(), outputEntry.getValue())));
      }
      components.mergeFrom(mergingComponentsBuilder.build(), null);

      return rootTransformBuilder.build();
    }
  }
}
