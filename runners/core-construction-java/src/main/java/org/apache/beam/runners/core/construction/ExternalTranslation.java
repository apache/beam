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
package org.apache.beam.runners.core.construction;

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkArgument;
import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkState;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.runners.AppliedPTransform;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;
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
      Set<String> namespaces = expandableTransform.getNamespaces();
      String impulsePrefix = expandableTransform.getImpulsePrefix();
      List<RunnerApi.PTransform> expandedTransforms = expandableTransform.getExpandedTransforms();
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
        if (namespaces.stream().anyMatch(entry.getKey()::startsWith)) {
          mergingComponentsBuilder.putCoders(entry.getKey(), entry.getValue());
        }
      }
      for (Map.Entry<String, RunnerApi.WindowingStrategy> entry :
          expandedComponents.getWindowingStrategiesMap().entrySet()) {
        if (namespaces.stream().anyMatch(entry.getKey()::startsWith)) {
          mergingComponentsBuilder.putWindowingStrategies(entry.getKey(), entry.getValue());
        }
      }
      for (Map.Entry<String, RunnerApi.Environment> entry :
          expandedComponents.getEnvironmentsMap().entrySet()) {
        if (namespaces.stream().anyMatch(entry.getKey()::startsWith)) {
          mergingComponentsBuilder.putEnvironments(entry.getKey(), entry.getValue());
        }
      }
      for (Map.Entry<String, RunnerApi.PCollection> entry :
          expandedComponents.getPcollectionsMap().entrySet()) {
        if (namespaces.stream().anyMatch(entry.getKey()::startsWith)) {
          String coderId = entry.getValue().getCoderId();
          mergingComponentsBuilder.putPcollections(
              entry.getKey(),
              entry
                  .getValue()
                  .toBuilder()
                  .setCoderId(coderRenameMap.getOrDefault(coderId, coderId))
                  .build());
        }
      }
      for (Map.Entry<String, RunnerApi.PTransform> entry :
          expandedComponents.getTransformsMap().entrySet()) {
        // ignore dummy Impulses we added for fake inputs
        if (entry.getKey().startsWith(impulsePrefix)) {
          continue;
        }
        checkState(
            namespaces.stream().anyMatch(entry.getKey()::startsWith), "unknown transform found");
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
              pColRenameMap.getOrDefault(inputEntry.getValue(), inputEntry.getValue()));
        }
        for (Map.Entry<String, String> outputEntry : proto.getOutputsMap().entrySet()) {
          transformBuilder.putOutputs(
              outputEntry.getKey(),
              pColRenameMap.getOrDefault(outputEntry.getValue(), outputEntry.getValue()));
        }
        mergingComponentsBuilder.putTransforms(entry.getKey(), transformBuilder.build());
      }

      RunnerApi.PTransform.Builder rootTransformBuilder = RunnerApi.PTransform.newBuilder();
      rootTransformBuilder
          .setUniqueName(External.EXPANDED_TRANSFORM_BASE_NAME + External.getFreshNamespaceIndex())
          .addAllSubtransforms(
              expandedTransforms.stream()
                  .flatMap(t -> t.getSubtransformsList().stream())
                  .collect(Collectors.toList()))
          .putAllInputs(expandedTransforms.get(0).getInputsMap());
      for (Map.Entry<String, String> outputEntry :
          expandedTransforms.get(expandedTransforms.size() - 1).getOutputsMap().entrySet()) {
        rootTransformBuilder.putOutputs(
            outputEntry.getKey(),
            pColRenameMap.getOrDefault(outputEntry.getValue(), outputEntry.getValue()));
      }
      components.mergeFrom(mergingComponentsBuilder.build(), null);

      return rootTransformBuilder.build();
    }
  }
}
