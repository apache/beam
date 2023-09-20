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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.net.ServerSocket;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.beam.model.expansion.v1.ExpansionApi;
import org.apache.beam.model.pipeline.v1.Endpoints;
import org.apache.beam.model.pipeline.v1.ExternalTransforms;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.model.pipeline.v1.RunnerApi.PTransform;
import org.apache.beam.model.pipeline.v1.SchemaApi;
import org.apache.beam.sdk.coders.RowCoder;
import org.apache.beam.sdk.schemas.SchemaTranslation;
import org.apache.beam.sdk.transformservice.launcher.TransformServiceLauncher;
import org.apache.beam.sdk.util.ByteStringOutputStream;
import org.apache.beam.sdk.values.PInput;
import org.apache.beam.sdk.values.POutput;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.grpc.v1p54p0.com.google.protobuf.ByteString;
import org.apache.beam.vendor.grpc.v1p54p0.io.grpc.ManagedChannelBuilder;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Strings;

/**
 * A utility class that allows upgrading transforms of a given pipeline using the Beam Transform
 * Service.
 */
public class TransformUpgrader implements AutoCloseable {
  private static final String UPGRADE_NAMESPACE = "transform:upgrade:";

  private ExpansionServiceClientFactory clientFactory;

  private static final ExpansionServiceClientFactory DEFAULT =
      DefaultExpansionServiceClientFactory.create(
          endPoint -> ManagedChannelBuilder.forTarget(endPoint.getUrl()).usePlaintext().build());

  private TransformUpgrader(ExpansionServiceClientFactory clientFactory) {
    this.clientFactory = clientFactory;
  }

  public static TransformUpgrader of() {
    return new TransformUpgrader(DEFAULT);
  }

  @VisibleForTesting
  static TransformUpgrader of(ExpansionServiceClientFactory clientFactory) {
    return new TransformUpgrader(clientFactory);
  }

  /**
   * Upgrade identified transforms in a given pipeline using the Transform Service.
   *
   * @param pipeline the pipeline proto.
   * @param urnsToOverride URNs of the transforms to be overridden.
   * @param options options for determining the transform service to use.
   * @return pipelines with transforms upgraded using the Transform Service.
   * @throws Exception
   */
  public RunnerApi.Pipeline upgradeTransformsViaTransformService(
      RunnerApi.Pipeline pipeline, List<String> urnsToOverride, ExternalTranslationOptions options)
      throws Exception {
    List<String> transformsToOverride =
        pipeline.getComponents().getTransformsMap().entrySet().stream()
            .filter(
                entry -> {
                  String urn = entry.getValue().getSpec().getUrn();
                  if (urn != null && urnsToOverride.contains(urn)) {
                    return true;
                  }
                  return false;
                })
            .map(
                entry -> {
                  return entry.getKey();
                })
            .collect(Collectors.toList());

    String serviceAddress;
    TransformServiceLauncher service = null;
    try {
      if (options.getTransformServiceAddress() != null) {
        serviceAddress = options.getTransformServiceAddress();
      } else if (options.getTransformServiceBeamVersion() != null) {
        String projectName = UUID.randomUUID().toString();
        int port = findAvailablePort();
        service = TransformServiceLauncher.forProject(projectName, port);
        service.setBeamVersion(options.getTransformServiceBeamVersion());

        // Starting the transform service.
        service.start();
        service.waitTillUp(40);
        serviceAddress = "localhost:" + Integer.toString(port);
        System.out.println("Done waiting ...");
      } else {
        throw new IllegalArgumentException(
            "Either option TransformServiceAddress or option TransformServiceBeamVersion should be "
                + "provided to override a transform using the transform service");
      }

      Endpoints.ApiServiceDescriptor expansionServiceEndpoint =
          Endpoints.ApiServiceDescriptor.newBuilder().setUrl(serviceAddress).build();

      for (String transformId : transformsToOverride) {
        pipeline =
            updateTransformViaTransformService(pipeline, transformId, expansionServiceEndpoint);
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }

    if (service != null) {
      try {
        service.shutdown();
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }

    return pipeline;
  }

  private <
          InputT extends PInput,
          OutputT extends POutput,
          TransformT extends org.apache.beam.sdk.transforms.PTransform<InputT, OutputT>>
      RunnerApi.Pipeline updateTransformViaTransformService(
          RunnerApi.Pipeline runnerAPIpipeline,
          String transformId,
          Endpoints.ApiServiceDescriptor transformServiceEndpoint)
          throws Exception {
    PTransform transformToUpgrade =
        runnerAPIpipeline.getComponents().getTransformsMap().get(transformId);
    if (transformToUpgrade == null) {
      throw new Exception("Could not find a transform with the ID " + transformId);
    }

    ByteString configRowBytes =
        transformToUpgrade.getAnnotationsOrThrow(PTransformTranslation.CONFIG_ROW_KEY);
    ByteString configRowSchemaBytes =
        transformToUpgrade.getAnnotationsOrThrow(PTransformTranslation.CONFIG_ROW_SCHEMA_KEY);
    SchemaApi.Schema configRowSchemaProto;
    try {
      configRowSchemaProto =
          (SchemaApi.Schema)
              new ObjectInputStream(new ByteArrayInputStream(configRowSchemaBytes.toByteArray()))
                  .readObject();
    } catch (ClassNotFoundException e) {
      throw new RuntimeException(e);
    }

    Row configRow =
        RowCoder.of(SchemaTranslation.schemaFromProto(configRowSchemaProto))
            .decode(new ByteArrayInputStream(configRowBytes.toByteArray()));
    ByteStringOutputStream outputStream = new ByteStringOutputStream();
    try {
      RowCoder.of(configRow.getSchema()).encode(configRow, outputStream);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    ExternalTransforms.ExternalConfigurationPayload payload =
        ExternalTransforms.ExternalConfigurationPayload.newBuilder()
            .setSchema(configRowSchemaProto)
            .setPayload(outputStream.toByteString())
            .build();

    RunnerApi.PTransform.Builder ptransformBuilder =
        RunnerApi.PTransform.newBuilder()
            .setUniqueName(transformToUpgrade.getUniqueName() + "_external")
            .setSpec(
                RunnerApi.FunctionSpec.newBuilder()
                    .setUrn(transformToUpgrade.getSpec().getUrn())
                    .setPayload(ByteString.copyFrom(payload.toByteArray()))
                    .build());

    for (Map.Entry<String, String> entry : transformToUpgrade.getInputsMap().entrySet()) {
      ptransformBuilder.putInputs(entry.getKey(), entry.getValue());
    }
    for (Map.Entry<String, String> entry : transformToUpgrade.getOutputsMap().entrySet()) {
      ptransformBuilder.putOutputs(entry.getKey(), entry.getValue());
    }

    ExpansionApi.ExpansionRequest.Builder requestBuilder =
        ExpansionApi.ExpansionRequest.newBuilder();
    ExpansionApi.ExpansionRequest request =
        requestBuilder
            .setComponents(runnerAPIpipeline.getComponents())
            .setTransform(ptransformBuilder.build())
            .setNamespace(UPGRADE_NAMESPACE)
            .build();

    ExpansionApi.ExpansionResponse response =
        clientFactory.getExpansionServiceClient(transformServiceEndpoint).expand(request);

    if (!Strings.isNullOrEmpty(response.getError())) {
      throw new IOException(String.format("expansion service error: %s", response.getError()));
    }

    Map<String, RunnerApi.Environment> newEnvironmentsWithDependencies =
        response.getComponents().getEnvironmentsMap().entrySet().stream()
            .filter(
                kv ->
                    !runnerAPIpipeline.getComponents().getEnvironmentsMap().containsKey(kv.getKey())
                        && kv.getValue().getDependenciesCount() != 0)
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

    RunnerApi.Components expandedComponents =
        response
            .getComponents()
            .toBuilder()
            .putAllEnvironments(
                External.ExpandableTransform.resolveArtifacts(
                    newEnvironmentsWithDependencies, transformServiceEndpoint))
            .build();
    RunnerApi.PTransform expandedTransform = response.getTransform();
    List<String> expandedRequirements = response.getRequirementsList();

    RunnerApi.Components.Builder newComponentsBuilder = expandedComponents.toBuilder();

    // Some transforms may refer to already overridden transform as one of their input. We record
    // such occurrences and correct them by referring to the upgraded transform instead.
    Collection<String> oldOutputs = transformToUpgrade.getOutputsMap().values();
    Map<String, String> inputReplacements = new HashMap<>();
    if (transformToUpgrade.getOutputsMap().size() == 1) {
      inputReplacements.put(
          oldOutputs.iterator().next(),
          expandedTransform.getOutputsMap().values().iterator().next());
    } else {
      for (Map.Entry<String, String> entry : transformToUpgrade.getOutputsMap().entrySet()) {
        if (expandedTransform.getOutputsMap().keySet().contains(entry.getKey())) {
          throw new Exception(
              "Original transform did not have an output with tag "
                  + entry.getKey()
                  + " but upgraded transform did.");
        }
        String newOutput = expandedTransform.getOutputsMap().get(entry.getKey());
        if (newOutput == null) {
          throw new Exception(
              "Could not find an output with tag "
                  + entry.getKey()
                  + " for the transform "
                  + expandedTransform);
        }
        inputReplacements.put(entry.getValue(), newOutput);
      }
    }

    String newTransformId = transformId + "_upgraded";

    // The list of obsolete (overridden) transforms that should be removed from the pipeline
    // produced by this method.
    List<String> transformsToRemove = new ArrayList<>();
    recursivelyFindSubTransforms(
        transformId, runnerAPIpipeline.getComponents(), transformsToRemove);

    Map<String, PTransform> updatedExpandedTransformMap =
        expandedComponents.getTransformsMap().entrySet().stream()
            .filter(
                entry -> {
                  // Do not include already overridden transforms.
                  return !transformsToRemove.contains(entry.getKey());
                })
            .collect(
                Collectors.toMap(
                    entry -> entry.getKey(),
                    entry -> {
                      // Fix inputs
                      Map<String, String> inputsMap = entry.getValue().getInputsMap();
                      PTransform.Builder transformBuilder = entry.getValue().toBuilder();
                      if (!Collections.disjoint(inputsMap.values(), inputReplacements.keySet())) {
                        Map<String, String> updatedInputsMap = new HashMap<>();
                        for (Map.Entry<String, String> inputEntry : inputsMap.entrySet()) {
                          String updaterValue =
                              inputReplacements.containsKey(inputEntry.getValue())
                                  ? inputReplacements.get(inputEntry.getValue())
                                  : inputEntry.getValue();
                          updatedInputsMap.put(inputEntry.getKey(), updaterValue);
                        }
                        transformBuilder.clearInputs();
                        transformBuilder.putAllInputs(updatedInputsMap);
                      }

                      // Fix sub-transforms
                      if (entry.getValue().getSubtransformsList().contains(transformId)) {
                        List<String> updatedSubTransforms =
                            entry.getValue().getSubtransformsList().stream()
                                .map(
                                    subtransformId -> {
                                      return subtransformId.equals(transformId)
                                          ? newTransformId
                                          : subtransformId;
                                    })
                                .collect(Collectors.toList());
                        transformBuilder.clearSubtransforms();
                        transformBuilder.addAllSubtransforms(updatedSubTransforms);
                      }

                      return transformBuilder.build();
                    }));

    newComponentsBuilder.clearTransforms();
    newComponentsBuilder.putAllTransforms(updatedExpandedTransformMap);
    newComponentsBuilder.putTransforms(newTransformId, expandedTransform);

    // We fix the root in case the overridden transform was one of the roots.
    List<String> rootTransformIds =
        runnerAPIpipeline.getRootTransformIdsList().stream()
            .map(id -> id.equals(transformId) ? newTransformId : id)
            .collect(Collectors.toList());

    RunnerApi.Pipeline.Builder newRunnerAPIPipelineBuilder = runnerAPIpipeline.toBuilder();
    newRunnerAPIPipelineBuilder.clearComponents();
    newRunnerAPIPipelineBuilder.setComponents(newComponentsBuilder.build());

    newRunnerAPIPipelineBuilder.addAllRequirements(expandedRequirements);
    newRunnerAPIPipelineBuilder.clearRootTransformIds();
    newRunnerAPIPipelineBuilder.addAllRootTransformIds(rootTransformIds);

    return newRunnerAPIPipelineBuilder.build();
  }

  private static void recursivelyFindSubTransforms(
      String transformId, RunnerApi.Components components, List<String> results) {
    results.add(transformId);
    PTransform transform = components.getTransformsMap().get(transformId);
    if (transform == null) {
      throw new IllegalArgumentException("Could not find a transform with id " + transformId);
    }
    List<String> subTransforms = transform.getSubtransformsList();
    if (subTransforms != null) {
      for (String subTransformId : subTransforms) {
        recursivelyFindSubTransforms(subTransformId, components, results);
      }
    }
  }

  private static int findAvailablePort() throws IOException {
    ServerSocket s = new ServerSocket(0);
    try {
      return s.getLocalPort();
    } finally {
      s.close();
      try {
        // Some systems don't free the port for future use immediately.
        Thread.sleep(100);
      } catch (InterruptedException exn) {
        // ignore
      }
    }
  }

  @Override
  public void close() throws Exception {
    clientFactory.close();
  }
}
