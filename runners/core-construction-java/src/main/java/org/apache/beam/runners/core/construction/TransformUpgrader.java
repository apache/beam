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

import java.io.IOException;
import java.net.ServerSocket;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.ServiceLoader;
import java.util.UUID;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import org.apache.beam.model.expansion.v1.ExpansionApi;
import org.apache.beam.model.pipeline.v1.Endpoints;
import org.apache.beam.model.pipeline.v1.ExternalTransforms;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.model.pipeline.v1.SchemaApi;
import org.apache.beam.runners.core.construction.PTransformTranslation.TransformPayloadTranslator;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transformservice.launcher.TransformServiceLauncher;
import org.apache.beam.sdk.values.PInput;
import org.apache.beam.sdk.values.POutput;
import org.apache.beam.vendor.grpc.v1p54p0.com.google.protobuf.ByteString;
import org.apache.beam.vendor.grpc.v1p54p0.io.grpc.ManagedChannelBuilder;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Strings;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * A utility class that allows upgrading transforms of a given pipeline using the Beam Transform
 * Service.
 */
public class TransformUpgrader implements AutoCloseable {
  private static final String UPGRADE_NAMESPACE = "transform:upgrade:";

  private ExpansionServiceClientFactory clientFactory;

  private TransformUpgrader() {
    // Creating a default 'ExpansionServiceClientFactory' instance per 'TransformUpgrader' instance
    // so that each instance can maintain a set of live channels and close them independently.
    clientFactory =
        DefaultExpansionServiceClientFactory.create(
            endPoint -> ManagedChannelBuilder.forTarget(endPoint.getUrl()).usePlaintext().build());
  }

  private TransformUpgrader(ExpansionServiceClientFactory clientFactory) {
    this.clientFactory = clientFactory;
  }

  public static TransformUpgrader of() {
    return new TransformUpgrader();
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
      throws IOException, TimeoutException {
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

    if (options.getTransformServiceAddress() != null) {
      serviceAddress = options.getTransformServiceAddress();
    } else if (options.getTransformServiceBeamVersion() != null) {
      String projectName = UUID.randomUUID().toString();
      int port = findAvailablePort();
      service = TransformServiceLauncher.forProject(projectName, port, null);
      service.setBeamVersion(options.getTransformServiceBeamVersion());

      // Starting the transform service.
      service.start();
      service.waitTillUp(-1);
      serviceAddress = "localhost:" + Integer.toString(port);
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

    if (service != null) {
      service.shutdown();
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
          throws IOException {
    RunnerApi.PTransform transformToUpgrade =
        runnerAPIpipeline.getComponents().getTransformsMap().get(transformId);
    if (transformToUpgrade == null) {
      throw new IllegalArgumentException("Could not find a transform with the ID " + transformId);
    }

    ByteString configRowBytes =
        transformToUpgrade.getAnnotationsOrThrow(PTransformTranslation.CONFIG_ROW_KEY);
    ByteString configRowSchemaBytes =
        transformToUpgrade.getAnnotationsOrThrow(PTransformTranslation.CONFIG_ROW_SCHEMA_KEY);
    SchemaApi.Schema configRowSchemaProto =
        SchemaApi.Schema.parseFrom(configRowSchemaBytes.toByteArray());

    ExternalTransforms.ExternalConfigurationPayload payload =
        ExternalTransforms.ExternalConfigurationPayload.newBuilder()
            .setSchema(configRowSchemaProto)
            .setPayload(configRowBytes)
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
            .addAllRequirements(runnerAPIpipeline.getRequirementsList())
            .build();

    ExpansionApi.ExpansionResponse response =
        clientFactory.getExpansionServiceClient(transformServiceEndpoint).expand(request);

    if (!Strings.isNullOrEmpty(response.getError())) {
      throw new RuntimeException(String.format("expansion service error: %s", response.getError()));
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

    // We record transforms that consume outputs of the old transform and update them to consume
    // outputs of the new (upgraded) transform.
    Collection<String> oldOutputs = transformToUpgrade.getOutputsMap().values();
    Map<String, String> inputReplacements = new HashMap<>();
    if (transformToUpgrade.getOutputsMap().size() == 1) {
      inputReplacements.put(
          oldOutputs.iterator().next(),
          expandedTransform.getOutputsMap().values().iterator().next());
    } else {
      for (Map.Entry<String, String> entry : transformToUpgrade.getOutputsMap().entrySet()) {
        if (expandedTransform.getOutputsMap().keySet().contains(entry.getKey())) {
          throw new IllegalArgumentException(
              "Original transform did not have an output with tag "
                  + entry.getKey()
                  + " but upgraded transform did.");
        }
        String newOutput = expandedTransform.getOutputsMap().get(entry.getKey());
        if (newOutput == null) {
          throw new IllegalArgumentException(
              "Could not find an output with tag "
                  + entry.getKey()
                  + " for the transform "
                  + expandedTransform);
        }
        inputReplacements.put(entry.getValue(), newOutput);
      }
    }

    // The list of obsolete (overridden) transforms that should be removed from the pipeline
    // produced by this method.
    List<String> transformsToRemove = new ArrayList<>();
    recursivelyFindSubTransforms(
        transformId, runnerAPIpipeline.getComponents(), transformsToRemove);

    Map<String, RunnerApi.PTransform> updatedExpandedTransformMap =
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
                      RunnerApi.PTransform.Builder transformBuilder = entry.getValue().toBuilder();
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
                      return transformBuilder.build();
                    }));

    newComponentsBuilder.clearTransforms();
    newComponentsBuilder.putAllTransforms(updatedExpandedTransformMap);
    newComponentsBuilder.putTransforms(transformId, expandedTransform);

    RunnerApi.Pipeline.Builder newRunnerAPIPipelineBuilder = runnerAPIpipeline.toBuilder();
    newRunnerAPIPipelineBuilder.clearComponents();
    newRunnerAPIPipelineBuilder.setComponents(newComponentsBuilder.build());

    newRunnerAPIPipelineBuilder.addAllRequirements(expandedRequirements);

    return newRunnerAPIPipelineBuilder.build();
  }

  private static void recursivelyFindSubTransforms(
      String transformId, RunnerApi.Components components, List<String> results) {
    results.add(transformId);
    RunnerApi.PTransform transform = components.getTransformsMap().get(transformId);
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

  /**
   * A utility to find the registered URN for a given transform.
   *
   * <p>This URN can be used to upgrade this transform to a new Beam version without upgrading the
   * rest of the pipeline. Please see <a
   * href="https://beam.apache.org/documentation/programming-guide/#transform-service">Beam
   * Transform Service documentation</a> for more details.
   *
   * <p>For this lookup to work, the a {@link TransformPayloadTranslatorRegistrar} for the transform
   * has to be available in the classpath.
   *
   * @param transform transform to lookup.
   * @return a URN if discovered. Returns {@code null} otherwise.
   */
  @SuppressWarnings({
    "rawtypes",
    "EqualsIncompatibleType",
  })
  public static @Nullable String findUpgradeURN(PTransform transform) {
    for (TransformPayloadTranslatorRegistrar registrar :
        ServiceLoader.load(TransformPayloadTranslatorRegistrar.class)) {

      for (Entry<
              ? extends Class<? extends org.apache.beam.sdk.transforms.PTransform>,
              ? extends TransformPayloadTranslator>
          entry : registrar.getTransformPayloadTranslators().entrySet()) {
        if (entry.getKey().equals(transform.getClass())) {
          return entry.getValue().getUrn();
        }
      }
    }

    return null;
  }
}
