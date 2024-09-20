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

import static org.apache.beam.model.pipeline.v1.ExternalTransforms.ExpansionMethods.Enum.SCHEMA_TRANSFORM;

import com.fasterxml.jackson.core.Version;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InvalidClassException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.ServerSocket;
import java.util.ArrayList;
import java.util.Arrays;
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
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.StreamingOptions;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transformservice.launcher.TransformServiceLauncher;
import org.apache.beam.sdk.util.ReleaseInfo;
import org.apache.beam.sdk.util.construction.PTransformTranslation.TransformPayloadTranslator;
import org.apache.beam.sdk.values.PInput;
import org.apache.beam.sdk.values.POutput;
import org.apache.beam.vendor.grpc.v1p60p1.com.google.protobuf.ByteString;
import org.apache.beam.vendor.grpc.v1p60p1.com.google.protobuf.InvalidProtocolBufferException;
import org.apache.beam.vendor.grpc.v1p60p1.io.grpc.ManagedChannelBuilder;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Splitter;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Strings;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A utility class that allows upgrading transforms of a given pipeline using the Beam Transform
 * Service.
 */
public class TransformUpgrader implements AutoCloseable {

  private static final Logger LOG = LoggerFactory.getLogger(TransformUpgrader.class);
  private static final String UPGRADE_NAMESPACE = "transform:upgrade:";

  @VisibleForTesting static final String UPGRADE_KEY = "upgraded_to_version";

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
      RunnerApi.Pipeline pipeline, List<String> urnsToOverride, PipelineOptions options)
      throws IOException, TimeoutException {
    List<String> transformsToOverride =
        pipeline.getComponents().getTransformsMap().entrySet().stream()
            .filter(
                entry -> {
                  String urn = entry.getValue().getSpec().getUrn();
                  if (urn != null && urnsToOverride.contains(urn)) {
                    return true;
                  }

                  // Also check if the URN is a schema-transform ID.
                  if (urn.equals(BeamUrns.getUrn(SCHEMA_TRANSFORM))) {
                    try {
                      ExternalTransforms.SchemaTransformPayload schemaTransformPayload =
                          ExternalTransforms.SchemaTransformPayload.parseFrom(
                              entry.getValue().getSpec().getPayload());
                      String schemaTransformId = schemaTransformPayload.getIdentifier();
                      if (urnsToOverride.contains(schemaTransformId)) {
                        return true;
                      }
                    } catch (InvalidProtocolBufferException e) {
                      throw new RuntimeException(e);
                    }
                  }

                  return false;
                })
            .map(
                entry -> {
                  return entry.getKey();
                })
            .collect(Collectors.toList());

    if (!urnsToOverride.isEmpty() && transformsToOverride.isEmpty()) {
      throw new IllegalArgumentException(
          "A list of URNs for overriding transforms was provided but the pipeline did not contain "
              + "any matching transforms. Either make sure to include at least one matching "
              + "transform in the pipeline or avoid setting the 'transformsToOverride' "
              + "PipelineOption. Provided list of URNs: "
              + urnsToOverride);
    }

    String serviceAddress;
    TransformServiceLauncher service = null;

    ExternalTranslationOptions externalTranslationOptions =
        options.as(ExternalTranslationOptions.class);
    if (externalTranslationOptions.getTransformServiceAddress() != null) {
      serviceAddress = externalTranslationOptions.getTransformServiceAddress();
    } else if (externalTranslationOptions.getTransformServiceBeamVersion() != null) {
      String projectName = UUID.randomUUID().toString();
      int port = findAvailablePort();
      service = TransformServiceLauncher.forProject(projectName, port, null);
      service.setBeamVersion(externalTranslationOptions.getTransformServiceBeamVersion());

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
          updateTransformViaTransformService(
              pipeline, transformId, expansionServiceEndpoint, options);
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
          Endpoints.ApiServiceDescriptor transformServiceEndpoint,
          PipelineOptions options)
          throws IOException {
    RunnerApi.PTransform transformToUpgrade =
        runnerAPIpipeline.getComponents().getTransformsMap().get(transformId);
    if (transformToUpgrade == null) {
      throw new IllegalArgumentException("Could not find a transform with the ID " + transformId);
    }

    byte[] payloadBytes = null;

    if (!transformToUpgrade.getSpec().getUrn().equals(BeamUrns.getUrn(SCHEMA_TRANSFORM))) {
      ByteString configRowBytes =
          transformToUpgrade.getAnnotationsOrThrow(
              BeamUrns.getConstant(ExternalTransforms.Annotations.Enum.CONFIG_ROW_KEY));
      ByteString configRowSchemaBytes =
          transformToUpgrade.getAnnotationsOrThrow(
              BeamUrns.getConstant(ExternalTransforms.Annotations.Enum.CONFIG_ROW_SCHEMA_KEY));
      SchemaApi.Schema configRowSchemaProto =
          SchemaApi.Schema.parseFrom(configRowSchemaBytes.toByteArray());
      payloadBytes =
          ExternalTransforms.ExternalConfigurationPayload.newBuilder()
              .setSchema(configRowSchemaProto)
              .setPayload(configRowBytes)
              .build()
              .toByteArray();
    } else {
      payloadBytes = transformToUpgrade.getSpec().getPayload().toByteArray();
    }

    RunnerApi.PTransform.Builder ptransformBuilder =
        RunnerApi.PTransform.newBuilder()
            .setUniqueName(transformToUpgrade.getUniqueName() + "_external")
            .setSpec(
                RunnerApi.FunctionSpec.newBuilder()
                    .setUrn(transformToUpgrade.getSpec().getUrn())
                    .setPayload(ByteString.copyFrom(payloadBytes))
                    .build());

    for (Map.Entry<String, String> entry : transformToUpgrade.getInputsMap().entrySet()) {
      ptransformBuilder.putInputs(entry.getKey(), entry.getValue());
    }
    for (Map.Entry<String, String> entry : transformToUpgrade.getOutputsMap().entrySet()) {
      ptransformBuilder.putOutputs(entry.getKey(), entry.getValue());
    }

    ExpansionApi.ExpansionRequest.Builder requestBuilder =
        ExpansionApi.ExpansionRequest.newBuilder();

    // Creating a clone here so that we can set properties without modifying the original
    // PipelineOptions object.
    PipelineOptions optionsClone =
        PipelineOptionsTranslation.fromProto(PipelineOptionsTranslation.toProto(options));
    String updateCompatibilityVersion =
        optionsClone.as(StreamingOptions.class).getUpdateCompatibilityVersion();
    if (updateCompatibilityVersion == null || updateCompatibilityVersion.isEmpty()) {
      // Setting the option 'updateCompatibilityVersion' to the current SDK version so that the
      // TransformService uses a compatible schema.
      optionsClone
          .as(StreamingOptions.class)
          .setUpdateCompatibilityVersion(ReleaseInfo.getReleaseInfo().getSdkVersion());
    }
    ExpansionApi.ExpansionRequest request =
        requestBuilder
            .setComponents(runnerAPIpipeline.getComponents())
            .setTransform(ptransformBuilder.build())
            .setNamespace(UPGRADE_NAMESPACE)
            .setPipelineOptions(PipelineOptionsTranslation.toProto(optionsClone))
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

    // Adds an annotation that denotes the Beam version the transform was upgraded to.
    RunnerApi.PTransform.Builder expandedTransformBuilder = expandedTransform.toBuilder();
    String transformServiceVersion =
        options.as(ExternalTranslationOptions.class).getTransformServiceBeamVersion();
    if (transformServiceVersion == null || transformServiceVersion.isEmpty()) {
      transformServiceVersion = "unknown";
    }
    expandedTransformBuilder.putAnnotations(
        UPGRADE_KEY, ByteString.copyFromUtf8(transformServiceVersion));
    expandedTransform = expandedTransformBuilder.build();

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
        if (!expandedTransform.getOutputsMap().keySet().contains(entry.getKey())) {
          throw new IllegalArgumentException(
              "Original transform had an output with tag "
                  + entry.getKey()
                  + " but upgraded transform did not.");
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

  /**
   * A utility method that converts an arbitrary serializable object into a byte array.
   *
   * @param object an instance of type {@code Serializable}
   * @return {@code object} converted into a byte array.
   */
  public static byte[] toByteArray(Object object) {
    try (ByteArrayOutputStream bos = new ByteArrayOutputStream();
        ObjectOutputStream out = new ObjectOutputStream(bos)) {
      out.writeObject(object);
      return bos.toByteArray();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * A utility method that converts a byte array obtained by invoking {@link #toByteArray(Object)}
   * back to a Java object.
   *
   * @param bytes a {@code byte} array generated by invoking the {@link #toByteArray(Object)}
   *     method.
   * @return re-generated object.
   */
  public static Object fromByteArray(byte[] bytes) throws InvalidClassException {
    try (ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
        ObjectInputStream in = new ObjectInputStream(bis)) {
      return in.readObject();
    } catch (InvalidClassException e) {
      LOG.info(
          "An object cannot be re-generated from the provided byte array. Caller may use the "
              + "default value for the parameter when upgrading. Underlying error: "
              + e);
      throw e;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @SuppressWarnings({
    "nullness" // TODO(https://github.com/apache/beam/issues/20497)
  })
  private static Version getVersionFromStr(String version) {
    String[] versionParts = Splitter.onPattern("\\.").splitToList(version).toArray(new String[0]);
    if (versionParts.length < 2) {
      throw new IllegalArgumentException(
          "Expected the version string to start with `<major>.<minor>` "
              + "but received "
              + version);
    }

    // Concatenating patch and suffix to determine the correct patch version.
    String patchAndSuffix =
        versionParts.length == 2
            ? ""
            : String.join(".", Arrays.copyOfRange(versionParts, 2, versionParts.length));
    StringBuilder patchVersionBuilder = new StringBuilder();
    for (int i = 0; i < patchAndSuffix.length(); i++) {
      if (Character.isDigit(patchAndSuffix.charAt(i))) {
        patchVersionBuilder.append(patchAndSuffix.charAt(i));
      } else {
        break;
      }
    }
    String patchVersion = patchVersionBuilder.toString();
    if (patchVersion.isEmpty()) {
      patchVersion = "0";
    }
    return new Version(
        Integer.parseInt(versionParts[0]),
        Integer.parseInt(versionParts[1]),
        Integer.parseInt(patchVersion),
        null,
        null,
        null);
  }

  /**
   * Compares two Beam versions. Expects the versions to be in the format
   * <major>.<minor>.<patch><suffix>. <patch> and <suffix> are optional. Version numbers should be
   * integers. When comparing suffix will be ignored.
   *
   * @param firstVersion first version to compare.
   * @param secondVersion second version to compare.
   * @return a negative number of first version is smaller than the second, a positive number if the
   *     first version is larger than the second, 0 if versions are equal.
   */
  public static int compareVersions(String firstVersion, String secondVersion) {
    if (firstVersion.equals(secondVersion)) {
      return 0;
    }
    return getVersionFromStr(firstVersion).compareTo(getVersionFromStr(secondVersion));
  }
}
