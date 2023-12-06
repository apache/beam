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

import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkArgument;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import org.apache.beam.model.expansion.v1.ExpansionApi;
import org.apache.beam.model.jobmanagement.v1.ArtifactApi;
import org.apache.beam.model.jobmanagement.v1.ArtifactRetrievalServiceGrpc;
import org.apache.beam.model.pipeline.v1.Endpoints;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.runners.AppliedPTransform;
import org.apache.beam.sdk.transforms.Impulse;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.resourcehints.ResourceHints;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.PInput;
import org.apache.beam.sdk.values.POutput;
import org.apache.beam.sdk.values.PValue;
import org.apache.beam.sdk.values.PValues;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.vendor.grpc.v1p54p0.com.google.protobuf.ByteString;
import org.apache.beam.vendor.grpc.v1p54p0.io.grpc.ManagedChannel;
import org.apache.beam.vendor.grpc.v1p54p0.io.grpc.ManagedChannelBuilder;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Strings;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Iterables;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * Cross-language external transform.
 *
 * <p>{@link External} provides a cross-language transform via expansion services in foreign SDKs.
 * In order to use {@link External} transform, a user should know 1) URN of the target transform 2)
 * bytes encoding schema for configuration parameters 3) connection endpoint of the expansion
 * service. Note that this is a low-level API and mainly for internal use. A user may want to use
 * high-level wrapper classes rather than this one.
 */
@SuppressWarnings({
  "rawtypes", // TODO(https://github.com/apache/beam/issues/20447)
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
public class External {
  private static final String EXPANDED_TRANSFORM_BASE_NAME = "external";
  private static final String IMPULSE_PREFIX = "IMPULSE";
  private static AtomicInteger namespaceCounter = new AtomicInteger(0);

  private static final ExpansionServiceClientFactory DEFAULT =
      DefaultExpansionServiceClientFactory.create(
          endPoint -> ManagedChannelBuilder.forTarget(endPoint.getUrl()).usePlaintext().build());

  private static int getFreshNamespaceIndex() {
    return namespaceCounter.getAndIncrement();
  }

  public static <InputT extends PInput, OutputT>
      SingleOutputExpandableTransform<InputT, OutputT> of(
          String urn, byte[] payload, String endpoint) {
    Endpoints.ApiServiceDescriptor apiDesc =
        Endpoints.ApiServiceDescriptor.newBuilder().setUrl(endpoint).build();
    return new SingleOutputExpandableTransform<>(
        urn, payload, apiDesc, DEFAULT, getFreshNamespaceIndex(), ImmutableMap.of());
  }

  @VisibleForTesting
  public static <InputT extends PInput, OutputT>
      SingleOutputExpandableTransform<InputT, OutputT> of(
          String urn,
          byte[] payload,
          String endpoint,
          ExpansionServiceClientFactory clientFactory) {
    Endpoints.ApiServiceDescriptor apiDesc =
        Endpoints.ApiServiceDescriptor.newBuilder().setUrl(endpoint).build();
    return new SingleOutputExpandableTransform<>(
        urn, payload, apiDesc, clientFactory, getFreshNamespaceIndex(), ImmutableMap.of());
  }

  /** Expandable transform for output type of PCollection. */
  public static class SingleOutputExpandableTransform<InputT extends PInput, OutputT>
      extends ExpandableTransform<InputT, PCollection<OutputT>> {
    SingleOutputExpandableTransform(
        String urn,
        byte[] payload,
        Endpoints.ApiServiceDescriptor endpoint,
        ExpansionServiceClientFactory clientFactory,
        Integer namespaceIndex,
        Map<String, Coder<?>> outputCoders) {
      super(urn, payload, endpoint, clientFactory, namespaceIndex, outputCoders);
    }

    @Override
    PCollection<OutputT> toOutputCollection(Map<TupleTag<?>, PCollection> output) {
      checkArgument(output.size() > 0, "output shouldn't be empty.");
      return Iterables.getOnlyElement(output.values());
    }

    public MultiOutputExpandableTransform<InputT> withMultiOutputs() {
      return new MultiOutputExpandableTransform<>(
          getUrn(),
          getPayload(),
          getEndpoint(),
          getClientFactory(),
          getNamespaceIndex(),
          getOutputCoders());
    }

    public SingleOutputExpandableTransform<InputT, OutputT> withOutputCoder(Coder<?> outputCoder) {
      return new SingleOutputExpandableTransform<>(
          getUrn(),
          getPayload(),
          getEndpoint(),
          getClientFactory(),
          getNamespaceIndex(),
          ImmutableMap.of("0", outputCoder));
    }
  }

  /** Expandable transform for output type of PCollectionTuple. */
  public static class MultiOutputExpandableTransform<InputT extends PInput>
      extends ExpandableTransform<InputT, PCollectionTuple> {
    MultiOutputExpandableTransform(
        String urn,
        byte[] payload,
        Endpoints.ApiServiceDescriptor endpoint,
        ExpansionServiceClientFactory clientFactory,
        Integer namespaceIndex,
        Map<String, Coder<?>> outputCoders) {
      super(urn, payload, endpoint, clientFactory, namespaceIndex, outputCoders);
    }

    @Override
    PCollectionTuple toOutputCollection(Map<TupleTag<?>, PCollection> output) {
      checkArgument(output.size() > 0, "output shouldn't be empty.");
      PCollection firstElem = Iterables.getFirst(output.values(), null);
      PCollectionTuple pCollectionTuple = PCollectionTuple.empty(firstElem.getPipeline());
      for (Map.Entry<TupleTag<?>, PCollection> entry : output.entrySet()) {
        pCollectionTuple = pCollectionTuple.and(entry.getKey(), entry.getValue());
      }
      return pCollectionTuple;
    }

    public MultiOutputExpandableTransform<InputT> withOutputCoder(
        Map<String, Coder<?>> outputCoders) {
      return new MultiOutputExpandableTransform<>(
          getUrn(),
          getPayload(),
          getEndpoint(),
          getClientFactory(),
          getNamespaceIndex(),
          outputCoders);
    }
  }

  /** Base Expandable Transform which calls ExpansionService to expand itself. */
  public abstract static class ExpandableTransform<InputT extends PInput, OutputT extends POutput>
      extends PTransform<InputT, OutputT> {
    private final String urn;
    private final byte[] payload;
    private final Endpoints.ApiServiceDescriptor endpoint;
    private final ExpansionServiceClientFactory clientFactory;
    private final Integer namespaceIndex;
    private final Map<String, Coder<?>> outputCoders;

    private transient RunnerApi.@Nullable Components expandedComponents;
    private transient RunnerApi.@Nullable PTransform expandedTransform;
    private transient @Nullable List<String> expandedRequirements;
    private transient @Nullable Map<PCollection, String> externalPCollectionIdMap;
    private transient @Nullable Map<Coder<?>, String> externalCoderIdMap;

    ExpandableTransform(
        String urn,
        byte[] payload,
        Endpoints.ApiServiceDescriptor endpoint,
        ExpansionServiceClientFactory clientFactory,
        Integer namespaceIndex,
        Map<String, Coder<?>> outputCoders) {
      this.urn = urn;
      this.payload = payload;
      this.endpoint = endpoint;
      this.clientFactory = clientFactory;
      this.namespaceIndex = namespaceIndex;
      this.outputCoders = outputCoders;
    }

    @Override
    public OutputT expand(InputT input) {
      Pipeline p = input.getPipeline();
      SdkComponents components = SdkComponents.create(p.getOptions());
      RunnerApi.PTransform.Builder ptransformBuilder =
          RunnerApi.PTransform.newBuilder()
              .setUniqueName(EXPANDED_TRANSFORM_BASE_NAME + namespaceIndex)
              .setSpec(
                  RunnerApi.FunctionSpec.newBuilder()
                      .setUrn(urn)
                      .setPayload(ByteString.copyFrom(payload))
                      .build());
      ImmutableMap.Builder<PCollection, String> externalPCollectionIdMapBuilder =
          ImmutableMap.builder();
      for (Map.Entry<TupleTag<?>, PValue> entry : input.expand().entrySet()) {
        if (entry.getValue() instanceof PCollection<?>) {
          try {
            String id = components.registerPCollection((PCollection) entry.getValue());
            externalPCollectionIdMapBuilder.put((PCollection) entry.getValue(), id);
            ptransformBuilder.putInputs(entry.getKey().getId(), id);
            AppliedPTransform<?, ?, ?> fakeImpulse =
                AppliedPTransform.of(
                    String.format("%s_%s", IMPULSE_PREFIX, entry.getKey().getId()),
                    PValues.expandInput(PBegin.in(p)),
                    ImmutableMap.of(entry.getKey(), (PCollection<?>) entry.getValue()),
                    Impulse.create(),
                    // TODO(https://github.com/apache/beam/issues/18371): Add proper support for
                    // Resource Hints with XLang.
                    ResourceHints.create(),
                    p);
            // using fake Impulses to provide inputs
            components.registerPTransform(fakeImpulse, Collections.emptyList());
          } catch (IOException e) {
            throw new RuntimeException(
                String.format("cannot register component: %s", e.getMessage()));
          }
        }
      }

      ExpansionApi.ExpansionRequest.Builder requestBuilder =
          ExpansionApi.ExpansionRequest.newBuilder();
      requestBuilder.putAllOutputCoderRequests(
          outputCoders.entrySet().stream()
              .collect(
                  Collectors.toMap(
                      Map.Entry::getKey,
                      kv -> {
                        try {
                          return components.registerCoder(kv.getValue());
                        } catch (IOException e) {
                          throw new RuntimeException(e);
                        }
                      })));
      RunnerApi.Components originalComponents = components.toComponents();
      ExpansionApi.ExpansionRequest request =
          requestBuilder
              .setComponents(originalComponents)
              .setTransform(ptransformBuilder.build())
              .setNamespace(getNamespace())
              .build();

      ExpansionApi.ExpansionResponse response =
          clientFactory.getExpansionServiceClient(endpoint).expand(request);

      if (!Strings.isNullOrEmpty(response.getError())) {
        throw new RuntimeException(
            String.format("expansion service error: %s", response.getError()));
      }

      Map<String, RunnerApi.Environment> newEnvironmentsWithDependencies =
          response.getComponents().getEnvironmentsMap().entrySet().stream()
              .filter(
                  kv ->
                      !originalComponents.getEnvironmentsMap().containsKey(kv.getKey())
                          && kv.getValue().getDependenciesCount() != 0)
              .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

      expandedComponents =
          response
              .getComponents()
              .toBuilder()
              .putAllEnvironments(resolveArtifacts(newEnvironmentsWithDependencies, endpoint))
              .build();
      expandedTransform = response.getTransform();
      expandedRequirements = response.getRequirementsList();

      RehydratedComponents rehydratedComponents =
          RehydratedComponents.forComponents(expandedComponents).withPipeline(p);

      ImmutableMap.Builder<TupleTag<?>, PCollection> outputMapBuilder = ImmutableMap.builder();
      expandedTransform
          .getOutputsMap()
          .forEach(
              (localId, pCollectionId) -> {
                try {
                  PCollection col = rehydratedComponents.getPCollection(pCollectionId);
                  externalPCollectionIdMapBuilder.put(col, pCollectionId);
                  outputMapBuilder.put(new TupleTag<>(localId), col);
                } catch (IOException e) {
                  throw new RuntimeException("cannot rehydrate PCollection.");
                }
              });
      externalPCollectionIdMap = externalPCollectionIdMapBuilder.build();

      Map<Coder<?>, String> externalCoderIdMapBuilder = new HashMap<>();
      expandedComponents
          .getPcollectionsMap()
          .forEach(
              (pcolId, pCol) -> {
                try {
                  String coderId = pCol.getCoderId();
                  if (isJavaSDKCompatible(expandedComponents, coderId)) {
                    Coder<?> coder = rehydratedComponents.getCoder(coderId);
                    externalCoderIdMapBuilder.putIfAbsent(coder, coderId);
                  }
                } catch (IOException e) {
                  throw new RuntimeException("cannot rehydrate Coder.");
                }
              });
      externalCoderIdMap = ImmutableMap.copyOf(externalCoderIdMapBuilder);

      return toOutputCollection(outputMapBuilder.build());
    }

    static Map<String, RunnerApi.Environment> resolveArtifacts(
        Map<String, RunnerApi.Environment> environments, Endpoints.ApiServiceDescriptor endpoint) {
      if (environments.size() == 0) {
        return environments;
      }
      ManagedChannel channel =
          ManagedChannelBuilder.forTarget(endpoint.getUrl())
              .usePlaintext()
              .maxInboundMessageSize(Integer.MAX_VALUE)
              .build();
      try {
        ArtifactRetrievalServiceGrpc.ArtifactRetrievalServiceBlockingStub retrievalStub =
            ArtifactRetrievalServiceGrpc.newBlockingStub(channel);
        return environments.entrySet().stream()
            .collect(
                Collectors.toMap(
                    Map.Entry::getKey,
                    kv -> {
                      try {
                        return resolveArtifacts(retrievalStub, kv.getValue());
                      } catch (IOException e) {
                        throw new RuntimeException(e);
                      }
                    }));
      } finally {
        channel.shutdown();
      }
    }

    private static RunnerApi.Environment resolveArtifacts(
        ArtifactRetrievalServiceGrpc.ArtifactRetrievalServiceBlockingStub retrievalStub,
        RunnerApi.Environment environment)
        throws IOException {
      return environment
          .toBuilder()
          .clearDependencies()
          .addAllDependencies(resolveArtifacts(retrievalStub, environment.getDependenciesList()))
          .build();
    }

    private static List<RunnerApi.ArtifactInformation> resolveArtifacts(
        ArtifactRetrievalServiceGrpc.ArtifactRetrievalServiceBlockingStub retrievalStub,
        List<RunnerApi.ArtifactInformation> artifacts)
        throws IOException {
      List<RunnerApi.ArtifactInformation> resolved = new ArrayList<>();
      for (RunnerApi.ArtifactInformation artifact :
          retrievalStub
              .resolveArtifacts(
                  ArtifactApi.ResolveArtifactsRequest.newBuilder()
                      .addAllArtifacts(artifacts)
                      .build())
              .getReplacementsList()) {
        Path path = Files.createTempFile("beam-artifact", "");
        File artifactFile = path.toFile();
        try (FileOutputStream fout = new FileOutputStream(artifactFile)) {
          for (Iterator<ArtifactApi.GetArtifactResponse> it =
                  retrievalStub.getArtifact(
                      ArtifactApi.GetArtifactRequest.newBuilder().setArtifact(artifact).build());
              it.hasNext(); ) {
            it.next().getData().writeTo(fout);
          }
        }
        resolved.add(
            artifact
                .toBuilder()
                .setTypeUrn("beam:artifact:type:file:v1")
                .setTypePayload(
                    RunnerApi.ArtifactFilePayload.newBuilder()
                        .setPath(path.toString())
                        .build()
                        .toByteString())
                .build());
        // Delete beam-artifact temp File on program exit
        artifactFile.deleteOnExit();
      }
      return resolved;
    }

    boolean isJavaSDKCompatible(RunnerApi.Components components, String coderId) {
      RunnerApi.Coder coder = components.getCodersOrThrow(coderId);
      if (!CoderTranslation.JAVA_SERIALIZED_CODER_URN.equals(coder.getSpec().getUrn())
          && !CoderTranslation.KNOWN_CODER_URNS.containsValue(coder.getSpec().getUrn())) {
        return false;
      }
      for (String componentId : coder.getComponentCoderIdsList()) {
        if (!isJavaSDKCompatible(components, componentId)) {
          return false;
        }
      }
      return true;
    }

    abstract OutputT toOutputCollection(Map<TupleTag<?>, PCollection> output);

    String getNamespace() {
      return String.format("External_%s", namespaceIndex);
    }

    String getImpulsePrefix() {
      return IMPULSE_PREFIX;
    }

    RunnerApi.PTransform getExpandedTransform() {
      return expandedTransform;
    }

    RunnerApi.Components getExpandedComponents() {
      return expandedComponents;
    }

    List<String> getExpandedRequirements() {
      return expandedRequirements;
    }

    Map<PCollection, String> getExternalPCollectionIdMap() {
      return externalPCollectionIdMap;
    }

    Map<Coder<?>, String> getExternalCoderIdMap() {
      return externalCoderIdMap;
    }

    String getUrn() {
      return urn;
    }

    byte[] getPayload() {
      return payload;
    }

    Endpoints.ApiServiceDescriptor getEndpoint() {
      return endpoint;
    }

    ExpansionServiceClientFactory getClientFactory() {
      return clientFactory;
    }

    Integer getNamespaceIndex() {
      return namespaceIndex;
    }

    Map<String, Coder<?>> getOutputCoders() {
      return outputCoders;
    }
  }
}
