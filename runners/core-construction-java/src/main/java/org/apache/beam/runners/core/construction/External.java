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

import com.google.auto.value.AutoValue;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import org.apache.beam.model.expansion.v1.ExpansionApi;
import org.apache.beam.model.jobmanagement.v1.ArtifactApi;
import org.apache.beam.model.jobmanagement.v1.ArtifactRetrievalServiceGrpc;
import org.apache.beam.model.pipeline.v1.Endpoints;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.annotations.Experimental.Kind;
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
import org.apache.beam.vendor.grpc.v1p43p2.com.google.protobuf.ByteString;
import org.apache.beam.vendor.grpc.v1p43p2.io.grpc.ManagedChannel;
import org.apache.beam.vendor.grpc.v1p43p2.io.grpc.ManagedChannelBuilder;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Strings;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Iterables;
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
@Experimental(Kind.PORTABILITY)
@SuppressWarnings({
  "rawtypes", // TODO(https://github.com/apache/beam/issues/20447)
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
public class External {
  protected static final String EXPANDED_TRANSFORM_BASE_NAME = "external";
  private static final String IMPULSE_PREFIX = "IMPULSE";
  private static AtomicInteger namespaceCounter = new AtomicInteger(0);

  private static final ExpansionServiceClientFactory DEFAULT =
      DefaultExpansionServiceClientFactory.create(
          endPoint -> ManagedChannelBuilder.forTarget(endPoint.getUrl()).usePlaintext().build());

  public static int getFreshNamespaceIndex() {
    return namespaceCounter.getAndIncrement();
  }

  public static <InputT extends PInput, OutputT>
      SingleOutputExpandableTransform<InputT, OutputT> of(
          String urn, byte[] payload, String endpoint) {
    return new SingleOutputExpandableTransform<>(
        ImmutableList.of(ExpansionInfo.create(urn, payload, endpoint, getFreshNamespaceIndex())),
        DEFAULT,
        ImmutableMap.of());
  }

  public static <InputT extends PInput, OutputT>
      SingleOutputExpandableTransform<InputT, OutputT> of(
          ComposableExternalTransform... transforms) {
    return new SingleOutputExpandableTransform<>(
        Arrays.stream(transforms)
            .flatMap(t -> t.getExpansionInfoList().stream())
            .collect(Collectors.toList()),
        DEFAULT,
        ImmutableMap.of());
  }

  @VisibleForTesting
  public static <InputT extends PInput, OutputT>
      SingleOutputExpandableTransform<InputT, OutputT> of(
          String urn,
          byte[] payload,
          String endpoint,
          ExpansionServiceClientFactory clientFactory) {
    return new SingleOutputExpandableTransform<>(
        ImmutableList.of(ExpansionInfo.create(urn, payload, endpoint, getFreshNamespaceIndex())),
        clientFactory,
        ImmutableMap.of());
  }

  /** Expandable transform for output type of PCollection. */
  public static class SingleOutputExpandableTransform<InputT extends PInput, OutputT>
      extends ExpandableTransform<InputT, PCollection<OutputT>> {
    SingleOutputExpandableTransform(
        List<ExpansionInfo> expansionInfoList,
        ExpansionServiceClientFactory clientFactory,
        Map<String, Coder<?>> outputCoders) {
      super(expansionInfoList, clientFactory, outputCoders);
    }

    @Override
    PCollection<OutputT> toOutputCollection(Map<TupleTag<?>, PCollection> output) {
      checkArgument(output.size() > 0, "output shouldn't be empty.");
      return Iterables.getOnlyElement(output.values());
    }

    public MultiOutputExpandableTransform<InputT> withMultiOutputs() {
      return new MultiOutputExpandableTransform<>(
          getExpansionInfoList(), getClientFactory(), getOutputCoders());
    }

    public SingleOutputExpandableTransform<InputT, OutputT> withOutputCoder(Coder<?> outputCoder) {
      return new SingleOutputExpandableTransform<>(
          getExpansionInfoList(), getClientFactory(), ImmutableMap.of("0", outputCoder));
    }
  }

  /** Expandable transform for output type of PCollectionTuple. */
  public static class MultiOutputExpandableTransform<InputT extends PInput>
      extends ExpandableTransform<InputT, PCollectionTuple> {
    MultiOutputExpandableTransform(
        List<ExpansionInfo> expansionInfoList,
        ExpansionServiceClientFactory clientFactory,
        Map<String, Coder<?>> outputCoders) {
      super(expansionInfoList, clientFactory, outputCoders);
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
          getExpansionInfoList(), getClientFactory(), outputCoders);
    }
  }

  /** Base Expandable Transform which calls ExpansionService to expand itself. */
  public abstract static class ExpandableTransform<InputT extends PInput, OutputT extends POutput>
      extends PTransform<InputT, OutputT> implements ComposableExternalTransform {
    private final List<ExpansionInfo> expansionInfoList;
    private final ExpansionServiceClientFactory clientFactory;
    private final Map<String, Coder<?>> outputCoders;

    private transient RunnerApi.@Nullable Components expandedComponents;
    private transient @Nullable List<RunnerApi.PTransform> expandedTransforms;
    private transient @Nullable List<String> expandedRequirements;
    private transient @Nullable Map<PCollection, String> externalPCollectionIdMap;
    private transient @Nullable Map<Coder<?>, String> externalCoderIdMap;
    private transient Set<String> namespaces;

    ExpandableTransform(
        List<ExpansionInfo> expansionInfoList,
        ExpansionServiceClientFactory clientFactory,
        Map<String, Coder<?>> outputCoders) {
      checkArgument(expansionInfoList.size() > 0);
      this.expansionInfoList = expansionInfoList;
      this.clientFactory = clientFactory;
      this.outputCoders = outputCoders;
      this.namespaces = new HashSet<>();
    }

    @Override
    public OutputT expand(InputT input) {
      Pipeline p = input.getPipeline();
      SdkComponents components = SdkComponents.create(p.getOptions());
      ImmutableMap.Builder<PCollection, String> externalPCollectionIdMapBuilder =
          ImmutableMap.builder();
      ImmutableMap.Builder<String, String> inputMapBuilder = ImmutableMap.builder();
      for (Map.Entry<TupleTag<?>, PValue> entry : input.expand().entrySet()) {
        if (entry.getValue() instanceof PCollection<?>) {
          try {
            String id = components.registerPCollection((PCollection) entry.getValue());
            externalPCollectionIdMapBuilder.put((PCollection) entry.getValue(), id);
            inputMapBuilder.put(entry.getKey().getId(), id);
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

      Map<String, String> outputCoderMap =
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
                      }));

      ExpansionApi.ExpansionResponse response =
          ExpansionApi.ExpansionResponse.newBuilder()
              .setComponents(components.toComponents())
              .setTransform(
                  RunnerApi.PTransform.newBuilder().putAllOutputs(inputMapBuilder.build()).build())
              .build();
      ImmutableList.Builder<RunnerApi.PTransform> expandedTransformsBuilder =
          ImmutableList.builder();
      for (int i = 0; i < expansionInfoList.size(); i++) {
        response =
            sendRequest(
                expansionInfoList.get(i),
                response.getComponents(),
                response.getTransform().getOutputsMap(),
                i == expansionInfoList.size() - 1 ? outputCoderMap : ImmutableMap.of());
        expandedTransformsBuilder.add(response.getTransform());
      }
      expandedComponents = response.getComponents();
      expandedTransforms = expandedTransformsBuilder.build();
      expandedRequirements = response.getRequirementsList();

      RehydratedComponents rehydratedComponents =
          RehydratedComponents.forComponents(expandedComponents).withPipeline(p);

      ImmutableMap.Builder<TupleTag<?>, PCollection> outputMapBuilder = ImmutableMap.builder();
      response
          .getTransform()
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

    private ExpansionApi.ExpansionResponse sendRequest(
        ExpansionInfo expansionInfo,
        RunnerApi.Components components,
        Map<String, String> inputMap,
        Map<String, String> outputCoderMap) {
      RunnerApi.PTransform.Builder ptransformBuilder =
          RunnerApi.PTransform.newBuilder()
              .setUniqueName(EXPANDED_TRANSFORM_BASE_NAME + expansionInfo.namespaceIndex())
              .setSpec(
                  RunnerApi.FunctionSpec.newBuilder()
                      .setUrn(expansionInfo.urn())
                      .setPayload(ByteString.copyFrom(expansionInfo.payload()))
                      .build());
      ptransformBuilder.putAllInputs(inputMap);
      String namespace = String.format("External_%s", expansionInfo.namespaceIndex());
      namespaces.add(namespace);
      ExpansionApi.ExpansionRequest.Builder requestBuilder =
          ExpansionApi.ExpansionRequest.newBuilder();
      requestBuilder.putAllOutputCoderRequests(outputCoderMap);
      ExpansionApi.ExpansionRequest request =
          requestBuilder
              .setComponents(components)
              .setTransform(ptransformBuilder.build())
              .setNamespace(namespace)
              .build();

      Endpoints.ApiServiceDescriptor apiDesc =
          Endpoints.ApiServiceDescriptor.newBuilder().setUrl(expansionInfo.endpoint()).build();
      ExpansionApi.ExpansionResponse response =
          clientFactory.getExpansionServiceClient(apiDesc).expand(request);

      if (!Strings.isNullOrEmpty(response.getError())) {
        throw new RuntimeException(
            String.format("expansion service error: %s", response.getError()));
      }

      RunnerApi.Components.Builder componentsBuilder = response.getComponents().toBuilder();
      componentsBuilder.putAllEnvironments(
          resolveArtifacts(
              componentsBuilder.getEnvironmentsMap().entrySet().stream()
                  .filter(
                      kv ->
                          !components.getEnvironmentsMap().containsKey(kv.getKey())
                              && kv.getValue().getDependenciesCount() != 0)
                  .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue)),
              expansionInfo.endpoint()));

      return response.toBuilder().setComponents(componentsBuilder).build();
    }

    private Map<String, RunnerApi.Environment> resolveArtifacts(
        Map<String, RunnerApi.Environment> environments, String endpoint) {
      if (environments.size() == 0) {
        return environments;
      }
      ManagedChannel channel =
          ManagedChannelBuilder.forTarget(endpoint)
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

    private RunnerApi.Environment resolveArtifacts(
        ArtifactRetrievalServiceGrpc.ArtifactRetrievalServiceBlockingStub retrievalStub,
        RunnerApi.Environment environment)
        throws IOException {
      return environment
          .toBuilder()
          .clearDependencies()
          .addAllDependencies(resolveArtifacts(retrievalStub, environment.getDependenciesList()))
          .build();
    }

    private List<RunnerApi.ArtifactInformation> resolveArtifacts(
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
        try (FileOutputStream fout = new FileOutputStream(path.toFile())) {
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

    String getImpulsePrefix() {
      return IMPULSE_PREFIX;
    }

    Set<String> getNamespaces() {
      return namespaces;
    }

    List<RunnerApi.PTransform> getExpandedTransforms() {
      return expandedTransforms;
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

    @Override
    public List<ExpansionInfo> getExpansionInfoList() {
      return expansionInfoList;
    }

    ExpansionServiceClientFactory getClientFactory() {
      return clientFactory;
    }

    Map<String, Coder<?>> getOutputCoders() {
      return outputCoders;
    }
  }

  @AutoValue
  public abstract static class ExpansionInfo {
    public static ExpansionInfo create(
        String urn, byte[] payload, String endpoint, Integer namespaceIndex) {
      return new AutoValue_External_ExpansionInfo(urn, payload, endpoint, namespaceIndex);
    }

    abstract String urn();

    @SuppressWarnings("mutable")
    abstract byte[] payload();

    abstract String endpoint();

    abstract Integer namespaceIndex();
  }

  public interface ComposableExternalTransform {
    List<ExpansionInfo> getExpansionInfoList();
  }
}
