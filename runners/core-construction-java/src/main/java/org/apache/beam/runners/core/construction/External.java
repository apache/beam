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

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import javax.annotation.Nullable;
import org.apache.beam.model.expansion.v1.ExpansionApi;
import org.apache.beam.model.pipeline.v1.Endpoints;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.runners.AppliedPTransform;
import org.apache.beam.sdk.transforms.Impulse;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.PInput;
import org.apache.beam.sdk.values.POutput;
import org.apache.beam.sdk.values.PValue;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.vendor.grpc.v1p21p0.com.google.protobuf.ByteString;
import org.apache.beam.vendor.grpc.v1p21p0.io.grpc.ManagedChannelBuilder;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Strings;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Iterables;

/**
 * Cross-language external transform.
 *
 * <p>{@link External} provides a cross-language transform via expansion services in foreign SDKs.
 * In order to use {@link External} transform, a user should know 1) URN of the target transform 2)
 * bytes encoding schema for configuration parameters 3) connection endpoint of the expansion
 * service. Note that this is a low-level API and mainly for internal use. A user may want to use
 * high-level wrapper classes rather than this one.
 */
public class External {
  private static final String EXPANDED_TRANSFORM_BASE_NAME = "external";
  private static final String IMPULSE_PREFIX = "IMPULSE";
  private static AtomicInteger namespaceCounter = new AtomicInteger(0);

  private static final ExpansionServiceClientFactory DEFAULT =
      new DefaultExpansionServiceClientFactory(
          endPoint -> ManagedChannelBuilder.forTarget(endPoint.getUrl()).usePlaintext().build());

  private static int getFreshNamespaceIndex() {
    return namespaceCounter.getAndIncrement();
  }

  public static <InputT extends PInput, OutputT>
      SingleOutputExpandableTransform<InputT, OutputT> of(
          String urn, byte[] payload, String endpoint) {
    Endpoints.ApiServiceDescriptor apiDesc =
        Endpoints.ApiServiceDescriptor.newBuilder().setUrl(endpoint).build();
    return new SingleOutputExpandableTransform<>(urn, payload, apiDesc, getFreshNamespaceIndex());
  }

  /** Expandable transform for output type of PCollection. */
  public static class SingleOutputExpandableTransform<InputT extends PInput, OutputT>
      extends ExpandableTransform<InputT, PCollection<OutputT>> {
    SingleOutputExpandableTransform(
        String urn,
        byte[] payload,
        Endpoints.ApiServiceDescriptor endpoint,
        Integer namespaceIndex) {
      super(urn, payload, endpoint, namespaceIndex);
    }

    @Override
    PCollection<OutputT> toOutputCollection(Map<TupleTag<?>, PCollection> output) {
      checkArgument(output.size() > 0, "output shouldn't be empty.");
      return Iterables.getOnlyElement(output.values());
    }

    public MultiOutputExpandableTransform<InputT> withMultiOutputs() {
      return new MultiOutputExpandableTransform<>(
          getUrn(), getPayload(), getEndpoint(), getNamespaceIndex());
    }

    public <T> SingleOutputExpandableTransform<InputT, T> withOutputType() {
      return new SingleOutputExpandableTransform<>(
          getUrn(), getPayload(), getEndpoint(), getNamespaceIndex());
    }
  }

  /** Expandable transform for output type of PCollectionTuple. */
  public static class MultiOutputExpandableTransform<InputT extends PInput>
      extends ExpandableTransform<InputT, PCollectionTuple> {
    MultiOutputExpandableTransform(
        String urn,
        byte[] payload,
        Endpoints.ApiServiceDescriptor endpoint,
        Integer namespaceIndex) {
      super(urn, payload, endpoint, namespaceIndex);
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
  }

  /** Base Expandable Transform which calls ExpansionService to expand itself. */
  public abstract static class ExpandableTransform<InputT extends PInput, OutputT extends POutput>
      extends PTransform<InputT, OutputT> {
    private final String urn;
    private final byte[] payload;
    private final Endpoints.ApiServiceDescriptor endpoint;
    private final Integer namespaceIndex;

    @Nullable private transient RunnerApi.Components expandedComponents;
    @Nullable private transient RunnerApi.PTransform expandedTransform;
    @Nullable private transient Map<PCollection, String> externalPCollectionIdMap;

    ExpandableTransform(
        String urn,
        byte[] payload,
        Endpoints.ApiServiceDescriptor endpoint,
        Integer namespaceIndex) {
      this.urn = urn;
      this.payload = payload;
      this.endpoint = endpoint;
      this.namespaceIndex = namespaceIndex;
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
                    PBegin.in(p).expand(),
                    ImmutableMap.of(entry.getKey(), entry.getValue()),
                    Impulse.create(),
                    p);
            // using fake Impulses to provide inputs
            components.registerPTransform(fakeImpulse, Collections.emptyList());
          } catch (IOException e) {
            throw new RuntimeException(
                String.format("cannot register component: %s", e.getMessage()));
          }
        }
      }

      ExpansionApi.ExpansionRequest request =
          ExpansionApi.ExpansionRequest.newBuilder()
              .setComponents(components.toComponents())
              .setTransform(ptransformBuilder.build())
              .setNamespace(getNamespace())
              .build();

      ExpansionApi.ExpansionResponse response =
          DEFAULT.getExpansionServiceClient(endpoint).expand(request);

      if (!Strings.isNullOrEmpty(response.getError())) {
        throw new RuntimeException(
            String.format("expansion service error: %s", response.getError()));
      }

      expandedComponents = response.getComponents();
      expandedTransform = response.getTransform();

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

      return toOutputCollection(outputMapBuilder.build());
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

    Map<PCollection, String> getExternalPCollectionIdMap() {
      return externalPCollectionIdMap;
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

    Integer getNamespaceIndex() {
      return namespaceIndex;
    }
  }
}
