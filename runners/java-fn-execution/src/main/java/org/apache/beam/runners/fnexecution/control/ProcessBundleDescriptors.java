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

package org.apache.beam.runners.fnexecution.control;

import static com.google.common.base.Preconditions.checkArgument;
import static org.apache.beam.runners.core.construction.UrnUtils.validateCommonUrn;

import com.google.auto.value.AutoValue;
import com.google.common.collect.Iterables;
import java.io.IOException;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import org.apache.beam.model.fnexecution.v1.BeamFnApi;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.ProcessBundleDescriptor;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.ProcessBundleDescriptor.Builder;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.RemoteGrpcPort;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.Target;
import org.apache.beam.model.pipeline.v1.Endpoints.ApiServiceDescriptor;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.model.pipeline.v1.RunnerApi.Components;
import org.apache.beam.model.pipeline.v1.RunnerApi.FunctionSpec;
import org.apache.beam.model.pipeline.v1.RunnerApi.MessageWithComponents;
import org.apache.beam.model.pipeline.v1.RunnerApi.PTransform;
import org.apache.beam.model.pipeline.v1.RunnerApi.SdkFunctionSpec;
import org.apache.beam.runners.core.construction.CoderTranslation;
import org.apache.beam.runners.core.construction.RehydratedComponents;
import org.apache.beam.runners.core.construction.graph.ExecutableStage;
import org.apache.beam.runners.core.construction.graph.PipelineNode.PCollectionNode;
import org.apache.beam.runners.core.construction.graph.PipelineNode.PTransformNode;
import org.apache.beam.runners.fnexecution.data.RemoteInputDestination;
import org.apache.beam.runners.fnexecution.graph.LengthPrefixUnknownCoders;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.fn.data.RemoteGrpcPortRead;
import org.apache.beam.sdk.fn.data.RemoteGrpcPortWrite;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.util.WindowedValue.FullWindowedValueCoder;

/** Utility methods for creating {@link ProcessBundleDescriptor} instances. */
// TODO: Rename to ExecutableStages?
public class ProcessBundleDescriptors {
  public static ExecutableProcessBundleDescriptor fromExecutableStage(
      String id, ExecutableStage stage, Components components, ApiServiceDescriptor dataEndpoint)
      throws IOException {
    // Create with all of the processing transforms, and all of the components.
    // TODO: Remove the unreachable subcomponents if the size of the descriptor matters.
    ProcessBundleDescriptor.Builder bundleDescriptorBuilder =
        ProcessBundleDescriptor.newBuilder()
            .setId(id)
            .putAllCoders(components.getCodersMap())
            .putAllEnvironments(components.getEnvironmentsMap())
            .putAllPcollections(components.getPcollectionsMap())
            .putAllWindowingStrategies(components.getWindowingStrategiesMap())
            .putAllTransforms(
                stage
                    .getTransforms()
                    .stream()
                    .collect(
                        Collectors.toMap(PTransformNode::getId, PTransformNode::getTransform)));

    RemoteInputDestination<WindowedValue<?>> inputDestination =
        addStageInput(
            stage.getInputPCollection(), components, dataEndpoint, bundleDescriptorBuilder);

    Map<BeamFnApi.Target, Coder<WindowedValue<?>>> outputTargetCoders = new LinkedHashMap<>();
    for (PCollectionNode outputPCollection : stage.getOutputPCollections()) {
      TargetEncoding targetEncoding =
          addStageOutput(components, dataEndpoint, bundleDescriptorBuilder, outputPCollection);
      outputTargetCoders.put(targetEncoding.getTarget(), targetEncoding.getCoder());
    }

    return ExecutableProcessBundleDescriptor.of(
        bundleDescriptorBuilder.build(), inputDestination, outputTargetCoders);
  }

  private static RemoteInputDestination<WindowedValue<?>> addStageInput(
      PCollectionNode inputPCollection,
      Components components,
      ApiServiceDescriptor dataEndpoint,
      ProcessBundleDescriptor.Builder bundleDescriptorBuilder) throws IOException {
    String inputWireCoderId = addWireCoder(inputPCollection, components, bundleDescriptorBuilder);
    RemoteGrpcPort inputPort =
        RemoteGrpcPort.newBuilder()
            .setApiServiceDescriptor(dataEndpoint)
            .setCoderId(inputWireCoderId)
            .build();
    String inputId =
        uniquifyId(
            String.format("fn/read/%s", inputPCollection.getId()),
            bundleDescriptorBuilder::containsTransforms);
    PTransform inputTransform =
        RemoteGrpcPortRead.readFromPort(inputPort, inputPCollection.getId()).toPTransform();
    bundleDescriptorBuilder.putTransforms(inputId, inputTransform);
    return RemoteInputDestination.of(
        instantiateWireCoder(inputPort, bundleDescriptorBuilder.getCodersMap()),
        Target.newBuilder()
            .setPrimitiveTransformReference(inputId)
            .setName(Iterables.getOnlyElement(inputTransform.getOutputsMap().keySet()))
            .build());
  }

  private static TargetEncoding addStageOutput(
      Components components,
      ApiServiceDescriptor dataEndpoint,
      Builder bundleDescriptorBuilder,
      PCollectionNode outputPCollection) throws IOException {
    String outputWireCoderId = addWireCoder(outputPCollection, components, bundleDescriptorBuilder);

    RemoteGrpcPort outputPort =
        RemoteGrpcPort.newBuilder()
            .setApiServiceDescriptor(dataEndpoint)
            .setCoderId(outputWireCoderId)
            .build();
    RemoteGrpcPortWrite outputWrite =
        RemoteGrpcPortWrite.writeToPort(outputPCollection.getId(), outputPort);
    String outputId =
        uniquifyId(
            String.format("fn/write/%s", outputPCollection.getId()),
            bundleDescriptorBuilder::containsTransforms);
    PTransform outputTransform = outputWrite.toPTransform();
    bundleDescriptorBuilder.putTransforms(outputId, outputTransform);
    return new AutoValue_ProcessBundleDescriptors_TargetEncoding(
        Target.newBuilder()
            .setPrimitiveTransformReference(outputId)
            .setName(Iterables.getOnlyElement(outputTransform.getInputsMap().keySet()))
            .build(),
        instantiateWireCoder(outputPort, bundleDescriptorBuilder.getCodersMap()));
  }

  @AutoValue
  abstract static class TargetEncoding {
    abstract BeamFnApi.Target getTarget();

    abstract Coder<WindowedValue<?>> getCoder();
  }

  /**
   * Add a {@link RunnerApi.Coder} suitable for using as the wire coder to the provided {@link
   * ProcessBundleDescriptor.Builder} and return the ID of that Coder.
   */
  private static String addWireCoder(
      PCollectionNode pCollection,
      Components components,
      ProcessBundleDescriptor.Builder bundleDescriptorBuilder) {
    MessageWithComponents wireCoder =
        getWireCoder(pCollection, components, bundleDescriptorBuilder::containsCoders);
    bundleDescriptorBuilder.putAllCoders(wireCoder.getComponents().getCodersMap());
    String wireCoderId =
        uniquifyId(
            String.format("fn/wire/%s", pCollection.getId()),
            bundleDescriptorBuilder::containsCoders);
    bundleDescriptorBuilder.putCoders(wireCoderId, wireCoder.getCoder());
    return wireCoderId;
  }

  private static MessageWithComponents getWireCoder(
      PCollectionNode pCollectionNode, Components components, Predicate<String> usedIds) {
    String elementCoderId = pCollectionNode.getPCollection().getCoderId();
    String windowingStrategyId = pCollectionNode.getPCollection().getWindowingStrategyId();
    String windowCoderId =
        components.getWindowingStrategiesOrThrow(windowingStrategyId).getWindowCoderId();
    // TODO: This is the wrong place to hand-construct a coder.
    RunnerApi.Coder windowedValueCoder =
        RunnerApi.Coder.newBuilder()
            .addComponentCoderIds(elementCoderId)
            .addComponentCoderIds(windowCoderId)
            .setSpec(
                SdkFunctionSpec.newBuilder()
                    .setSpec(
                        FunctionSpec.newBuilder()
                            .setUrn(validateCommonUrn("beam:coder:windowed_value:v1"))))
            .build();
    // Add the original WindowedValue<T, W> coder to the components;
    String windowedValueId =
        uniquifyId(String.format("fn/wire/%s", pCollectionNode.getId()), usedIds);
    return LengthPrefixUnknownCoders.forCoder(
        windowedValueId,
        components.toBuilder().putCoders(windowedValueId, windowedValueCoder).build(),
        false);
  }

  private static String uniquifyId(String idBase, Predicate<String> idUsed) {
    if (!idUsed.test(idBase)) {
      return idBase;
    }
    int i = 0;
    while (idUsed.test(String.format("%s_%s", idBase, i))) {
      i++;
    }
    return String.format("%s_%s", idBase, i);
  }

  private static Coder<WindowedValue<?>> instantiateWireCoder(
      RemoteGrpcPort port, Map<String, RunnerApi.Coder> components) throws IOException {
    MessageWithComponents byteArrayCoder =
        LengthPrefixUnknownCoders.forCoder(
            port.getCoderId(), Components.newBuilder().putAllCoders(components).build(), true);
    Coder<?> javaCoder =
        CoderTranslation.fromProto(
            byteArrayCoder.getCoder(),
            RehydratedComponents.forComponents(byteArrayCoder.getComponents()));
    checkArgument(
        javaCoder instanceof WindowedValue.FullWindowedValueCoder,
        "Unexpected Deserialized %s type, expected %s, got %s",
        RunnerApi.Coder.class.getSimpleName(),
        FullWindowedValueCoder.class.getSimpleName(),
        javaCoder.getClass());
    return (Coder<WindowedValue<?>>) javaCoder;
  }

  /** */
  @AutoValue
  public abstract static class ExecutableProcessBundleDescriptor {
    public static ExecutableProcessBundleDescriptor of(
        ProcessBundleDescriptor descriptor,
        RemoteInputDestination<WindowedValue<?>> inputDestination,
        Map<BeamFnApi.Target, Coder<WindowedValue<?>>> outputTargetCoders) {
      return new AutoValue_ProcessBundleDescriptors_ExecutableProcessBundleDescriptor(
          descriptor, inputDestination, Collections.unmodifiableMap(outputTargetCoders));
    }

    public abstract ProcessBundleDescriptor getProcessBundleDescriptor();

    /**
     * Get the {@link RemoteInputDestination} that input data are sent to the {@link
     * ProcessBundleDescriptor} over.
     */
    public abstract RemoteInputDestination<WindowedValue<?>> getRemoteInputDestination();

    /**
     * Get all of the targets materialized by this {@link ExecutableProcessBundleDescriptor} and the
     * java {@link Coder} for the wire format of that {@link BeamFnApi.Target}.
     */
    public abstract Map<BeamFnApi.Target, Coder<WindowedValue<?>>> getOutputTargetCoders();
  }
}
