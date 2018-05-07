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

import static org.apache.beam.runners.core.construction.SyntheticComponents.uniqueId;

import com.google.auto.value.AutoValue;
import com.google.common.collect.Iterables;
import java.io.IOException;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.beam.model.fnexecution.v1.BeamFnApi;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.ProcessBundleDescriptor;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.ProcessBundleDescriptor.Builder;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.RemoteGrpcPort;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.Target;
import org.apache.beam.model.pipeline.v1.Endpoints.ApiServiceDescriptor;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.model.pipeline.v1.RunnerApi.Components;
import org.apache.beam.model.pipeline.v1.RunnerApi.MessageWithComponents;
import org.apache.beam.model.pipeline.v1.RunnerApi.PTransform;
import org.apache.beam.runners.core.construction.graph.ExecutableStage;
import org.apache.beam.runners.core.construction.graph.PipelineNode.PCollectionNode;
import org.apache.beam.runners.core.construction.graph.PipelineNode.PTransformNode;
import org.apache.beam.runners.fnexecution.data.RemoteInputDestination;
import org.apache.beam.runners.fnexecution.wire.WireCoders;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.fn.data.RemoteGrpcPortRead;
import org.apache.beam.sdk.fn.data.RemoteGrpcPortWrite;
import org.apache.beam.sdk.util.WindowedValue;

/** Utility methods for creating {@link ProcessBundleDescriptor} instances. */
// TODO: Rename to ExecutableStages?
public class ProcessBundleDescriptors {
  public static ExecutableProcessBundleDescriptor fromExecutableStage(
      String id, ExecutableStage stage, ApiServiceDescriptor dataEndpoint) throws IOException {
    Components components = stage.getComponents();
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
      ProcessBundleDescriptor.Builder bundleDescriptorBuilder)
      throws IOException {
    String inputWireCoderId = addWireCoder(inputPCollection, components, bundleDescriptorBuilder);
    @SuppressWarnings("unchecked")
    Coder<WindowedValue<?>> wireCoder =
        (Coder) WireCoders.instantiateRunnerWireCoder(inputPCollection, components);

    RemoteGrpcPort inputPort =
        RemoteGrpcPort.newBuilder()
            .setApiServiceDescriptor(dataEndpoint)
            .setCoderId(inputWireCoderId)
            .build();
    String inputId =
        uniqueId(
            String.format("fn/read/%s", inputPCollection.getId()),
            bundleDescriptorBuilder::containsTransforms);
    PTransform inputTransform =
        RemoteGrpcPortRead.readFromPort(inputPort, inputPCollection.getId()).toPTransform();
    bundleDescriptorBuilder.putTransforms(inputId, inputTransform);
    return RemoteInputDestination.of(
        wireCoder,
        Target.newBuilder()
            .setPrimitiveTransformReference(inputId)
            .setName(Iterables.getOnlyElement(inputTransform.getOutputsMap().keySet()))
            .build());
  }

  private static TargetEncoding addStageOutput(
      Components components,
      ApiServiceDescriptor dataEndpoint,
      Builder bundleDescriptorBuilder,
      PCollectionNode outputPCollection)
      throws IOException {
    String outputWireCoderId = addWireCoder(outputPCollection, components, bundleDescriptorBuilder);
    @SuppressWarnings("unchecked")
    Coder<WindowedValue<?>> wireCoder =
        (Coder) WireCoders.instantiateRunnerWireCoder(outputPCollection, components);
    RemoteGrpcPort outputPort =
        RemoteGrpcPort.newBuilder()
            .setApiServiceDescriptor(dataEndpoint)
            .setCoderId(outputWireCoderId)
            .build();
    RemoteGrpcPortWrite outputWrite =
        RemoteGrpcPortWrite.writeToPort(outputPCollection.getId(), outputPort);
    String outputId =
        uniqueId(
            String.format("fn/write/%s", outputPCollection.getId()),
            bundleDescriptorBuilder::containsTransforms);
    PTransform outputTransform = outputWrite.toPTransform();
    bundleDescriptorBuilder.putTransforms(outputId, outputTransform);
    return new AutoValue_ProcessBundleDescriptors_TargetEncoding(
        Target.newBuilder()
            .setPrimitiveTransformReference(outputId)
            .setName(Iterables.getOnlyElement(outputTransform.getInputsMap().keySet()))
            .build(),
        wireCoder);
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
        WireCoders.createSdkWireCoder(
            pCollection, components, bundleDescriptorBuilder::containsCoders);
    bundleDescriptorBuilder.putAllCoders(wireCoder.getComponents().getCodersMap());
    String wireCoderId =
        uniqueId(
            String.format("fn/wire/%s", pCollection.getId()),
            bundleDescriptorBuilder::containsCoders);
    bundleDescriptorBuilder.putCoders(wireCoderId, wireCoder.getCoder());
    return wireCoderId;
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
    public abstract RemoteInputDestination<? super WindowedValue<?>> getRemoteInputDestination();

    /**
     * Get all of the targets materialized by this {@link ExecutableProcessBundleDescriptor} and the
     * java {@link Coder} for the wire format of that {@link BeamFnApi.Target}.
     */
    public abstract Map<BeamFnApi.Target, Coder<WindowedValue<?>>> getOutputTargetCoders();
  }
}
