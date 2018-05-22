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

import static com.google.common.base.Preconditions.checkState;
import static org.apache.beam.runners.core.construction.SyntheticComponents.uniqueId;

import com.google.auto.value.AutoValue;
import com.google.common.collect.ImmutableTable;
import com.google.common.collect.Iterables;
import java.io.IOException;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
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
import org.apache.beam.runners.core.construction.SyntheticComponents;
import org.apache.beam.runners.core.construction.graph.ExecutableStage;
import org.apache.beam.runners.core.construction.graph.PipelineNode.PCollectionNode;
import org.apache.beam.runners.core.construction.graph.PipelineNode.PTransformNode;
import org.apache.beam.runners.core.construction.graph.SideInputReference;
import org.apache.beam.runners.fnexecution.data.RemoteInputDestination;
import org.apache.beam.runners.fnexecution.wire.LengthPrefixUnknownCoders;
import org.apache.beam.runners.fnexecution.wire.WireCoders;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.fn.data.RemoteGrpcPortRead;
import org.apache.beam.sdk.fn.data.RemoteGrpcPortWrite;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.util.WindowedValue.FullWindowedValueCoder;
import org.apache.beam.sdk.values.KV;

/**
 * Utility methods for creating {@link ProcessBundleDescriptor} instances.
 */
// TODO: Rename to ExecutableStages?
public class ProcessBundleDescriptors {

  /**
   * Note that the {@link ProcessBundleDescriptor} is constructed by:
   * <ul>
   *   <li>Adding gRPC read and write nodes wiring them to the specified data endpoint.</li>
   *   <li>Setting the state {@link ApiServiceDescriptor} to the specified state endpoint.</li>
   *   <li>Modifying the coder on PCollections that are accessed as side inputs to be length
   *   prefixed making them binary compatible with the coder chosen when that side input is
   *   materialized.</li>
   * </ul>
   */
  public static ExecutableProcessBundleDescriptor fromExecutableStage(
      String id,
      ExecutableStage stage,
      ApiServiceDescriptor dataEndpoint,
      ApiServiceDescriptor stateEndpoint) throws IOException {
    checkState(id != null, "id must be specified.");
    checkState(stage != null, "stage must be specified.");
    checkState(dataEndpoint != null, "dataEndpoint must be specified.");
    checkState(stateEndpoint != null, "stateEndpoint must be specified.");
    return fromExecutableStageInternal(id, stage, dataEndpoint, stateEndpoint);
  }

  public static ExecutableProcessBundleDescriptor fromExecutableStage(
      String id, ExecutableStage stage, ApiServiceDescriptor dataEndpoint) throws IOException {
    checkState(id != null, "id must be specified.");
    checkState(stage != null, "stage must be specified.");
    checkState(dataEndpoint != null, "dateEndpoint must be specified.");
    return fromExecutableStageInternal(id, stage, dataEndpoint, null);
  }

  private static ExecutableProcessBundleDescriptor fromExecutableStageInternal(
      String id,
      ExecutableStage stage,
      ApiServiceDescriptor dataEndpoint,
      @Nullable ApiServiceDescriptor stateEndpoint) throws IOException {
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
    if (stateEndpoint != null) {
      bundleDescriptorBuilder.setStateApiServiceDescriptor(stateEndpoint);
    }

    RemoteInputDestination<WindowedValue<?>> inputDestination =
        addStageInput(
            stage.getInputPCollection(), components, dataEndpoint, bundleDescriptorBuilder);

    Map<BeamFnApi.Target, Coder<WindowedValue<?>>> outputTargetCoders = new LinkedHashMap<>();
    for (PCollectionNode outputPCollection : stage.getOutputPCollections()) {
      TargetEncoding targetEncoding =
          addStageOutput(components, dataEndpoint, bundleDescriptorBuilder, outputPCollection);
      outputTargetCoders.put(targetEncoding.getTarget(), targetEncoding.getCoder());
    }

    Map<String, Map<String, MultimapSideInputSpec>> multimapSideInputSpecs =
        forMultimapSideInputs(stage, components, bundleDescriptorBuilder);

    return ExecutableProcessBundleDescriptor.of(
        bundleDescriptorBuilder.build(),
        inputDestination,
        outputTargetCoders,
        multimapSideInputSpecs);
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

  private static Map<String, Map<String, MultimapSideInputSpec>> forMultimapSideInputs(
      ExecutableStage stage,
      Components components,
      ProcessBundleDescriptor.Builder bundleDescriptorBuilder) throws IOException {
    ImmutableTable.Builder<String, String, MultimapSideInputSpec> idsToSpec =
        ImmutableTable.builder();
    for (SideInputReference sideInputReference : stage.getSideInputs()) {
      // Update the coder specification for side inputs to be length prefixed so that the
      // SDK and Runner agree on how to encode/decode the key, window, and values for multimap
      // side inputs.
      String pCollectionId = sideInputReference.collection().getId();
      RunnerApi.MessageWithComponents lengthPrefixedSideInputCoder =
          LengthPrefixUnknownCoders.forCoder(
              components.getPcollectionsOrThrow(pCollectionId).getCoderId(),
              components,
              false);
      String lengthPrefixedSideInputCoderId = SyntheticComponents.uniqueId(
          String.format(
              "fn/side_input/%s",
              components.getPcollectionsOrThrow(pCollectionId).getCoderId()),
          bundleDescriptorBuilder.getCodersMap().keySet()::contains);

      bundleDescriptorBuilder.putCoders(
          lengthPrefixedSideInputCoderId, lengthPrefixedSideInputCoder.getCoder());
      bundleDescriptorBuilder.putAllCoders(
          lengthPrefixedSideInputCoder.getComponents().getCodersMap());
      bundleDescriptorBuilder.putPcollections(
          pCollectionId,
          bundleDescriptorBuilder
              .getPcollectionsMap()
              .get(pCollectionId)
              .toBuilder()
              .setCoderId(lengthPrefixedSideInputCoderId)
              .build());

      FullWindowedValueCoder<KV<?, ?>> coder =
          (FullWindowedValueCoder) WireCoders.instantiateRunnerWireCoder(
              sideInputReference.collection(), components);
      idsToSpec.put(
          sideInputReference.transform().getId(),
          sideInputReference.localName(),
          MultimapSideInputSpec.of(
              sideInputReference.transform().getId(),
              sideInputReference.localName(),
              ((KvCoder) coder.getValueCoder()).getKeyCoder(),
              ((KvCoder) coder.getValueCoder()).getValueCoder(),
              coder.getWindowCoder()));
    }
    return idsToSpec.build().rowMap();
  }

  @AutoValue
  abstract static class TargetEncoding {
    abstract BeamFnApi.Target getTarget();

    abstract Coder<WindowedValue<?>> getCoder();
  }

  /**
   * A container type storing references to the key, value, and window {@link Coder} used when
   * handling multimap side input state requests.
   */
  @AutoValue
  public abstract static class MultimapSideInputSpec<K, V, W extends BoundedWindow> {
    static <K, V, W extends BoundedWindow> MultimapSideInputSpec<K, V, W> of(
        String transformId,
        String sideInputId,
        Coder<K> keyCoder,
        Coder<V> valueCoder,
        Coder<W> windowCoder) {
      return new AutoValue_ProcessBundleDescriptors_MultimapSideInputSpec(
          transformId, sideInputId, keyCoder, valueCoder, windowCoder);
    }

    public abstract String transformId();
    public abstract String sideInputId();
    public abstract Coder<K> keyCoder();
    public abstract Coder<V> valueCoder();
    public abstract Coder<W> windowCoder();
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
        Map<BeamFnApi.Target, Coder<WindowedValue<?>>> outputTargetCoders,
        Map<String, Map<String, MultimapSideInputSpec>> multimapSideInputSpecs) {
      ImmutableTable.Builder copyOfMultimapSideInputSpecs = ImmutableTable.builder();
      for (Map.Entry<String, Map<String, MultimapSideInputSpec>> outer
          : multimapSideInputSpecs.entrySet()) {
        for (Map.Entry<String, MultimapSideInputSpec> inner : outer.getValue().entrySet()) {
          copyOfMultimapSideInputSpecs.put(outer.getKey(), inner.getKey(), inner.getValue());
        }
      }
      return new AutoValue_ProcessBundleDescriptors_ExecutableProcessBundleDescriptor(
          descriptor,
          inputDestination,
          Collections.unmodifiableMap(outputTargetCoders),
          copyOfMultimapSideInputSpecs.build().rowMap());
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


    /**
     * Get a mapping from PTransform id to multimap side input id to {@link MultimapSideInputSpec
     * multimap side inputs} that are used during execution.
     */
    public abstract Map<String, Map<String, MultimapSideInputSpec>> getMultimapSideInputSpecs();
  }
}
