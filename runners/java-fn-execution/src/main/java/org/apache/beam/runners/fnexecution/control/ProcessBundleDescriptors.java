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

import static org.apache.beam.sdk.util.construction.SyntheticComponents.uniqueId;
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkArgument;
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkState;

import com.google.auto.value.AutoValue;
import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.ProcessBundleDescriptor;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.RemoteGrpcPort;
import org.apache.beam.model.pipeline.v1.Endpoints.ApiServiceDescriptor;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.model.pipeline.v1.RunnerApi.Components;
import org.apache.beam.model.pipeline.v1.RunnerApi.ExecutableStagePayload.WireCoderSetting;
import org.apache.beam.model.pipeline.v1.RunnerApi.PCollection;
import org.apache.beam.model.pipeline.v1.RunnerApi.PTransform;
import org.apache.beam.runners.fnexecution.data.RemoteInputDestination;
import org.apache.beam.runners.fnexecution.wire.ByteStringCoder;
import org.apache.beam.runners.fnexecution.wire.LengthPrefixUnknownCoders;
import org.apache.beam.runners.fnexecution.wire.WireCoders;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.fn.data.RemoteGrpcPortRead;
import org.apache.beam.sdk.fn.data.RemoteGrpcPortWrite;
import org.apache.beam.sdk.state.TimeDomain;
import org.apache.beam.sdk.state.TimerSpecs;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.util.WindowedValue.FullWindowedValueCoder;
import org.apache.beam.sdk.util.construction.RehydratedComponents;
import org.apache.beam.sdk.util.construction.Timer;
import org.apache.beam.sdk.util.construction.graph.ExecutableStage;
import org.apache.beam.sdk.util.construction.graph.PipelineNode.PCollectionNode;
import org.apache.beam.sdk.util.construction.graph.PipelineNode.PTransformNode;
import org.apache.beam.sdk.util.construction.graph.SideInputReference;
import org.apache.beam.sdk.util.construction.graph.TimerReference;
import org.apache.beam.sdk.util.construction.graph.UserStateReference;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.vendor.grpc.v1p60p1.com.google.protobuf.InvalidProtocolBufferException;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableTable;
import org.checkerframework.checker.nullness.qual.Nullable;

/** Utility methods for creating {@link ProcessBundleDescriptor} instances. */
// TODO: Rename to ExecutableStages?
@SuppressWarnings({
  "rawtypes", // TODO(https://github.com/apache/beam/issues/20447)
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
public class ProcessBundleDescriptors {

  /**
   * Note that the {@link ProcessBundleDescriptor} is constructed by:
   *
   * <ul>
   *   <li>Adding gRPC read and write nodes wiring them to the specified data endpoint.
   *   <li>Setting the state {@link ApiServiceDescriptor} to the specified state endpoint.
   *   <li>Modifying the coder on PCollections that are accessed as side inputs to be length
   *       prefixed making them binary compatible with the coder chosen when that side input is
   *       materialized.
   * </ul>
   */
  public static ExecutableProcessBundleDescriptor fromExecutableStage(
      String id,
      ExecutableStage stage,
      ApiServiceDescriptor dataEndpoint,
      ApiServiceDescriptor stateEndpoint)
      throws IOException {
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
      @Nullable ApiServiceDescriptor stateEndpoint)
      throws IOException {
    // Create with all of the processing transforms, and all of the components.
    // TODO: Remove the unreachable subcomponents if the size of the descriptor matters.
    Map<String, PTransform> stageTransforms =
        stage.getTransforms().stream()
            .collect(Collectors.toMap(PTransformNode::getId, PTransformNode::getTransform));

    Components.Builder components =
        stage.getComponents().toBuilder().clearTransforms().putAllTransforms(stageTransforms);

    ImmutableList.Builder<RemoteInputDestination> inputDestinationsBuilder =
        ImmutableList.builder();
    ImmutableMap.Builder<String, Coder> remoteOutputCodersBuilder = ImmutableMap.builder();

    WireCoderSetting wireCoderSetting =
        stage.getWireCoderSettings().stream()
            .filter(ws -> ws.getInputOrOutputId().equals(stage.getInputPCollection().getId()))
            .findAny()
            .orElse(WireCoderSetting.getDefaultInstance());
    // The order of these does not matter.
    inputDestinationsBuilder.add(
        addStageInput(dataEndpoint, stage.getInputPCollection(), components, wireCoderSetting));

    remoteOutputCodersBuilder.putAll(
        addStageOutputs(
            dataEndpoint, stage.getOutputPCollections(), components, stage.getWireCoderSettings()));

    Map<String, Map<String, SideInputSpec>> sideInputSpecs = addSideInputs(stage, components);

    Map<String, Map<String, BagUserStateSpec>> bagUserStateSpecs =
        forBagUserStates(stage, components.build());

    Map<String, Map<String, TimerSpec>> timerSpecs = forTimerSpecs(stage, components);

    lengthPrefixAnyInputCoder(stage.getInputPCollection().getId(), components);

    // Copy data from components to ProcessBundleDescriptor.
    ProcessBundleDescriptor.Builder bundleDescriptorBuilder =
        ProcessBundleDescriptor.newBuilder().setId(id);
    if (stateEndpoint != null) {
      bundleDescriptorBuilder.setStateApiServiceDescriptor(stateEndpoint);
    }
    if (timerSpecs.size() > 0) {
      // By default use the data endpoint for timers, in the future considering enabling specifying
      // a different ApiServiceDescriptor for timers.
      bundleDescriptorBuilder.setTimerApiServiceDescriptor(dataEndpoint);
    }

    bundleDescriptorBuilder
        .putAllCoders(components.getCodersMap())
        .putAllEnvironments(components.getEnvironmentsMap())
        .putAllPcollections(components.getPcollectionsMap())
        .putAllWindowingStrategies(components.getWindowingStrategiesMap())
        .putAllTransforms(components.getTransformsMap());

    return ExecutableProcessBundleDescriptor.of(
        bundleDescriptorBuilder.build(),
        inputDestinationsBuilder.build(),
        remoteOutputCodersBuilder.build(),
        sideInputSpecs,
        bagUserStateSpecs,
        timerSpecs);
  }

  /**
   * Patches the input coder of the transform to ensure that the byte representation of input used
   * at the Runner, matches the byte representation received from the SDK Harness.
   */
  private static void lengthPrefixAnyInputCoder(
      String inputPCollectionId, Components.Builder componentsBuilder) {
    RunnerApi.PCollection pcollection =
        componentsBuilder.getPcollectionsOrThrow(inputPCollectionId);
    String newInputCoderId =
        LengthPrefixUnknownCoders.addLengthPrefixedCoder(
            pcollection.getCoderId(), componentsBuilder, false);
    componentsBuilder.putPcollections(
        inputPCollectionId, pcollection.toBuilder().setCoderId(newInputCoderId).build());
  }

  private static Map<String, Coder<WindowedValue<?>>> addStageOutputs(
      ApiServiceDescriptor dataEndpoint,
      Collection<PCollectionNode> outputPCollections,
      Components.Builder components,
      Collection<WireCoderSetting> wireCoderSettings)
      throws IOException {
    Map<String, Coder<WindowedValue<?>>> remoteOutputCoders = new LinkedHashMap<>();
    for (PCollectionNode outputPCollection : outputPCollections) {
      WireCoderSetting wireCoderSetting =
          wireCoderSettings.stream()
              .filter(ws -> ws.getInputOrOutputId().equals(outputPCollection.getId()))
              .findAny()
              .orElse(WireCoderSetting.getDefaultInstance());
      OutputEncoding outputEncoding =
          addStageOutput(dataEndpoint, components, outputPCollection, wireCoderSetting);
      remoteOutputCoders.put(outputEncoding.getPTransformId(), outputEncoding.getCoder());
    }
    return remoteOutputCoders;
  }

  private static RemoteInputDestination<WindowedValue<?>> addStageInput(
      ApiServiceDescriptor dataEndpoint,
      PCollectionNode inputPCollection,
      Components.Builder components,
      WireCoderSetting wireCoderSetting)
      throws IOException {
    String inputWireCoderId =
        WireCoders.addSdkWireCoder(inputPCollection, components, wireCoderSetting);
    @SuppressWarnings("unchecked")
    Coder<WindowedValue<?>> wireCoder =
        (Coder)
            WireCoders.instantiateRunnerWireCoder(
                inputPCollection, components.build(), wireCoderSetting);

    RemoteGrpcPort inputPort =
        RemoteGrpcPort.newBuilder()
            .setApiServiceDescriptor(dataEndpoint)
            .setCoderId(inputWireCoderId)
            .build();
    String inputId =
        uniqueId(
            String.format("fn/read/%s", inputPCollection.getId()), components::containsTransforms);
    PTransform inputTransform =
        RemoteGrpcPortRead.readFromPort(inputPort, inputPCollection.getId()).toPTransform();
    components.putTransforms(inputId, inputTransform);
    return RemoteInputDestination.of(wireCoder, inputId);
  }

  private static OutputEncoding addStageOutput(
      ApiServiceDescriptor dataEndpoint,
      Components.Builder components,
      PCollectionNode outputPCollection,
      WireCoderSetting wireCoderSetting)
      throws IOException {
    String outputWireCoderId =
        WireCoders.addSdkWireCoder(outputPCollection, components, wireCoderSetting);
    @SuppressWarnings("unchecked")
    Coder<WindowedValue<?>> wireCoder =
        (Coder)
            WireCoders.instantiateRunnerWireCoder(
                outputPCollection, components.build(), wireCoderSetting);
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
            components::containsTransforms);
    PTransform outputTransform = outputWrite.toPTransform();
    components.putTransforms(outputId, outputTransform);
    return new AutoValue_ProcessBundleDescriptors_OutputEncoding(outputId, wireCoder);
  }

  public static Map<String, Map<String, SideInputSpec>> getSideInputs(ExecutableStage stage)
      throws IOException {
    return addSideInputs(stage, stage.getComponents().toBuilder());
  }

  private static Map<String, Map<String, SideInputSpec>> addSideInputs(
      ExecutableStage stage, Components.Builder components) throws IOException {
    ImmutableTable.Builder<String, String, SideInputSpec> idsToSpec = ImmutableTable.builder();
    for (SideInputReference sideInputReference : stage.getSideInputs()) {
      // Update the coder specification for side inputs to be length prefixed so that the
      // SDK and Runner agree on how to encode/decode the key, window, and values for
      // side inputs.
      PCollectionNode pcNode = sideInputReference.collection();
      PCollection pc = pcNode.getPCollection();
      String lengthPrefixedCoderId =
          LengthPrefixUnknownCoders.addLengthPrefixedCoder(pc.getCoderId(), components, false);
      components.putPcollections(
          pcNode.getId(), pc.toBuilder().setCoderId(lengthPrefixedCoderId).build());

      FullWindowedValueCoder<KV<?, ?>> coder =
          (FullWindowedValueCoder)
              WireCoders.instantiateRunnerWireCoder(pcNode, components.build());
      idsToSpec.put(
          sideInputReference.transform().getId(),
          sideInputReference.localName(),
          SideInputSpec.of(
              sideInputReference.transform().getId(),
              sideInputReference.localName(),
              getAccessPattern(sideInputReference),
              coder.getValueCoder(),
              coder.getWindowCoder()));
    }
    return idsToSpec.build().rowMap();
  }

  private static RunnerApi.FunctionSpec getAccessPattern(SideInputReference sideInputReference) {
    try {
      return RunnerApi.ParDoPayload.parseFrom(
              sideInputReference.transform().getTransform().getSpec().getPayload())
          .getSideInputsMap()
          .get(sideInputReference.localName())
          .getAccessPattern();
    } catch (InvalidProtocolBufferException e) {
      throw new RuntimeException(e);
    }
  }

  private static Map<String, Map<String, BagUserStateSpec>> forBagUserStates(
      ExecutableStage stage, Components components) throws IOException {
    ImmutableTable.Builder<String, String, BagUserStateSpec> idsToSpec = ImmutableTable.builder();
    for (UserStateReference userStateReference : stage.getUserStates()) {
      FullWindowedValueCoder<KV<?, ?>> coder =
          (FullWindowedValueCoder)
              WireCoders.instantiateRunnerWireCoder(userStateReference.collection(), components);
      idsToSpec.put(
          userStateReference.transform().getId(),
          userStateReference.localName(),
          BagUserStateSpec.of(
              userStateReference.transform().getId(),
              userStateReference.localName(),
              // We use the ByteString coder to save on encoding and decoding the actual key.
              ByteStringCoder.of(),
              // Usage of the ByteStringCoder provides a significant simplification for handling
              // a logical stream of values by not needing to know where the element boundaries
              // actually are. See StateRequestHandlers.java for further details.
              ByteStringCoder.of(),
              coder.getWindowCoder()));
    }
    return idsToSpec.build().rowMap();
  }

  private static Map<String, Map<String, TimerSpec>> forTimerSpecs(
      ExecutableStage stage, Components.Builder components) throws IOException {
    ImmutableTable.Builder<String, String, TimerSpec> idsToSpec = ImmutableTable.builder();
    for (TimerReference timerReference : stage.getTimers()) {
      RunnerApi.ParDoPayload payload =
          RunnerApi.ParDoPayload.parseFrom(
              timerReference.transform().getTransform().getSpec().getPayload());
      RunnerApi.TimerFamilySpec timerFamilySpec =
          payload.getTimerFamilySpecsOrThrow(timerReference.localName());
      org.apache.beam.sdk.state.TimerSpec spec;
      switch (timerFamilySpec.getTimeDomain()) {
        case EVENT_TIME:
          spec = TimerSpecs.timer(TimeDomain.EVENT_TIME);
          break;
        case PROCESSING_TIME:
          spec = TimerSpecs.timer(TimeDomain.PROCESSING_TIME);
          break;
        default:
          throw new IllegalArgumentException(
              String.format(
                  "Unknown or unsupported time domain %s", timerFamilySpec.getTimeDomain()));
      }

      for (WireCoderSetting wireCoderSetting : stage.getWireCoderSettings()) {
        if (wireCoderSetting.hasTimer()
            && wireCoderSetting
                .getTimer()
                .getTransformId()
                .equals(timerReference.transform().getId())
            && wireCoderSetting.getTimer().getLocalName().equals(timerReference.localName())) {
          throw new UnsupportedOperationException(
              "WireCoderSetting for timer is yet to be supported.");
        }
      }

      String originalTimerCoderId = timerFamilySpec.getTimerFamilyCoderId();
      String sdkCoderId =
          LengthPrefixUnknownCoders.addLengthPrefixedCoder(originalTimerCoderId, components, false);
      String runnerCoderId =
          LengthPrefixUnknownCoders.addLengthPrefixedCoder(originalTimerCoderId, components, true);
      Coder<?> timerCoder =
          RehydratedComponents.forComponents(components.build()).getCoder(runnerCoderId);
      checkArgument(
          timerCoder instanceof Timer.Coder, "Expected a timer coder but received %s.", timerCoder);

      RunnerApi.FunctionSpec.Builder updatedSpec =
          components
              .getTransformsOrThrow(timerReference.transform().getId())
              .toBuilder()
              .getSpecBuilder();
      RunnerApi.ParDoPayload.Builder updatedPayload =
          RunnerApi.ParDoPayload.parseFrom(updatedSpec.getPayload()).toBuilder();
      updatedPayload.putTimerFamilySpecs(
          timerReference.localName(),
          updatedPayload
              .getTimerFamilySpecsOrThrow(timerReference.localName())
              .toBuilder()
              .setTimerFamilyCoderId(sdkCoderId)
              .build());
      updatedSpec.setPayload(updatedPayload.build().toByteString());
      components.putTransforms(
          timerReference.transform().getId(),
          // Since a transform can have more then one timer, update the transform inside components
          // and not the original
          components
              .getTransformsOrThrow(timerReference.transform().getId())
              .toBuilder()
              .setSpec(updatedSpec)
              .build());

      idsToSpec.put(
          timerReference.transform().getId(),
          timerReference.localName(),
          TimerSpec.of(
              timerReference.transform().getId(),
              timerReference.localName(),
              spec,
              (Coder) timerCoder));
    }
    return idsToSpec.build().rowMap();
  }

  @AutoValue
  @AutoValue.CopyAnnotations
  @SuppressWarnings({"rawtypes"})
  abstract static class OutputEncoding {
    abstract String getPTransformId();

    abstract Coder<WindowedValue<?>> getCoder();
  }

  /**
   * A container type storing references to the value, and window {@link Coder} used when handling
   * side input state requests.
   */
  @AutoValue
  @AutoValue.CopyAnnotations
  @SuppressWarnings({"rawtypes"})
  public abstract static class SideInputSpec<T, W extends BoundedWindow> {
    public static <T, W extends BoundedWindow> SideInputSpec of(
        String transformId,
        String sideInputId,
        RunnerApi.FunctionSpec accessPattern,
        Coder<T> elementCoder,
        Coder<W> windowCoder) {
      return new AutoValue_ProcessBundleDescriptors_SideInputSpec(
          transformId, sideInputId, accessPattern, elementCoder, windowCoder);
    }

    public abstract String transformId();

    public abstract String sideInputId();

    public abstract RunnerApi.FunctionSpec accessPattern();

    public abstract Coder<T> elementCoder();

    public abstract Coder<W> windowCoder();
  }

  /**
   * A container type storing references to the key, value, and window {@link Coder} used when
   * handling bag user state requests.
   */
  @AutoValue
  @AutoValue.CopyAnnotations
  @SuppressWarnings({"rawtypes"})
  public abstract static class BagUserStateSpec<K, V, W extends BoundedWindow> {
    static <K, V, W extends BoundedWindow> BagUserStateSpec<K, V, W> of(
        String transformId,
        String userStateId,
        Coder<K> keyCoder,
        Coder<V> valueCoder,
        Coder<W> windowCoder) {
      return new AutoValue_ProcessBundleDescriptors_BagUserStateSpec(
          transformId, userStateId, keyCoder, valueCoder, windowCoder);
    }

    public abstract String transformId();

    public abstract String userStateId();

    public abstract Coder<K> keyCoder();

    public abstract Coder<V> valueCoder();

    public abstract Coder<W> windowCoder();
  }

  /**
   * A container type storing references to the key, timer and payload coders and the remote input
   * destination used when handling timer requests.
   */
  @AutoValue
  @AutoValue.CopyAnnotations
  @SuppressWarnings({"rawtypes"})
  public abstract static class TimerSpec<K, V, W extends BoundedWindow> {
    static <K, V, W extends BoundedWindow> TimerSpec<K, V, W> of(
        String transformId,
        String timerId,
        org.apache.beam.sdk.state.TimerSpec timerSpec,
        Coder<Timer<K>> coder) {
      return new AutoValue_ProcessBundleDescriptors_TimerSpec(
          transformId, timerId, timerSpec, coder);
    }

    public abstract String transformId();

    public abstract String timerId();

    public abstract org.apache.beam.sdk.state.TimerSpec getTimerSpec();

    public abstract Coder<K> coder();
  }

  /** */
  @AutoValue
  @AutoValue.CopyAnnotations
  @SuppressWarnings({"rawtypes"})
  public abstract static class ExecutableProcessBundleDescriptor {
    public static ExecutableProcessBundleDescriptor of(
        ProcessBundleDescriptor descriptor,
        List<RemoteInputDestination> inputDestinations,
        Map<String, Coder> outputTransformCoders,
        Map<String, Map<String, SideInputSpec>> sideInputSpecs,
        Map<String, Map<String, BagUserStateSpec>> bagUserStateSpecs,
        Map<String, Map<String, TimerSpec>> timerSpecs) {
      ImmutableTable.Builder copyOfSideInputSpecs = ImmutableTable.builder();
      for (Map.Entry<String, Map<String, SideInputSpec>> outer : sideInputSpecs.entrySet()) {
        for (Map.Entry<String, SideInputSpec> inner : outer.getValue().entrySet()) {
          copyOfSideInputSpecs.put(outer.getKey(), inner.getKey(), inner.getValue());
        }
      }
      ImmutableTable.Builder copyOfBagUserStateSpecs = ImmutableTable.builder();
      for (Map.Entry<String, Map<String, BagUserStateSpec>> outer : bagUserStateSpecs.entrySet()) {
        for (Map.Entry<String, BagUserStateSpec> inner : outer.getValue().entrySet()) {
          copyOfBagUserStateSpecs.put(outer.getKey(), inner.getKey(), inner.getValue());
        }
      }
      ImmutableTable.Builder copyOfTimerSpecs = ImmutableTable.builder();
      for (Map.Entry<String, Map<String, TimerSpec>> outer : timerSpecs.entrySet()) {
        for (Map.Entry<String, TimerSpec> inner : outer.getValue().entrySet()) {
          copyOfTimerSpecs.put(outer.getKey(), inner.getKey(), inner.getValue());
        }
      }
      return new AutoValue_ProcessBundleDescriptors_ExecutableProcessBundleDescriptor(
          descriptor,
          inputDestinations,
          Collections.unmodifiableMap(outputTransformCoders),
          copyOfSideInputSpecs.build().rowMap(),
          copyOfBagUserStateSpecs.build().rowMap(),
          copyOfTimerSpecs.build().rowMap());
    }

    public abstract ProcessBundleDescriptor getProcessBundleDescriptor();

    /**
     * Get {@link RemoteInputDestination}s that input data are sent to the {@link
     * ProcessBundleDescriptor} over.
     */
    public abstract List<RemoteInputDestination> getRemoteInputDestinations();

    /**
     * Get all of the transforms materialized by this {@link ExecutableProcessBundleDescriptor} and
     * the Java {@link Coder} for the wire format of that transform.
     */
    public abstract Map<String, Coder> getRemoteOutputCoders();

    /**
     * Get a mapping from PTransform id to side input id to {@link SideInputSpec side inputs} that
     * are used during execution.
     */
    public abstract Map<String, Map<String, SideInputSpec>> getSideInputSpecs();

    /**
     * Get a mapping from PTransform id to user state input id to {@link BagUserStateSpec bag user
     * states} that are used during execution.
     */
    public abstract Map<String, Map<String, BagUserStateSpec>> getBagUserStateSpecs();

    /**
     * Get a mapping from PTransform id to timer id to {@link TimerSpec timer specs} that are used
     * during execution.
     */
    public abstract Map<String, Map<String, TimerSpec>> getTimerSpecs();
  }
}
