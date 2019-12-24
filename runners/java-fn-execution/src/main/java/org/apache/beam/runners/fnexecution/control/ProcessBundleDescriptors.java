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
import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkState;

import com.google.auto.value.AutoValue;
import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.ProcessBundleDescriptor;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.RemoteGrpcPort;
import org.apache.beam.model.pipeline.v1.Endpoints.ApiServiceDescriptor;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.model.pipeline.v1.RunnerApi.Components;
import org.apache.beam.model.pipeline.v1.RunnerApi.PCollection;
import org.apache.beam.model.pipeline.v1.RunnerApi.PTransform;
import org.apache.beam.runners.core.construction.ModelCoders;
import org.apache.beam.runners.core.construction.SyntheticComponents;
import org.apache.beam.runners.core.construction.graph.ExecutableStage;
import org.apache.beam.runners.core.construction.graph.PipelineNode;
import org.apache.beam.runners.core.construction.graph.PipelineNode.PCollectionNode;
import org.apache.beam.runners.core.construction.graph.PipelineNode.PTransformNode;
import org.apache.beam.runners.core.construction.graph.SideInputReference;
import org.apache.beam.runners.core.construction.graph.TimerReference;
import org.apache.beam.runners.core.construction.graph.UserStateReference;
import org.apache.beam.runners.fnexecution.data.RemoteInputDestination;
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
import org.apache.beam.sdk.values.KV;
import org.apache.beam.vendor.grpc.v1p21p0.com.google.protobuf.InvalidProtocolBufferException;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableTable;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Iterables;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Sets;
import org.apache.beam.vendor.sdk.v2.sdk.extensions.protobuf.ByteStringCoder;

/** Utility methods for creating {@link ProcessBundleDescriptor} instances. */
// TODO: Rename to ExecutableStages?
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

    ImmutableMap.Builder<String, RemoteInputDestination> inputDestinationsBuilder =
        ImmutableMap.builder();
    ImmutableMap.Builder<String, Coder> remoteOutputCodersBuilder = ImmutableMap.builder();

    // The order of these does not matter.
    inputDestinationsBuilder.put(
        stage.getInputPCollection().getId(),
        addStageInput(
            dataEndpoint, stage.getInputPCollection(), components, stage.getWireCoderSetting()));

    remoteOutputCodersBuilder.putAll(
        addStageOutputs(
            dataEndpoint, stage.getOutputPCollections(), components, stage.getWireCoderSetting()));

    Map<String, Map<String, SideInputSpec>> sideInputSpecs = addSideInputs(stage, components);

    Map<String, Map<String, BagUserStateSpec>> bagUserStateSpecs =
        forBagUserStates(stage, components.build());

    Map<String, Map<String, TimerSpec>> timerSpecs =
        forTimerSpecs(
            dataEndpoint, stage, components, inputDestinationsBuilder, remoteOutputCodersBuilder);

    if (bagUserStateSpecs.size() > 0 || timerSpecs.size() > 0) {
      lengthPrefixKeyCoder(stage.getInputPCollection().getId(), components);
    }

    // Copy data from components to ProcessBundleDescriptor.
    ProcessBundleDescriptor.Builder bundleDescriptorBuilder =
        ProcessBundleDescriptor.newBuilder().setId(id);
    if (stateEndpoint != null) {
      bundleDescriptorBuilder.setStateApiServiceDescriptor(stateEndpoint);
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
   * Patches the input coder of a stateful transform to ensure that the byte representation of a key
   * used to partition the input element at the Runner, matches the key byte representation received
   * for state requests and timers from the SDK Harness. Stateful transforms always have a KvCoder
   * as input.
   */
  private static void lengthPrefixKeyCoder(
      String inputColId, Components.Builder componentsBuilder) {
    RunnerApi.PCollection pcollection = componentsBuilder.getPcollectionsOrThrow(inputColId);
    RunnerApi.Coder kvCoder = componentsBuilder.getCodersOrThrow(pcollection.getCoderId());
    Preconditions.checkState(
        ModelCoders.KV_CODER_URN.equals(kvCoder.getSpec().getUrn()),
        "Stateful executable stages must use a KV coder, but is: %s",
        kvCoder.getSpec().getUrn());
    String keyCoderId = ModelCoders.getKvCoderComponents(kvCoder).keyCoderId();
    // Retain the original coder, but wrap in LengthPrefixCoder
    String newKeyCoderId =
        LengthPrefixUnknownCoders.addLengthPrefixedCoder(keyCoderId, componentsBuilder, false);
    // Replace old key coder with LengthPrefixCoder<old_key_coder>
    kvCoder = kvCoder.toBuilder().setComponentCoderIds(0, newKeyCoderId).build();
    componentsBuilder.putCoders(pcollection.getCoderId(), kvCoder);
  }

  private static Map<String, Coder<WindowedValue<?>>> addStageOutputs(
      ApiServiceDescriptor dataEndpoint,
      Collection<PCollectionNode> outputPCollections,
      Components.Builder components,
      RunnerApi.WireCoderSetting wireCoderSetting)
      throws IOException {
    Map<String, Coder<WindowedValue<?>>> remoteOutputCoders = new LinkedHashMap<>();
    for (PCollectionNode outputPCollection : outputPCollections) {
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
      RunnerApi.WireCoderSetting wireCoderSetting)
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
      RunnerApi.WireCoderSetting wireCoderSetting)
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
      ApiServiceDescriptor dataEndpoint,
      ExecutableStage stage,
      Components.Builder components,
      ImmutableMap.Builder<String, RemoteInputDestination> remoteInputsBuilder,
      ImmutableMap.Builder<String, Coder> outputTransformCodersBuilder)
      throws IOException {
    ImmutableTable.Builder<String, String, TimerSpec> idsToSpec = ImmutableTable.builder();
    for (TimerReference timerReference : stage.getTimers()) {
      RunnerApi.ParDoPayload payload =
          RunnerApi.ParDoPayload.parseFrom(
              timerReference.transform().getTransform().getSpec().getPayload());
      RunnerApi.TimeDomain.Enum timeDomain =
          payload.getTimerSpecsOrThrow(timerReference.localName()).getTimeDomain();
      org.apache.beam.sdk.state.TimerSpec spec;
      switch (timeDomain) {
        case EVENT_TIME:
          spec = TimerSpecs.timer(TimeDomain.EVENT_TIME);
          break;
        case PROCESSING_TIME:
          spec = TimerSpecs.timer(TimeDomain.PROCESSING_TIME);
          break;
        case SYNCHRONIZED_PROCESSING_TIME:
          spec = TimerSpecs.timer(TimeDomain.SYNCHRONIZED_PROCESSING_TIME);
          break;
        default:
          throw new IllegalArgumentException(String.format("Unknown time domain %s", timeDomain));
      }

      String mainInputName =
          timerReference
              .transform()
              .getTransform()
              .getInputsOrThrow(
                  Iterables.getOnlyElement(
                      Sets.difference(
                          timerReference.transform().getTransform().getInputsMap().keySet(),
                          Sets.union(
                              payload.getSideInputsMap().keySet(),
                              payload.getTimerSpecsMap().keySet()))));
      String timerCoderId =
          keyValueCoderId(
              components
                  .getCodersOrThrow(components.getPcollectionsOrThrow(mainInputName).getCoderId())
                  .getComponentCoderIds(0),
              payload.getTimerSpecsOrThrow(timerReference.localName()).getTimerCoderId(),
              components);
      RunnerApi.PCollection timerCollectionSpec =
          components
              .getPcollectionsOrThrow(mainInputName)
              .toBuilder()
              .setCoderId(timerCoderId)
              .build();

      // "Unroll" the timers into PCollections.
      String inputTimerPCollectionId =
          SyntheticComponents.uniqueId(
              String.format(
                  "%s.timer.%s.in", timerReference.transform().getId(), timerReference.localName()),
              components.getPcollectionsMap()::containsKey);
      components.putPcollections(inputTimerPCollectionId, timerCollectionSpec);
      remoteInputsBuilder.put(
          inputTimerPCollectionId,
          addStageInput(
              dataEndpoint,
              PipelineNode.pCollection(inputTimerPCollectionId, timerCollectionSpec),
              components,
              stage.getWireCoderSetting()));
      String outputTimerPCollectionId =
          SyntheticComponents.uniqueId(
              String.format(
                  "%s.timer.%s.out",
                  timerReference.transform().getId(), timerReference.localName()),
              components.getPcollectionsMap()::containsKey);
      components.putPcollections(outputTimerPCollectionId, timerCollectionSpec);
      OutputEncoding outputEncoding =
          addStageOutput(
              dataEndpoint,
              components,
              PipelineNode.pCollection(outputTimerPCollectionId, timerCollectionSpec),
              stage.getWireCoderSetting());
      outputTransformCodersBuilder.put(outputEncoding.getPTransformId(), outputEncoding.getCoder());
      components.putTransforms(
          timerReference.transform().getId(),
          // Since a transform can have more then one timer, update the transform inside components
          // and not the original
          components
              .getTransformsOrThrow(timerReference.transform().getId())
              .toBuilder()
              .putInputs(timerReference.localName(), inputTimerPCollectionId)
              .putOutputs(timerReference.localName(), outputTimerPCollectionId)
              .build());

      idsToSpec.put(
          timerReference.transform().getId(),
          timerReference.localName(),
          TimerSpec.of(
              timerReference.transform().getId(),
              timerReference.localName(),
              inputTimerPCollectionId,
              outputTimerPCollectionId,
              outputEncoding.getPTransformId(),
              spec));
    }
    return idsToSpec.build().rowMap();
  }

  private static String keyValueCoderId(
      String keyCoderId, String valueCoderId, Components.Builder components) {
    String id =
        uniqueId(
            String.format("kv-%s-%s", keyCoderId, valueCoderId),
            components.getCodersMap()::containsKey);
    RunnerApi.Coder.Builder coder;
    components.putCoders(
        id,
        RunnerApi.Coder.newBuilder()
            .setSpec(RunnerApi.FunctionSpec.newBuilder().setUrn(ModelCoders.KV_CODER_URN))
            .addComponentCoderIds(keyCoderId)
            .addComponentCoderIds(valueCoderId)
            .build());
    return id;
  }

  @AutoValue
  abstract static class OutputEncoding {
    abstract String getPTransformId();

    abstract Coder<WindowedValue<?>> getCoder();
  }

  /**
   * A container type storing references to the value, and window {@link Coder} used when handling
   * side input state requests.
   */
  @AutoValue
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
  public abstract static class TimerSpec<K, V, W extends BoundedWindow> {
    static <K, V, W extends BoundedWindow> TimerSpec<K, V, W> of(
        String transformId,
        String timerId,
        String inputCollectionId,
        String outputCollectionId,
        String outputTransformId,
        org.apache.beam.sdk.state.TimerSpec timerSpec) {
      return new AutoValue_ProcessBundleDescriptors_TimerSpec(
          transformId,
          timerId,
          inputCollectionId,
          outputCollectionId,
          outputTransformId,
          timerSpec);
    }

    public abstract String transformId();

    public abstract String timerId();

    public abstract String inputCollectionId();

    public abstract String outputCollectionId();

    public abstract String outputTransformId();

    public abstract org.apache.beam.sdk.state.TimerSpec getTimerSpec();
  }

  /** */
  @AutoValue
  public abstract static class ExecutableProcessBundleDescriptor {
    public static ExecutableProcessBundleDescriptor of(
        ProcessBundleDescriptor descriptor,
        Map<String, RemoteInputDestination> inputDestinations,
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
     * Get {@link RemoteInputDestination}s that input data/timers are sent to the {@link
     * ProcessBundleDescriptor} over.
     */
    public abstract Map<String, RemoteInputDestination> getRemoteInputDestinations();

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
