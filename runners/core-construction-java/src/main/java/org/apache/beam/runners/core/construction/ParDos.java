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

import static com.google.common.base.Preconditions.checkArgument;

import com.google.auto.service.AutoService;
import com.google.auto.value.AutoValue;
import com.google.common.base.Optional;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import com.google.protobuf.Any;
import com.google.protobuf.ByteString;
import com.google.protobuf.BytesValue;
import com.google.protobuf.InvalidProtocolBufferException;
import java.io.IOException;
import java.io.Serializable;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.beam.runners.core.construction.PTransforms.TransformPayloadTranslator;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.IterableCoder;
import org.apache.beam.sdk.common.runner.v1.RunnerApi;
import org.apache.beam.sdk.common.runner.v1.RunnerApi.Components;
import org.apache.beam.sdk.common.runner.v1.RunnerApi.FunctionSpec;
import org.apache.beam.sdk.common.runner.v1.RunnerApi.ParDoPayload;
import org.apache.beam.sdk.common.runner.v1.RunnerApi.Parameter.Type;
import org.apache.beam.sdk.common.runner.v1.RunnerApi.SdkFunctionSpec;
import org.apache.beam.sdk.common.runner.v1.RunnerApi.SideInput;
import org.apache.beam.sdk.common.runner.v1.RunnerApi.SideInput.Builder;
import org.apache.beam.sdk.common.runner.v1.RunnerApi.StateSpec;
import org.apache.beam.sdk.common.runner.v1.RunnerApi.TimerSpec;
import org.apache.beam.sdk.runners.AppliedPTransform;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Materializations;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.ParDo.MultiOutput;
import org.apache.beam.sdk.transforms.ViewFn;
import org.apache.beam.sdk.transforms.reflect.DoFnSignature;
import org.apache.beam.sdk.transforms.reflect.DoFnSignature.Parameter;
import org.apache.beam.sdk.transforms.reflect.DoFnSignature.Parameter.Cases;
import org.apache.beam.sdk.transforms.reflect.DoFnSignature.Parameter.RestrictionTrackerParameter;
import org.apache.beam.sdk.transforms.reflect.DoFnSignature.Parameter.WindowParameter;
import org.apache.beam.sdk.transforms.reflect.DoFnSignature.StateDeclaration;
import org.apache.beam.sdk.transforms.reflect.DoFnSignature.TimerDeclaration;
import org.apache.beam.sdk.transforms.reflect.DoFnSignatures;
import org.apache.beam.sdk.transforms.windowing.WindowMappingFn;
import org.apache.beam.sdk.util.SerializableUtils;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.util.WindowedValue.FullWindowedValueCoder;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.WindowingStrategy;

/**
 * Utilities for interacting with {@link ParDo} instances and {@link ParDoPayload} protos.
 */
public class ParDos {
  /**
   * The URN for a {@link ParDoPayload}.
   */
  public static final String PAR_DO_PAYLOAD_URN = "urn:beam:pardo:v1";
  /**
   * The URN for an unknown Java {@link DoFn}.
   */
  public static final String CUSTOM_JAVA_DO_FN_URN = "urn:beam:dofn:javasdk:0.1";
  /**
   * The URN for an unknown Java {@link ViewFn}.
   */
  public static final String CUSTOM_JAVA_VIEW_FN_URN = "urn:beam:viewfn:javasdk:0.1";
  /**
   * The URN for an unknown Java {@link WindowMappingFn}.
   */
  public static final String CUSTOM_JAVA_WINDOW_MAPPING_FN_URN =
      "urn:beam:windowmappingfn:javasdk:0.1";

  /**
   * A {@link TransformPayloadTranslator} for {@link ParDo}.
   */
  public static class ParDoPayloadTranslator
      implements PTransforms.TransformPayloadTranslator<ParDo.MultiOutput<?, ?>> {
    public static TransformPayloadTranslator create() {
      return new ParDoPayloadTranslator();
    }

    private ParDoPayloadTranslator() {}

    @Override
    public FunctionSpec translate(
        AppliedPTransform<?, ?, MultiOutput<?, ?>> transform, SdkComponents components) {
      ParDoPayload payload = toProto(transform.getTransform(), components);
      return RunnerApi.FunctionSpec.newBuilder()
          .setUrn(PAR_DO_PAYLOAD_URN)
          .setParameter(Any.pack(payload))
          .build();
    }

    /**
     * Registers {@link ParDoPayloadTranslator}.
     */
    @AutoService(TransformPayloadTranslatorRegistrar.class)
    public static class Registrar implements TransformPayloadTranslatorRegistrar {
      @Override
      public Map<? extends Class<? extends PTransform>, ? extends TransformPayloadTranslator>
          getTransformPayloadTranslators() {
        return Collections.singletonMap(ParDo.MultiOutput.class, new ParDoPayloadTranslator());
      }
    }
  }

  public static ParDoPayload toProto(ParDo.MultiOutput<?, ?> parDo, SdkComponents components) {
    DoFnSignature signature = DoFnSignatures.getSignature(parDo.getFn().getClass());
    Map<String, StateDeclaration> states = signature.stateDeclarations();
    Map<String, TimerDeclaration> timers = signature.timerDeclarations();
    List<Parameter> parameters = signature.processElement().extraParameters();

    ParDoPayload.Builder builder = ParDoPayload.newBuilder();
    builder.setDoFn(toProto(parDo.getFn(), parDo.getMainOutputTag()));
    for (PCollectionView<?> sideInput : parDo.getSideInputs()) {
      builder.putSideInputs(sideInput.getTagInternal().getId(), toProto(sideInput));
    }
    for (Parameter parameter : parameters) {
      Optional<RunnerApi.Parameter> protoParameter = toProto(parameter);
      if (protoParameter.isPresent()) {
        builder.addParameters(protoParameter.get());
      }
    }
    for (Map.Entry<String, StateDeclaration> state : states.entrySet()) {
      StateSpec spec = toProto(state.getValue());
      builder.putStateSpecs(state.getKey(), spec);
    }
    for (Map.Entry<String, TimerDeclaration> timer : timers.entrySet()) {
      TimerSpec spec = toProto(timer.getValue());
      builder.putTimerSpecs(timer.getKey(), spec);
    }
    return builder.build();
  }

  public static DoFn<?, ?> getDoFn(ParDoPayload payload) throws InvalidProtocolBufferException {
    return doFnAndMainOutputTagFromProto(payload.getDoFn()).getDoFn();
  }

  public static TupleTag<?> getMainOutputTag(ParDoPayload payload)
      throws InvalidProtocolBufferException {
    return doFnAndMainOutputTagFromProto(payload.getDoFn()).getMainOutputTag();
  }

  public static RunnerApi.PCollection getMainInput(
      RunnerApi.PTransform ptransform, Components components) throws IOException {
    checkArgument(
        ptransform.getSpec().getUrn().equals(PAR_DO_PAYLOAD_URN),
        "Unexpected payload type %s",
        ptransform.getSpec().getUrn());
    ParDoPayload payload = ptransform.getSpec().getParameter().unpack(ParDoPayload.class);
    String mainInputId =
        Iterables.getOnlyElement(
            Sets.difference(
                ptransform.getInputsMap().keySet(), payload.getSideInputsMap().keySet()));
    return components.getPcollectionsOrThrow(ptransform.getInputsOrThrow(mainInputId));
  }

  // TODO: Implement
  private static StateSpec toProto(StateDeclaration state) {
    throw new UnsupportedOperationException("Not yet supported");
  }

  // TODO: Implement
  private static TimerSpec toProto(TimerDeclaration timer) {
    throw new UnsupportedOperationException("Not yet supported");
  }

  @AutoValue
  abstract static class DoFnAndMainOutput implements Serializable {
    public static DoFnAndMainOutput of(
        DoFn<?, ?> fn, TupleTag<?> tag) {
      return new AutoValue_ParDos_DoFnAndMainOutput(fn, tag);
    }

    abstract DoFn<?, ?> getDoFn();
    abstract TupleTag<?> getMainOutputTag();
  }

  private static SdkFunctionSpec toProto(DoFn<?, ?> fn, TupleTag<?> tag) {
    return SdkFunctionSpec.newBuilder()
        .setSpec(
            FunctionSpec.newBuilder()
                .setUrn(CUSTOM_JAVA_DO_FN_URN)
                .setParameter(
                    Any.pack(
                        BytesValue.newBuilder()
                            .setValue(
                                ByteString.copyFrom(
                                    SerializableUtils.serializeToByteArray(
                                        DoFnAndMainOutput.of(fn, tag))))
                            .build())))
        .build();
  }

  private static DoFnAndMainOutput doFnAndMainOutputTagFromProto(SdkFunctionSpec fnSpec)
      throws InvalidProtocolBufferException {
    checkArgument(fnSpec.getSpec().getUrn().equals(CUSTOM_JAVA_DO_FN_URN));
    byte[] serializedFn =
        fnSpec.getSpec().getParameter().unpack(BytesValue.class).getValue().toByteArray();
    return (DoFnAndMainOutput)
        SerializableUtils.deserializeFromByteArray(serializedFn, "Custom DoFn And Main Output tag");
  }

  private static Optional<RunnerApi.Parameter> toProto(Parameter parameter) {
    return parameter.match(
        new Cases.WithDefault<Optional<RunnerApi.Parameter>>() {
          @Override
          public Optional<RunnerApi.Parameter> dispatch(WindowParameter p) {
            return Optional.of(RunnerApi.Parameter.newBuilder().setType(Type.WINDOW).build());
          }

          @Override
          public Optional<RunnerApi.Parameter> dispatch(RestrictionTrackerParameter p) {
            return Optional.of(
                RunnerApi.Parameter.newBuilder().setType(Type.RESTRICTION_TRACKER).build());
          }

          @Override
          protected Optional<RunnerApi.Parameter> dispatchDefault(Parameter p) {
            return Optional.absent();
          }
        });
  }

  private static SideInput toProto(PCollectionView<?> view) {
    Builder builder = SideInput.newBuilder();
    builder.setAccessPattern(
        FunctionSpec.newBuilder()
            .setUrn(view.getViewFn().getMaterialization().getUrn())
            .build());
    builder.setViewFn(toProto(view.getViewFn()));
    builder.setWindowMappingFn(toProto(view.getWindowMappingFn()));
    return builder.build();
  }

  public static PCollectionView<?> fromProto(
      SideInput sideInput, String id, RunnerApi.PTransform parDoTransform, Components components)
      throws IOException {
    TupleTag<?> tag = new TupleTag<>(id);
    WindowMappingFn<?> windowMappingFn = windowMappingFnFromProto(sideInput.getWindowMappingFn());
    ViewFn<?, ?> viewFn = viewFnFromProto(sideInput.getViewFn());

    RunnerApi.PCollection inputCollection =
        components.getPcollectionsOrThrow(parDoTransform.getInputsOrThrow(id));
    WindowingStrategy<?, ?> windowingStrategy =
        WindowingStrategies.fromProto(
            components.getWindowingStrategiesOrThrow(inputCollection.getWindowingStrategyId()),
            components);
    Coder<?> elemCoder =
        Coders.fromProto(components.getCodersOrThrow(inputCollection.getCoderId()), components);
    Coder<Iterable<WindowedValue<?>>> coder =
        (Coder)
            IterableCoder.of(
                FullWindowedValueCoder.of(
                    elemCoder, windowingStrategy.getWindowFn().windowCoder()));
    checkArgument(
        sideInput.getAccessPattern().getUrn().equals(Materializations.ITERABLE_MATERIALIZATION_URN),
        "Unknown View Materialization URN %s",
        sideInput.getAccessPattern().getUrn());

    PCollectionView<?> view =
        new RunnerPCollectionView<>(
            (TupleTag<Iterable<WindowedValue<?>>>) tag,
            (ViewFn<Iterable<WindowedValue<?>>, ?>) viewFn,
            windowMappingFn,
            windowingStrategy,
            coder);
    return view;
  }

  private static SdkFunctionSpec toProto(ViewFn<?, ?> viewFn) {
    return SdkFunctionSpec.newBuilder()
        .setSpec(
            FunctionSpec.newBuilder()
                .setUrn(CUSTOM_JAVA_VIEW_FN_URN)
                .setParameter(
                    Any.pack(
                        BytesValue.newBuilder()
                            .setValue(
                                ByteString.copyFrom(SerializableUtils.serializeToByteArray(viewFn)))
                            .build())))
        .build();
  }

  private static ViewFn<?, ?> viewFnFromProto(SdkFunctionSpec viewFn)
      throws InvalidProtocolBufferException {
    FunctionSpec spec = viewFn.getSpec();
    checkArgument(
        spec.getUrn().equals(CUSTOM_JAVA_VIEW_FN_URN),
        "Can't deserialize unknown %s type %s",
        ViewFn.class.getSimpleName(),
        spec.getUrn());
    return (ViewFn<?, ?>)
        SerializableUtils.deserializeFromByteArray(
            spec.getParameter().unpack(BytesValue.class).getValue().toByteArray(), "Custom ViewFn");
  }

  private static SdkFunctionSpec toProto(WindowMappingFn<?> windowMappingFn) {
    return SdkFunctionSpec.newBuilder()
        .setSpec(
            FunctionSpec.newBuilder()
                .setUrn(CUSTOM_JAVA_WINDOW_MAPPING_FN_URN)
                .setParameter(
                    Any.pack(
                        BytesValue.newBuilder()
                            .setValue(
                                ByteString.copyFrom(
                                    SerializableUtils.serializeToByteArray(windowMappingFn)))
                            .build())))
        .build();
  }

  private static WindowMappingFn<?> windowMappingFnFromProto(SdkFunctionSpec windowMappingFn)
      throws InvalidProtocolBufferException {
    FunctionSpec spec = windowMappingFn.getSpec();
    checkArgument(
        spec.getUrn().equals(CUSTOM_JAVA_WINDOW_MAPPING_FN_URN),
        "Can't deserialize unknown %s type %s",
        WindowMappingFn.class.getSimpleName(),
        spec.getUrn());
    return (WindowMappingFn<?>)
        SerializableUtils.deserializeFromByteArray(
            spec.getParameter().unpack(BytesValue.class).getValue().toByteArray(),
            "Custom WinodwMappingFn");
  }
}
