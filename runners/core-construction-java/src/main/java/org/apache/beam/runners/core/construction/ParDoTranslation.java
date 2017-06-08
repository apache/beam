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
import static com.google.common.base.Preconditions.checkState;
import static org.apache.beam.runners.core.construction.PTransformTranslation.PAR_DO_TRANSFORM_URN;

import com.google.auto.service.AutoService;
import com.google.auto.value.AutoValue;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Optional;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import com.google.protobuf.Any;
import com.google.protobuf.ByteString;
import com.google.protobuf.BytesValue;
import com.google.protobuf.InvalidProtocolBufferException;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.beam.runners.core.construction.PTransformTranslation.TransformPayloadTranslator;
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
import org.apache.beam.sdk.runners.AppliedPTransform;
import org.apache.beam.sdk.state.StateSpec;
import org.apache.beam.sdk.state.StateSpecs;
import org.apache.beam.sdk.state.TimeDomain;
import org.apache.beam.sdk.state.TimerSpec;
import org.apache.beam.sdk.transforms.Combine;
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
import org.apache.beam.sdk.values.TupleTagList;
import org.apache.beam.sdk.values.WindowingStrategy;

/**
 * Utilities for interacting with {@link ParDo} instances and {@link ParDoPayload} protos.
 */
public class ParDoTranslation {
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
      implements PTransformTranslation.TransformPayloadTranslator<ParDo.MultiOutput<?, ?>> {
    public static TransformPayloadTranslator create() {
      return new ParDoPayloadTranslator();
    }

    private ParDoPayloadTranslator() {}

    @Override
    public String getUrn(ParDo.MultiOutput<?, ?> transform) {
      return PAR_DO_TRANSFORM_URN;
    }

    @Override
    public FunctionSpec translate(
        AppliedPTransform<?, ?, MultiOutput<?, ?>> transform, SdkComponents components)
        throws IOException {
      ParDoPayload payload = toProto(transform.getTransform(), components);
      return RunnerApi.FunctionSpec.newBuilder()
          .setUrn(PAR_DO_TRANSFORM_URN)
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

  public static ParDoPayload toProto(ParDo.MultiOutput<?, ?> parDo, SdkComponents components)
  throws IOException {
    DoFn<?, ?> doFn = parDo.getFn();
    DoFnSignature signature = DoFnSignatures.getSignature(doFn.getClass());
    Map<String, StateDeclaration> states = signature.stateDeclarations();
    Map<String, TimerDeclaration> timers = signature.timerDeclarations();
    List<Parameter> parameters = signature.processElement().extraParameters();

    ParDoPayload.Builder builder = ParDoPayload.newBuilder();
    builder.setDoFn(toProto(parDo.getFn(), parDo.getMainOutputTag()));
    builder.setSplittable(signature.processElement().isSplittable());
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
      RunnerApi.StateSpec spec =
          toProto(getStateSpecOrCrash(state.getValue(), doFn), components);
      builder.putStateSpecs(state.getKey(), spec);
    }
    for (Map.Entry<String, TimerDeclaration> timer : timers.entrySet()) {
      RunnerApi.TimerSpec spec =
          toProto(getTimerSpecOrCrash(timer.getValue(), doFn));
      builder.putTimerSpecs(timer.getKey(), spec);
    }
    return builder.build();
  }

  private static StateSpec<?> getStateSpecOrCrash(
      StateDeclaration stateDeclaration, DoFn<?, ?> target) {
    try {
      Object fieldValue = stateDeclaration.field().get(target);
      checkState(fieldValue instanceof StateSpec,
          "Malformed %s class %s: state declaration field %s does not have type %s.",
          DoFn.class.getSimpleName(),
          target.getClass().getName(),
          stateDeclaration.field().getName(),
          StateSpec.class);

      return (StateSpec<?>) stateDeclaration.field().get(target);
    } catch (IllegalAccessException exc) {
      throw new RuntimeException(
          String.format(
              "Malformed %s class %s: state declaration field %s is not accessible.",
              DoFn.class.getSimpleName(),
              target.getClass().getName(),
              stateDeclaration.field().getName()));
    }
  }

  private static TimerSpec getTimerSpecOrCrash(
      TimerDeclaration timerDeclaration, DoFn<?, ?> target) {
    try {
      Object fieldValue = timerDeclaration.field().get(target);
      checkState(fieldValue instanceof TimerSpec,
          "Malformed %s class %s: timer declaration field %s does not have type %s.",
          DoFn.class.getSimpleName(),
          target.getClass().getName(),
          timerDeclaration.field().getName(),
          TimerSpec.class);

      return (TimerSpec) timerDeclaration.field().get(target);
    } catch (IllegalAccessException exc) {
      throw new RuntimeException(
          String.format(
              "Malformed %s class %s: timer declaration field %s is not accessible.",
              DoFn.class.getSimpleName(),
              target.getClass().getName(),
              timerDeclaration.field().getName()));
    }
  }

  public static DoFn<?, ?> getDoFn(ParDoPayload payload) throws InvalidProtocolBufferException {
    return doFnAndMainOutputTagFromProto(payload.getDoFn()).getDoFn();
  }

  public static DoFn<?, ?> getDoFn(AppliedPTransform<?, ?, ?> application) throws IOException {
    return getDoFn(getParDoPayload(application));
  }

  public static TupleTag<?> getMainOutputTag(ParDoPayload payload)
      throws InvalidProtocolBufferException {
    return doFnAndMainOutputTagFromProto(payload.getDoFn()).getMainOutputTag();
  }

  public static TupleTag<?> getMainOutputTag(AppliedPTransform<?, ?, ?> application)
      throws IOException {
    return getMainOutputTag(getParDoPayload(application));
  }

  public static TupleTagList getAdditionalOutputTags(AppliedPTransform<?, ?, ?> application)
      throws IOException {

    RunnerApi.PTransform protoTransform =
        PTransformTranslation.toProto(application, SdkComponents.create());

    ParDoPayload payload = protoTransform.getSpec().getParameter().unpack(ParDoPayload.class);

    TupleTag<?> mainOutputTag = getMainOutputTag(payload);

    Set<String> outputTags = protoTransform.getOutputsMap().keySet();

    outputTags = Sets.difference(outputTags, Collections.singleton(mainOutputTag.getId()));

    ArrayList<TupleTag<?>> additionalOutputTags = new ArrayList<>();
    for (String outputTag : outputTags) {
      additionalOutputTags.add(new TupleTag<>(outputTag));
    }
    return TupleTagList.of(additionalOutputTags);
  }

  public static List<PCollectionView<?>> getSideInputs(AppliedPTransform<?, ?, ?> application)
      throws IOException {

    SdkComponents sdkComponents = SdkComponents.create();
    RunnerApi.PTransform parDoProto =
        PTransformTranslation.toProto(application, sdkComponents);
    ParDoPayload payload = parDoProto.getSpec().getParameter().unpack(ParDoPayload.class);

    List<PCollectionView<?>> views = new ArrayList<>();
    for (Map.Entry<String, SideInput> sideInput : payload.getSideInputsMap().entrySet()) {
      views.add(
          fromProto(
              sideInput.getValue(), sideInput.getKey(), parDoProto, sdkComponents.toComponents()));
    }
    return views;
  }

  public static RunnerApi.PCollection getMainInput(
      RunnerApi.PTransform ptransform, Components components) throws IOException {
    checkArgument(
        ptransform.getSpec().getUrn().equals(PAR_DO_TRANSFORM_URN),
        "Unexpected payload type %s",
        ptransform.getSpec().getUrn());
    ParDoPayload payload = ptransform.getSpec().getParameter().unpack(ParDoPayload.class);
    String mainInputId =
        Iterables.getOnlyElement(
            Sets.difference(
                ptransform.getInputsMap().keySet(), payload.getSideInputsMap().keySet()));
    return components.getPcollectionsOrThrow(ptransform.getInputsOrThrow(mainInputId));
  }

  @VisibleForTesting
  static RunnerApi.StateSpec toProto(StateSpec<?> stateSpec, final SdkComponents components)
      throws IOException {
    final RunnerApi.StateSpec.Builder builder = RunnerApi.StateSpec.newBuilder();

    return stateSpec.match(
        new StateSpec.Cases<RunnerApi.StateSpec>() {
          @Override
          public RunnerApi.StateSpec dispatchValue(Coder<?> valueCoder) {
            return builder
                .setValueSpec(
                    RunnerApi.ValueStateSpec.newBuilder()
                        .setCoderId(registerCoderOrThrow(components, valueCoder)))
                .build();
          }

          @Override
          public RunnerApi.StateSpec dispatchBag(Coder<?> elementCoder) {
            return builder
                .setBagSpec(
                    RunnerApi.BagStateSpec.newBuilder()
                        .setElementCoderId(registerCoderOrThrow(components, elementCoder)))
                .build();
          }

          @Override
          public RunnerApi.StateSpec dispatchCombining(
              Combine.CombineFn<?, ?, ?> combineFn, Coder<?> accumCoder) {
            return builder
                .setCombiningSpec(
                    RunnerApi.CombiningStateSpec.newBuilder()
                        .setAccumulatorCoderId(registerCoderOrThrow(components, accumCoder))
                        .setCombineFn(CombineTranslation.toProto(combineFn)))
                .build();
          }

          @Override
          public RunnerApi.StateSpec dispatchMap(Coder<?> keyCoder, Coder<?> valueCoder) {
            return builder
                .setMapSpec(
                    RunnerApi.MapStateSpec.newBuilder()
                        .setKeyCoderId(registerCoderOrThrow(components, keyCoder))
                        .setValueCoderId(registerCoderOrThrow(components, valueCoder)))
                .build();
          }

          @Override
          public RunnerApi.StateSpec dispatchSet(Coder<?> elementCoder) {
            return builder
                .setSetSpec(
                    RunnerApi.SetStateSpec.newBuilder()
                        .setElementCoderId(registerCoderOrThrow(components, elementCoder)))
                .build();
          }
        });
  }

  @VisibleForTesting
  static StateSpec<?> fromProto(RunnerApi.StateSpec stateSpec, RunnerApi.Components components)
      throws IOException {
    switch (stateSpec.getSpecCase()) {
      case VALUE_SPEC:
        return StateSpecs.value(
            CoderTranslation.fromProto(
                components.getCodersMap().get(stateSpec.getValueSpec().getCoderId()), components));
      case BAG_SPEC:
        return StateSpecs.bag(
            CoderTranslation.fromProto(
                components.getCodersMap().get(stateSpec.getBagSpec().getElementCoderId()),
                components));
      case COMBINING_SPEC:
        FunctionSpec combineFnSpec = stateSpec.getCombiningSpec().getCombineFn().getSpec();

        if (!combineFnSpec.getUrn().equals(CombineTranslation.JAVA_SERIALIZED_COMBINE_FN_URN)) {
          throw new UnsupportedOperationException(
              String.format(
                  "Cannot create %s from non-Java %s: %s",
                  StateSpec.class.getSimpleName(),
                  Combine.CombineFn.class.getSimpleName(),
                  combineFnSpec.getUrn()));
        }

        Combine.CombineFn<?, ?, ?> combineFn =
            (Combine.CombineFn<?, ?, ?>)
                SerializableUtils.deserializeFromByteArray(
                    combineFnSpec.getParameter().unpack(BytesValue.class).toByteArray(),
                    Combine.CombineFn.class.getSimpleName());

        // Rawtype coder cast because it is required to be a valid accumulator coder
        // for the CombineFn, by construction
        return StateSpecs.combining(
            (Coder)
                CoderTranslation.fromProto(
                    components
                        .getCodersMap()
                        .get(stateSpec.getCombiningSpec().getAccumulatorCoderId()),
                    components),
            combineFn);

      case MAP_SPEC:
        return StateSpecs.map(
            CoderTranslation.fromProto(
                components.getCodersOrThrow(stateSpec.getMapSpec().getKeyCoderId()), components),
            CoderTranslation.fromProto(
                components.getCodersOrThrow(stateSpec.getMapSpec().getValueCoderId()), components));

      case SET_SPEC:
        return StateSpecs.set(
            CoderTranslation.fromProto(
                components.getCodersMap().get(stateSpec.getSetSpec().getElementCoderId()),
                components));

      case SPEC_NOT_SET:
      default:
        throw new IllegalArgumentException(
            String.format("Unknown %s: %s", RunnerApi.StateSpec.class.getName(), stateSpec));

    }
  }

  private static String registerCoderOrThrow(SdkComponents components, Coder coder) {
    try {
      return components.registerCoder(coder);
    } catch (IOException exc) {
      throw new RuntimeException("Failure to register coder", exc);
    }
  }

  private static RunnerApi.TimerSpec toProto(TimerSpec timer) {
    return RunnerApi.TimerSpec.newBuilder().setTimeDomain(toProto(timer.getTimeDomain())).build();
  }

  private static RunnerApi.TimeDomain toProto(TimeDomain timeDomain) {
    switch(timeDomain) {
      case EVENT_TIME:
        return RunnerApi.TimeDomain.EVENT_TIME;
      case PROCESSING_TIME:
        return RunnerApi.TimeDomain.PROCESSING_TIME;
      case SYNCHRONIZED_PROCESSING_TIME:
        return RunnerApi.TimeDomain.SYNCHRONIZED_PROCESSING_TIME;
      default:
        throw new IllegalArgumentException("Unknown time domain");
    }
  }

  @AutoValue
  abstract static class DoFnAndMainOutput implements Serializable {
    public static DoFnAndMainOutput of(
        DoFn<?, ?> fn, TupleTag<?> tag) {
      return new AutoValue_ParDoTranslation_DoFnAndMainOutput(fn, tag);
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
        WindowingStrategyTranslation.fromProto(
            components.getWindowingStrategiesOrThrow(inputCollection.getWindowingStrategyId()),
            components);
    Coder<?> elemCoder =
        CoderTranslation
            .fromProto(components.getCodersOrThrow(inputCollection.getCoderId()), components);
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

  private static <T> ParDoPayload getParDoPayload(AppliedPTransform<?, ?, ?> transform)
      throws IOException {
    return PTransformTranslation.toProto(
            transform, Collections.<AppliedPTransform<?, ?, ?>>emptyList(), SdkComponents.create())
        .getSpec()
        .getParameter()
        .unpack(ParDoPayload.class);
  }

  public static boolean usesStateOrTimers(AppliedPTransform<?, ?, ?> transform) throws IOException {
    ParDoPayload payload = getParDoPayload(transform);
    return payload.getStateSpecsCount() > 0 || payload.getTimerSpecsCount() > 0;
  }

  public static boolean isSplittable(AppliedPTransform<?, ?, ?> transform) throws IOException {
    ParDoPayload payload = getParDoPayload(transform);
    return payload.getSplittable();
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
