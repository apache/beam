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
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static org.apache.beam.runners.core.construction.PTransformTranslation.PAR_DO_TRANSFORM_URN;

import com.google.auto.service.AutoService;
import com.google.auto.value.AutoValue;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.MoreObjects;
import com.google.common.base.Optional;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.model.pipeline.v1.RunnerApi.Components;
import org.apache.beam.model.pipeline.v1.RunnerApi.FunctionSpec;
import org.apache.beam.model.pipeline.v1.RunnerApi.ParDoPayload;
import org.apache.beam.model.pipeline.v1.RunnerApi.Parameter.Type;
import org.apache.beam.model.pipeline.v1.RunnerApi.SdkFunctionSpec;
import org.apache.beam.model.pipeline.v1.RunnerApi.SideInput;
import org.apache.beam.model.pipeline.v1.RunnerApi.SideInput.Builder;
import org.apache.beam.runners.core.construction.PTransformTranslation.TransformPayloadTranslator;
import org.apache.beam.sdk.coders.Coder;
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
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.PValue;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.apache.beam.sdk.values.WindowingStrategy;

/** Utilities for interacting with {@link ParDo} instances and {@link ParDoPayload} protos. */
public class ParDoTranslation {
  /** The URN for an unknown Java {@link DoFn}. */
  public static final String CUSTOM_JAVA_DO_FN_URN = "urn:beam:dofn:javasdk:0.1";
  /** The URN for an unknown Java {@link ViewFn}. */
  public static final String CUSTOM_JAVA_VIEW_FN_URN = "urn:beam:viewfn:javasdk:0.1";
  /** The URN for an unknown Java {@link WindowMappingFn}. */
  public static final String CUSTOM_JAVA_WINDOW_MAPPING_FN_URN =
      "urn:beam:windowmappingfn:javasdk:0.1";

  /** A {@link TransformPayloadTranslator} for {@link ParDo}. */
  public static class ParDoPayloadTranslator
      implements TransformPayloadTranslator<MultiOutput<?, ?>> {
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
          .setPayload(payload.toByteString())
          .build();
    }

    @Override
    public PTransformTranslation.RawPTransform<?, ?> rehydrate(
        RunnerApi.PTransform protoTransform, RehydratedComponents rehydratedComponents)
        throws IOException {
      return new RawParDo<>(protoTransform, rehydratedComponents);
    }

    /** Registers {@link ParDoPayloadTranslator}. */
    @AutoService(TransformPayloadTranslatorRegistrar.class)
    public static class Registrar implements TransformPayloadTranslatorRegistrar {
      @Override
      public Map<? extends Class<? extends PTransform>, ? extends TransformPayloadTranslator>
          getTransformPayloadTranslators() {
        return Collections.singletonMap(ParDo.MultiOutput.class, new ParDoPayloadTranslator());
      }

      @Override
      public Map<String, ? extends TransformPayloadTranslator> getTransformRehydrators() {
        return Collections.singletonMap(PAR_DO_TRANSFORM_URN, new ParDoPayloadTranslator());
      }
    }
  }

  public static ParDoPayload toProto(final ParDo.MultiOutput<?, ?> parDo, SdkComponents components)
      throws IOException {

    final DoFn<?, ?> doFn = parDo.getFn();
    final DoFnSignature signature = DoFnSignatures.getSignature(doFn.getClass());

    return payloadForParDoLike(
        new ParDoLike() {
          @Override
          public SdkFunctionSpec translateDoFn(SdkComponents newComponents) {
            return toProto(parDo.getFn(), parDo.getMainOutputTag());
          }

          @Override
          public List<RunnerApi.Parameter> translateParameters() {
            List<RunnerApi.Parameter> parameters = new ArrayList<>();
            for (Parameter parameter : signature.processElement().extraParameters()) {
              Optional<RunnerApi.Parameter> protoParameter = toProto(parameter);
              if (protoParameter.isPresent()) {
                parameters.add(protoParameter.get());
              }
            }
            return parameters;
          }

          @Override
          public Map<String, SideInput> translateSideInputs(SdkComponents components) {
            Map<String, SideInput> sideInputs = new HashMap<>();
            for (PCollectionView<?> sideInput : parDo.getSideInputs()) {
              sideInputs.put(sideInput.getTagInternal().getId(), toProto(sideInput));
            }
            return sideInputs;
          }

          @Override
          public Map<String, RunnerApi.StateSpec> translateStateSpecs(SdkComponents components)
              throws IOException {
            Map<String, RunnerApi.StateSpec> stateSpecs = new HashMap<>();
            for (Map.Entry<String, StateDeclaration> state :
                signature.stateDeclarations().entrySet()) {
              RunnerApi.StateSpec spec =
                  toProto(getStateSpecOrCrash(state.getValue(), doFn), components);
              stateSpecs.put(state.getKey(), spec);
            }
            return stateSpecs;
          }

          @Override
          public Map<String, RunnerApi.TimerSpec> translateTimerSpecs(SdkComponents newComponents) {
            Map<String, RunnerApi.TimerSpec> timerSpecs = new HashMap<>();
            for (Map.Entry<String, TimerDeclaration> timer :
                signature.timerDeclarations().entrySet()) {
              RunnerApi.TimerSpec spec = toProto(getTimerSpecOrCrash(timer.getValue(), doFn));
              timerSpecs.put(timer.getKey(), spec);
            }
            return timerSpecs;
          }

          @Override
          public boolean isSplittable() {
            return signature.processElement().isSplittable();
          }
        },
        components);
  }

  private static StateSpec<?> getStateSpecOrCrash(
      StateDeclaration stateDeclaration, DoFn<?, ?> target) {
    try {
      Object fieldValue = stateDeclaration.field().get(target);
      checkState(
          fieldValue instanceof StateSpec,
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
      checkState(
          fieldValue instanceof TimerSpec,
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
    PTransform<?, ?> transform = application.getTransform();
    if (transform instanceof ParDo.MultiOutput) {
      return ((ParDo.MultiOutput<?, ?>) transform).getFn();
    }

    return getDoFn(getParDoPayload(application));
  }

  public static TupleTag<?> getMainOutputTag(ParDoPayload payload)
      throws InvalidProtocolBufferException {
    return doFnAndMainOutputTagFromProto(payload.getDoFn()).getMainOutputTag();
  }

  public static TupleTag<?> getMainOutputTag(AppliedPTransform<?, ?, ?> application)
      throws IOException {
    PTransform<?, ?> transform = application.getTransform();
    if (transform instanceof ParDo.MultiOutput) {
      return ((ParDo.MultiOutput<?, ?>) transform).getMainOutputTag();
    }

    return getMainOutputTag(getParDoPayload(application));
  }

  public static TupleTagList getAdditionalOutputTags(AppliedPTransform<?, ?, ?> application)
      throws IOException {
    PTransform<?, ?> transform = application.getTransform();
    if (transform instanceof ParDo.MultiOutput) {
      return ((ParDo.MultiOutput<?, ?>) transform).getAdditionalOutputTags();
    }

    RunnerApi.PTransform protoTransform =
        PTransformTranslation.toProto(application, SdkComponents.create());

    ParDoPayload payload = ParDoPayload.parseFrom(protoTransform.getSpec().getPayload());
    TupleTag<?> mainOutputTag = getMainOutputTag(payload);
    Set<String> outputTags =
        Sets.difference(
            protoTransform.getOutputsMap().keySet(), Collections.singleton(mainOutputTag.getId()));

    ArrayList<TupleTag<?>> additionalOutputTags = new ArrayList<>();
    for (String outputTag : outputTags) {
      additionalOutputTags.add(new TupleTag<>(outputTag));
    }
    return TupleTagList.of(additionalOutputTags);
  }

  public static List<PCollectionView<?>> getSideInputs(AppliedPTransform<?, ?, ?> application)
      throws IOException {
    PTransform<?, ?> transform = application.getTransform();
    if (transform instanceof ParDo.MultiOutput) {
      return ((ParDo.MultiOutput<?, ?>) transform).getSideInputs();
    }

    SdkComponents sdkComponents = SdkComponents.create();
    RunnerApi.PTransform parDoProto = PTransformTranslation.toProto(application, sdkComponents);
    ParDoPayload payload = ParDoPayload.parseFrom(parDoProto.getSpec().getPayload());

    List<PCollectionView<?>> views = new ArrayList<>();
    RehydratedComponents components =
        RehydratedComponents.forComponents(sdkComponents.toComponents());
    for (Map.Entry<String, SideInput> sideInputEntry : payload.getSideInputsMap().entrySet()) {
      String sideInputTag = sideInputEntry.getKey();
      RunnerApi.SideInput sideInput = sideInputEntry.getValue();
      PCollection<?> originalPCollection =
          checkNotNull(
              (PCollection<?>) application.getInputs().get(new TupleTag<>(sideInputTag)),
              "no input with tag %s",
              sideInputTag);
      views.add(
          viewFromProto(sideInput, sideInputTag, originalPCollection, parDoProto, components));
    }
    return views;
  }

  public static RunnerApi.PCollection getMainInput(
      RunnerApi.PTransform ptransform, Components components) throws IOException {
    checkArgument(
        ptransform.getSpec().getUrn().equals(PAR_DO_TRANSFORM_URN),
        "Unexpected payload type %s",
        ptransform.getSpec().getUrn());
    ParDoPayload payload = ParDoPayload.parseFrom(ptransform.getSpec().getPayload());
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
  static StateSpec<?> fromProto(RunnerApi.StateSpec stateSpec, RehydratedComponents components)
      throws IOException {
    switch (stateSpec.getSpecCase()) {
      case VALUE_SPEC:
        return StateSpecs.value(components.getCoder(stateSpec.getValueSpec().getCoderId()));
      case BAG_SPEC:
        return StateSpecs.bag(components.getCoder(stateSpec.getBagSpec().getElementCoderId()));
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
                    combineFnSpec.getPayload().toByteArray(),
                    Combine.CombineFn.class.getSimpleName());

        // Rawtype coder cast because it is required to be a valid accumulator coder
        // for the CombineFn, by construction
        return StateSpecs.combining(
            (Coder) components.getCoder(stateSpec.getCombiningSpec().getAccumulatorCoderId()),
            combineFn);

      case MAP_SPEC:
        return StateSpecs.map(
            components.getCoder(stateSpec.getMapSpec().getKeyCoderId()),
            components.getCoder(stateSpec.getMapSpec().getValueCoderId()));

      case SET_SPEC:
        return StateSpecs.set(components.getCoder(stateSpec.getSetSpec().getElementCoderId()));

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

  private static RunnerApi.TimeDomain.Enum toProto(TimeDomain timeDomain) {
    switch (timeDomain) {
      case EVENT_TIME:
        return RunnerApi.TimeDomain.Enum.EVENT_TIME;
      case PROCESSING_TIME:
        return RunnerApi.TimeDomain.Enum.PROCESSING_TIME;
      case SYNCHRONIZED_PROCESSING_TIME:
        return RunnerApi.TimeDomain.Enum.SYNCHRONIZED_PROCESSING_TIME;
      default:
        throw new IllegalArgumentException("Unknown time domain");
    }
  }

  @AutoValue
  abstract static class DoFnAndMainOutput implements Serializable {
    public static DoFnAndMainOutput of(DoFn<?, ?> fn, TupleTag<?> tag) {
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
                .setPayload(
                    ByteString.copyFrom(
                        SerializableUtils.serializeToByteArray(DoFnAndMainOutput.of(fn, tag))))
                .build())
        .build();
  }

  private static DoFnAndMainOutput doFnAndMainOutputTagFromProto(SdkFunctionSpec fnSpec)
      throws InvalidProtocolBufferException {
    checkArgument(
        fnSpec.getSpec().getUrn().equals(CUSTOM_JAVA_DO_FN_URN),
        "Expected %s to be %s with URN %s, but URN was %s",
        DoFn.class.getSimpleName(),
        FunctionSpec.class.getSimpleName(),
        CUSTOM_JAVA_DO_FN_URN,
        fnSpec.getSpec().getUrn());
    byte[] serializedFn = fnSpec.getSpec().getPayload().toByteArray();
    return (DoFnAndMainOutput)
        SerializableUtils.deserializeFromByteArray(serializedFn, "Custom DoFn And Main Output tag");
  }

  private static Optional<RunnerApi.Parameter> toProto(Parameter parameter) {
    return parameter.match(
        new Cases.WithDefault<Optional<RunnerApi.Parameter>>() {
          @Override
          public Optional<RunnerApi.Parameter> dispatch(WindowParameter p) {
            return Optional.of(RunnerApi.Parameter.newBuilder().setType(Type.Enum.WINDOW).build());
          }

          @Override
          public Optional<RunnerApi.Parameter> dispatch(RestrictionTrackerParameter p) {
            return Optional.of(
                RunnerApi.Parameter.newBuilder().setType(Type.Enum.RESTRICTION_TRACKER).build());
          }

          @Override
          protected Optional<RunnerApi.Parameter> dispatchDefault(Parameter p) {
            return Optional.absent();
          }
        });
  }

  public static SideInput toProto(PCollectionView<?> view) {
    Builder builder = SideInput.newBuilder();
    builder.setAccessPattern(
        FunctionSpec.newBuilder().setUrn(view.getViewFn().getMaterialization().getUrn()).build());
    builder.setViewFn(toProto(view.getViewFn()));
    builder.setWindowMappingFn(toProto(view.getWindowMappingFn()));
    return builder.build();
  }

  /**
   * Create a {@link PCollectionView} from a side input spec and an already-deserialized {@link
   * PCollection} that should be wired up.
   */
  public static PCollectionView<?> viewFromProto(
      SideInput sideInput,
      String localName,
      PCollection<?> pCollection,
      RunnerApi.PTransform parDoTransform,
      RehydratedComponents components)
      throws IOException {
    checkArgument(
        localName != null,
        "%s.viewFromProto: localName must not be null",
        ParDoTranslation.class.getSimpleName());
    TupleTag<?> tag = new TupleTag<>(localName);
    WindowMappingFn<?> windowMappingFn = windowMappingFnFromProto(sideInput.getWindowMappingFn());
    ViewFn<?, ?> viewFn = viewFnFromProto(sideInput.getViewFn());

    WindowingStrategy<?, ?> windowingStrategy = pCollection.getWindowingStrategy().fixDefaults();
    checkArgument(
        sideInput.getAccessPattern().getUrn().equals(Materializations.MULTIMAP_MATERIALIZATION_URN),
        "Unknown View Materialization URN %s",
        sideInput.getAccessPattern().getUrn());

    PCollectionView<?> view =
        new RunnerPCollectionView<>(
            pCollection,
            (TupleTag) tag,
            (ViewFn) viewFn,
            windowMappingFn,
            windowingStrategy,
            (Coder) pCollection.getCoder());
    return view;
  }

  private static SdkFunctionSpec toProto(ViewFn<?, ?> viewFn) {
    return SdkFunctionSpec.newBuilder()
        .setSpec(
            FunctionSpec.newBuilder()
                .setUrn(CUSTOM_JAVA_VIEW_FN_URN)
                .setPayload(ByteString.copyFrom(SerializableUtils.serializeToByteArray(viewFn)))
                .build())
        .build();
  }

  private static <T> ParDoPayload getParDoPayload(AppliedPTransform<?, ?, ?> transform)
      throws IOException {
    RunnerApi.PTransform parDoPTransform =
        PTransformTranslation.toProto(
            transform, Collections.<AppliedPTransform<?, ?, ?>>emptyList(), SdkComponents.create());
    return ParDoPayload.parseFrom(parDoPTransform.getSpec().getPayload());
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
            spec.getPayload().toByteArray(), "Custom ViewFn");
  }

  private static SdkFunctionSpec toProto(WindowMappingFn<?> windowMappingFn) {
    return SdkFunctionSpec.newBuilder()
        .setSpec(
            FunctionSpec.newBuilder()
                .setUrn(CUSTOM_JAVA_WINDOW_MAPPING_FN_URN)
                .setPayload(
                    ByteString.copyFrom(SerializableUtils.serializeToByteArray(windowMappingFn)))
                .build())
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
            spec.getPayload().toByteArray(), "Custom WinodwMappingFn");
  }

  static class RawParDo<InputT, OutputT>
      extends PTransformTranslation.RawPTransform<PCollection<InputT>, PCollection<OutputT>>
      implements ParDoLike {

    private final RunnerApi.PTransform protoTransform;
    private final transient RehydratedComponents rehydratedComponents;

    // Parsed from protoTransform and cached
    private final FunctionSpec spec;
    private final ParDoPayload payload;

    public RawParDo(RunnerApi.PTransform protoTransform, RehydratedComponents rehydratedComponents)
        throws IOException {
      this.rehydratedComponents = rehydratedComponents;
      this.protoTransform = protoTransform;
      this.spec = protoTransform.getSpec();
      this.payload = ParDoPayload.parseFrom(spec.getPayload());
    }

    @Override
    public FunctionSpec getSpec() {
      return spec;
    }

    @Override
    public FunctionSpec migrate(SdkComponents components) throws IOException {
      return FunctionSpec.newBuilder()
          .setUrn(PAR_DO_TRANSFORM_URN)
          .setPayload(payloadForParDoLike(this, components).toByteString())
          .build();
    }

    @Override
    public Map<TupleTag<?>, PValue> getAdditionalInputs() {
      Map<TupleTag<?>, PValue> additionalInputs = new HashMap<>();
      for (Map.Entry<String, SideInput> sideInputEntry : payload.getSideInputsMap().entrySet()) {
        try {
          additionalInputs.put(
              new TupleTag<>(sideInputEntry.getKey()),
              rehydratedComponents.getPCollection(
                  protoTransform.getInputsOrThrow(sideInputEntry.getKey())));
        } catch (IOException exc) {
          throw new IllegalStateException(
              String.format(
                  "Could not find input with name %s for %s transform",
                  sideInputEntry.getKey(), ParDo.class.getSimpleName()));
        }
      }
      return additionalInputs;
    }

    @Override
    public SdkFunctionSpec translateDoFn(SdkComponents newComponents) {
      // TODO: re-register the environment with the new components
      return payload.getDoFn();
    }

    @Override
    public List<RunnerApi.Parameter> translateParameters() {
      return MoreObjects.firstNonNull(
          payload.getParametersList(), Collections.<RunnerApi.Parameter>emptyList());
    }

    @Override
    public Map<String, SideInput> translateSideInputs(SdkComponents components) {
      // TODO: re-register the PCollections and UDF environments
      return MoreObjects.firstNonNull(
          payload.getSideInputsMap(), Collections.<String, SideInput>emptyMap());
    }

    @Override
    public Map<String, RunnerApi.StateSpec> translateStateSpecs(SdkComponents components) {
      // TODO: re-register the coders
      return MoreObjects.firstNonNull(
          payload.getStateSpecsMap(), Collections.<String, RunnerApi.StateSpec>emptyMap());
    }

    @Override
    public Map<String, RunnerApi.TimerSpec> translateTimerSpecs(SdkComponents newComponents) {
      return MoreObjects.firstNonNull(
          payload.getTimerSpecsMap(), Collections.<String, RunnerApi.TimerSpec>emptyMap());
    }

    @Override
    public boolean isSplittable() {
      return payload.getSplittable();
    }
  }

  /** These methods drive to-proto translation from Java and from rehydrated ParDos. */
  private interface ParDoLike {
    SdkFunctionSpec translateDoFn(SdkComponents newComponents);

    List<RunnerApi.Parameter> translateParameters();

    Map<String, RunnerApi.SideInput> translateSideInputs(SdkComponents components);

    Map<String, RunnerApi.StateSpec> translateStateSpecs(SdkComponents components)
        throws IOException;

    Map<String, RunnerApi.TimerSpec> translateTimerSpecs(SdkComponents newComponents);

    boolean isSplittable();
  }

  public static ParDoPayload payloadForParDoLike(ParDoLike parDo, SdkComponents components)
      throws IOException {

    return ParDoPayload.newBuilder()
        .setDoFn(parDo.translateDoFn(components))
        .addAllParameters(parDo.translateParameters())
        .putAllStateSpecs(parDo.translateStateSpecs(components))
        .putAllTimerSpecs(parDo.translateTimerSpecs(components))
        .putAllSideInputs(parDo.translateSideInputs(components))
        .setSplittable(parDo.isSplittable())
        .build();
  }
}
