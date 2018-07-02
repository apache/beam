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
import static org.apache.beam.runners.core.construction.PTransformTranslation.PAR_DO_TRANSFORM_URN;
import static org.apache.beam.sdk.transforms.reflect.DoFnSignatures.getStateSpecOrThrow;
import static org.apache.beam.sdk.transforms.reflect.DoFnSignatures.getTimerSpecOrThrow;

import com.google.auto.service.AutoService;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.model.pipeline.v1.RunnerApi.Components;
import org.apache.beam.model.pipeline.v1.RunnerApi.FunctionSpec;
import org.apache.beam.model.pipeline.v1.RunnerApi.ParDoPayload;
import org.apache.beam.model.pipeline.v1.RunnerApi.Parameter.Type;
import org.apache.beam.model.pipeline.v1.RunnerApi.SdkFunctionSpec;
import org.apache.beam.model.pipeline.v1.RunnerApi.SideInput;
import org.apache.beam.model.pipeline.v1.RunnerApi.SideInput.Builder;
import org.apache.beam.runners.core.construction.PTransformTranslation.TransformPayloadTranslator;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.options.PortablePipelineOptions;
import org.apache.beam.sdk.runners.AppliedPTransform;
import org.apache.beam.sdk.state.StateSpec;
import org.apache.beam.sdk.state.StateSpecs;
import org.apache.beam.sdk.state.TimeDomain;
import org.apache.beam.sdk.state.TimerSpec;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.ParDo.MultiOutput;
import org.apache.beam.sdk.transforms.ViewFn;
import org.apache.beam.sdk.transforms.reflect.DoFnInvokers;
import org.apache.beam.sdk.transforms.reflect.DoFnSignature;
import org.apache.beam.sdk.transforms.reflect.DoFnSignature.Parameter;
import org.apache.beam.sdk.transforms.reflect.DoFnSignature.Parameter.Cases;
import org.apache.beam.sdk.transforms.reflect.DoFnSignature.Parameter.RestrictionTrackerParameter;
import org.apache.beam.sdk.transforms.reflect.DoFnSignature.Parameter.WindowParameter;
import org.apache.beam.sdk.transforms.reflect.DoFnSignature.StateDeclaration;
import org.apache.beam.sdk.transforms.reflect.DoFnSignature.TimerDeclaration;
import org.apache.beam.sdk.transforms.reflect.DoFnSignatures;
import org.apache.beam.sdk.transforms.windowing.WindowMappingFn;
import org.apache.beam.sdk.util.DoFnAndMainOutput;
import org.apache.beam.sdk.util.SerializableUtils;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;

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
      ParDoPayload payload =
          translateParDo(transform.getTransform(), transform.getPipeline(), components);
      return RunnerApi.FunctionSpec.newBuilder()
          .setUrn(PAR_DO_TRANSFORM_URN)
          .setPayload(payload.toByteString())
          .build();
    }

    /** Registers {@link ParDoPayloadTranslator}. */
    @AutoService(TransformPayloadTranslatorRegistrar.class)
    public static class Registrar implements TransformPayloadTranslatorRegistrar {
      @Override
      public Map<? extends Class<? extends PTransform>, ? extends TransformPayloadTranslator>
          getTransformPayloadTranslators() {
        return Collections.singletonMap(ParDo.MultiOutput.class, new ParDoPayloadTranslator());
      }
    }
  }

  public static ParDoPayload translateParDo(
      final ParDo.MultiOutput<?, ?> parDo, Pipeline pipeline, SdkComponents components)
      throws IOException {

    final DoFn<?, ?> doFn = parDo.getFn();
    final DoFnSignature signature = DoFnSignatures.getSignature(doFn.getClass());
    final String restrictionCoderId;
    if (signature.processElement().isSplittable()) {
      final Coder<?> restrictionCoder =
          DoFnInvokers.invokerFor(doFn).invokeGetRestrictionCoder(pipeline.getCoderRegistry());
      restrictionCoderId = components.registerCoder(restrictionCoder);
    } else {
      restrictionCoderId = "";
    }

    return payloadForParDoLike(
        new ParDoLike() {
          @Override
          public SdkFunctionSpec translateDoFn(SdkComponents newComponents) {
            return ParDoTranslation.translateDoFn(
                parDo.getFn(), parDo.getMainOutputTag(), newComponents);
          }

          @Override
          public List<RunnerApi.Parameter> translateParameters() {
            return ParDoTranslation.translateParameters(
                signature.processElement().extraParameters());
          }

          @Override
          public Map<String, SideInput> translateSideInputs(SdkComponents components) {
            Map<String, SideInput> sideInputs = new HashMap<>();
            for (PCollectionView<?> sideInput : parDo.getSideInputs()) {
              sideInputs.put(
                  sideInput.getTagInternal().getId(), translateView(sideInput, components));
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
                  translateStateSpec(getStateSpecOrThrow(state.getValue(), doFn), components);
              stateSpecs.put(state.getKey(), spec);
            }
            return stateSpecs;
          }

          @Override
          public Map<String, RunnerApi.TimerSpec> translateTimerSpecs(SdkComponents newComponents) {
            Map<String, RunnerApi.TimerSpec> timerSpecs = new HashMap<>();
            for (Map.Entry<String, TimerDeclaration> timer :
                signature.timerDeclarations().entrySet()) {
              RunnerApi.TimerSpec spec =
                  translateTimerSpec(getTimerSpecOrThrow(timer.getValue(), doFn));
              timerSpecs.put(timer.getKey(), spec);
            }
            return timerSpecs;
          }

          @Override
          public boolean isSplittable() {
            return signature.processElement().isSplittable();
          }

          @Override
          public String translateRestrictionCoderId(SdkComponents newComponents) {
            return restrictionCoderId;
          }
        },
        components);
  }

  public static List<RunnerApi.Parameter> translateParameters(List<Parameter> params) {
    List<RunnerApi.Parameter> parameters = new ArrayList<>();
    for (Parameter parameter : params) {
      RunnerApi.Parameter protoParameter = translateParameter(parameter);
      if (protoParameter != null) {
        parameters.add(protoParameter);
      }
    }
    return parameters;
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

    SdkComponents components = SdkComponents.create();
    components.registerEnvironment(
        Environments.createEnvironment(
            application
                .getPipeline()
                .getOptions()
                .as(PortablePipelineOptions.class)
                .getWorkerDockerImage()));
    RunnerApi.PTransform protoTransform = PTransformTranslation.toProto(application, components);

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
    sdkComponents.registerEnvironment(
        Environments.createEnvironment(
            application
                .getPipeline()
                .getOptions()
                .as(PortablePipelineOptions.class)
                .getWorkerDockerImage()));
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
          PCollectionViewTranslation.viewFromProto(
              sideInput, sideInputTag, originalPCollection, parDoProto, components));
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

  public static RunnerApi.StateSpec translateStateSpec(
      StateSpec<?> stateSpec, final SdkComponents components) throws IOException {
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
                        .setCombineFn(CombineTranslation.toProto(combineFn, components)))
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

  public static RunnerApi.TimerSpec translateTimerSpec(TimerSpec timer) {
    return RunnerApi.TimerSpec.newBuilder()
        .setTimeDomain(translateTimeDomain(timer.getTimeDomain()))
        .build();
  }

  private static RunnerApi.TimeDomain.Enum translateTimeDomain(TimeDomain timeDomain) {
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

  public static SdkFunctionSpec translateDoFn(
      DoFn<?, ?> fn, TupleTag<?> tag, SdkComponents components) {
    return SdkFunctionSpec.newBuilder()
        .setEnvironmentId(Iterables.getOnlyElement(components.getEnvironmentIds()))
        .setSpec(
            FunctionSpec.newBuilder()
                .setUrn(CUSTOM_JAVA_DO_FN_URN)
                .setPayload(
                    ByteString.copyFrom(
                        SerializableUtils.serializeToByteArray(DoFnAndMainOutput.of(fn, tag))))
                .build())
        .build();
  }

  public static DoFnAndMainOutput doFnAndMainOutputTagFromProto(SdkFunctionSpec fnSpec) {
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

  /**
   * Translates a Java DoFn parameter to a proto representation.
   *
   * <p>Returns {@code null} rather than crashing for parameters that are not yet supported, to
   * allow legacy Java-based runners to perform a proto round-trip and afterwards use {@link
   * DoFnSignatures} to analyze.
   *
   * <p>The proto definition for parameters is provisional and those parameters that are not needed
   * for portability will be removed from the enum.
   */
  // Using nullability instead of optional because of shading
  public static @Nullable RunnerApi.Parameter translateParameter(Parameter parameter) {
    return parameter.match(
        new Cases.WithDefault</* @Nullable in Java 8 */ RunnerApi.Parameter>() {
          @Override
          public RunnerApi.Parameter dispatch(WindowParameter p) {
            return RunnerApi.Parameter.newBuilder().setType(Type.Enum.WINDOW).build();
          }

          @Override
          public RunnerApi.Parameter dispatch(RestrictionTrackerParameter p) {
            return RunnerApi.Parameter.newBuilder().setType(Type.Enum.RESTRICTION_TRACKER).build();
          }

          @Override
          // Java 7 + findbugs limitation. The return type is nullable.
          protected @Nullable RunnerApi.Parameter dispatchDefault(Parameter p) {
            return null;
          }
        });
  }

  public static Map<String, SideInput> translateSideInputs(
      List<PCollectionView<?>> views, SdkComponents components) {
    Map<String, SideInput> sideInputs = new HashMap<>();
    for (PCollectionView<?> sideInput : views) {
      sideInputs.put(
          sideInput.getTagInternal().getId(),
          ParDoTranslation.translateView(sideInput, components));
    }
    return sideInputs;
  }

  public static SideInput translateView(PCollectionView<?> view, SdkComponents components) {
    Builder builder = SideInput.newBuilder();
    builder.setAccessPattern(
        FunctionSpec.newBuilder().setUrn(view.getViewFn().getMaterialization().getUrn()).build());
    builder.setViewFn(translateViewFn(view.getViewFn(), components));
    builder.setWindowMappingFn(translateWindowMappingFn(view.getWindowMappingFn(), components));
    return builder.build();
  }

  public static SdkFunctionSpec translateViewFn(ViewFn<?, ?> viewFn, SdkComponents components) {
    return SdkFunctionSpec.newBuilder()
        .setEnvironmentId(Iterables.getOnlyElement(components.getEnvironmentIds()))
        .setSpec(
            FunctionSpec.newBuilder()
                .setUrn(CUSTOM_JAVA_VIEW_FN_URN)
                .setPayload(ByteString.copyFrom(SerializableUtils.serializeToByteArray(viewFn)))
                .build())
        .build();
  }

  private static <T> ParDoPayload getParDoPayload(AppliedPTransform<?, ?, ?> transform)
      throws IOException {
    SdkComponents components = SdkComponents.create();
    components.registerEnvironment(
        Environments.createEnvironment(
            transform
                .getPipeline()
                .getOptions()
                .as(PortablePipelineOptions.class)
                .getWorkerDockerImage()));
    RunnerApi.PTransform parDoPTransform =
        PTransformTranslation.toProto(transform, Collections.emptyList(), components);
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

  public static SdkFunctionSpec translateWindowMappingFn(
      WindowMappingFn<?> windowMappingFn, SdkComponents components) {
    return SdkFunctionSpec.newBuilder()
        .setEnvironmentId(Iterables.getOnlyElement(components.getEnvironmentIds()))
        .setSpec(
            FunctionSpec.newBuilder()
                .setUrn(CUSTOM_JAVA_WINDOW_MAPPING_FN_URN)
                .setPayload(
                    ByteString.copyFrom(SerializableUtils.serializeToByteArray(windowMappingFn)))
                .build())
        .build();
  }

  /** These methods drive to-proto translation from Java and from rehydrated ParDos. */
  public interface ParDoLike {
    SdkFunctionSpec translateDoFn(SdkComponents newComponents);

    List<RunnerApi.Parameter> translateParameters();

    Map<String, RunnerApi.SideInput> translateSideInputs(SdkComponents components);

    Map<String, RunnerApi.StateSpec> translateStateSpecs(SdkComponents components)
        throws IOException;

    Map<String, RunnerApi.TimerSpec> translateTimerSpecs(SdkComponents newComponents);

    boolean isSplittable();

    String translateRestrictionCoderId(SdkComponents newComponents);
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
        .setRestrictionCoderId(parDo.translateRestrictionCoderId(components))
        .build();
  }
}
