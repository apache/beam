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

import static org.apache.beam.runners.core.construction.BeamUrns.getUrn;
import static org.apache.beam.runners.core.construction.PTransformTranslation.PAR_DO_TRANSFORM_URN;
import static org.apache.beam.runners.core.construction.PTransformTranslation.SPLITTABLE_PAIR_WITH_RESTRICTION_URN;
import static org.apache.beam.runners.core.construction.PTransformTranslation.SPLITTABLE_PROCESS_ELEMENTS_URN;
import static org.apache.beam.runners.core.construction.PTransformTranslation.SPLITTABLE_PROCESS_SIZED_ELEMENTS_AND_RESTRICTIONS_URN;
import static org.apache.beam.runners.core.construction.PTransformTranslation.SPLITTABLE_SPLIT_AND_SIZE_RESTRICTIONS_URN;
import static org.apache.beam.runners.core.construction.PTransformTranslation.SPLITTABLE_TRUNCATE_SIZED_RESTRICTION_URN;
import static org.apache.beam.sdk.transforms.reflect.DoFnSignatures.getStateSpecOrThrow;
import static org.apache.beam.sdk.transforms.reflect.DoFnSignatures.getTimerSpecOrThrow;
import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkArgument;
import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkNotNull;
import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkState;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.model.pipeline.v1.RunnerApi.Components;
import org.apache.beam.model.pipeline.v1.RunnerApi.FunctionSpec;
import org.apache.beam.model.pipeline.v1.RunnerApi.ParDoPayload;
import org.apache.beam.model.pipeline.v1.RunnerApi.SideInput;
import org.apache.beam.model.pipeline.v1.RunnerApi.SideInput.Builder;
import org.apache.beam.model.pipeline.v1.RunnerApi.StandardRequirements;
import org.apache.beam.runners.core.construction.PTransformTranslation.TransformPayloadTranslator;
import org.apache.beam.runners.core.construction.PTransformTranslation.TransformTranslator;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.runners.AppliedPTransform;
import org.apache.beam.sdk.state.StateSpec;
import org.apache.beam.sdk.state.StateSpecs;
import org.apache.beam.sdk.state.TimeDomain;
import org.apache.beam.sdk.state.TimerSpec;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.DoFnSchemaInformation;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.ParDo.MultiOutput;
import org.apache.beam.sdk.transforms.ViewFn;
import org.apache.beam.sdk.transforms.reflect.DoFnInvoker;
import org.apache.beam.sdk.transforms.reflect.DoFnInvokers;
import org.apache.beam.sdk.transforms.reflect.DoFnSignature;
import org.apache.beam.sdk.transforms.reflect.DoFnSignature.Parameter;
import org.apache.beam.sdk.transforms.reflect.DoFnSignature.StateDeclaration;
import org.apache.beam.sdk.transforms.reflect.DoFnSignature.TimerDeclaration;
import org.apache.beam.sdk.transforms.reflect.DoFnSignatures;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.WindowMappingFn;
import org.apache.beam.sdk.util.DoFnWithExecutionInformation;
import org.apache.beam.sdk.util.SerializableUtils;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.apache.beam.vendor.grpc.v1p26p0.com.google.protobuf.ByteString;
import org.apache.beam.vendor.grpc.v1p26p0.com.google.protobuf.InvalidProtocolBufferException;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Iterables;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Sets;

/** Utilities for interacting with {@link ParDo} instances and {@link ParDoPayload} protos. */
public class ParDoTranslation {
  /**
   * This requirement indicates the state_spec and time_spec fields of ParDo transform payloads must
   * be inspected.
   */
  public static final String REQUIRES_STATEFUL_PROCESSING_URN =
      "beam:requirement:pardo:stateful:v1";
  /**
   * This requirement indicates the requests_finalization field of ParDo transform payloads must be
   * inspected.
   */
  public static final String REQUIRES_BUNDLE_FINALIZATION_URN =
      "beam:requirement:pardo:finalization:v1";
  /**
   * This requirement indicates the requires_stable_input field of ParDo transform payloads must be
   * inspected.
   */
  public static final String REQUIRES_STABLE_INPUT_URN = "beam:requirement:pardo:stable_input:v1";
  /**
   * This requirement indicates the requires_time_sorted_input field of ParDo transform payloads
   * must be inspected.
   */
  public static final String REQUIRES_TIME_SORTED_INPUT_URN =
      "beam:requirement:pardo:time_sorted_input:v1";
  /**
   * This requirement indicates the restriction_coder_id field of ParDo transform payloads must be
   * inspected.
   */
  public static final String REQUIRES_SPLITTABLE_DOFN_URN =
      "beam:requirement:pardo:splittable_dofn:v1";

  static {
    checkState(
        REQUIRES_STATEFUL_PROCESSING_URN.equals(
            getUrn(StandardRequirements.Enum.REQUIRES_STATEFUL_PROCESSING)));
    checkState(
        REQUIRES_BUNDLE_FINALIZATION_URN.equals(
            getUrn(StandardRequirements.Enum.REQUIRES_BUNDLE_FINALIZATION)));
    checkState(
        REQUIRES_STABLE_INPUT_URN.equals(getUrn(StandardRequirements.Enum.REQUIRES_STABLE_INPUT)));
    checkState(
        REQUIRES_TIME_SORTED_INPUT_URN.equals(
            getUrn(StandardRequirements.Enum.REQUIRES_TIME_SORTED_INPUT)));
    checkState(
        REQUIRES_SPLITTABLE_DOFN_URN.equals(
            getUrn(StandardRequirements.Enum.REQUIRES_SPLITTABLE_DOFN)));
  }

  /** The URN for an unknown Java {@link DoFn}. */
  public static final String CUSTOM_JAVA_DO_FN_URN = "beam:dofn:javasdk:0.1";
  /** The URN for an unknown Java {@link ViewFn}. */
  public static final String CUSTOM_JAVA_VIEW_FN_URN = "beam:viewfn:javasdk:0.1";
  /** The URN for an unknown Java {@link WindowMappingFn}. */
  public static final String CUSTOM_JAVA_WINDOW_MAPPING_FN_URN = "beam:windowmappingfn:javasdk:0.1";

  /** A {@link TransformPayloadTranslator} for {@link ParDo}. */
  public static class ParDoTranslator implements TransformTranslator<MultiOutput<?, ?>> {

    public static TransformTranslator create() {
      return new ParDoTranslator();
    }

    private ParDoTranslator() {}

    @Override
    public String getUrn(ParDo.MultiOutput<?, ?> transform) {
      return PAR_DO_TRANSFORM_URN;
    }

    @Override
    public boolean canTranslate(PTransform<?, ?> pTransform) {
      return pTransform instanceof ParDo.MultiOutput;
    }

    @Override
    public RunnerApi.PTransform translate(
        AppliedPTransform<?, ?, ?> appliedPTransform,
        List<AppliedPTransform<?, ?, ?>> subtransforms,
        SdkComponents components)
        throws IOException {
      RunnerApi.PTransform.Builder builder =
          PTransformTranslation.translateAppliedPTransform(
              appliedPTransform, subtransforms, components);

      AppliedPTransform<?, ?, ParDo.MultiOutput<?, ?>> appliedParDo =
          (AppliedPTransform<?, ?, ParDo.MultiOutput<?, ?>>) appliedPTransform;
      ParDoPayload payload = translateParDo(appliedParDo, components);
      builder.setSpec(
          RunnerApi.FunctionSpec.newBuilder()
              .setUrn(PAR_DO_TRANSFORM_URN)
              .setPayload(payload.toByteString())
              .build());
      builder.setEnvironmentId(components.getOnlyEnvironmentId());

      return builder.build();
    }
  }

  public static ParDoPayload translateParDo(
      AppliedPTransform<?, ?, ParDo.MultiOutput<?, ?>> appliedPTransform, SdkComponents components)
      throws IOException {
    final ParDo.MultiOutput<?, ?> parDo = appliedPTransform.getTransform();
    final Pipeline pipeline = appliedPTransform.getPipeline();
    final DoFn<?, ?> doFn = parDo.getFn();

    // Get main input.
    Set<String> allInputs =
        appliedPTransform.getInputs().keySet().stream()
            .map(TupleTag::getId)
            .collect(Collectors.toSet());
    Set<String> sideInputs =
        parDo.getSideInputs().values().stream()
            .map(s -> s.getTagInternal().getId())
            .collect(Collectors.toSet());
    String mainInputName = Iterables.getOnlyElement(Sets.difference(allInputs, sideInputs));
    PCollection<?> mainInput =
        (PCollection<?>) appliedPTransform.getInputs().get(new TupleTag<>(mainInputName));

    final DoFnSchemaInformation doFnSchemaInformation =
        ParDo.getDoFnSchemaInformation(doFn, mainInput);
    return translateParDo(
        (ParDo.MultiOutput) parDo, mainInput, doFnSchemaInformation, pipeline, components);
  }

  /** Translate a ParDo. */
  public static <InputT> ParDoPayload translateParDo(
      ParDo.MultiOutput<InputT, ?> parDo,
      PCollection<InputT> mainInput,
      DoFnSchemaInformation doFnSchemaInformation,
      Pipeline pipeline,
      SdkComponents components)
      throws IOException {
    final DoFn<?, ?> doFn = parDo.getFn();
    final DoFnSignature signature = DoFnSignatures.getSignature(doFn.getClass());
    final String restrictionCoderId;
    if (signature.processElement().isSplittable()) {
      DoFnInvoker<?, ?> doFnInvoker = DoFnInvokers.invokerFor(doFn);
      final Coder<?> restrictionAndWatermarkStateCoder =
          KvCoder.of(
              doFnInvoker.invokeGetRestrictionCoder(pipeline.getCoderRegistry()),
              doFnInvoker.invokeGetWatermarkEstimatorStateCoder(pipeline.getCoderRegistry()));
      restrictionCoderId = components.registerCoder(restrictionAndWatermarkStateCoder);
    } else {
      restrictionCoderId = "";
    }

    Coder<BoundedWindow> windowCoder =
        (Coder<BoundedWindow>) mainInput.getWindowingStrategy().getWindowFn().windowCoder();
    Coder<?> keyCoder;
    if (signature.usesState() || signature.usesTimers()) {
      checkArgument(
          mainInput.getCoder() instanceof KvCoder,
          "DoFn's that use state or timers must have an input PCollection with a KvCoder but received %s",
          mainInput.getCoder());
      keyCoder = ((KvCoder) mainInput.getCoder()).getKeyCoder();
    } else {
      keyCoder = null;
    }

    return payloadForParDoLike(
        new ParDoLike() {
          @Override
          public FunctionSpec translateDoFn(SdkComponents newComponents) {
            return ParDoTranslation.translateDoFn(
                parDo.getFn(),
                parDo.getMainOutputTag(),
                parDo.getSideInputs(),
                doFnSchemaInformation,
                newComponents);
          }

          @Override
          public Map<String, SideInput> translateSideInputs(SdkComponents components) {
            Map<String, SideInput> sideInputs = new HashMap<>();
            for (PCollectionView<?> sideInput : parDo.getSideInputs().values()) {
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
          public Map<String, RunnerApi.TimerFamilySpec> translateTimerFamilySpecs(
              SdkComponents newComponents) {
            Map<String, RunnerApi.TimerFamilySpec> timerFamilySpecs = new HashMap<>();

            for (Map.Entry<String, TimerDeclaration> timer :
                signature.timerDeclarations().entrySet()) {
              RunnerApi.TimerFamilySpec spec =
                  translateTimerFamilySpec(
                      getTimerSpecOrThrow(timer.getValue(), doFn),
                      newComponents,
                      keyCoder,
                      windowCoder);
              timerFamilySpecs.put(timer.getKey(), spec);
            }

            for (Map.Entry<String, DoFnSignature.TimerFamilyDeclaration> timerFamily :
                signature.timerFamilyDeclarations().entrySet()) {
              RunnerApi.TimerFamilySpec spec =
                  translateTimerFamilySpec(
                      DoFnSignatures.getTimerFamilySpecOrThrow(timerFamily.getValue(), doFn),
                      newComponents,
                      keyCoder,
                      windowCoder);
              timerFamilySpecs.put(timerFamily.getKey(), spec);
            }
            return timerFamilySpecs;
          }

          @Override
          public boolean isStateful() {
            return !signature.stateDeclarations().isEmpty()
                || !signature.timerDeclarations().isEmpty()
                || !signature.timerFamilyDeclarations().isEmpty();
          }

          @Override
          public boolean isSplittable() {
            return signature.processElement().isSplittable();
          }

          @Override
          public boolean isRequiresStableInput() {
            return signature.processElement().requiresStableInput();
          }

          @Override
          public boolean isRequiresTimeSortedInput() {
            return signature.processElement().requiresTimeSortedInput();
          }

          @Override
          public boolean requestsFinalization() {
            return (signature.startBundle() != null
                    && signature
                        .startBundle()
                        .extraParameters()
                        .contains(Parameter.bundleFinalizer()))
                || (signature.processElement() != null
                    && signature
                        .processElement()
                        .extraParameters()
                        .contains(Parameter.bundleFinalizer()))
                || (signature.finishBundle() != null
                    && signature
                        .finishBundle()
                        .extraParameters()
                        .contains(Parameter.bundleFinalizer()));
          }

          @Override
          public String translateRestrictionCoderId(SdkComponents newComponents) {
            return restrictionCoderId;
          }
        },
        components);
  }

  public static DoFn<?, ?> getDoFn(ParDoPayload payload) throws InvalidProtocolBufferException {
    return doFnWithExecutionInformationFromProto(payload.getDoFn()).getDoFn();
  }

  public static DoFn<?, ?> getDoFn(AppliedPTransform<?, ?, ?> application) throws IOException {
    PTransform<?, ?> transform = application.getTransform();
    if (transform instanceof ParDo.MultiOutput) {
      return ((ParDo.MultiOutput<?, ?>) transform).getFn();
    }

    return getDoFn(getParDoPayload(application));
  }

  public static DoFnSchemaInformation getSchemaInformation(AppliedPTransform<?, ?, ?> application) {
    try {
      return getSchemaInformation(getParDoPayload(application));
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public static DoFnSchemaInformation getSchemaInformation(RunnerApi.PTransform pTransform) {
    try {
      return getSchemaInformation(getParDoPayload(pTransform));
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public static DoFnSchemaInformation getSchemaInformation(ParDoPayload payload) {
    return doFnWithExecutionInformationFromProto(payload.getDoFn()).getSchemaInformation();
  }

  public static TupleTag<?> getMainOutputTag(ParDoPayload payload)
      throws InvalidProtocolBufferException {
    return doFnWithExecutionInformationFromProto(payload.getDoFn()).getMainOutputTag();
  }

  public static Map<String, PCollectionView<?>> getSideInputMapping(
      AppliedPTransform<?, ?, ?> application) {
    try {
      return getSideInputMapping(getParDoPayload(application));
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public static Map<String, PCollectionView<?>> getSideInputMapping(
      RunnerApi.PTransform pTransform) {
    try {
      return getSideInputMapping(getParDoPayload(pTransform));
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public static Map<String, PCollectionView<?>> getSideInputMapping(ParDoPayload payload) {
    return doFnWithExecutionInformationFromProto(payload.getDoFn()).getSideInputMapping();
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
        PTransformTranslation.toProto(
            application, SdkComponents.create(application.getPipeline().getOptions()));

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

  public static Map<TupleTag<?>, Coder<?>> getOutputCoders(AppliedPTransform<?, ?, ?> application) {
    return application.getOutputs().entrySet().stream()
        .filter(e -> e.getValue() instanceof PCollection)
        .collect(Collectors.toMap(e -> e.getKey(), e -> ((PCollection) e.getValue()).getCoder()));
  }

  public static List<PCollectionView<?>> getSideInputs(AppliedPTransform<?, ?, ?> application)
      throws IOException {
    PTransform<?, ?> transform = application.getTransform();
    if (transform instanceof ParDo.MultiOutput) {
      return ((ParDo.MultiOutput<?, ?>) transform)
          .getSideInputs().values().stream().collect(Collectors.toList());
    }

    SdkComponents sdkComponents = SdkComponents.create(application.getPipeline().getOptions());
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
        PAR_DO_TRANSFORM_URN.equals(ptransform.getSpec().getUrn())
            || SPLITTABLE_PAIR_WITH_RESTRICTION_URN.equals(ptransform.getSpec().getUrn())
            || SPLITTABLE_SPLIT_AND_SIZE_RESTRICTIONS_URN.equals(ptransform.getSpec().getUrn())
            || SPLITTABLE_TRUNCATE_SIZED_RESTRICTION_URN.equals(ptransform.getSpec().getUrn())
            || SPLITTABLE_PROCESS_ELEMENTS_URN.equals(ptransform.getSpec().getUrn())
            || SPLITTABLE_PROCESS_SIZED_ELEMENTS_AND_RESTRICTIONS_URN.equals(
                ptransform.getSpec().getUrn()),
        "Unexpected payload type %s",
        ptransform.getSpec().getUrn());
    return components.getPcollectionsOrThrow(
        ptransform.getInputsOrThrow(getMainInputName(ptransform)));
  }

  /** Returns the name of the main input of the ptransform. */
  public static String getMainInputName(RunnerApi.PTransformOrBuilder ptransform)
      throws IOException {
    checkArgument(
        PAR_DO_TRANSFORM_URN.equals(ptransform.getSpec().getUrn())
            || SPLITTABLE_PAIR_WITH_RESTRICTION_URN.equals(ptransform.getSpec().getUrn())
            || SPLITTABLE_SPLIT_AND_SIZE_RESTRICTIONS_URN.equals(ptransform.getSpec().getUrn())
            || SPLITTABLE_TRUNCATE_SIZED_RESTRICTION_URN.equals(ptransform.getSpec().getUrn())
            || SPLITTABLE_PROCESS_ELEMENTS_URN.equals(ptransform.getSpec().getUrn())
            || SPLITTABLE_PROCESS_SIZED_ELEMENTS_AND_RESTRICTIONS_URN.equals(
                ptransform.getSpec().getUrn()),
        "Unexpected payload type %s",
        ptransform.getSpec().getUrn());
    ParDoPayload payload = ParDoPayload.parseFrom(ptransform.getSpec().getPayload());
    return getMainInputName(ptransform, payload);
  }

  /** Returns the name of the main input of the ptransform. */
  private static String getMainInputName(
      RunnerApi.PTransformOrBuilder ptransform, RunnerApi.ParDoPayload payload) {
    return Iterables.getOnlyElement(
        Sets.difference(
            ptransform.getInputsMap().keySet(),
            Sets.union(
                payload.getSideInputsMap().keySet(), payload.getTimerFamilySpecsMap().keySet())));
  }

  /** Translate state specs. */
  public static RunnerApi.StateSpec translateStateSpec(
      StateSpec<?> stateSpec, final SdkComponents components) throws IOException {
    final RunnerApi.StateSpec.Builder builder = RunnerApi.StateSpec.newBuilder();

    return stateSpec.match(
        new StateSpec.Cases<RunnerApi.StateSpec>() {
          @Override
          public RunnerApi.StateSpec dispatchValue(Coder<?> valueCoder) {
            return builder
                .setReadModifyWriteSpec(
                    RunnerApi.ReadModifyWriteStateSpec.newBuilder()
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
      case READ_MODIFY_WRITE_SPEC:
        return StateSpecs.value(
            components.getCoder(stateSpec.getReadModifyWriteSpec().getCoderId()));
      case BAG_SPEC:
        return StateSpecs.bag(components.getCoder(stateSpec.getBagSpec().getElementCoderId()));
      case COMBINING_SPEC:
        FunctionSpec combineFnSpec = stateSpec.getCombiningSpec().getCombineFn();

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

  public static RunnerApi.TimerFamilySpec translateTimerFamilySpec(
      TimerSpec timer,
      SdkComponents components,
      Coder<?> keyCoder,
      Coder<BoundedWindow> windowCoder) {
    return RunnerApi.TimerFamilySpec.newBuilder()
        .setTimeDomain(translateTimeDomain(timer.getTimeDomain()))
        .setTimerFamilyCoderId(
            registerCoderOrThrow(components, Timer.Coder.of(keyCoder, windowCoder)))
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

  public static FunctionSpec translateDoFn(
      DoFn<?, ?> fn,
      TupleTag<?> tag,
      Map<String, PCollectionView<?>> sideInputMapping,
      DoFnSchemaInformation doFnSchemaInformation,
      SdkComponents components) {
    return FunctionSpec.newBuilder()
        .setUrn(CUSTOM_JAVA_DO_FN_URN)
        .setPayload(
            ByteString.copyFrom(
                SerializableUtils.serializeToByteArray(
                    DoFnWithExecutionInformation.of(
                        fn, tag, sideInputMapping, doFnSchemaInformation))))
        .build();
  }

  public static DoFnWithExecutionInformation doFnWithExecutionInformationFromProto(
      FunctionSpec fnSpec) {
    checkArgument(
        fnSpec.getUrn().equals(CUSTOM_JAVA_DO_FN_URN),
        "Expected %s to be %s with URN %s, but URN was %s",
        DoFn.class.getSimpleName(),
        FunctionSpec.class.getSimpleName(),
        CUSTOM_JAVA_DO_FN_URN,
        fnSpec.getUrn());
    byte[] serializedFn = fnSpec.getPayload().toByteArray();
    return (DoFnWithExecutionInformation)
        SerializableUtils.deserializeFromByteArray(serializedFn, "Custom DoFn With Execution Info");
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

  public static FunctionSpec translateViewFn(ViewFn<?, ?> viewFn, SdkComponents components) {
    return FunctionSpec.newBuilder()
        .setUrn(CUSTOM_JAVA_VIEW_FN_URN)
        .setPayload(ByteString.copyFrom(SerializableUtils.serializeToByteArray(viewFn)))
        .build();
  }

  private static <T> ParDoPayload getParDoPayload(AppliedPTransform<?, ?, ?> transform)
      throws IOException {
    SdkComponents components = SdkComponents.create(transform.getPipeline().getOptions());
    RunnerApi.PTransform parDoPTransform =
        PTransformTranslation.toProto(transform, Collections.emptyList(), components);
    return getParDoPayload(parDoPTransform);
  }

  private static ParDoPayload getParDoPayload(RunnerApi.PTransform parDoPTransform)
      throws IOException {
    return ParDoPayload.parseFrom(parDoPTransform.getSpec().getPayload());
  }

  public static boolean usesStateOrTimers(AppliedPTransform<?, ?, ?> transform) throws IOException {
    ParDoPayload payload = getParDoPayload(transform);
    return payload.getStateSpecsCount() > 0 || payload.getTimerFamilySpecsCount() > 0;
  }

  public static boolean isSplittable(AppliedPTransform<?, ?, ?> transform) throws IOException {
    ParDoPayload payload = getParDoPayload(transform);
    return !payload.getRestrictionCoderId().isEmpty();
  }

  public static FunctionSpec translateWindowMappingFn(
      WindowMappingFn<?> windowMappingFn, SdkComponents components) {
    return FunctionSpec.newBuilder()
        .setUrn(CUSTOM_JAVA_WINDOW_MAPPING_FN_URN)
        .setPayload(ByteString.copyFrom(SerializableUtils.serializeToByteArray(windowMappingFn)))
        .build();
  }

  /** These methods drive to-proto translation from Java and from rehydrated ParDos. */
  public interface ParDoLike {
    FunctionSpec translateDoFn(SdkComponents newComponents);

    Map<String, RunnerApi.SideInput> translateSideInputs(SdkComponents components);

    Map<String, RunnerApi.StateSpec> translateStateSpecs(SdkComponents components)
        throws IOException;

    Map<String, RunnerApi.TimerFamilySpec> translateTimerFamilySpecs(SdkComponents newComponents);

    boolean isStateful();

    boolean isSplittable();

    boolean isRequiresStableInput();

    boolean isRequiresTimeSortedInput();

    boolean requestsFinalization();

    String translateRestrictionCoderId(SdkComponents newComponents);
  }

  public static ParDoPayload payloadForParDoLike(ParDoLike parDo, SdkComponents components)
      throws IOException {

    if (parDo.isStateful()) {
      components.addRequirement(REQUIRES_STATEFUL_PROCESSING_URN);
    }
    if (parDo.isSplittable()) {
      components.addRequirement(REQUIRES_SPLITTABLE_DOFN_URN);
    }
    if (parDo.requestsFinalization()) {
      components.addRequirement(REQUIRES_BUNDLE_FINALIZATION_URN);
    }
    if (parDo.isRequiresStableInput()) {
      components.addRequirement(REQUIRES_STABLE_INPUT_URN);
    }
    if (parDo.isRequiresTimeSortedInput()) {
      components.addRequirement(REQUIRES_TIME_SORTED_INPUT_URN);
    }

    return ParDoPayload.newBuilder()
        .setDoFn(parDo.translateDoFn(components))
        .putAllStateSpecs(parDo.translateStateSpecs(components))
        .putAllTimerFamilySpecs(parDo.translateTimerFamilySpecs(components))
        .putAllSideInputs(parDo.translateSideInputs(components))
        .setRequiresStableInput(parDo.isRequiresStableInput())
        .setRequiresTimeSortedInput(parDo.isRequiresTimeSortedInput())
        .setRestrictionCoderId(parDo.translateRestrictionCoderId(components))
        .setRequestsFinalization(parDo.requestsFinalization())
        .build();
  }
}
