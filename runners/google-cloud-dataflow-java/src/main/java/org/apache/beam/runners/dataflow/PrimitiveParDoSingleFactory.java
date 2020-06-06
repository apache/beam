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
package org.apache.beam.runners.dataflow;

import static org.apache.beam.runners.core.construction.PTransformTranslation.PAR_DO_TRANSFORM_URN;
import static org.apache.beam.runners.core.construction.ParDoTranslation.translateTimerFamilySpec;
import static org.apache.beam.sdk.options.ExperimentalOptions.hasExperiment;
import static org.apache.beam.sdk.transforms.reflect.DoFnSignatures.getStateSpecOrThrow;
import static org.apache.beam.sdk.transforms.reflect.DoFnSignatures.getTimerFamilySpecOrThrow;
import static org.apache.beam.sdk.transforms.reflect.DoFnSignatures.getTimerSpecOrThrow;
import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkArgument;

import com.google.auto.service.AutoService;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.model.pipeline.v1.RunnerApi.DisplayData;
import org.apache.beam.runners.core.construction.ForwardingPTransform;
import org.apache.beam.runners.core.construction.PTransformReplacements;
import org.apache.beam.runners.core.construction.PTransformTranslation;
import org.apache.beam.runners.core.construction.ParDoTranslation;
import org.apache.beam.runners.core.construction.SdkComponents;
import org.apache.beam.runners.core.construction.SingleInputOutputOverrideFactory;
import org.apache.beam.runners.core.construction.TransformPayloadTranslatorRegistrar;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.runners.AppliedPTransform;
import org.apache.beam.sdk.runners.PTransformOverrideFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.DoFnSchemaInformation;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.ParDo.SingleOutput;
import org.apache.beam.sdk.transforms.reflect.DoFnInvoker;
import org.apache.beam.sdk.transforms.reflect.DoFnInvokers;
import org.apache.beam.sdk.transforms.reflect.DoFnSignature;
import org.apache.beam.sdk.transforms.reflect.DoFnSignature.Parameter;
import org.apache.beam.sdk.transforms.reflect.DoFnSignatures;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.PCollectionViews;
import org.apache.beam.sdk.values.PValue;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Iterables;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Sets;

/**
 * A {@link PTransformOverrideFactory} that produces {@link ParDoSingle} instances from {@link
 * ParDo.SingleOutput} instances. {@link ParDoSingle} is a primitive {@link PTransform}, to ensure
 * that {@link DisplayData} appears on all {@link ParDo ParDos} in the {@link DataflowRunner}.
 */
public class PrimitiveParDoSingleFactory<InputT, OutputT>
    extends SingleInputOutputOverrideFactory<
        PCollection<? extends InputT>, PCollection<OutputT>, ParDo.SingleOutput<InputT, OutputT>> {
  @Override
  public PTransformReplacement<PCollection<? extends InputT>, PCollection<OutputT>>
      getReplacementTransform(
          AppliedPTransform<
                  PCollection<? extends InputT>,
                  PCollection<OutputT>,
                  SingleOutput<InputT, OutputT>>
              transform) {
    return PTransformReplacement.of(
        PTransformReplacements.getSingletonMainInput(transform),
        new ParDoSingle<>(
            transform.getTransform(),
            Iterables.getOnlyElement(transform.getOutputs().keySet()),
            PTransformReplacements.getSingletonMainOutput(transform).getCoder()));
  }

  /** A single-output primitive {@link ParDo}. */
  public static class ParDoSingle<InputT, OutputT>
      extends ForwardingPTransform<PCollection<? extends InputT>, PCollection<OutputT>> {
    private final ParDo.SingleOutput<InputT, OutputT> original;
    private final TupleTag<?> onlyOutputTag;
    private final Coder<OutputT> outputCoder;

    private ParDoSingle(
        SingleOutput<InputT, OutputT> original,
        TupleTag<?> onlyOutputTag,
        Coder<OutputT> outputCoder) {
      this.original = original;
      this.onlyOutputTag = onlyOutputTag;
      this.outputCoder = outputCoder;
    }

    @Override
    public PCollection<OutputT> expand(PCollection<? extends InputT> input) {
      return PCollection.createPrimitiveOutputInternal(
          input.getPipeline(),
          input.getWindowingStrategy(),
          input.isBounded(),
          outputCoder,
          onlyOutputTag);
    }

    public DoFn<InputT, OutputT> getFn() {
      return original.getFn();
    }

    public TupleTag<?> getMainOutputTag() {
      return onlyOutputTag;
    }

    public Map<String, PCollectionView<?>> getSideInputs() {
      return original.getSideInputs();
    }

    @Override
    public Map<TupleTag<?>, PValue> getAdditionalInputs() {
      return PCollectionViews.toAdditionalInputs(getSideInputs().values());
    }

    @Override
    protected PTransform<PCollection<? extends InputT>, PCollection<OutputT>> delegate() {
      return original;
    }
  }

  /** A translator for {@link ParDoSingle}. */
  public static class PayloadTranslator
      implements PTransformTranslation.TransformPayloadTranslator<ParDoSingle<?, ?>> {
    public static PTransformTranslation.TransformPayloadTranslator create() {
      return new PayloadTranslator();
    }

    private PayloadTranslator() {}

    @Override
    public String getUrn(ParDoSingle<?, ?> transform) {
      return PAR_DO_TRANSFORM_URN;
    }

    @Override
    public RunnerApi.FunctionSpec translate(
        AppliedPTransform<?, ?, ParDoSingle<?, ?>> transform, SdkComponents components)
        throws IOException {
      RunnerApi.ParDoPayload payload = payloadForParDoSingle(transform, components);

      return RunnerApi.FunctionSpec.newBuilder()
          .setUrn(PAR_DO_TRANSFORM_URN)
          .setPayload(payload.toByteString())
          .build();
    }

    private static RunnerApi.ParDoPayload payloadForParDoSingle(
        final AppliedPTransform<?, ?, ParDoSingle<?, ?>> transform, SdkComponents components)
        throws IOException {
      final ParDoSingle<?, ?> parDo = transform.getTransform();
      final DoFn<?, ?> doFn = parDo.getFn();
      final DoFnSignature signature = DoFnSignatures.getSignature(doFn.getClass());

      if (!hasExperiment(transform.getPipeline().getOptions(), "beam_fn_api")) {
        checkArgument(
            !signature.processElement().isSplittable(),
            String.format(
                "Not expecting a splittable %s: should have been overridden",
                ParDoSingle.class.getSimpleName()));
      }

      // TODO: Is there a better way to do this?
      Set<String> allInputs =
          transform.getInputs().keySet().stream().map(TupleTag::getId).collect(Collectors.toSet());
      Set<String> sideInputs =
          parDo.getSideInputs().values().stream()
              .map(s -> s.getTagInternal().getId())
              .collect(Collectors.toSet());
      String mainInputName = Iterables.getOnlyElement(Sets.difference(allInputs, sideInputs));
      PCollection<?> mainInput =
          (PCollection<?>) transform.getInputs().get(new TupleTag<>(mainInputName));

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

      final DoFnSchemaInformation doFnSchemaInformation =
          ParDo.getDoFnSchemaInformation(doFn, mainInput);

      return ParDoTranslation.payloadForParDoLike(
          new ParDoTranslation.ParDoLike() {
            @Override
            public RunnerApi.FunctionSpec translateDoFn(SdkComponents newComponents) {
              return ParDoTranslation.translateDoFn(
                  parDo.getFn(),
                  parDo.getMainOutputTag(),
                  parDo.getSideInputs(),
                  doFnSchemaInformation,
                  newComponents);
            }

            @Override
            public Map<String, RunnerApi.SideInput> translateSideInputs(SdkComponents components) {
              return ParDoTranslation.translateSideInputs(
                  parDo.getSideInputs().values().stream().collect(Collectors.toList()), components);
            }

            @Override
            public Map<String, RunnerApi.StateSpec> translateStateSpecs(SdkComponents components)
                throws IOException {
              Map<String, RunnerApi.StateSpec> stateSpecs = new HashMap<>();
              for (Map.Entry<String, DoFnSignature.StateDeclaration> state :
                  signature.stateDeclarations().entrySet()) {
                RunnerApi.StateSpec spec =
                    ParDoTranslation.translateStateSpec(
                        getStateSpecOrThrow(state.getValue(), doFn), components);
                stateSpecs.put(state.getKey(), spec);
              }
              return stateSpecs;
            }

            @Override
            public Map<String, RunnerApi.TimerFamilySpec> translateTimerFamilySpecs(
                SdkComponents newComponents) {
              Map<String, RunnerApi.TimerFamilySpec> timerFamilySpecs = new HashMap<>();
              for (Map.Entry<String, DoFnSignature.TimerFamilyDeclaration> timerFamily :
                  signature.timerFamilyDeclarations().entrySet()) {
                RunnerApi.TimerFamilySpec spec =
                    translateTimerFamilySpec(
                        getTimerFamilySpecOrThrow(timerFamily.getValue(), doFn),
                        newComponents,
                        keyCoder,
                        windowCoder);
                timerFamilySpecs.put(timerFamily.getKey(), spec);
              }
              for (Map.Entry<String, DoFnSignature.TimerDeclaration> timer :
                  signature.timerDeclarations().entrySet()) {
                RunnerApi.TimerFamilySpec spec =
                    translateTimerFamilySpec(
                        getTimerSpecOrThrow(timer.getValue(), doFn),
                        newComponents,
                        keyCoder,
                        windowCoder);
                timerFamilySpecs.put(timer.getKey(), spec);
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
              if (signature.processElement().isSplittable()) {
                DoFnInvoker<?, ?> doFnInvoker = DoFnInvokers.invokerFor(doFn);
                final Coder<?> restrictionAndWatermarkStateCoder =
                    KvCoder.of(
                        doFnInvoker.invokeGetRestrictionCoder(
                            transform.getPipeline().getCoderRegistry()),
                        doFnInvoker.invokeGetWatermarkEstimatorStateCoder(
                            transform.getPipeline().getCoderRegistry()));
                try {
                  return newComponents.registerCoder(restrictionAndWatermarkStateCoder);
                } catch (IOException e) {
                  throw new IllegalStateException(
                      String.format(
                          "Unable to register restriction coder for %s.", transform.getFullName()),
                      e);
                }
              }
              return "";
            }
          },
          components);
    }
  }

  /** Registers {@link PayloadTranslator}. */
  @AutoService(TransformPayloadTranslatorRegistrar.class)
  public static class Registrar implements TransformPayloadTranslatorRegistrar {
    @Override
    public Map<
            ? extends Class<? extends PTransform>,
            ? extends PTransformTranslation.TransformPayloadTranslator>
        getTransformPayloadTranslators() {
      return Collections.singletonMap(ParDoSingle.class, new PayloadTranslator());
    }
  }
}
