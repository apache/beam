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
package org.apache.beam.fn.harness;

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkArgument;

import java.io.IOException;
import java.util.Map;
import java.util.function.Supplier;
import org.apache.beam.fn.harness.control.BundleSplitListener;
import org.apache.beam.fn.harness.data.BeamFnDataClient;
import org.apache.beam.fn.harness.data.PCollectionConsumerRegistry;
import org.apache.beam.fn.harness.data.PTransformFunctionRegistry;
import org.apache.beam.fn.harness.state.BeamFnStateClient;
import org.apache.beam.fn.harness.state.SideInputSpec;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.model.pipeline.v1.RunnerApi.PCollection;
import org.apache.beam.model.pipeline.v1.RunnerApi.PTransform;
import org.apache.beam.model.pipeline.v1.RunnerApi.ParDoPayload;
import org.apache.beam.runners.core.construction.PCollectionViewTranslation;
import org.apache.beam.runners.core.construction.ParDoTranslation;
import org.apache.beam.runners.core.construction.RehydratedComponents;
import org.apache.beam.runners.core.construction.Timer;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.fn.data.FnDataReceiver;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.schemas.SchemaCoder;
import org.apache.beam.sdk.state.TimeDomain;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Materializations;
import org.apache.beam.sdk.transforms.reflect.DoFnSignature;
import org.apache.beam.sdk.transforms.reflect.DoFnSignatures;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.util.WindowedValue.WindowedValueCoder;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableListMultimap;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Iterables;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ListMultimap;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Maps;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Sets;

/** A {@link PTransformRunnerFactory} for transforms invoking a {@link DoFn}. */
abstract class DoFnPTransformRunnerFactory<
        TransformInputT,
        FnInputT,
        OutputT,
        RunnerT extends DoFnPTransformRunnerFactory.DoFnPTransformRunner<TransformInputT>>
    implements PTransformRunnerFactory<RunnerT> {
  interface DoFnPTransformRunner<T> {
    void startBundle() throws Exception;

    void processElement(WindowedValue<T> input) throws Exception;

    void processTimer(
        String timerId, TimeDomain timeDomain, WindowedValue<KV<Object, Timer>> input);

    void finishBundle() throws Exception;
  }

  @Override
  public final RunnerT createRunnerForPTransform(
      PipelineOptions pipelineOptions,
      BeamFnDataClient beamFnDataClient,
      BeamFnStateClient beamFnStateClient,
      String pTransformId,
      PTransform pTransform,
      Supplier<String> processBundleInstructionId,
      Map<String, PCollection> pCollections,
      Map<String, RunnerApi.Coder> coders,
      Map<String, RunnerApi.WindowingStrategy> windowingStrategies,
      PCollectionConsumerRegistry pCollectionConsumerRegistry,
      PTransformFunctionRegistry startFunctionRegistry,
      PTransformFunctionRegistry finishFunctionRegistry,
      BundleSplitListener splitListener) {
    Context<FnInputT, OutputT> context =
        new Context<>(
            pipelineOptions,
            beamFnStateClient,
            pTransformId,
            pTransform,
            processBundleInstructionId,
            pCollections,
            coders,
            windowingStrategies,
            pCollectionConsumerRegistry,
            splitListener);

    RunnerT runner = createRunner(context);

    // Register the appropriate handlers.
    startFunctionRegistry.register(pTransformId, runner::startBundle);
    Iterable<String> mainInput =
        Sets.difference(
            pTransform.getInputsMap().keySet(),
            Sets.union(
                context.parDoPayload.getSideInputsMap().keySet(),
                context.parDoPayload.getTimerSpecsMap().keySet()));
    for (String localInputName : mainInput) {
      pCollectionConsumerRegistry.register(
          pTransform.getInputsOrThrow(localInputName),
          pTransformId,
          (FnDataReceiver) (FnDataReceiver<WindowedValue<TransformInputT>>) runner::processElement);
    }

    // Register as a consumer for each timer PCollection.
    for (String localName : context.parDoPayload.getTimerSpecsMap().keySet()) {
      TimeDomain timeDomain =
          DoFnSignatures.getTimerSpecOrThrow(
                  context.doFnSignature.timerDeclarations().get(localName), context.doFn)
              .getTimeDomain();
      pCollectionConsumerRegistry.register(
          pTransform.getInputsOrThrow(localName),
          pTransformId,
          (FnDataReceiver)
              timer ->
                  runner.processTimer(
                      localName, timeDomain, (WindowedValue<KV<Object, Timer>>) timer));
    }

    finishFunctionRegistry.register(pTransformId, runner::finishBundle);
    return runner;
  }

  abstract RunnerT createRunner(Context<FnInputT, OutputT> context);

  static class Context<InputT, OutputT> {
    final PipelineOptions pipelineOptions;
    final BeamFnStateClient beamFnStateClient;
    final String ptransformId;
    final PTransform pTransform;
    final Supplier<String> processBundleInstructionId;
    final RehydratedComponents rehydratedComponents;
    final DoFn<InputT, OutputT> doFn;
    final DoFnSignature doFnSignature;
    final TupleTag<OutputT> mainOutputTag;
    final Coder<?> inputCoder;
    final SchemaCoder<InputT> schemaCoder;
    final Coder<?> keyCoder;
    final SchemaCoder<OutputT> mainOutputSchemaCoder;
    final Coder<? extends BoundedWindow> windowCoder;
    final WindowingStrategy<InputT, ?> windowingStrategy;
    final Map<TupleTag<?>, SideInputSpec> tagToSideInputSpecMap;
    Map<TupleTag<?>, Coder<?>> outputCoders;
    final ParDoPayload parDoPayload;
    final ListMultimap<String, FnDataReceiver<WindowedValue<?>>> localNameToConsumer;
    final BundleSplitListener splitListener;

    Context(
        PipelineOptions pipelineOptions,
        BeamFnStateClient beamFnStateClient,
        String ptransformId,
        PTransform pTransform,
        Supplier<String> processBundleInstructionId,
        Map<String, PCollection> pCollections,
        Map<String, RunnerApi.Coder> coders,
        Map<String, RunnerApi.WindowingStrategy> windowingStrategies,
        PCollectionConsumerRegistry pCollectionConsumerRegistry,
        BundleSplitListener splitListener) {
      this.pipelineOptions = pipelineOptions;
      this.beamFnStateClient = beamFnStateClient;
      this.ptransformId = ptransformId;
      this.pTransform = pTransform;
      this.processBundleInstructionId = processBundleInstructionId;
      ImmutableMap.Builder<TupleTag<?>, SideInputSpec> tagToSideInputSpecMapBuilder =
          ImmutableMap.builder();
      try {
        rehydratedComponents =
            RehydratedComponents.forComponents(
                    RunnerApi.Components.newBuilder()
                        .putAllCoders(coders)
                        .putAllPcollections(pCollections)
                        .putAllWindowingStrategies(windowingStrategies)
                        .build())
                .withPipeline(Pipeline.create());
        parDoPayload = ParDoPayload.parseFrom(pTransform.getSpec().getPayload());
        doFn = (DoFn) ParDoTranslation.getDoFn(parDoPayload);
        doFnSignature = DoFnSignatures.signatureForDoFn(doFn);
        mainOutputTag = (TupleTag) ParDoTranslation.getMainOutputTag(parDoPayload);
        String mainInputTag =
            Iterables.getOnlyElement(
                Sets.difference(
                    pTransform.getInputsMap().keySet(),
                    Sets.union(
                        parDoPayload.getSideInputsMap().keySet(),
                        parDoPayload.getTimerSpecsMap().keySet())));
        PCollection mainInput = pCollections.get(pTransform.getInputsOrThrow(mainInputTag));
        inputCoder = rehydratedComponents.getCoder(mainInput.getCoderId());
        if (inputCoder instanceof KvCoder
            // TODO: Stop passing windowed value coders within PCollections.
            || (inputCoder instanceof WindowedValue.WindowedValueCoder
                && (((WindowedValueCoder) inputCoder).getValueCoder() instanceof KvCoder))) {
          this.keyCoder =
              inputCoder instanceof WindowedValueCoder
                  ? ((KvCoder) ((WindowedValueCoder) inputCoder).getValueCoder()).getKeyCoder()
                  : ((KvCoder) inputCoder).getKeyCoder();
        } else {
          this.keyCoder = null;
        }
        if (inputCoder instanceof SchemaCoder
            // TODO: Stop passing windowed value coders within PCollections.
            || (inputCoder instanceof WindowedValue.WindowedValueCoder
                && (((WindowedValueCoder) inputCoder).getValueCoder() instanceof SchemaCoder))) {
          this.schemaCoder =
              inputCoder instanceof WindowedValueCoder
                  ? (SchemaCoder<InputT>) ((WindowedValueCoder) inputCoder).getValueCoder()
                  : ((SchemaCoder<InputT>) inputCoder);
        } else {
          this.schemaCoder = null;
        }

        windowingStrategy =
            (WindowingStrategy)
                rehydratedComponents.getWindowingStrategy(mainInput.getWindowingStrategyId());
        windowCoder = windowingStrategy.getWindowFn().windowCoder();

        outputCoders = Maps.newHashMap();
        for (Map.Entry<String, String> entry : pTransform.getOutputsMap().entrySet()) {
          TupleTag<?> outputTag = new TupleTag<>(entry.getKey());
          RunnerApi.PCollection outputPCollection = pCollections.get(entry.getValue());
          Coder<?> outputCoder = rehydratedComponents.getCoder(outputPCollection.getCoderId());
          if (outputCoder instanceof WindowedValueCoder) {
            outputCoder = ((WindowedValueCoder) outputCoder).getValueCoder();
          }
          outputCoders.put(outputTag, outputCoder);
        }
        Coder<OutputT> outputCoder = (Coder<OutputT>) outputCoders.get(mainOutputTag);
        mainOutputSchemaCoder =
            (outputCoder instanceof SchemaCoder) ? (SchemaCoder<OutputT>) outputCoder : null;

        // Build the map from tag id to side input specification
        for (Map.Entry<String, RunnerApi.SideInput> entry :
            parDoPayload.getSideInputsMap().entrySet()) {
          String sideInputTag = entry.getKey();
          RunnerApi.SideInput sideInput = entry.getValue();
          checkArgument(
              Materializations.MULTIMAP_MATERIALIZATION_URN.equals(
                  sideInput.getAccessPattern().getUrn()),
              "This SDK is only capable of dealing with %s materializations "
                  + "but was asked to handle %s for PCollectionView with tag %s.",
              Materializations.MULTIMAP_MATERIALIZATION_URN,
              sideInput.getAccessPattern().getUrn(),
              sideInputTag);

          PCollection sideInputPCollection =
              pCollections.get(pTransform.getInputsOrThrow(sideInputTag));
          WindowingStrategy sideInputWindowingStrategy =
              rehydratedComponents.getWindowingStrategy(
                  sideInputPCollection.getWindowingStrategyId());
          tagToSideInputSpecMapBuilder.put(
              new TupleTag<>(entry.getKey()),
              SideInputSpec.create(
                  rehydratedComponents.getCoder(sideInputPCollection.getCoderId()),
                  sideInputWindowingStrategy.getWindowFn().windowCoder(),
                  PCollectionViewTranslation.viewFnFromProto(entry.getValue().getViewFn()),
                  PCollectionViewTranslation.windowMappingFnFromProto(
                      entry.getValue().getWindowMappingFn())));
        }
      } catch (IOException exn) {
        throw new IllegalArgumentException("Malformed ParDoPayload", exn);
      }

      ImmutableListMultimap.Builder<String, FnDataReceiver<WindowedValue<?>>>
          localNameToConsumerBuilder = ImmutableListMultimap.builder();
      for (Map.Entry<String, String> entry : pTransform.getOutputsMap().entrySet()) {
        localNameToConsumerBuilder.putAll(
            entry.getKey(), pCollectionConsumerRegistry.getMultiplexingConsumer(entry.getValue()));
      }
      localNameToConsumer = localNameToConsumerBuilder.build();
      tagToSideInputSpecMap = tagToSideInputSpecMapBuilder.build();
      this.splitListener = splitListener;
    }
  }
}
