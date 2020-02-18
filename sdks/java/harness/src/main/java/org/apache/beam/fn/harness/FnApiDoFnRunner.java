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
import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkNotNull;
import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkState;

import com.google.auto.service.AutoService;
import com.google.auto.value.AutoValue;
import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import javax.annotation.Nullable;
import org.apache.beam.fn.harness.control.BundleSplitListener;
import org.apache.beam.fn.harness.data.BeamFnDataClient;
import org.apache.beam.fn.harness.data.PCollectionConsumerRegistry;
import org.apache.beam.fn.harness.data.PTransformFunctionRegistry;
import org.apache.beam.fn.harness.state.BeamFnStateClient;
import org.apache.beam.fn.harness.state.FnApiStateAccessor;
import org.apache.beam.fn.harness.state.SideInputSpec;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.BundleApplication;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.DelayedBundleApplication;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.model.pipeline.v1.RunnerApi.PCollection;
import org.apache.beam.model.pipeline.v1.RunnerApi.PTransform;
import org.apache.beam.model.pipeline.v1.RunnerApi.ParDoPayload;
import org.apache.beam.runners.core.DoFnRunner;
import org.apache.beam.runners.core.LateDataUtils;
import org.apache.beam.runners.core.construction.PCollectionViewTranslation;
import org.apache.beam.runners.core.construction.PTransformTranslation;
import org.apache.beam.runners.core.construction.ParDoTranslation;
import org.apache.beam.runners.core.construction.RehydratedComponents;
import org.apache.beam.runners.core.construction.Timer;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.fn.data.FnDataReceiver;
import org.apache.beam.sdk.function.ThrowingRunnable;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.schemas.SchemaCoder;
import org.apache.beam.sdk.state.ReadableState;
import org.apache.beam.sdk.state.State;
import org.apache.beam.sdk.state.StateSpec;
import org.apache.beam.sdk.state.TimeDomain;
import org.apache.beam.sdk.state.TimerMap;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.DoFn.MultiOutputReceiver;
import org.apache.beam.sdk.transforms.DoFn.OutputReceiver;
import org.apache.beam.sdk.transforms.DoFnOutputReceivers;
import org.apache.beam.sdk.transforms.DoFnSchemaInformation;
import org.apache.beam.sdk.transforms.Materializations;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.reflect.DoFnInvoker;
import org.apache.beam.sdk.transforms.reflect.DoFnInvoker.DelegatingArgumentProvider;
import org.apache.beam.sdk.transforms.reflect.DoFnInvokers;
import org.apache.beam.sdk.transforms.reflect.DoFnSignature;
import org.apache.beam.sdk.transforms.reflect.DoFnSignature.StateDeclaration;
import org.apache.beam.sdk.transforms.reflect.DoFnSignature.TimerDeclaration;
import org.apache.beam.sdk.transforms.reflect.DoFnSignatures;
import org.apache.beam.sdk.transforms.splittabledofn.RestrictionTracker;
import org.apache.beam.sdk.transforms.splittabledofn.SplitResult;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.util.UserCodeException;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.util.WindowedValue.WindowedValueCoder;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.apache.beam.vendor.grpc.v1p26p0.com.google.protobuf.ByteString;
import org.apache.beam.vendor.grpc.v1p26p0.com.google.protobuf.util.Durations;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.MoreObjects;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableListMultimap;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Iterables;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ListMultimap;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Maps;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Sets;
import org.joda.time.DateTimeUtils;
import org.joda.time.Duration;
import org.joda.time.Instant;

/**
 * A {@link DoFnRunner} specific to integrating with the Fn Api. This is to remove the layers of
 * abstraction caused by StateInternals/TimerInternals since they model state and timer concepts
 * differently.
 */
public class FnApiDoFnRunner<InputT, RestrictionT, PositionT, OutputT> {
  /** A registrar which provides a factory to handle Java {@link DoFn}s. */
  @AutoService(PTransformRunnerFactory.Registrar.class)
  public static class Registrar implements PTransformRunnerFactory.Registrar {
    @Override
    public Map<String, PTransformRunnerFactory> getPTransformRunnerFactories() {
      Factory factory = new Factory();
      return ImmutableMap.<String, PTransformRunnerFactory>builder()
          .put(PTransformTranslation.PAR_DO_TRANSFORM_URN, factory)
          .put(PTransformTranslation.SPLITTABLE_PAIR_WITH_RESTRICTION_URN, factory)
          .put(PTransformTranslation.SPLITTABLE_SPLIT_RESTRICTION_URN, factory)
          .put(PTransformTranslation.SPLITTABLE_SPLIT_AND_SIZE_RESTRICTIONS_URN, factory)
          .put(PTransformTranslation.SPLITTABLE_PROCESS_ELEMENTS_URN, factory)
          .put(
              PTransformTranslation.SPLITTABLE_PROCESS_SIZED_ELEMENTS_AND_RESTRICTIONS_URN, factory)
          .build();
    }
  }

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
        switch (pTransform.getSpec().getUrn()) {
          case PTransformTranslation.SPLITTABLE_PROCESS_ELEMENTS_URN:
          case PTransformTranslation.SPLITTABLE_PROCESS_SIZED_ELEMENTS_AND_RESTRICTIONS_URN:
          case PTransformTranslation.PAR_DO_TRANSFORM_URN:
            mainOutputTag = (TupleTag) ParDoTranslation.getMainOutputTag(parDoPayload);
            break;
          case PTransformTranslation.SPLITTABLE_PAIR_WITH_RESTRICTION_URN:
          case PTransformTranslation.SPLITTABLE_SPLIT_AND_SIZE_RESTRICTIONS_URN:
          case PTransformTranslation.SPLITTABLE_SPLIT_RESTRICTION_URN:
            mainOutputTag =
                new TupleTag(Iterables.getOnlyElement(pTransform.getOutputsMap().keySet()));
            break;
          default:
            throw new IllegalStateException(
                String.format("Unknown urn: %s", pTransform.getSpec().getUrn()));
        }
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

  static class Factory<InputT, RestrictionT, PositionT, OutputT>
      implements PTransformRunnerFactory<
          FnApiDoFnRunner<InputT, RestrictionT, PositionT, OutputT>> {

    @Override
    public final FnApiDoFnRunner<InputT, RestrictionT, PositionT, OutputT>
        createRunnerForPTransform(
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
            Consumer<ThrowingRunnable> tearDownFunctions,
            BundleSplitListener splitListener) {
      Context<InputT, OutputT> context =
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

      FnApiDoFnRunner<InputT, RestrictionT, PositionT, OutputT> runner =
          new FnApiDoFnRunner<>(context);

      // Register the appropriate handlers.
      startFunctionRegistry.register(pTransformId, runner::startBundle);
      String mainInput;
      try {
        mainInput = ParDoTranslation.getMainInputName(pTransform);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
      FnDataReceiver<WindowedValue> mainInputConsumer;
      switch (pTransform.getSpec().getUrn()) {
        case PTransformTranslation.PAR_DO_TRANSFORM_URN:
          mainInputConsumer = runner::processElementForParDo;
          break;
        case PTransformTranslation.SPLITTABLE_PAIR_WITH_RESTRICTION_URN:
          mainInputConsumer = runner::processElementForPairWithRestriction;
          break;
        case PTransformTranslation.SPLITTABLE_SPLIT_RESTRICTION_URN:
        case PTransformTranslation.SPLITTABLE_SPLIT_AND_SIZE_RESTRICTIONS_URN:
          mainInputConsumer = runner::processElementForSplitRestriction;
          break;
        case PTransformTranslation.SPLITTABLE_PROCESS_ELEMENTS_URN:
          mainInputConsumer = runner::processElementForElementAndRestriction;
          break;
        case PTransformTranslation.SPLITTABLE_PROCESS_SIZED_ELEMENTS_AND_RESTRICTIONS_URN:
          mainInputConsumer = runner::processElementForSizedElementAndRestriction;
          break;
        default:
          throw new IllegalStateException("Unknown urn: " + pTransform.getSpec().getUrn());
      }
      pCollectionConsumerRegistry.register(
          pTransform.getInputsOrThrow(mainInput), pTransformId, (FnDataReceiver) mainInputConsumer);

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
      tearDownFunctions.accept(runner::tearDown);
      return runner;
    }
  }

  //////////////////////////////////////////////////////////////////////////////////////////////////

  private final Context<InputT, OutputT> context;
  private final Collection<FnDataReceiver<WindowedValue<OutputT>>> mainOutputConsumers;
  private final String mainInputId;
  private FnApiStateAccessor stateAccessor;
  private final DoFnInvoker<InputT, OutputT> doFnInvoker;
  private final DoFn<InputT, OutputT>.StartBundleContext startBundleContext;
  private final ProcessBundleContext processContext;
  private final OnTimerContext onTimerContext;
  private final DoFn<InputT, OutputT>.FinishBundleContext finishBundleContext;

  /**
   * Only set for {@link PTransformTranslation#SPLITTABLE_PROCESS_ELEMENTS_URN} and {@link
   * PTransformTranslation#SPLITTABLE_PROCESS_SIZED_ELEMENTS_AND_RESTRICTIONS_URN} transforms. Can
   * only be invoked from within {@code processElement...} methods.
   */
  private final Function<SplitResult<RestrictionT>, WindowedSplitResult>
      convertSplitResultToWindowedSplitResult;

  private final DoFnSchemaInformation doFnSchemaInformation;
  private final Map<String, PCollectionView<?>> sideInputMapping;

  // The member variables below are only valid for the lifetime of certain methods.
  /** Only valid during {@code processElement...} methods, null otherwise. */
  private WindowedValue<InputT> currentElement;

  /** Only valid during {@link #processElementForSplitRestriction}, null otherwise. */
  private RestrictionT currentRestriction;

  /**
   * Only valid during {@code processElement...} and {@link #processTimer} methods, null otherwise.
   */
  private BoundedWindow currentWindow;

  /**
   * Only valid during {@link #processElementForElementAndRestriction} and {@link
   * #processElementForSizedElementAndRestriction}, null otherwise.
   */
  private RestrictionTracker<RestrictionT, PositionT> currentTracker;

  /** Only valid during {@link #processTimer}, null otherwise. */
  private WindowedValue<KV<Object, Timer>> currentTimer;

  /** Only valid during {@link #processTimer}, null otherwise. */
  private TimeDomain currentTimeDomain;

  FnApiDoFnRunner(Context<InputT, OutputT> context) {
    this.context = context;
    try {
      this.mainInputId = ParDoTranslation.getMainInputName(context.pTransform);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    this.mainOutputConsumers =
        (Collection<FnDataReceiver<WindowedValue<OutputT>>>)
            (Collection) context.localNameToConsumer.get(context.mainOutputTag.getId());
    this.doFnSchemaInformation = ParDoTranslation.getSchemaInformation(context.parDoPayload);
    this.sideInputMapping = ParDoTranslation.getSideInputMapping(context.parDoPayload);
    this.doFnInvoker = DoFnInvokers.invokerFor(context.doFn);
    this.doFnInvoker.invokeSetup();

    this.startBundleContext =
        this.context.doFn.new StartBundleContext() {
          @Override
          public PipelineOptions getPipelineOptions() {
            return context.pipelineOptions;
          }
        };
    switch (context.pTransform.getSpec().getUrn()) {
      case PTransformTranslation.SPLITTABLE_SPLIT_RESTRICTION_URN:
        // OutputT == RestrictionT
        this.processContext =
            new ProcessBundleContext() {
              @Override
              public void outputWithTimestamp(OutputT output, Instant timestamp) {
                outputTo(
                    mainOutputConsumers,
                    (WindowedValue<OutputT>)
                        WindowedValue.of(
                            KV.of(currentElement.getValue(), output),
                            timestamp,
                            currentWindow,
                            currentElement.getPane()));
              }
            };
        break;
      case PTransformTranslation.SPLITTABLE_SPLIT_AND_SIZE_RESTRICTIONS_URN:
        // OutputT == RestrictionT
        this.processContext =
            new ProcessBundleContext() {
              @Override
              public void outputWithTimestamp(OutputT output, Instant timestamp) {
                double size =
                    doFnInvoker.invokeGetSize(
                        new DelegatingArgumentProvider<InputT, OutputT>(
                            this,
                            PTransformTranslation.SPLITTABLE_SPLIT_AND_SIZE_RESTRICTIONS_URN
                                + "/GetSize") {
                          @Override
                          public Object restriction() {
                            return output;
                          }

                          @Override
                          public Instant timestamp(DoFn<InputT, OutputT> doFn) {
                            return timestamp;
                          }

                          @Override
                          public RestrictionTracker<?, ?> restrictionTracker() {
                            return doFnInvoker.invokeNewTracker(this);
                          }
                        });

                outputTo(
                    mainOutputConsumers,
                    (WindowedValue<OutputT>)
                        WindowedValue.of(
                            KV.of(KV.of(currentElement.getValue(), output), size),
                            timestamp,
                            currentWindow,
                            currentElement.getPane()));
              }
            };
        break;
      case PTransformTranslation.SPLITTABLE_PAIR_WITH_RESTRICTION_URN:
      case PTransformTranslation.SPLITTABLE_PROCESS_ELEMENTS_URN:
      case PTransformTranslation.SPLITTABLE_PROCESS_SIZED_ELEMENTS_AND_RESTRICTIONS_URN:
      case PTransformTranslation.PAR_DO_TRANSFORM_URN:
        this.processContext = new ProcessBundleContext();
        break;
      default:
        throw new IllegalStateException(
            String.format("Unknown URN %s", context.pTransform.getSpec().getUrn()));
    }
    this.onTimerContext = new OnTimerContext();
    this.finishBundleContext =
        this.context.doFn.new FinishBundleContext() {
          @Override
          public PipelineOptions getPipelineOptions() {
            return context.pipelineOptions;
          }

          @Override
          public void output(OutputT output, Instant timestamp, BoundedWindow window) {
            outputTo(
                mainOutputConsumers,
                WindowedValue.of(output, timestamp, window, PaneInfo.NO_FIRING));
          }

          @Override
          public <T> void output(
              TupleTag<T> tag, T output, Instant timestamp, BoundedWindow window) {
            Collection<FnDataReceiver<WindowedValue<T>>> consumers =
                (Collection) context.localNameToConsumer.get(tag.getId());
            if (consumers == null) {
              throw new IllegalArgumentException(String.format("Unknown output tag %s", tag));
            }
            outputTo(consumers, WindowedValue.of(output, timestamp, window, PaneInfo.NO_FIRING));
          }
        };

    switch (context.pTransform.getSpec().getUrn()) {
      case PTransformTranslation.SPLITTABLE_PROCESS_ELEMENTS_URN:
        this.convertSplitResultToWindowedSplitResult =
            (splitResult) ->
                WindowedSplitResult.forRoots(
                    currentElement.withValue(
                        KV.of(currentElement.getValue(), splitResult.getPrimary())),
                    currentElement.withValue(
                        KV.of(currentElement.getValue(), splitResult.getResidual())));
        break;
      case PTransformTranslation.SPLITTABLE_PROCESS_SIZED_ELEMENTS_AND_RESTRICTIONS_URN:
        this.convertSplitResultToWindowedSplitResult =
            (splitResult) -> {
              double primarySize =
                  doFnInvoker.invokeGetSize(
                      new DelegatingArgumentProvider<InputT, OutputT>(
                          processContext,
                          PTransformTranslation
                                  .SPLITTABLE_PROCESS_SIZED_ELEMENTS_AND_RESTRICTIONS_URN
                              + "/GetPrimarySize") {
                        @Override
                        public Object restriction() {
                          return splitResult.getPrimary();
                        }

                        @Override
                        public RestrictionTracker<?, ?> restrictionTracker() {
                          return doFnInvoker.invokeNewTracker(this);
                        }
                      });
              double residualSize =
                  doFnInvoker.invokeGetSize(
                      new DelegatingArgumentProvider<InputT, OutputT>(
                          processContext,
                          PTransformTranslation
                                  .SPLITTABLE_PROCESS_SIZED_ELEMENTS_AND_RESTRICTIONS_URN
                              + "/GetResidualSize") {
                        @Override
                        public Object restriction() {
                          return splitResult.getResidual();
                        }

                        @Override
                        public RestrictionTracker<?, ?> restrictionTracker() {
                          return doFnInvoker.invokeNewTracker(this);
                        }
                      });
              return WindowedSplitResult.forRoots(
                  currentElement.withValue(
                      KV.of(
                          KV.of(currentElement.getValue(), splitResult.getPrimary()), primarySize)),
                  currentElement.withValue(
                      KV.of(
                          KV.of(currentElement.getValue(), splitResult.getResidual()),
                          residualSize)));
            };
        break;
      default:
        this.convertSplitResultToWindowedSplitResult =
            (splitResult) -> {
              throw new IllegalStateException(
                  String.format(
                      "Unimplemented split conversion handler for %s.",
                      context.pTransform.getSpec().getUrn()));
            };
    }
  }

  public void startBundle() {
    this.stateAccessor =
        new FnApiStateAccessor(
            context.pipelineOptions,
            context.ptransformId,
            context.processBundleInstructionId,
            context.tagToSideInputSpecMap,
            context.beamFnStateClient,
            context.keyCoder,
            (Coder<BoundedWindow>) context.windowCoder,
            () -> MoreObjects.firstNonNull(currentElement, currentTimer),
            () -> currentWindow);

    doFnInvoker.invokeStartBundle(startBundleContext);
  }

  public void processElementForParDo(WindowedValue<InputT> elem) {
    currentElement = elem;
    try {
      Iterator<BoundedWindow> windowIterator =
          (Iterator<BoundedWindow>) elem.getWindows().iterator();
      while (windowIterator.hasNext()) {
        currentWindow = windowIterator.next();
        doFnInvoker.invokeProcessElement(processContext);
      }
    } finally {
      currentElement = null;
      currentWindow = null;
    }
  }

  public void processElementForPairWithRestriction(WindowedValue<InputT> elem) {
    currentElement = elem;
    try {
      Iterator<BoundedWindow> windowIterator =
          (Iterator<BoundedWindow>) elem.getWindows().iterator();
      while (windowIterator.hasNext()) {
        currentWindow = windowIterator.next();
        outputTo(
            mainOutputConsumers,
            (WindowedValue)
                elem.withValue(
                    KV.of(
                        elem.getValue(), doFnInvoker.invokeGetInitialRestriction(processContext))));
      }
    } finally {
      currentElement = null;
      currentWindow = null;
    }
  }

  public void processElementForSplitRestriction(WindowedValue<KV<InputT, RestrictionT>> elem) {
    currentElement = elem.withValue(elem.getValue().getKey());
    currentRestriction = elem.getValue().getValue();
    try {
      Iterator<BoundedWindow> windowIterator =
          (Iterator<BoundedWindow>) elem.getWindows().iterator();
      while (windowIterator.hasNext()) {
        currentWindow = windowIterator.next();
        doFnInvoker.invokeSplitRestriction(processContext);
      }
    } finally {
      currentElement = null;
      currentRestriction = null;
      currentWindow = null;
    }
  }

  /** Internal class to hold the primary and residual roots when converted to an input element. */
  @AutoValue
  abstract static class WindowedSplitResult {
    public static WindowedSplitResult forRoots(
        WindowedValue primaryRoot, WindowedValue residualRoot) {
      return new AutoValue_FnApiDoFnRunner_WindowedSplitResult(primaryRoot, residualRoot);
    }

    public abstract WindowedValue getPrimaryRoot();

    public abstract WindowedValue getResidualRoot();
  }

  public void processElementForSizedElementAndRestriction(
      WindowedValue<KV<KV<InputT, RestrictionT>, Double>> elem) {
    processElementForElementAndRestriction(elem.withValue(elem.getValue().getKey()));
  }

  public void processElementForElementAndRestriction(WindowedValue<KV<InputT, RestrictionT>> elem) {
    currentElement = elem.withValue(elem.getValue().getKey());
    try {
      Iterator<BoundedWindow> windowIterator =
          (Iterator<BoundedWindow>) elem.getWindows().iterator();
      while (windowIterator.hasNext()) {
        currentRestriction = elem.getValue().getValue();
        currentWindow = windowIterator.next();
        currentTracker = doFnInvoker.invokeNewTracker(processContext);
        DoFn.ProcessContinuation continuation = doFnInvoker.invokeProcessElement(processContext);
        // Ensure that all the work is done if the user tells us that they don't want to
        // resume processing.
        if (!continuation.shouldResume()) {
          currentTracker.checkDone();
          continue;
        }

        SplitResult<RestrictionT> result = currentTracker.trySplit(0);
        // After the user has chosen to resume processing later, the Runner may have stolen
        // the remainder of work through a split call so the above trySplit may fail. If so,
        // the current restriction must be done.
        if (result == null) {
          currentTracker.checkDone();
          continue;
        }

        // Otherwise we have a successful self checkpoint.
        WindowedSplitResult windowedSplitResult =
            convertSplitResultToWindowedSplitResult.apply(result);
        ByteString.Output primaryBytes = ByteString.newOutput();
        ByteString.Output residualBytes = ByteString.newOutput();
        try {
          Coder fullInputCoder =
              WindowedValue.getFullCoder(context.inputCoder, context.windowCoder);
          fullInputCoder.encode(windowedSplitResult.getPrimaryRoot(), primaryBytes);
          fullInputCoder.encode(windowedSplitResult.getResidualRoot(), residualBytes);
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
        BundleApplication primaryApplication =
            BundleApplication.newBuilder()
                .setTransformId(context.ptransformId)
                .setInputId(mainInputId)
                .setElement(primaryBytes.toByteString())
                .build();
        BundleApplication residualApplication =
            BundleApplication.newBuilder()
                .setTransformId(context.ptransformId)
                .setInputId(mainInputId)
                .setElement(residualBytes.toByteString())
                .build();
        context.splitListener.split(
            ImmutableList.of(primaryApplication),
            ImmutableList.of(
                DelayedBundleApplication.newBuilder()
                    .setApplication(residualApplication)
                    .setRequestedTimeDelay(
                        Durations.fromMillis(continuation.resumeDelay().getMillis()))
                    .build()));
      }
    } finally {
      currentElement = null;
      currentRestriction = null;
      currentWindow = null;
      currentTracker = null;
    }
  }

  public void processTimer(
      String timerId, TimeDomain timeDomain, WindowedValue<KV<Object, Timer>> timer) {
    currentTimer = timer;
    currentTimeDomain = timeDomain;
    try {
      Iterator<BoundedWindow> windowIterator =
          (Iterator<BoundedWindow>) timer.getWindows().iterator();
      while (windowIterator.hasNext()) {
        currentWindow = windowIterator.next();
        doFnInvoker.invokeOnTimer(timerId, "", onTimerContext);
      }
    } finally {
      currentTimer = null;
      currentTimeDomain = null;
      currentWindow = null;
    }
  }

  public void finishBundle() {
    doFnInvoker.invokeFinishBundle(finishBundleContext);

    // TODO: Support caching state data across bundle boundaries.
    this.stateAccessor.finalizeState();
    this.stateAccessor = null;
  }

  public void tearDown() {
    doFnInvoker.invokeTeardown();
  }

  /** Outputs the given element to the specified set of consumers wrapping any exceptions. */
  private <T> void outputTo(
      Collection<FnDataReceiver<WindowedValue<T>>> consumers, WindowedValue<T> output) {
    try {
      for (FnDataReceiver<WindowedValue<T>> consumer : consumers) {
        consumer.accept(output);
      }
    } catch (Throwable t) {
      throw UserCodeException.wrap(t);
    }
  }

  private class FnApiTimer implements org.apache.beam.sdk.state.Timer {
    private final String timerId;
    private final TimeDomain timeDomain;
    private final Instant currentTimestamp;
    private final Duration allowedLateness;
    private final WindowedValue<?> currentElementOrTimer;
    @Nullable private Instant currentOutputTimestamp;

    private Duration period = Duration.ZERO;
    private Duration offset = Duration.ZERO;

    FnApiTimer(String timerId, WindowedValue<KV<?, ?>> currentElementOrTimer) {
      this.timerId = timerId;
      this.currentElementOrTimer = currentElementOrTimer;

      TimerDeclaration timerDeclaration = context.doFnSignature.timerDeclarations().get(timerId);
      this.timeDomain =
          DoFnSignatures.getTimerSpecOrThrow(timerDeclaration, context.doFn).getTimeDomain();

      switch (timeDomain) {
        case EVENT_TIME:
          this.currentTimestamp = currentElementOrTimer.getTimestamp();
          break;
        case PROCESSING_TIME:
          this.currentTimestamp = new Instant(DateTimeUtils.currentTimeMillis());
          break;
        case SYNCHRONIZED_PROCESSING_TIME:
          this.currentTimestamp = new Instant(DateTimeUtils.currentTimeMillis());
          break;
        default:
          throw new IllegalArgumentException(String.format("Unknown time domain %s", timeDomain));
      }

      try {
        this.allowedLateness =
            context
                .rehydratedComponents
                .getPCollection(context.pTransform.getInputsOrThrow(timerId))
                .getWindowingStrategy()
                .getAllowedLateness();
      } catch (IOException e) {
        throw new IllegalArgumentException(
            String.format("Unable to get allowed lateness for timer %s", timerId));
      }
    }

    @Override
    public void set(Instant absoluteTime) {
      // Verifies that the time domain of this timer is acceptable for absolute timers.
      if (!TimeDomain.EVENT_TIME.equals(timeDomain)) {
        throw new IllegalArgumentException(
            "Can only set relative timers in processing time domain. Use #setRelative()");
      }

      // Ensures that the target time is reasonable. For event time timers this means that the time
      // should be prior to window GC time.
      if (TimeDomain.EVENT_TIME.equals(timeDomain)) {
        Instant windowExpiry = LateDataUtils.garbageCollectionTime(currentWindow, allowedLateness);
        checkArgument(
            !absoluteTime.isAfter(windowExpiry),
            "Attempted to set event time timer for %s but that is after"
                + " the expiration of window %s",
            absoluteTime,
            windowExpiry);
      }

      output(absoluteTime);
    }

    @Override
    public void setRelative() {
      Instant target;
      if (period.equals(Duration.ZERO)) {
        target = currentTimestamp.plus(offset);
      } else {
        long millisSinceStart = currentTimestamp.plus(offset).getMillis() % period.getMillis();
        target =
            millisSinceStart == 0
                ? currentTimestamp
                : currentTimestamp.plus(period).minus(millisSinceStart);
      }
      target = minTargetAndGcTime(target);
      output(target);
    }

    @Override
    public org.apache.beam.sdk.state.Timer offset(Duration offset) {
      this.offset = offset;
      return this;
    }

    @Override
    public org.apache.beam.sdk.state.Timer align(Duration period) {
      this.period = period;
      return this;
    }

    @Override
    public org.apache.beam.sdk.state.Timer withOutputTimestamp(Instant outputTime) {
      Instant windowExpiry = LateDataUtils.garbageCollectionTime(currentWindow, allowedLateness);
      checkArgument(
          !outputTime.isAfter(windowExpiry),
          "Attempted to set timer with output timestamp %s but that is after"
              + " the expiration of window %s",
          outputTime,
          windowExpiry);

      this.currentOutputTimestamp = outputTime;
      return this;
    }
    /**
     * For event time timers the target time should be prior to window GC time. So it returns
     * min(time to set, GC Time of window).
     */
    private Instant minTargetAndGcTime(Instant target) {
      if (TimeDomain.EVENT_TIME.equals(timeDomain)) {
        Instant windowExpiry = LateDataUtils.garbageCollectionTime(currentWindow, allowedLateness);
        if (target.isAfter(windowExpiry)) {
          return windowExpiry;
        }
      }
      return target;
    }

    private void output(Instant scheduledTime) {
      Object key = ((KV) currentElementOrTimer.getValue()).getKey();
      Collection<FnDataReceiver<WindowedValue<KV<Object, Timer>>>> consumers =
          (Collection) context.localNameToConsumer.get(timerId);

      if (currentOutputTimestamp == null) {
        if (TimeDomain.EVENT_TIME.equals(timeDomain)) {
          currentOutputTimestamp = scheduledTime;
        } else {
          currentOutputTimestamp = currentElementOrTimer.getTimestamp();
        }
      }
      outputTo(
          consumers,
          WindowedValue.of(
              KV.of(key, Timer.of(scheduledTime)),
              currentOutputTimestamp,
              currentElementOrTimer.getWindows(),
              currentElementOrTimer.getPane()));
    }
  }

  private static class FnApiTimerMap implements TimerMap {
    FnApiTimerMap() {}

    @Override
    public void set(String timerId, Instant absoluteTime) {}

    @Override
    public org.apache.beam.sdk.state.Timer get(String timerId) {
      return null;
    }
  }

  /**
   * Provides arguments for a {@link DoFnInvoker} for {@link DoFn.ProcessElement @ProcessElement}.
   */
  private class ProcessBundleContext extends DoFn<InputT, OutputT>.ProcessContext
      implements DoFnInvoker.ArgumentProvider<InputT, OutputT> {

    private ProcessBundleContext() {
      context.doFn.super();
    }

    @Override
    public BoundedWindow window() {
      return currentWindow;
    }

    @Override
    public PaneInfo paneInfo(DoFn<InputT, OutputT> doFn) {
      return pane();
    }

    @Override
    public DoFn<InputT, OutputT>.StartBundleContext startBundleContext(DoFn<InputT, OutputT> doFn) {
      throw new UnsupportedOperationException(
          "Cannot access StartBundleContext outside of @StartBundle method.");
    }

    @Override
    public DoFn<InputT, OutputT>.FinishBundleContext finishBundleContext(
        DoFn<InputT, OutputT> doFn) {
      throw new UnsupportedOperationException(
          "Cannot access FinishBundleContext outside of @FinishBundle method.");
    }

    @Override
    public DoFn<InputT, OutputT>.ProcessContext processContext(DoFn<InputT, OutputT> doFn) {
      return this;
    }

    @Override
    public InputT element(DoFn<InputT, OutputT> doFn) {
      return element();
    }

    @Override
    public Object sideInput(String tagId) {
      return sideInput(sideInputMapping.get(tagId));
    }

    @Override
    public Object schemaElement(int index) {
      SerializableFunction converter = doFnSchemaInformation.getElementConverters().get(index);
      return converter.apply(element());
    }

    @Override
    public Instant timestamp(DoFn<InputT, OutputT> doFn) {
      return timestamp();
    }

    @Override
    public String timerId(DoFn<InputT, OutputT> doFn) {
      throw new UnsupportedOperationException(
          "Cannot access timerId as parameter outside of @OnTimer method.");
    }

    @Override
    public TimeDomain timeDomain(DoFn<InputT, OutputT> doFn) {
      throw new UnsupportedOperationException(
          "Cannot access time domain outside of @ProcessTimer method.");
    }

    @Override
    public OutputReceiver<OutputT> outputReceiver(DoFn<InputT, OutputT> doFn) {
      return DoFnOutputReceivers.windowedReceiver(this, null);
    }

    @Override
    public OutputReceiver<Row> outputRowReceiver(DoFn<InputT, OutputT> doFn) {
      return DoFnOutputReceivers.rowReceiver(this, null, context.mainOutputSchemaCoder);
    }

    @Override
    public MultiOutputReceiver taggedOutputReceiver(DoFn<InputT, OutputT> doFn) {
      return DoFnOutputReceivers.windowedMultiReceiver(this, context.outputCoders);
    }

    @Override
    public Object restriction() {
      return currentRestriction;
    }

    @Override
    public DoFn<InputT, OutputT>.OnTimerContext onTimerContext(DoFn<InputT, OutputT> doFn) {
      throw new UnsupportedOperationException(
          "Cannot access OnTimerContext outside of @OnTimer methods.");
    }

    @Override
    public RestrictionTracker<?, ?> restrictionTracker() {
      return currentTracker;
    }

    @Override
    public State state(String stateId, boolean alwaysFetched) {
      StateDeclaration stateDeclaration = context.doFnSignature.stateDeclarations().get(stateId);
      checkNotNull(stateDeclaration, "No state declaration found for %s", stateId);
      StateSpec<?> spec;
      try {
        spec = (StateSpec<?>) stateDeclaration.field().get(context.doFn);
      } catch (IllegalAccessException e) {
        throw new RuntimeException(e);
      }
      State state = spec.bind(stateId, stateAccessor);
      if (alwaysFetched) {
        return (State) ((ReadableState) state).readLater();
      } else {
        return state;
      }
    }

    @Override
    public org.apache.beam.sdk.state.Timer timer(String timerId) {
      checkState(
          currentElement.getValue() instanceof KV,
          "Accessing timer in unkeyed context. Current element is not a KV: %s.",
          currentElement.getValue());

      return new FnApiTimer(timerId, (WindowedValue) currentElement);
    }

    @Override
    public TimerMap timerFamily(String tagId) {
      // TODO: implement timerFamily
      return null;
    }

    @Override
    public PipelineOptions getPipelineOptions() {
      return context.pipelineOptions;
    }

    @Override
    public PipelineOptions pipelineOptions() {
      return context.pipelineOptions;
    }

    @Override
    public void output(OutputT output) {
      outputWithTimestamp(output, currentElement.getTimestamp());
    }

    @Override
    public void outputWithTimestamp(OutputT output, Instant timestamp) {
      outputTo(
          mainOutputConsumers,
          WindowedValue.of(output, timestamp, currentWindow, currentElement.getPane()));
    }

    @Override
    public <T> void output(TupleTag<T> tag, T output) {
      outputWithTimestamp(tag, output, currentElement.getTimestamp());
    }

    @Override
    public <T> void outputWithTimestamp(TupleTag<T> tag, T output, Instant timestamp) {
      Collection<FnDataReceiver<WindowedValue<T>>> consumers =
          (Collection) context.localNameToConsumer.get(tag.getId());
      if (consumers == null) {
        throw new IllegalArgumentException(String.format("Unknown output tag %s", tag));
      }
      outputTo(
          consumers, WindowedValue.of(output, timestamp, currentWindow, currentElement.getPane()));
    }

    @Override
    public InputT element() {
      return currentElement.getValue();
    }

    @Override
    public <T> T sideInput(PCollectionView<T> view) {
      return stateAccessor.get(view, currentWindow);
    }

    @Override
    public Instant timestamp() {
      return currentElement.getTimestamp();
    }

    @Override
    public PaneInfo pane() {
      return currentElement.getPane();
    }

    @Override
    public void updateWatermark(Instant watermark) {
      throw new UnsupportedOperationException("TODO: Add support for SplittableDoFn");
    }
  }

  /** Provides arguments for a {@link DoFnInvoker} for {@link DoFn.OnTimer @OnTimer}. */
  private class OnTimerContext extends DoFn<InputT, OutputT>.OnTimerContext
      implements DoFnInvoker.ArgumentProvider<InputT, OutputT> {

    private OnTimerContext() {
      context.doFn.super();
    }

    @Override
    public BoundedWindow window() {
      return currentWindow;
    }

    @Override
    public PaneInfo paneInfo(DoFn<InputT, OutputT> doFn) {
      throw new UnsupportedOperationException(
          "Cannot access paneInfo outside of @ProcessElement methods.");
    }

    @Override
    public DoFn<InputT, OutputT>.StartBundleContext startBundleContext(DoFn<InputT, OutputT> doFn) {
      throw new UnsupportedOperationException(
          "Cannot access StartBundleContext outside of @StartBundle method.");
    }

    @Override
    public DoFn<InputT, OutputT>.FinishBundleContext finishBundleContext(
        DoFn<InputT, OutputT> doFn) {
      throw new UnsupportedOperationException(
          "Cannot access FinishBundleContext outside of @FinishBundle method.");
    }

    @Override
    public DoFn<InputT, OutputT>.ProcessContext processContext(DoFn<InputT, OutputT> doFn) {
      throw new UnsupportedOperationException(
          "Cannot access ProcessContext outside of @ProcessElement method.");
    }

    @Override
    public InputT element(DoFn<InputT, OutputT> doFn) {
      throw new UnsupportedOperationException("Element parameters are not supported.");
    }

    @Override
    public InputT sideInput(String tagId) {
      throw new UnsupportedOperationException("SideInput parameters are not supported.");
    }

    @Override
    public Object schemaElement(int index) {
      throw new UnsupportedOperationException("Element parameters are not supported.");
    }

    @Override
    public Instant timestamp(DoFn<InputT, OutputT> doFn) {
      return timestamp();
    }

    @Override
    public String timerId(DoFn<InputT, OutputT> doFn) {
      throw new UnsupportedOperationException("TimerId parameters are not supported.");
    }

    @Override
    public TimeDomain timeDomain(DoFn<InputT, OutputT> doFn) {
      return timeDomain();
    }

    @Override
    public OutputReceiver<OutputT> outputReceiver(DoFn<InputT, OutputT> doFn) {
      return DoFnOutputReceivers.windowedReceiver(this, null);
    }

    @Override
    public OutputReceiver<Row> outputRowReceiver(DoFn<InputT, OutputT> doFn) {
      return DoFnOutputReceivers.rowReceiver(this, null, context.mainOutputSchemaCoder);
    }

    @Override
    public MultiOutputReceiver taggedOutputReceiver(DoFn<InputT, OutputT> doFn) {
      return DoFnOutputReceivers.windowedMultiReceiver(this);
    }

    @Override
    public Object restriction() {
      throw new UnsupportedOperationException("Restriction parameters are not supported.");
    }

    @Override
    public DoFn<InputT, OutputT>.OnTimerContext onTimerContext(DoFn<InputT, OutputT> doFn) {
      return this;
    }

    @Override
    public RestrictionTracker<?, ?> restrictionTracker() {
      throw new UnsupportedOperationException("RestrictionTracker parameters are not supported.");
    }

    @Override
    public State state(String stateId, boolean alwaysFetched) {
      StateDeclaration stateDeclaration = context.doFnSignature.stateDeclarations().get(stateId);
      checkNotNull(stateDeclaration, "No state declaration found for %s", stateId);
      StateSpec<?> spec;
      try {
        spec = (StateSpec<?>) stateDeclaration.field().get(context.doFn);
      } catch (IllegalAccessException e) {
        throw new RuntimeException(e);
      }
      State state = spec.bind(stateId, stateAccessor);
      if (alwaysFetched) {
        return (State) ((ReadableState) state).readLater();
      } else {
        return state;
      }
    }

    @Override
    public org.apache.beam.sdk.state.Timer timer(String timerId) {
      checkState(
          currentTimer.getValue() instanceof KV,
          "Accessing timer in unkeyed context. Current timer is not a KV: %s.",
          currentTimer);

      return new FnApiTimer(timerId, (WindowedValue) currentTimer);
    }

    @Override
    public TimerMap timerFamily(String tagId) {
      // TODO: implement timerFamily
      throw new UnsupportedOperationException("TimerFamily parameters are not supported.");
    }

    @Override
    public PipelineOptions getPipelineOptions() {
      return context.pipelineOptions;
    }

    @Override
    public PipelineOptions pipelineOptions() {
      return context.pipelineOptions;
    }

    @Override
    public void output(OutputT output) {
      outputTo(
          mainOutputConsumers,
          WindowedValue.of(output, currentTimer.getTimestamp(), currentWindow, PaneInfo.NO_FIRING));
    }

    @Override
    public void outputWithTimestamp(OutputT output, Instant timestamp) {
      checkArgument(
          !currentTimer.getTimestamp().isAfter(timestamp),
          "Output time %s can not be before timer timestamp %s.",
          timestamp,
          currentTimer.getTimestamp());
      outputTo(
          mainOutputConsumers,
          WindowedValue.of(output, timestamp, currentWindow, PaneInfo.NO_FIRING));
    }

    @Override
    public <T> void output(TupleTag<T> tag, T output) {
      Collection<FnDataReceiver<WindowedValue<T>>> consumers =
          (Collection) context.localNameToConsumer.get(tag.getId());
      if (consumers == null) {
        throw new IllegalArgumentException(String.format("Unknown output tag %s", tag));
      }
      outputTo(
          consumers,
          WindowedValue.of(output, currentTimer.getTimestamp(), currentWindow, PaneInfo.NO_FIRING));
    }

    @Override
    public <T> void outputWithTimestamp(TupleTag<T> tag, T output, Instant timestamp) {
      checkArgument(
          !currentTimer.getTimestamp().isAfter(timestamp),
          "Output time %s can not be before timer timestamp %s.",
          timestamp,
          currentTimer.getTimestamp());
      Collection<FnDataReceiver<WindowedValue<T>>> consumers =
          (Collection) context.localNameToConsumer.get(tag.getId());
      if (consumers == null) {
        throw new IllegalArgumentException(String.format("Unknown output tag %s", tag));
      }
      outputTo(consumers, WindowedValue.of(output, timestamp, currentWindow, PaneInfo.NO_FIRING));
    }

    @Override
    public TimeDomain timeDomain() {
      return currentTimeDomain;
    }

    @Override
    public Instant fireTimestamp() {
      return currentTimer.getValue().getValue().getTimestamp();
    }

    @Override
    public Instant timestamp() {
      return currentTimer.getTimestamp();
    }
  }
}
