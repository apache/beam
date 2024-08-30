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
package org.apache.beam.runners.samza.runtime;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.runners.core.DoFnRunner;
import org.apache.beam.runners.core.DoFnRunners;
import org.apache.beam.runners.core.SideInputHandler;
import org.apache.beam.runners.core.StateInternals;
import org.apache.beam.runners.core.StateNamespaces;
import org.apache.beam.runners.core.StateTags;
import org.apache.beam.runners.core.StatefulDoFnRunner;
import org.apache.beam.runners.core.StepContext;
import org.apache.beam.runners.core.TimerInternals;
import org.apache.beam.runners.fnexecution.control.OutputReceiverFactory;
import org.apache.beam.runners.fnexecution.control.RemoteBundle;
import org.apache.beam.runners.fnexecution.control.StageBundleFactory;
import org.apache.beam.runners.fnexecution.control.TimerReceiverFactory;
import org.apache.beam.runners.fnexecution.state.StateRequestHandler;
import org.apache.beam.runners.samza.SamzaExecutionContext;
import org.apache.beam.runners.samza.SamzaPipelineOptions;
import org.apache.beam.runners.samza.metrics.DoFnRunnerWithMetrics;
import org.apache.beam.runners.samza.util.StateUtils;
import org.apache.beam.runners.samza.util.WindowUtils;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.fn.data.FnDataReceiver;
import org.apache.beam.sdk.state.BagState;
import org.apache.beam.sdk.state.TimeDomain;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.DoFnSchemaInformation;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.util.construction.Timer;
import org.apache.beam.sdk.util.construction.graph.ExecutableStage;
import org.apache.beam.sdk.util.construction.graph.PipelineNode;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Iterables;
import org.apache.samza.context.Context;
import org.joda.time.Instant;

/** A factory for Samza runner translator to create underlying DoFnRunner used in {@link DoFnOp}. */
@SuppressWarnings({
  "rawtypes", // TODO(https://github.com/apache/beam/issues/20447)
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
public class SamzaDoFnRunners {

  /** Create DoFnRunner for java runner. */
  public static <InT, FnOutT> DoFnRunner<InT, FnOutT> create(
      SamzaPipelineOptions pipelineOptions,
      DoFn<InT, FnOutT> doFn,
      WindowingStrategy<?, ?> windowingStrategy,
      String transformFullName,
      String transformId,
      Context context,
      TupleTag<FnOutT> mainOutputTag,
      SideInputHandler sideInputHandler,
      SamzaTimerInternalsFactory<?> timerInternalsFactory,
      Coder<?> keyCoder,
      DoFnRunners.OutputManager outputManager,
      Coder<InT> inputCoder,
      List<TupleTag<?>> sideOutputTags,
      Map<TupleTag<?>, Coder<?>> outputCoders,
      DoFnSchemaInformation doFnSchemaInformation,
      Map<String, PCollectionView<?>> sideInputMapping,
      Map<String, String> stateIdToStoreIdMapping,
      OpEmitter emitter,
      FutureCollector futureCollector) {
    final KeyedInternals keyedInternals;
    final TimerInternals timerInternals;
    final StateInternals stateInternals;
    final SamzaStoreStateInternals.Factory<?> stateInternalsFactory =
        SamzaStoreStateInternals.createStateInternalsFactory(
            transformId,
            keyCoder,
            context.getTaskContext(),
            pipelineOptions,
            stateIdToStoreIdMapping);

    final SamzaExecutionContext executionContext =
        (SamzaExecutionContext) context.getApplicationContainerContext();
    if (StateUtils.isStateful(doFn)) {
      keyedInternals = new KeyedInternals(stateInternalsFactory, timerInternalsFactory);
      stateInternals = keyedInternals.stateInternals();
      timerInternals = keyedInternals.timerInternals();
    } else {
      keyedInternals = null;
      stateInternals = stateInternalsFactory.stateInternalsForKey(null);
      timerInternals = timerInternalsFactory.timerInternalsForKey(null);
    }

    final StepContext stepContext = createStepContext(stateInternals, timerInternals);
    final DoFnRunner<InT, FnOutT> underlyingRunner =
        DoFnRunners.simpleRunner(
            pipelineOptions,
            doFn,
            sideInputHandler,
            outputManager,
            mainOutputTag,
            sideOutputTags,
            stepContext,
            inputCoder,
            outputCoders,
            windowingStrategy,
            doFnSchemaInformation,
            sideInputMapping);

    final DoFnRunner<InT, FnOutT> doFnRunnerWithMetrics =
        pipelineOptions.getEnableMetrics()
            ? DoFnRunnerWithMetrics.wrap(
                underlyingRunner, executionContext.getMetricsContainer(), transformFullName)
            : underlyingRunner;

    final DoFnRunner<InT, FnOutT> doFnRunnerWithStates;
    if (keyedInternals != null) {
      final DoFnRunner<InT, FnOutT> statefulDoFnRunner =
          DoFnRunners.defaultStatefulDoFnRunner(
              doFn,
              inputCoder,
              doFnRunnerWithMetrics,
              stepContext,
              windowingStrategy,
              new StatefulDoFnRunner.TimeInternalsCleanupTimer(timerInternals, windowingStrategy),
              createStateCleaner(doFn, windowingStrategy, keyedInternals.stateInternals()));

      doFnRunnerWithStates = new DoFnRunnerWithKeyedInternals<>(statefulDoFnRunner, keyedInternals);
    } else {
      doFnRunnerWithStates = doFnRunnerWithMetrics;
    }

    return pipelineOptions.getNumThreadsForProcessElement() > 1
        ? AsyncDoFnRunner.create(
            doFnRunnerWithStates, emitter, futureCollector, keyedInternals != null, pipelineOptions)
        : doFnRunnerWithStates;
  }

  /** Creates a {@link StepContext} that allows accessing state and timer internals. */
  private static StepContext createStepContext(
      StateInternals stateInternals, TimerInternals timerInternals) {
    return new StepContext() {
      @Override
      public StateInternals stateInternals() {
        return stateInternals;
      }

      @Override
      public TimerInternals timerInternals() {
        return timerInternals;
      }
    };
  }

  @SuppressWarnings("unchecked")
  private static <InT, FnOutT> StatefulDoFnRunner.StateCleaner<?> createStateCleaner(
      DoFn<InT, FnOutT> doFn,
      WindowingStrategy<?, ?> windowingStrategy,
      StateInternals stateInternals) {
    final TypeDescriptor windowType = windowingStrategy.getWindowFn().getWindowTypeDescriptor();
    if (windowType.isSubtypeOf(TypeDescriptor.of(BoundedWindow.class))) {
      final Coder<? extends BoundedWindow> windowCoder =
          windowingStrategy.getWindowFn().windowCoder();
      return new StatefulDoFnRunner.StateInternalsStateCleaner<>(doFn, stateInternals, windowCoder);
    } else {
      return null;
    }
  }

  /** Create DoFnRunner for portable runner. */
  @SuppressWarnings("unchecked")
  public static <InT, FnOutT> DoFnRunner<InT, FnOutT> createPortable(
      String transformId,
      String stepName,
      String bundleStateId,
      Coder<WindowedValue<InT>> windowedValueCoder,
      ExecutableStage executableStage,
      Map<?, PCollectionView<?>> sideInputMapping,
      SideInputHandler sideInputHandler,
      SamzaStoreStateInternals.Factory<?> nonKeyedStateInternalsFactory,
      SamzaTimerInternalsFactory<?> timerInternalsFactory,
      SamzaPipelineOptions pipelineOptions,
      DoFnRunners.OutputManager outputManager,
      StageBundleFactory stageBundleFactory,
      SamzaExecutionContext samzaExecutionContext,
      TupleTag<FnOutT> mainOutputTag,
      Map<String, TupleTag<?>> idToTupleTagMap,
      Context context,
      String transformFullName) {
    // storing events within a bundle in states
    final BagState<WindowedValue<InT>> bundledEventsBag =
        nonKeyedStateInternalsFactory
            .stateInternalsForKey(null)
            .state(StateNamespaces.global(), StateTags.bag(bundleStateId, windowedValueCoder));

    final StateRequestHandler stateRequestHandler =
        SamzaStateRequestHandlers.of(
            transformId,
            context.getTaskContext(),
            pipelineOptions,
            executableStage,
            stageBundleFactory,
            (Map<RunnerApi.ExecutableStagePayload.SideInputId, PCollectionView<?>>)
                sideInputMapping,
            sideInputHandler);

    final SamzaExecutionContext executionContext =
        (SamzaExecutionContext) context.getApplicationContainerContext();
    final DoFnRunner<InT, FnOutT> underlyingRunner =
        new SdkHarnessDoFnRunner<>(
            pipelineOptions,
            stepName,
            timerInternalsFactory,
            WindowUtils.getWindowStrategy(
                executableStage.getInputPCollection().getId(), executableStage.getComponents()),
            outputManager,
            stageBundleFactory,
            idToTupleTagMap,
            bundledEventsBag,
            stateRequestHandler,
            samzaExecutionContext,
            executableStage.getTransforms());
    return pipelineOptions.getEnableMetrics()
        ? DoFnRunnerWithMetrics.wrap(
            underlyingRunner, executionContext.getMetricsContainer(), transformFullName)
        : underlyingRunner;
  }

  static class SdkHarnessDoFnRunner<InT, FnOutT> implements DoFnRunner<InT, FnOutT> {

    private static final int DEFAULT_METRIC_SAMPLE_RATE = 100;

    private final SamzaPipelineOptions pipelineOptions;
    private final SamzaTimerInternalsFactory timerInternalsFactory;
    private final WindowingStrategy windowingStrategy;
    private final DoFnRunners.OutputManager outputManager;
    private final StageBundleFactory stageBundleFactory;
    private final Map<String, TupleTag<?>> idToTupleTagMap;
    private final LinkedBlockingQueue<KV<String, FnOutT>> outputQueue = new LinkedBlockingQueue<>();
    private final BagState<WindowedValue<InT>> bundledEventsBag;
    private RemoteBundle remoteBundle;
    private FnDataReceiver<WindowedValue<?>> inputReceiver;
    private final StateRequestHandler stateRequestHandler;
    private final SamzaExecutionContext samzaExecutionContext;
    private long startBundleTime;
    private final String stepName;
    private final Collection<PipelineNode.PTransformNode> pTransformNodes;

    private SdkHarnessDoFnRunner(
        SamzaPipelineOptions pipelineOptions,
        String stepName,
        SamzaTimerInternalsFactory<?> timerInternalsFactory,
        WindowingStrategy windowingStrategy,
        DoFnRunners.OutputManager outputManager,
        StageBundleFactory stageBundleFactory,
        Map<String, TupleTag<?>> idToTupleTagMap,
        BagState<WindowedValue<InT>> bundledEventsBag,
        StateRequestHandler stateRequestHandler,
        SamzaExecutionContext samzaExecutionContext,
        Collection<PipelineNode.PTransformNode> pTransformNodes) {
      this.pipelineOptions = pipelineOptions;
      this.timerInternalsFactory = timerInternalsFactory;
      this.windowingStrategy = windowingStrategy;
      this.outputManager = outputManager;
      this.stageBundleFactory = stageBundleFactory;
      this.idToTupleTagMap = idToTupleTagMap;
      this.bundledEventsBag = bundledEventsBag;
      this.stateRequestHandler = stateRequestHandler;
      this.samzaExecutionContext = samzaExecutionContext;
      this.stepName = stepName;
      this.pTransformNodes = pTransformNodes;
    }

    @SuppressWarnings("unchecked")
    private void timerDataConsumer(Timer<?> timerElement, TimerInternals.TimerData timerData) {
      TimerInternals timerInternals =
          timerInternalsFactory.timerInternalsForKey(timerElement.getUserKey());
      if (timerElement.getClearBit()) {
        timerInternals.deleteTimer(timerData);
      } else {
        timerInternals.setTimer(timerData);
      }
    }

    @Override
    public void startBundle() {
      try {
        OutputReceiverFactory receiverFactory =
            new OutputReceiverFactory() {
              @Override
              public FnDataReceiver<FnOutT> create(String pCollectionId) {
                return (receivedElement) -> {
                  // handover to queue, do not block the grpc thread
                  outputQueue.put(KV.of(pCollectionId, receivedElement));
                };
              }
            };

        final Coder<BoundedWindow> windowCoder = windowingStrategy.getWindowFn().windowCoder();
        final TimerReceiverFactory timerReceiverFactory =
            new TimerReceiverFactory(stageBundleFactory, this::timerDataConsumer, windowCoder);

        Map<String, String> transformFullNameToUniqueName =
            pTransformNodes.stream()
                .collect(
                    Collectors.toMap(
                        pTransformNode -> pTransformNode.getId(),
                        pTransformNode -> pTransformNode.getTransform().getUniqueName()));

        SamzaMetricsBundleProgressHandler samzaMetricsBundleProgressHandler =
            new SamzaMetricsBundleProgressHandler(
                stepName,
                samzaExecutionContext.getMetricsContainer(),
                transformFullNameToUniqueName);

        remoteBundle =
            stageBundleFactory.getBundle(
                receiverFactory,
                timerReceiverFactory,
                stateRequestHandler,
                samzaMetricsBundleProgressHandler);

        startBundleTime = getStartBundleTime();

        inputReceiver = Iterables.getOnlyElement(remoteBundle.getInputReceivers().values());
        bundledEventsBag
            .read()
            .forEach(
                elem -> {
                  try {
                    inputReceiver.accept(elem);
                  } catch (Exception e) {
                    throw new RuntimeException(e);
                  }
                });
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }

    @SuppressWarnings({
      "RandomModInteger" // https://errorprone.info/bugpattern/RandomModInteger
    })
    private long getStartBundleTime() {
      /*
       * Use random number for sampling purpose instead of counting as
       * SdkHarnessDoFnRunner is stateless and counters won't persist
       * between invocations of DoFn(s).
       */
      return ThreadLocalRandom.current().nextInt() % DEFAULT_METRIC_SAMPLE_RATE == 0
          ? System.nanoTime()
          : 0;
    }

    @Override
    public void processElement(WindowedValue<InT> elem) {
      try {
        bundledEventsBag.add(elem);
        inputReceiver.accept(elem);
        emitResults();
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }

    private void emitResults() {
      KV<String, FnOutT> result;
      while ((result = outputQueue.poll()) != null) {
        outputManager.output(
            idToTupleTagMap.get(result.getKey()), (WindowedValue) result.getValue());
      }
    }

    private void emitMetrics() {
      if (startBundleTime <= 0) {
        return;
      }

      final long count = Iterables.size(bundledEventsBag.read());

      if (count <= 0) {
        return;
      }

      final long finishBundleTime = System.nanoTime();
      final long averageProcessTime = (finishBundleTime - startBundleTime) / count;

      String metricName = "ExecutableStage-" + stepName + "-process-ns";
      samzaExecutionContext
          .getMetricsContainer()
          .updateExecutableStageBundleMetric(metricName, averageProcessTime);
    }

    @Override
    public <KeyT> void onTimer(
        String timerId,
        String timerFamilyId,
        KeyT key,
        BoundedWindow window,
        Instant timestamp,
        Instant outputTimestamp,
        TimeDomain timeDomain) {
      final KV<String, String> timerReceiverKey =
          TimerReceiverFactory.decodeTimerDataTimerId(timerFamilyId);
      final FnDataReceiver<Timer> timerReceiver =
          remoteBundle.getTimerReceivers().get(timerReceiverKey);
      final Timer timerValue =
          Timer.of(
              key,
              timerId,
              Collections.singletonList(window),
              timestamp,
              outputTimestamp,
              // TODO: Support propagating the PaneInfo through.
              PaneInfo.NO_FIRING);
      try {
        timerReceiver.accept(timerValue);
      } catch (Exception e) {
        throw new RuntimeException(
            String.format(Locale.ENGLISH, "Failed to process timer %s", timerReceiver), e);
      }
    }

    @Override
    public void finishBundle() {
      try {
        runWithTimeout(
            pipelineOptions.getBundleProcessingTimeout(),
            () -> {
              // RemoteBundle close blocks until all results are received
              try {
                remoteBundle.close();
              } catch (Exception e) {
                throw new RuntimeException(e);
              }
            });
        emitResults();
        emitMetrics();
        bundledEventsBag.clear();
      } catch (Exception e) {
        throw new RuntimeException("Failed to finish remote bundle", e);
      } finally {
        remoteBundle = null;
        inputReceiver = null;
      }
    }

    /**
     * Run a function and wait for at most the given time (in milliseconds).
     *
     * @param timeoutInMs the time to wait for completing the function call. If the value of timeout
     *     is negative, wait forever until the function call is completed
     * @param runnable the main function
     */
    static void runWithTimeout(long timeoutInMs, Runnable runnable)
        throws ExecutionException, InterruptedException, TimeoutException {
      if (timeoutInMs < 0) {
        runnable.run();
      } else {
        CompletableFuture.runAsync(runnable).get(timeoutInMs, TimeUnit.MILLISECONDS);
      }
    }

    @Override
    public <KeyT> void onWindowExpiration(BoundedWindow window, Instant timestamp, KeyT key) {}

    @Override
    public DoFn<InT, FnOutT> getFn() {
      throw new UnsupportedOperationException();
    }
  }
}
