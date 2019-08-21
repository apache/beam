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

import java.util.List;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import org.apache.beam.runners.core.DoFnRunner;
import org.apache.beam.runners.core.DoFnRunners;
import org.apache.beam.runners.core.SideInputHandler;
import org.apache.beam.runners.core.StateInternals;
import org.apache.beam.runners.core.StatefulDoFnRunner;
import org.apache.beam.runners.core.StepContext;
import org.apache.beam.runners.core.TimerInternals;
import org.apache.beam.runners.fnexecution.control.BundleProgressHandler;
import org.apache.beam.runners.fnexecution.control.OutputReceiverFactory;
import org.apache.beam.runners.fnexecution.control.RemoteBundle;
import org.apache.beam.runners.fnexecution.control.StageBundleFactory;
import org.apache.beam.runners.fnexecution.state.StateRequestHandler;
import org.apache.beam.runners.samza.SamzaExecutionContext;
import org.apache.beam.runners.samza.SamzaPipelineOptions;
import org.apache.beam.runners.samza.metrics.DoFnRunnerWithMetrics;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.fn.data.FnDataReceiver;
import org.apache.beam.sdk.state.TimeDomain;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.DoFnSchemaInformation;
import org.apache.beam.sdk.transforms.reflect.DoFnSignature;
import org.apache.beam.sdk.transforms.reflect.DoFnSignatures;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Iterables;
import org.apache.samza.context.Context;
import org.joda.time.Instant;

/** A factory for Samza runner translator to create underlying DoFnRunner used in {@link DoFnOp}. */
public class SamzaDoFnRunners {

  /** Create DoFnRunner for java runner. */
  public static <InT, FnOutT> DoFnRunner<InT, FnOutT> create(
      SamzaPipelineOptions pipelineOptions,
      DoFn<InT, FnOutT> doFn,
      WindowingStrategy<?, ?> windowingStrategy,
      String stepName,
      String stateId,
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
      Map<String, PCollectionView<?>> sideInputMapping) {
    final KeyedInternals keyedInternals;
    final TimerInternals timerInternals;
    final StateInternals stateInternals;
    final DoFnSignature signature = DoFnSignatures.getSignature(doFn.getClass());
    final SamzaStoreStateInternals.Factory<?> stateInternalsFactory =
        SamzaStoreStateInternals.createStateInternalFactory(
            stateId, keyCoder, context.getTaskContext(), pipelineOptions, signature);

    final SamzaExecutionContext executionContext =
        (SamzaExecutionContext) context.getApplicationContainerContext();
    if (signature.usesState()) {
      keyedInternals = new KeyedInternals(stateInternalsFactory, timerInternalsFactory);
      stateInternals = keyedInternals.stateInternals();
      timerInternals = keyedInternals.timerInternals();
    } else {
      keyedInternals = null;
      stateInternals = stateInternalsFactory.stateInternalsForKey(null);
      timerInternals = timerInternalsFactory.timerInternalsForKey(null);
    }

    final DoFnRunner<InT, FnOutT> underlyingRunner =
        DoFnRunners.simpleRunner(
            pipelineOptions,
            doFn,
            sideInputHandler,
            outputManager,
            mainOutputTag,
            sideOutputTags,
            createStepContext(stateInternals, timerInternals),
            inputCoder,
            outputCoders,
            windowingStrategy,
            doFnSchemaInformation,
            sideInputMapping);

    final DoFnRunner<InT, FnOutT> doFnRunnerWithMetrics =
        pipelineOptions.getEnableMetrics()
            ? DoFnRunnerWithMetrics.wrap(
                underlyingRunner, executionContext.getMetricsContainer(), stepName)
            : underlyingRunner;

    if (keyedInternals != null) {
      final DoFnRunner<InT, FnOutT> statefulDoFnRunner =
          DoFnRunners.defaultStatefulDoFnRunner(
              doFn,
              doFnRunnerWithMetrics,
              windowingStrategy,
              new StatefulDoFnRunner.TimeInternalsCleanupTimer(timerInternals, windowingStrategy),
              createStateCleaner(doFn, windowingStrategy, keyedInternals.stateInternals()));

      return new DoFnRunnerWithKeyedInternals<>(statefulDoFnRunner, keyedInternals);
    } else {
      return doFnRunnerWithMetrics;
    }
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
  public static <InT, FnOutT> DoFnRunner<InT, FnOutT> createPortable(
      DoFnRunners.OutputManager outputManager,
      StageBundleFactory stageBundleFactory,
      TupleTag<FnOutT> mainOutputTag,
      Map<String, TupleTag<?>> idToTupleTagMap,
      Context context,
      String stepName) {
    final SamzaExecutionContext executionContext =
        (SamzaExecutionContext) context.getApplicationContainerContext();
    final DoFnRunner<InT, FnOutT> sdkHarnessDoFnRunner =
        new SdkHarnessDoFnRunner<>(
            outputManager, stageBundleFactory, mainOutputTag, idToTupleTagMap);
    return DoFnRunnerWithMetrics.wrap(
        sdkHarnessDoFnRunner, executionContext.getMetricsContainer(), stepName);
  }

  private static class SdkHarnessDoFnRunner<InT, FnOutT> implements DoFnRunner<InT, FnOutT> {
    private final DoFnRunners.OutputManager outputManager;
    private final StageBundleFactory stageBundleFactory;
    private final TupleTag<FnOutT> mainOutputTag;
    private final Map<String, TupleTag<?>> idToTupleTagMap;
    private final LinkedBlockingQueue<KV<String, FnOutT>> outputQueue = new LinkedBlockingQueue<>();

    private SdkHarnessDoFnRunner(
        DoFnRunners.OutputManager outputManager,
        StageBundleFactory stageBundleFactory,
        TupleTag<FnOutT> mainOutputTag,
        Map<String, TupleTag<?>> idToTupleTagMap) {
      this.outputManager = outputManager;
      this.stageBundleFactory = stageBundleFactory;
      this.mainOutputTag = mainOutputTag;
      this.idToTupleTagMap = idToTupleTagMap;
    }

    @Override
    public void startBundle() {}

    @Override
    public void processElement(WindowedValue<InT> elem) {
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

        try (RemoteBundle bundle =
            stageBundleFactory.getBundle(
                receiverFactory,
                StateRequestHandler.unsupported(),
                BundleProgressHandler.ignored())) {
          Iterables.getOnlyElement(bundle.getInputReceivers().values()).accept(elem);
        }

        // RemoteBundle close blocks until all results are received
        KV<String, FnOutT> result;
        while ((result = outputQueue.poll()) != null) {
          outputManager.output(
              idToTupleTagMap.get(result.getKey()), (WindowedValue) result.getValue());
        }
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    public void onTimer(
        String timerId, BoundedWindow window, Instant timestamp, TimeDomain timeDomain) {}

    @Override
    public void finishBundle() {}

    @Override
    public DoFn<InT, FnOutT> getFn() {
      throw new UnsupportedOperationException();
    }
  }
}
