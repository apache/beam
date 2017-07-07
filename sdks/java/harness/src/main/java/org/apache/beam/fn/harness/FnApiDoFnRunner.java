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

import static com.google.common.base.Preconditions.checkArgument;

import com.google.auto.service.AutoService;
import com.google.common.collect.Collections2;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.Multimap;
import com.google.protobuf.ByteString;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.function.Supplier;
import org.apache.beam.fn.harness.data.BeamFnDataClient;
import org.apache.beam.fn.harness.fn.ThrowingConsumer;
import org.apache.beam.fn.harness.fn.ThrowingRunnable;
import org.apache.beam.fn.harness.state.BeamFnStateClient;
import org.apache.beam.runners.core.DoFnRunner;
import org.apache.beam.runners.core.construction.ParDoTranslation;
import org.apache.beam.runners.dataflow.util.DoFnInfo;
import org.apache.beam.sdk.common.runner.v1.RunnerApi;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.state.State;
import org.apache.beam.sdk.state.TimeDomain;
import org.apache.beam.sdk.state.Timer;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.DoFn.OnTimerContext;
import org.apache.beam.sdk.transforms.DoFn.ProcessContext;
import org.apache.beam.sdk.transforms.reflect.DoFnInvoker;
import org.apache.beam.sdk.transforms.reflect.DoFnInvokers;
import org.apache.beam.sdk.transforms.reflect.DoFnSignature;
import org.apache.beam.sdk.transforms.reflect.DoFnSignatures;
import org.apache.beam.sdk.transforms.splittabledofn.RestrictionTracker;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.util.SerializableUtils;
import org.apache.beam.sdk.util.UserCodeException;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.joda.time.Instant;

/**
 * A {@link DoFnRunner} specific to integrating with the Fn Api. This is to remove the layers
 * of abstraction caused by StateInternals/TimerInternals since they model state and timer
 * concepts differently.
 */
public class FnApiDoFnRunner<InputT, OutputT> implements DoFnRunner<InputT, OutputT> {
  /**
   * A registrar which provides a factory to handle Java {@link DoFn}s.
   */
  @AutoService(PTransformRunnerFactory.Registrar.class)
  public static class Registrar implements
      PTransformRunnerFactory.Registrar {

    @Override
    public Map<String, PTransformRunnerFactory> getPTransformRunnerFactories() {
      return ImmutableMap.of(ParDoTranslation.CUSTOM_JAVA_DO_FN_URN, new Factory());
    }
  }

  /** A factory for {@link FnApiDoFnRunner}. */
  static class Factory<InputT, OutputT>
      implements PTransformRunnerFactory<DoFnRunner<InputT, OutputT>> {

    @Override
    public DoFnRunner<InputT, OutputT> createRunnerForPTransform(
        PipelineOptions pipelineOptions,
        BeamFnDataClient beamFnDataClient,
        BeamFnStateClient beamFnStateClient,
        String pTransformId,
        RunnerApi.PTransform pTransform,
        Supplier<String> processBundleInstructionId,
        Map<String, RunnerApi.PCollection> pCollections,
        Map<String, RunnerApi.Coder> coders,
        Multimap<String, ThrowingConsumer<WindowedValue<?>>> pCollectionIdsToConsumers,
        Consumer<ThrowingRunnable> addStartFunction,
        Consumer<ThrowingRunnable> addFinishFunction) {

      // For every output PCollection, create a map from output name to Consumer
      ImmutableMap.Builder<String, Collection<ThrowingConsumer<WindowedValue<?>>>>
          outputMapBuilder = ImmutableMap.builder();
      for (Map.Entry<String, String> entry : pTransform.getOutputsMap().entrySet()) {
        outputMapBuilder.put(
            entry.getKey(),
            pCollectionIdsToConsumers.get(entry.getValue()));
      }
      ImmutableMap<String, Collection<ThrowingConsumer<WindowedValue<?>>>> outputMap =
          outputMapBuilder.build();

      // Get the DoFnInfo from the serialized blob.
      ByteString serializedFn = pTransform.getSpec().getPayload();
      @SuppressWarnings({"unchecked", "rawtypes"})
      DoFnInfo<InputT, OutputT> doFnInfo = (DoFnInfo) SerializableUtils.deserializeFromByteArray(
          serializedFn.toByteArray(), "DoFnInfo");

      // Verify that the DoFnInfo tag to output map matches the output map on the PTransform.
      checkArgument(
          Objects.equals(
              new HashSet<>(Collections2.transform(outputMap.keySet(), Long::parseLong)),
              doFnInfo.getOutputMap().keySet()),
          "Unexpected mismatch between transform output map %s and DoFnInfo output map %s.",
          outputMap.keySet(),
          doFnInfo.getOutputMap());

      ImmutableMultimap.Builder<TupleTag<?>,
          ThrowingConsumer<WindowedValue<?>>> tagToOutputMapBuilder =
          ImmutableMultimap.builder();
      for (Map.Entry<Long, TupleTag<?>> entry : doFnInfo.getOutputMap().entrySet()) {
        @SuppressWarnings({"unchecked", "rawtypes"})
        Collection<ThrowingConsumer<WindowedValue<?>>> consumers =
            outputMap.get(Long.toString(entry.getKey()));
        tagToOutputMapBuilder.putAll(entry.getValue(), consumers);
      }

      ImmutableMultimap<TupleTag<?>, ThrowingConsumer<WindowedValue<?>>> tagToOutputMap =
          tagToOutputMapBuilder.build();

      @SuppressWarnings({"unchecked", "rawtypes"})
      DoFnRunner<InputT, OutputT> runner = new FnApiDoFnRunner<>(
          pipelineOptions,
          doFnInfo.getDoFn(),
          (Collection<ThrowingConsumer<WindowedValue<OutputT>>>) (Collection)
              tagToOutputMap.get(doFnInfo.getOutputMap().get(doFnInfo.getMainOutput())),
          tagToOutputMap,
          doFnInfo.getWindowingStrategy());

      // Register the appropriate handlers.
      addStartFunction.accept(runner::startBundle);
      for (String pcollectionId : pTransform.getInputsMap().values()) {
        pCollectionIdsToConsumers.put(
            pcollectionId,
            (ThrowingConsumer) (ThrowingConsumer<WindowedValue<InputT>>) runner::processElement);
      }
      addFinishFunction.accept(runner::finishBundle);
      return runner;
    }
  }

  //////////////////////////////////////////////////////////////////////////////////////////////////

  private final PipelineOptions pipelineOptions;
  private final DoFn<InputT, OutputT> doFn;
  private final Collection<ThrowingConsumer<WindowedValue<OutputT>>> mainOutputConsumers;
  private final Multimap<TupleTag<?>, ThrowingConsumer<WindowedValue<?>>> outputMap;
  private final DoFnInvoker<InputT, OutputT> doFnInvoker;
  private final StartBundleContext startBundleContext;
  private final ProcessBundleContext processBundleContext;
  private final FinishBundleContext finishBundleContext;
  private final WindowingStrategy windowingStrategy;
  private final DoFnSignature doFnSignature;

  /**
   * The lifetime of this member is only valid during {@link #processElement(WindowedValue)}.
   */
  private WindowedValue<InputT> currentElement;

  /**
   * The lifetime of this member is only valid during {@link #processElement(WindowedValue)}.
   */
  private BoundedWindow currentWindow;

  FnApiDoFnRunner(
      PipelineOptions pipelineOptions,
      DoFn<InputT, OutputT> doFn,
      Collection<ThrowingConsumer<WindowedValue<OutputT>>> mainOutputConsumers,
      Multimap<TupleTag<?>, ThrowingConsumer<WindowedValue<?>>> outputMap,
      WindowingStrategy windowingStrategy) {
    this.pipelineOptions = pipelineOptions;
    this.doFn = doFn;
    this.mainOutputConsumers = mainOutputConsumers;
    this.outputMap = outputMap;
    this.windowingStrategy = windowingStrategy;
    this.doFnSignature = DoFnSignatures.signatureForDoFn(doFn);
    this.doFnInvoker = DoFnInvokers.invokerFor(doFn);
    this.startBundleContext = new StartBundleContext();
    this.processBundleContext = new ProcessBundleContext();
    this.finishBundleContext = new FinishBundleContext();
  }

  @Override
  public void startBundle() {
    doFnInvoker.invokeStartBundle(startBundleContext);
  }

  @Override
  public void processElement(WindowedValue<InputT> elem) {
    currentElement = elem;
    try {
      Iterator<BoundedWindow> windowIterator =
          (Iterator<BoundedWindow>) elem.getWindows().iterator();
      while (windowIterator.hasNext()) {
        currentWindow = windowIterator.next();
        doFnInvoker.invokeProcessElement(processBundleContext);
      }
    } finally {
      currentElement = null;
      currentWindow = null;
    }
  }

  @Override
  public void onTimer(
      String timerId,
      BoundedWindow window,
      Instant timestamp,
      TimeDomain timeDomain) {
    throw new UnsupportedOperationException("TODO: Add support for timers");
  }

  @Override
  public void finishBundle() {
    doFnInvoker.invokeFinishBundle(finishBundleContext);
  }

  /**
   * Outputs the given element to the specified set of consumers wrapping any exceptions.
   */
  private <T> void outputTo(
      Collection<ThrowingConsumer<WindowedValue<T>>> consumers,
      WindowedValue<T> output) {
    Iterator<ThrowingConsumer<WindowedValue<T>>> consumerIterator;
    try {
      for (ThrowingConsumer<WindowedValue<T>> consumer : consumers) {
        consumer.accept(output);
      }
    } catch (Throwable t) {
      throw UserCodeException.wrap(t);
    }
  }

  /**
   * Provides arguments for a {@link DoFnInvoker} for {@link DoFn.StartBundle @StartBundle}.
   */
  private class StartBundleContext
      extends DoFn<InputT, OutputT>.StartBundleContext
      implements DoFnInvoker.ArgumentProvider<InputT, OutputT> {

    private StartBundleContext() {
      doFn.super();
    }

    @Override
    public PipelineOptions getPipelineOptions() {
      return pipelineOptions;
    }

    @Override
    public PipelineOptions pipelineOptions() {
      return pipelineOptions;
    }

    @Override
    public BoundedWindow window() {
      throw new UnsupportedOperationException(
          "Cannot access window outside of @ProcessElement and @OnTimer methods.");
    }

    @Override
    public DoFn<InputT, OutputT>.StartBundleContext startBundleContext(
        DoFn<InputT, OutputT> doFn) {
      return this;
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
    public DoFn<InputT, OutputT>.OnTimerContext onTimerContext(DoFn<InputT, OutputT> doFn) {
      throw new UnsupportedOperationException(
          "Cannot access OnTimerContext outside of @OnTimer methods.");
    }

    @Override
    public RestrictionTracker<?> restrictionTracker() {
      throw new UnsupportedOperationException(
          "Cannot access RestrictionTracker outside of @ProcessElement method.");
    }

    @Override
    public State state(String stateId) {
      throw new UnsupportedOperationException(
          "Cannot access state outside of @ProcessElement and @OnTimer methods.");
    }

    @Override
    public Timer timer(String timerId) {
      throw new UnsupportedOperationException(
          "Cannot access timers outside of @ProcessElement and @OnTimer methods.");
    }
  }

  /**
   * Provides arguments for a {@link DoFnInvoker} for {@link DoFn.ProcessElement @ProcessElement}.
   */
  private class ProcessBundleContext
      extends DoFn<InputT, OutputT>.ProcessContext
      implements DoFnInvoker.ArgumentProvider<InputT, OutputT> {

    private ProcessBundleContext() {
      doFn.super();
    }

    @Override
    public BoundedWindow window() {
      return currentWindow;
    }

    @Override
    public DoFn.StartBundleContext startBundleContext(DoFn<InputT, OutputT> doFn) {
      throw new UnsupportedOperationException(
          "Cannot access StartBundleContext outside of @StartBundle method.");
    }

    @Override
    public DoFn.FinishBundleContext finishBundleContext(DoFn<InputT, OutputT> doFn) {
      throw new UnsupportedOperationException(
          "Cannot access FinishBundleContext outside of @FinishBundle method.");
    }

    @Override
    public ProcessContext processContext(DoFn<InputT, OutputT> doFn) {
      return this;
    }

    @Override
    public OnTimerContext onTimerContext(DoFn<InputT, OutputT> doFn) {
      throw new UnsupportedOperationException("TODO: Add support for timers");
    }

    @Override
    public RestrictionTracker<?> restrictionTracker() {
      throw new UnsupportedOperationException("TODO: Add support for SplittableDoFn");
    }

    @Override
    public State state(String stateId) {
      throw new UnsupportedOperationException("TODO: Add support for state");
    }

    @Override
    public Timer timer(String timerId) {
      throw new UnsupportedOperationException("TODO: Add support for timers");
    }

    @Override
    public PipelineOptions getPipelineOptions() {
      return pipelineOptions;
    }

    @Override
    public PipelineOptions pipelineOptions() {
      return pipelineOptions;
    }

    @Override
    public void output(OutputT output) {
      outputTo(mainOutputConsumers,
          WindowedValue.of(
              output,
              currentElement.getTimestamp(),
              currentWindow,
              currentElement.getPane()));
    }

    @Override
    public void outputWithTimestamp(OutputT output, Instant timestamp) {
      outputTo(mainOutputConsumers,
          WindowedValue.of(
              output,
              timestamp,
              currentWindow,
              currentElement.getPane()));
    }

    @Override
    public <T> void output(TupleTag<T> tag, T output) {
      Collection<ThrowingConsumer<WindowedValue<T>>> consumers = (Collection) outputMap.get(tag);
      if (consumers == null) {
        throw new IllegalArgumentException(String.format("Unknown output tag %s", tag));
      }
      outputTo(consumers,
          WindowedValue.of(
              output,
              currentElement.getTimestamp(),
              currentWindow,
              currentElement.getPane()));
    }

    @Override
    public <T> void outputWithTimestamp(TupleTag<T> tag, T output, Instant timestamp) {
      Collection<ThrowingConsumer<WindowedValue<T>>> consumers = (Collection) outputMap.get(tag);
      if (consumers == null) {
        throw new IllegalArgumentException(String.format("Unknown output tag %s", tag));
      }
      outputTo(consumers,
          WindowedValue.of(
              output,
              timestamp,
              currentWindow,
              currentElement.getPane()));
    }

    @Override
    public InputT element() {
      return currentElement.getValue();
    }

    @Override
    public <T> T sideInput(PCollectionView<T> view) {
      throw new UnsupportedOperationException("TODO: Support side inputs");
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

  /**
   * Provides arguments for a {@link DoFnInvoker} for {@link DoFn.FinishBundle @FinishBundle}.
   */
  private class FinishBundleContext
      extends DoFn<InputT, OutputT>.FinishBundleContext
      implements DoFnInvoker.ArgumentProvider<InputT, OutputT> {

    private FinishBundleContext() {
      doFn.super();
    }

    @Override
    public PipelineOptions getPipelineOptions() {
      return pipelineOptions;
    }

    @Override
    public PipelineOptions pipelineOptions() {
      return pipelineOptions;
    }

    @Override
    public BoundedWindow window() {
      throw new UnsupportedOperationException(
          "Cannot access window outside of @ProcessElement and @OnTimer methods.");
    }

    @Override
    public DoFn<InputT, OutputT>.StartBundleContext startBundleContext(
        DoFn<InputT, OutputT> doFn) {
      throw new UnsupportedOperationException(
          "Cannot access StartBundleContext outside of @StartBundle method.");
    }

    @Override
    public DoFn<InputT, OutputT>.FinishBundleContext finishBundleContext(
        DoFn<InputT, OutputT> doFn) {
      return this;
    }

    @Override
    public DoFn<InputT, OutputT>.ProcessContext processContext(DoFn<InputT, OutputT> doFn) {
      throw new UnsupportedOperationException(
          "Cannot access ProcessContext outside of @ProcessElement method.");
    }

    @Override
    public DoFn<InputT, OutputT>.OnTimerContext onTimerContext(DoFn<InputT, OutputT> doFn) {
      throw new UnsupportedOperationException(
          "Cannot access OnTimerContext outside of @OnTimer methods.");
    }

    @Override
    public RestrictionTracker<?> restrictionTracker() {
      throw new UnsupportedOperationException(
          "Cannot access RestrictionTracker outside of @ProcessElement method.");
    }

    @Override
    public State state(String stateId) {
      throw new UnsupportedOperationException(
          "Cannot access state outside of @ProcessElement and @OnTimer methods.");
    }

    @Override
    public Timer timer(String timerId) {
      throw new UnsupportedOperationException(
          "Cannot access timers outside of @ProcessElement and @OnTimer methods.");
    }

    @Override
    public void output(OutputT output, Instant timestamp, BoundedWindow window) {
      outputTo(mainOutputConsumers,
          WindowedValue.of(output, timestamp, window, PaneInfo.NO_FIRING));
    }

    @Override
    public <T> void output(TupleTag<T> tag, T output, Instant timestamp, BoundedWindow window) {
      Collection<ThrowingConsumer<WindowedValue<T>>> consumers = (Collection) outputMap.get(tag);
      if (consumers == null) {
        throw new IllegalArgumentException(String.format("Unknown output tag %s", tag));
      }
      outputTo(consumers,
          WindowedValue.of(output, timestamp, window, PaneInfo.NO_FIRING));
    }
  }
}
