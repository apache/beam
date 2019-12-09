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

import com.google.auto.service.AutoService;
import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import org.apache.beam.fn.harness.DoFnPTransformRunnerFactory.Context;
import org.apache.beam.fn.harness.state.FnApiStateAccessor;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.BundleApplication;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.DelayedBundleApplication;
import org.apache.beam.runners.core.OutputAndTimeBoundedSplittableProcessElementInvoker;
import org.apache.beam.runners.core.OutputWindowedValue;
import org.apache.beam.runners.core.SplittableProcessElementInvoker;
import org.apache.beam.runners.core.construction.PTransformTranslation;
import org.apache.beam.runners.core.construction.Timer;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.fn.data.FnDataReceiver;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.state.TimeDomain;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.reflect.DoFnInvoker;
import org.apache.beam.sdk.transforms.reflect.DoFnInvokers;
import org.apache.beam.sdk.transforms.splittabledofn.RestrictionTracker;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.util.UserCodeException;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.util.WindowedValue.FullWindowedValueCoder;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.vendor.grpc.v1p21p0.com.google.protobuf.ByteString;
import org.apache.beam.vendor.grpc.v1p21p0.com.google.protobuf.util.Durations;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Iterables;
import org.joda.time.Duration;
import org.joda.time.Instant;

/** Runs the {@link PTransformTranslation#SPLITTABLE_PROCESS_ELEMENTS_URN} transform. */
public class SplittableProcessElementsRunner<InputT, RestrictionT, OutputT>
    implements DoFnPTransformRunnerFactory.DoFnPTransformRunner<KV<InputT, RestrictionT>> {
  /** A registrar which provides a factory to handle Java {@link DoFn}s. */
  @AutoService(PTransformRunnerFactory.Registrar.class)
  public static class Registrar implements PTransformRunnerFactory.Registrar {
    @Override
    public Map<String, PTransformRunnerFactory> getPTransformRunnerFactories() {
      return ImmutableMap.of(PTransformTranslation.SPLITTABLE_PROCESS_ELEMENTS_URN, new Factory());
    }
  }

  static class Factory<InputT, RestrictionT, OutputT>
      extends DoFnPTransformRunnerFactory<
          KV<InputT, RestrictionT>,
          InputT,
          OutputT,
          SplittableProcessElementsRunner<InputT, RestrictionT, OutputT>> {

    @Override
    SplittableProcessElementsRunner<InputT, RestrictionT, OutputT> createRunner(
        Context<InputT, OutputT> context) {
      Coder<WindowedValue<KV<InputT, RestrictionT>>> windowedCoder =
          FullWindowedValueCoder.of(
              (Coder<KV<InputT, RestrictionT>>) context.inputCoder, context.windowCoder);

      return new SplittableProcessElementsRunner<>(
          context,
          windowedCoder,
          (Collection<FnDataReceiver<WindowedValue<OutputT>>>)
              (Collection) context.localNameToConsumer.get(context.mainOutputTag.getId()),
          Iterables.getOnlyElement(context.pTransform.getInputsMap().keySet()));
    }
  }

  //////////////////////////////////////////////////////////////////////////////////////////////////

  private final Context<InputT, OutputT> context;
  private final String mainInputId;
  private final Coder<WindowedValue<KV<InputT, RestrictionT>>> inputCoder;
  private final Collection<FnDataReceiver<WindowedValue<OutputT>>> mainOutputConsumers;
  private final DoFnInvoker<InputT, OutputT> doFnInvoker;
  private final ScheduledExecutorService executor;

  private FnApiStateAccessor stateAccessor;

  private final DoFn<InputT, OutputT>.StartBundleContext startBundleContext;
  private final DoFn<InputT, OutputT>.FinishBundleContext finishBundleContext;

  SplittableProcessElementsRunner(
      Context<InputT, OutputT> context,
      Coder<WindowedValue<KV<InputT, RestrictionT>>> inputCoder,
      Collection<FnDataReceiver<WindowedValue<OutputT>>> mainOutputConsumers,
      String mainInputId) {
    this.context = context;
    this.mainInputId = mainInputId;
    this.inputCoder = inputCoder;
    this.mainOutputConsumers = mainOutputConsumers;
    this.doFnInvoker = DoFnInvokers.invokerFor(context.doFn);
    this.doFnInvoker.invokeSetup();
    this.executor = Executors.newSingleThreadScheduledExecutor();

    this.startBundleContext =
        context.doFn.new StartBundleContext() {
          @Override
          public PipelineOptions getPipelineOptions() {
            return context.pipelineOptions;
          }
        };
    this.finishBundleContext =
        context.doFn.new FinishBundleContext() {
          @Override
          public PipelineOptions getPipelineOptions() {
            return context.pipelineOptions;
          }

          @Override
          public void output(OutputT output, Instant timestamp, BoundedWindow window) {
            throw new UnsupportedOperationException();
          }

          @Override
          public <T> void output(
              TupleTag<T> tag, T output, Instant timestamp, BoundedWindow window) {
            throw new UnsupportedOperationException();
          }
        };
  }

  @Override
  public void startBundle() {
    doFnInvoker.invokeStartBundle(startBundleContext);
  }

  @Override
  public void processElement(WindowedValue<KV<InputT, RestrictionT>> elem) {
    processElementTyped(elem);
  }

  private <PositionT> void processElementTyped(WindowedValue<KV<InputT, RestrictionT>> elem) {
    checkArgument(
        elem.getWindows().size() == 1,
        "SPLITTABLE_PROCESS_ELEMENTS expects its input to be in 1 window, but got %s windows",
        elem.getWindows().size());
    WindowedValue<InputT> element = elem.withValue(elem.getValue().getKey());
    BoundedWindow window = elem.getWindows().iterator().next();
    this.stateAccessor =
        new FnApiStateAccessor(
            context.pipelineOptions,
            context.ptransformId,
            context.processBundleInstructionId,
            context.tagToSideInputSpecMap,
            context.beamFnStateClient,
            context.keyCoder,
            (Coder<BoundedWindow>) context.windowCoder,
            () -> elem,
            () -> window);
    RestrictionTracker<RestrictionT, PositionT> tracker =
        doFnInvoker.invokeNewTracker(elem.getValue().getValue());
    OutputAndTimeBoundedSplittableProcessElementInvoker<InputT, OutputT, RestrictionT, PositionT>
        processElementInvoker =
            new OutputAndTimeBoundedSplittableProcessElementInvoker<>(
                context.doFn,
                context.pipelineOptions,
                new OutputWindowedValue<OutputT>() {
                  @Override
                  public void outputWindowedValue(
                      OutputT output,
                      Instant timestamp,
                      Collection<? extends BoundedWindow> windows,
                      PaneInfo pane) {
                    outputTo(
                        mainOutputConsumers, WindowedValue.of(output, timestamp, windows, pane));
                  }

                  @Override
                  public <AdditionalOutputT> void outputWindowedValue(
                      TupleTag<AdditionalOutputT> tag,
                      AdditionalOutputT output,
                      Instant timestamp,
                      Collection<? extends BoundedWindow> windows,
                      PaneInfo pane) {
                    Collection<FnDataReceiver<WindowedValue<AdditionalOutputT>>> consumers =
                        (Collection) context.localNameToConsumer.get(tag.getId());
                    if (consumers == null) {
                      throw new IllegalArgumentException(
                          String.format("Unknown output tag %s", tag));
                    }
                    outputTo(consumers, WindowedValue.of(output, timestamp, windows, pane));
                  }
                },
                stateAccessor,
                executor,
                10000,
                Duration.standardSeconds(10));
    SplittableProcessElementInvoker<InputT, OutputT, RestrictionT, PositionT>.Result result =
        processElementInvoker.invokeProcessElement(doFnInvoker, element, tracker);
    this.stateAccessor = null;

    if (result.getContinuation().shouldResume()) {
      WindowedValue<KV<InputT, RestrictionT>> primary =
          element.withValue(KV.of(element.getValue(), tracker.currentRestriction()));
      WindowedValue<KV<InputT, RestrictionT>> residual =
          element.withValue(KV.of(element.getValue(), result.getResidualRestriction()));
      ByteString.Output primaryBytes = ByteString.newOutput();
      ByteString.Output residualBytes = ByteString.newOutput();
      try {
        inputCoder.encode(primary, primaryBytes);
        inputCoder.encode(residual, residualBytes);
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
                      Durations.fromMillis(result.getContinuation().resumeDelay().getMillis()))
                  .build()));
    }
  }

  @Override
  public void processTimer(
      String timerId, TimeDomain timeDomain, WindowedValue<KV<Object, Timer>> input) {
    throw new UnsupportedOperationException("Timers are unsupported in a SplittableDoFn.");
  }

  @Override
  public void finishBundle() {
    doFnInvoker.invokeFinishBundle(finishBundleContext);
  }

  @Override
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
}
