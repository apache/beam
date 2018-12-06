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
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import org.apache.beam.fn.harness.DoFnPTransformRunnerFactory.Context;
import org.apache.beam.fn.harness.state.FnApiStateAccessor;
import org.apache.beam.model.fnexecution.v1.BeamFnApi;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.BundleApplication;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.DelayedBundleApplication;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.runners.core.OutputAndTimeBoundedSplittableProcessElementInvoker;
import org.apache.beam.runners.core.OutputWindowedValue;
import org.apache.beam.runners.core.SplittableProcessElementInvoker;
import org.apache.beam.runners.core.construction.PTransformTranslation;
import org.apache.beam.runners.core.construction.Timer;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.fn.data.FnDataReceiver;
import org.apache.beam.sdk.fn.splittabledofn.DecimalTranslator;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.state.TimeDomain;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.reflect.DoFnInvoker;
import org.apache.beam.sdk.transforms.reflect.DoFnInvokers;
import org.apache.beam.sdk.transforms.splittabledofn.Backlog;
import org.apache.beam.sdk.transforms.splittabledofn.Backlogs;
import org.apache.beam.sdk.transforms.splittabledofn.RestrictionTracker;
import org.apache.beam.sdk.transforms.splittabledofn.Restrictions;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.util.UserCodeException;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.util.WindowedValue.FullWindowedValueCoder;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.vendor.grpc.v1_13_1.com.google.protobuf.ByteString;
import org.apache.beam.vendor.grpc.v1_13_1.com.google.protobuf.util.Timestamps;
import org.joda.time.DateTimeUtils;
import org.joda.time.Duration;
import org.joda.time.Instant;

/** Runs the {@link PTransformTranslation#SPLITTABLE_PROCESS_ELEMENTS_URN} transform. */
public class SplittableProcessElementsRunner<InputT, RestrictionT, PositionT, OutputT>
    implements DoFnPTransformRunnerFactory.DoFnPTransformRunner<KV<InputT, RestrictionT>> {
  /** A registrar which provides a factory to handle Java {@link DoFn}s. */
  @AutoService(PTransformRunnerFactory.Registrar.class)
  public static class Registrar implements PTransformRunnerFactory.Registrar {
    @Override
    public Map<String, PTransformRunnerFactory> getPTransformRunnerFactories() {
      return ImmutableMap.of(PTransformTranslation.SPLITTABLE_PROCESS_ELEMENTS_URN, new Factory());
    }
  }

  static class Factory<InputT, RestrictionT, PositionT, OutputT>
      extends DoFnPTransformRunnerFactory<
          KV<InputT, RestrictionT>, InputT, OutputT,
          SplittableProcessElementsRunner<InputT, RestrictionT, PositionT, OutputT>> {

    @Override
    SplittableProcessElementsRunner<InputT, RestrictionT, PositionT, OutputT> createRunner(
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

  // Lifetime is only valid when a process element call is on the call stack.
  private WindowedValue<KV<InputT, RestrictionT>> currentElement;
  private RestrictionTracker<RestrictionT, PositionT> currentRestrictionTracker;
  private RestrictionT checkpointedRestriction;

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
    this.context.bundleExecutionController.registerCheckpointListener(
        this::checkpointCurrentTracker);

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

  /**
   * Checkpoints the currently executing restriction tracker.
   *
   * <p>Should only be invoked by the same thread that invokes {@link #processElement}.
   */
  public void checkpointCurrentTracker() {
    // This will be non-null if this SDF processElement call is on the call stack
    // and we have accepted the current restriction (it is after the shouldBuffer check).
    if (currentRestrictionTracker != null) {
      checkpointedRestriction = currentRestrictionTracker.checkpoint();
    }
  }

  @Override
  public void startBundle() {
    doFnInvoker.invokeStartBundle(startBundleContext);
  }

  @Override
  public void processElement(WindowedValue<KV<InputT, RestrictionT>> elem) throws Exception {
    try {
      this.currentElement = elem;
      checkArgument(
          elem.getWindows().size() == 1,
          "SPLITTABLE_PROCESS_ELEMENTS expects its input to be in 1 window, but got %s windows",
          elem.getWindows().size());
      WindowedValue<InputT> element = elem.withValue(elem.getValue().getKey());
      BoundedWindow window = elem.getWindows().iterator().next();

      // We first create the restriction tracker and see if we should buffer before assigning
      // the restriction tracker as something we are responsible for processing.
      RestrictionTracker<RestrictionT, PositionT> restrictionTracker =
          doFnInvoker.invokeNewTracker(elem.getValue().getValue());
      if (context.bundleExecutionController.shouldBuffer()) {
        context.bundleExecutionController.buffer(
            createBundleApplicationFor(currentRestrictionTracker));
        return;
      }
      this.currentRestrictionTracker = restrictionTracker;

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
      // TODO: Remove this implementation since we will be able to checkpoint now.
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
          processElementInvoker.invokeProcessElement(
              doFnInvoker, element, currentRestrictionTracker);
      this.stateAccessor = null;

      // Inform the BundleExecutionController of a Runner or user initiated checkpoint
      // TODO: Should we check if the primary / residual restrictions are empty?
      if (result.getContinuation().shouldResume() || checkpointedRestriction != null) {
        BundleApplication primaryApplication =
            createBundleApplicationFor(currentRestrictionTracker);
        List<DelayedBundleApplication> residualRoots = new ArrayList<>();
        if (result.getContinuation().shouldResume()) {
          RestrictionTracker<RestrictionT, PositionT> residualTracker =
              doFnInvoker.invokeNewTracker(result.getResidualRestriction());
          residualRoots.add(
              DelayedBundleApplication.newBuilder()
                  .setApplication(createBundleApplicationFor(residualTracker))
                  .setRequestedExecutionTime(
                      Timestamps.fromMillis(result.getContinuation().resumeTime().getMillis()))
                  .build());
        }
        if (checkpointedRestriction != null) {
          RestrictionTracker<RestrictionT, PositionT> residualTracker =
              doFnInvoker.invokeNewTracker(checkpointedRestriction);
          residualRoots.add(
              DelayedBundleApplication.newBuilder()
                  .setApplication(createBundleApplicationFor(residualTracker))
                  .setRequestedExecutionTime(
                      Timestamps.fromMillis(DateTimeUtils.currentTimeMillis()))
                  .build());
        }
        context.bundleExecutionController.checkpoint(primaryApplication, residualRoots);
      }
    } finally {
      this.currentElement = null;
      this.currentRestrictionTracker = null;
      this.checkpointedRestriction = null;
    }
  }

  private BeamFnApi.BundleApplication createBundleApplicationFor(
      RestrictionTracker<RestrictionT, PositionT> tracker) {
    // Note that the restriction associated with value is the original restriction that was asked
    // to be processed.

    RestrictionT restriction = tracker.currentRestriction();

    ByteString bytes;
    try {
      ByteString.Output bytesOut = ByteString.newOutput();
      inputCoder.encode(
          currentElement.withValue(KV.of(currentElement.getValue().getKey(), restriction)),
          bytesOut);
      bytes = bytesOut.toByteString();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    BeamFnApi.BundleApplication.Backlog.Builder backlogBuilder =
        BeamFnApi.BundleApplication.Backlog.newBuilder();
    if (tracker instanceof Backlogs.HasPartitionedBacklog) {
      backlogBuilder.setPartition(
          ByteString.copyFrom(((Backlogs.HasPartitionedBacklog) tracker).getBacklogPartition()));
    }
    if (tracker instanceof Backlogs.HasBacklog) {
      Backlog backlog = ((Backlogs.HasBacklog) tracker).getBacklog();
      if (backlog.isUnknown()) {
        backlogBuilder.setIsUnknown(true);
      } else {
        backlogBuilder.setValue(DecimalTranslator.toProto(backlog.backlog()));
      }
    } else {
      backlogBuilder.setIsUnknown(true);
    }

    return BundleApplication.newBuilder()
        .setPtransformId(context.ptransformId)
        .setInputId(mainInputId)
        .setElement(bytes)
        .setIsBounded(
            restriction instanceof Restrictions.IsBounded
                ? RunnerApi.IsBounded.Enum.BOUNDED
                : RunnerApi.IsBounded.Enum.UNBOUNDED)
        .setBacklog(backlogBuilder.build())
        .build();
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
