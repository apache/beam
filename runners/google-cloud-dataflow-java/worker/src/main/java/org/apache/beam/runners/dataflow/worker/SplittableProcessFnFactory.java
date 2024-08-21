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
package org.apache.beam.runners.dataflow.worker;

import static org.apache.beam.runners.dataflow.util.CloudObject.fromSpec;
import static org.apache.beam.runners.dataflow.util.CloudObjects.coderFromCloudObject;
import static org.apache.beam.runners.dataflow.util.Structs.getBytes;
import static org.apache.beam.runners.dataflow.util.Structs.getObject;
import static org.apache.beam.sdk.util.SerializableUtils.deserializeFromByteArray;
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkState;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.beam.runners.core.DoFnRunner;
import org.apache.beam.runners.core.DoFnRunners.OutputManager;
import org.apache.beam.runners.core.KeyedWorkItem;
import org.apache.beam.runners.core.KeyedWorkItemCoder;
import org.apache.beam.runners.core.OutputAndTimeBoundedSplittableProcessElementInvoker;
import org.apache.beam.runners.core.OutputWindowedValue;
import org.apache.beam.runners.core.SideInputReader;
import org.apache.beam.runners.core.SimpleDoFnRunner;
import org.apache.beam.runners.core.SplittableParDoViaKeyedWorkItems.ProcessFn;
import org.apache.beam.runners.core.StateInternals;
import org.apache.beam.runners.dataflow.util.CloudObject;
import org.apache.beam.runners.dataflow.util.PropertyNames;
import org.apache.beam.runners.dataflow.worker.util.WorkerPropertyNames;
import org.apache.beam.sdk.coders.ByteArrayCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.StreamingOptions;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.DoFnSchemaInformation;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.util.DoFnInfo;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.joda.time.Duration;
import org.joda.time.Instant;

/**
 * A {@link ParDoFnFactory} to create instances of user {@link ProcessFn} according to
 * specifications from the Dataflow service.
 */
@SuppressWarnings({
  "rawtypes" // TODO(https://github.com/apache/beam/issues/20447)
})
class SplittableProcessFnFactory {
  static final ParDoFnFactory createDefault() {
    return new UserParDoFnFactory(new ProcessFnExtractor(), new SplittableDoFnRunnerFactory());
  }

  private static class ProcessFnExtractor implements UserParDoFnFactory.DoFnExtractor {
    @Override
    public DoFnInfo<?, ?> getDoFnInfo(CloudObject cloudUserFn) throws Exception {
      DoFnInfo<?, ?> doFnInfo =
          (DoFnInfo<?, ?>)
              deserializeFromByteArray(
                  getBytes(cloudUserFn, PropertyNames.SERIALIZED_FN), "Serialized DoFnInfo");
      Coder restrictionAndStateCoder =
          coderFromCloudObject(
              fromSpec(getObject(cloudUserFn, WorkerPropertyNames.RESTRICTION_CODER)));
      checkState(
          restrictionAndStateCoder instanceof KvCoder,
          "Expected pair coder with restriction as key coder and watermark estimator state as value coder, but received %s.",
          restrictionAndStateCoder);
      Coder restrictionCoder = ((KvCoder) restrictionAndStateCoder).getKeyCoder();
      Coder watermarkEstimatorStateCoder = ((KvCoder) restrictionAndStateCoder).getValueCoder();

      ProcessFn processFn =
          new ProcessFn(
              doFnInfo.getDoFn(),
              doFnInfo.getInputCoder(),
              restrictionCoder,
              watermarkEstimatorStateCoder,
              doFnInfo.getWindowingStrategy(),
              doFnInfo.getSideInputMapping());

      return DoFnInfo.forFn(
          processFn,
          doFnInfo.getWindowingStrategy(),
          doFnInfo.getSideInputViews(),
          KeyedWorkItemCoder.of(
              ByteArrayCoder.of(),
              KvCoder.of(doFnInfo.getInputCoder(), restrictionCoder),
              doFnInfo.getWindowingStrategy().getWindowFn().windowCoder()),
          doFnInfo.getOutputCoders(),
          doFnInfo.getMainOutput(),
          doFnInfo.getDoFnSchemaInformation(),
          doFnInfo.getSideInputMapping());
    }
  }

  private static class SplittableDoFnRunnerFactory<
          InputT, OutputT, RestrictionT, PositionT, WatermarkEstimatorStateT>
      implements DoFnRunnerFactory<KeyedWorkItem<byte[], KV<InputT, RestrictionT>>, OutputT> {
    private final AtomicReference<ScheduledExecutorService> ses = new AtomicReference<>();

    @Override
    @SuppressWarnings("nullness") // nullable atomic reference guaranteed nonnull when get
    public DoFnRunner<KeyedWorkItem<byte[], KV<InputT, RestrictionT>>, OutputT> createRunner(
        DoFn<KeyedWorkItem<byte[], KV<InputT, RestrictionT>>, OutputT> fn,
        PipelineOptions options,
        TupleTag<OutputT> mainOutputTag,
        List<TupleTag<?>> sideOutputTags,
        Iterable<PCollectionView<?>> sideInputViews,
        SideInputReader sideInputReader,
        Coder<KeyedWorkItem<byte[], KV<InputT, RestrictionT>>> inputCoder,
        Map<TupleTag<?>, Coder<?>> outputCoders,
        WindowingStrategy<?, ?> windowingStrategy,
        DataflowExecutionContext.DataflowStepContext stepContext,
        DataflowExecutionContext.DataflowStepContext userStepContext,
        OutputManager outputManager,
        DoFnSchemaInformation doFnSchemaInformation,
        Map<String, PCollectionView<?>> sideInputMapping) {
      if (this.ses.get() == null) {
        this.ses.compareAndSet(
            null,
            Executors.newScheduledThreadPool(
                Runtime.getRuntime().availableProcessors(),
                new ThreadFactoryBuilder().setNameFormat("df-sdf-executor-%d").build()));
      }
      ProcessFn<InputT, OutputT, RestrictionT, PositionT, WatermarkEstimatorStateT> processFn =
          (ProcessFn<InputT, OutputT, RestrictionT, PositionT, WatermarkEstimatorStateT>) fn;
      processFn.setStateInternalsFactory(key -> (StateInternals) stepContext.stateInternals());
      processFn.setTimerInternalsFactory(key -> stepContext.timerInternals());
      processFn.setSideInputReader(sideInputReader);
      processFn.setProcessElementInvoker(
          new OutputAndTimeBoundedSplittableProcessElementInvoker<>(
              processFn.getFn(),
              options,
              new OutputWindowedValue<OutputT>() {
                @Override
                public void outputWindowedValue(
                    OutputT output,
                    Instant timestamp,
                    Collection<? extends BoundedWindow> windows,
                    PaneInfo pane) {
                  outputManager.output(
                      mainOutputTag, WindowedValue.of(output, timestamp, windows, pane));
                }

                @Override
                public <T> void outputWindowedValue(
                    TupleTag<T> tag,
                    T output,
                    Instant timestamp,
                    Collection<? extends BoundedWindow> windows,
                    PaneInfo pane) {
                  outputManager.output(tag, WindowedValue.of(output, timestamp, windows, pane));
                }
              },
              sideInputReader,
              ses.get(),
              // Commit at least once every 10 seconds or 10k records.  This keeps the watermark
              // advancing smoothly, and ensures that not too much work will have to be reprocessed
              // in the event of a crash.
              10000,
              Duration.standardSeconds(10),
              () -> {
                throw new UnsupportedOperationException(
                    "BundleFinalizer unsupported by non-portable Dataflow.");
              }));
      DoFnRunner<KeyedWorkItem<byte[], KV<InputT, RestrictionT>>, OutputT> simpleRunner =
          new SimpleDoFnRunner<>(
              options,
              processFn,
              sideInputReader,
              outputManager,
              mainOutputTag,
              sideOutputTags,
              userStepContext,
              inputCoder,
              outputCoders,
              processFn.getInputWindowingStrategy(),
              doFnSchemaInformation,
              sideInputMapping);
      DoFnRunner<KeyedWorkItem<byte[], KV<InputT, RestrictionT>>, OutputT> fnRunner =
          new DataflowProcessFnRunner<>(simpleRunner);
      boolean hasStreamingSideInput =
          options.as(StreamingOptions.class).isStreaming() && !sideInputReader.isEmpty();
      KeyedWorkItemCoder<byte[], KV<InputT, RestrictionT>> kwiCoder =
          (KeyedWorkItemCoder<byte[], KV<InputT, RestrictionT>>) inputCoder;
      if (hasStreamingSideInput) {
        fnRunner =
            new StreamingKeyedWorkItemSideInputDoFnRunner<>(
                fnRunner,
                ByteArrayCoder.of(),
                new StreamingSideInputFetcher<>(
                    sideInputViews,
                    kwiCoder.getElementCoder(),
                    processFn.getInputWindowingStrategy(),
                    (StreamingModeExecutionContext.StreamingModeStepContext) userStepContext),
                userStepContext);
      }
      return fnRunner;
    }
  }
}
