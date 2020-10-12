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
import java.util.concurrent.Executors;
import org.apache.beam.runners.core.DoFnRunner;
import org.apache.beam.runners.core.DoFnRunners;
import org.apache.beam.runners.core.KeyedWorkItem;
import org.apache.beam.runners.core.KeyedWorkItems;
import org.apache.beam.runners.core.NullSideInputReader;
import org.apache.beam.runners.core.OutputAndTimeBoundedSplittableProcessElementInvoker;
import org.apache.beam.runners.core.OutputWindowedValue;
import org.apache.beam.runners.core.SplittableParDoViaKeyedWorkItems;
import org.apache.beam.runners.core.SplittableParDoViaKeyedWorkItems.ProcessElements;
import org.apache.beam.runners.core.StateInternals;
import org.apache.beam.runners.core.StateInternalsFactory;
import org.apache.beam.runners.core.StepContext;
import org.apache.beam.runners.core.TimerInternals;
import org.apache.beam.runners.core.TimerInternals.TimerData;
import org.apache.beam.runners.core.construction.SerializablePipelineOptions;
import org.apache.beam.runners.core.construction.SplittableParDo;
import org.apache.beam.runners.core.serialization.Base64Serializer;
import org.apache.beam.runners.samza.SamzaPipelineOptions;
import org.apache.beam.sdk.coders.ByteArrayCoder;
import org.apache.beam.sdk.transforms.DoFnSchemaInformation;
import org.apache.beam.sdk.transforms.join.RawUnionValue;
import org.apache.beam.sdk.transforms.reflect.DoFnInvokers;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection.IsBounded;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.apache.samza.config.Config;
import org.apache.samza.context.Context;
import org.apache.samza.operators.Scheduler;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Samza operator for {@link org.apache.beam.sdk.transforms.GroupByKey}. */
public class SplittableParDoProcessKeyedElementsOp<
        InputT, OutputT, RestrictionT, PositionT, WatermarkEstimatorStateT>
    implements Op<KeyedWorkItem<byte[], KV<InputT, RestrictionT>>, RawUnionValue, byte[]> {
  private static final Logger LOG =
      LoggerFactory.getLogger(SplittableParDoProcessKeyedElementsOp.class);
  private static final String TIMER_STATE_ID = "timer";

  private final TupleTag<OutputT> mainOutputTag;
  private final WindowingStrategy<?, BoundedWindow> windowingStrategy;
  private final OutputManagerFactory<RawUnionValue> outputManagerFactory;
  private final SplittableParDoViaKeyedWorkItems.ProcessElements<
          InputT, OutputT, RestrictionT, PositionT, WatermarkEstimatorStateT>
      processElements;
  private final String transformFullName;
  private final String transformId;
  private final IsBounded isBounded;

  private transient StateInternalsFactory<byte[]> stateInternalsFactory;
  private transient SamzaTimerInternalsFactory<byte[]> timerInternalsFactory;
  private transient DoFnRunner<KeyedWorkItem<byte[], KV<InputT, RestrictionT>>, OutputT> fnRunner;
  private transient SamzaPipelineOptions pipelineOptions;

  public SplittableParDoProcessKeyedElementsOp(
      TupleTag<OutputT> mainOutputTag,
      SplittableParDo.ProcessKeyedElements<InputT, OutputT, RestrictionT, WatermarkEstimatorStateT>
          processKeyedElements,
      WindowingStrategy<?, BoundedWindow> windowingStrategy,
      OutputManagerFactory<RawUnionValue> outputManagerFactory,
      String transformFullName,
      String transformId,
      IsBounded isBounded) {
    this.mainOutputTag = mainOutputTag;
    this.windowingStrategy = windowingStrategy;
    this.outputManagerFactory = outputManagerFactory;
    this.transformFullName = transformFullName;
    this.transformId = transformId;
    this.isBounded = isBounded;

    this.processElements = new ProcessElements<>(processKeyedElements);
  }

  @Override
  public void open(
      Config config,
      Context context,
      Scheduler<KeyedTimerData<byte[]>> timerRegistry,
      OpEmitter<RawUnionValue> emitter) {
    this.pipelineOptions =
        Base64Serializer.deserializeUnchecked(
                config.get("beamPipelineOptions"), SerializablePipelineOptions.class)
            .get()
            .as(SamzaPipelineOptions.class);

    final SamzaStoreStateInternals.Factory<?> nonKeyedStateInternalsFactory =
        SamzaStoreStateInternals.createStateInternalFactory(
            transformId, null, context.getTaskContext(), pipelineOptions, null);

    final DoFnRunners.OutputManager outputManager = outputManagerFactory.create(emitter);

    this.stateInternalsFactory =
        new SamzaStoreStateInternals.Factory<>(
            transformId,
            Collections.singletonMap(
                SamzaStoreStateInternals.BEAM_STORE,
                SamzaStoreStateInternals.getBeamStore(context.getTaskContext())),
            ByteArrayCoder.of(),
            pipelineOptions.getStoreBatchGetSize());

    this.timerInternalsFactory =
        SamzaTimerInternalsFactory.createTimerInternalFactory(
            ByteArrayCoder.of(),
            timerRegistry,
            TIMER_STATE_ID,
            nonKeyedStateInternalsFactory,
            windowingStrategy,
            isBounded,
            pipelineOptions);

    final KeyedInternals<byte[]> keyedInternals =
        new KeyedInternals<>(stateInternalsFactory, timerInternalsFactory);

    SplittableParDoViaKeyedWorkItems.ProcessFn<
            InputT, OutputT, RestrictionT, PositionT, WatermarkEstimatorStateT>
        processFn = processElements.newProcessFn(processElements.getFn());
    DoFnInvokers.tryInvokeSetupFor(processFn);
    processFn.setStateInternalsFactory(stateInternalsFactory);
    processFn.setTimerInternalsFactory(timerInternalsFactory);
    processFn.setProcessElementInvoker(
        new OutputAndTimeBoundedSplittableProcessElementInvoker<>(
            processElements.getFn(),
            pipelineOptions,
            new OutputWindowedValue<OutputT>() {
              @Override
              public void outputWindowedValue(
                  OutputT output,
                  Instant timestamp,
                  Collection<? extends BoundedWindow> windows,
                  PaneInfo pane) {
                outputWindowedValue(mainOutputTag, output, timestamp, windows, pane);
              }

              @Override
              public <AdditionalOutputT> void outputWindowedValue(
                  TupleTag<AdditionalOutputT> tag,
                  AdditionalOutputT output,
                  Instant timestamp,
                  Collection<? extends BoundedWindow> windows,
                  PaneInfo pane) {
                outputManager.output(tag, WindowedValue.of(output, timestamp, windows, pane));
              }
            },
            NullSideInputReader.empty(),
            Executors.newSingleThreadScheduledExecutor(Executors.defaultThreadFactory()),
            10000,
            Duration.standardSeconds(10),
            () -> {
              throw new UnsupportedOperationException("BundleFinalizer unsupported in Samza");
            }));

    final StepContext stepContext =
        new StepContext() {
          @Override
          public StateInternals stateInternals() {
            return keyedInternals.stateInternals();
          }

          @Override
          public TimerInternals timerInternals() {
            return keyedInternals.timerInternals();
          }
        };

    this.fnRunner =
        DoFnRunners.simpleRunner(
            pipelineOptions,
            processFn,
            NullSideInputReader.of(Collections.emptyList()),
            outputManager,
            mainOutputTag,
            Collections.emptyList(),
            stepContext,
            null,
            Collections.emptyMap(),
            windowingStrategy,
            DoFnSchemaInformation.create(),
            Collections.emptyMap());
  }

  @Override
  public void processElement(
      WindowedValue<KeyedWorkItem<byte[], KV<InputT, RestrictionT>>> inputElement,
      OpEmitter<RawUnionValue> emitter) {
    fnRunner.startBundle();
    fnRunner.processElement(inputElement);
    fnRunner.finishBundle();
  }

  @Override
  public void processWatermark(Instant watermark, OpEmitter<RawUnionValue> emitter) {
    timerInternalsFactory.setInputWatermark(watermark);

    Collection<KeyedTimerData<byte[]>> readyTimers = timerInternalsFactory.removeReadyTimers();
    if (!readyTimers.isEmpty()) {
      fnRunner.startBundle();
      for (KeyedTimerData<byte[]> keyedTimerData : readyTimers) {
        fireTimer(keyedTimerData.getKey(), keyedTimerData.getTimerData());
      }
      fnRunner.finishBundle();
    }

    if (timerInternalsFactory.getOutputWatermark() == null
        || timerInternalsFactory.getOutputWatermark().isBefore(watermark)) {
      timerInternalsFactory.setOutputWatermark(watermark);
      emitter.emitWatermark(timerInternalsFactory.getOutputWatermark());
    }
  }

  @Override
  public void processTimer(
      KeyedTimerData<byte[]> keyedTimerData, OpEmitter<RawUnionValue> emitter) {
    fnRunner.startBundle();
    fireTimer(keyedTimerData.getKey(), keyedTimerData.getTimerData());
    fnRunner.finishBundle();

    timerInternalsFactory.removeProcessingTimer(keyedTimerData);
  }

  private void fireTimer(byte[] key, TimerData timer) {
    LOG.debug("Firing timer {} for key {}", timer, key);
    fnRunner.processElement(
        WindowedValue.valueInGlobalWindow(
            KeyedWorkItems.timersWorkItem(key, Collections.singletonList(timer))));
  }
}
