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
import org.apache.beam.runners.core.DoFnRunner;
import org.apache.beam.runners.core.DoFnRunners;
import org.apache.beam.runners.core.GroupAlsoByWindowViaWindowSetNewDoFn;
import org.apache.beam.runners.core.KeyedWorkItem;
import org.apache.beam.runners.core.KeyedWorkItemCoder;
import org.apache.beam.runners.core.KeyedWorkItems;
import org.apache.beam.runners.core.NullSideInputReader;
import org.apache.beam.runners.core.StateInternals;
import org.apache.beam.runners.core.StateInternalsFactory;
import org.apache.beam.runners.core.StepContext;
import org.apache.beam.runners.core.SystemReduceFn;
import org.apache.beam.runners.core.TimerInternals;
import org.apache.beam.runners.core.TimerInternals.TimerData;
import org.apache.beam.runners.core.construction.SerializablePipelineOptions;
import org.apache.beam.runners.core.serialization.Base64Serializer;
import org.apache.beam.runners.samza.SamzaExecutionContext;
import org.apache.beam.runners.samza.SamzaPipelineOptions;
import org.apache.beam.runners.samza.metrics.DoFnRunnerWithMetrics;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.DoFnSchemaInformation;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection.IsBounded;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.apache.samza.config.Config;
import org.apache.samza.context.Context;
import org.apache.samza.operators.Scheduler;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Samza operator for {@link org.apache.beam.sdk.transforms.GroupByKey}. */
public class GroupByKeyOp<K, InputT, OutputT>
    implements Op<KeyedWorkItem<K, InputT>, KV<K, OutputT>, K> {
  private static final Logger LOG = LoggerFactory.getLogger(GroupByKeyOp.class);
  private static final String TIMER_STATE_ID = "timer";

  private final TupleTag<KV<K, OutputT>> mainOutputTag;
  private final KeyedWorkItemCoder<K, InputT> inputCoder;
  private final WindowingStrategy<?, BoundedWindow> windowingStrategy;
  private final OutputManagerFactory<KV<K, OutputT>> outputManagerFactory;
  private final Coder<K> keyCoder;
  private final SystemReduceFn<K, InputT, ?, OutputT, BoundedWindow> reduceFn;
  private final String transformFullName;
  private final String transformId;
  private final IsBounded isBounded;

  private transient StateInternalsFactory<K> stateInternalsFactory;
  private transient SamzaTimerInternalsFactory<K> timerInternalsFactory;
  private transient DoFnRunner<KeyedWorkItem<K, InputT>, KV<K, OutputT>> fnRunner;
  private transient SamzaPipelineOptions pipelineOptions;

  public GroupByKeyOp(
      TupleTag<KV<K, OutputT>> mainOutputTag,
      Coder<KeyedWorkItem<K, InputT>> inputCoder,
      SystemReduceFn<K, InputT, ?, OutputT, BoundedWindow> reduceFn,
      WindowingStrategy<?, BoundedWindow> windowingStrategy,
      OutputManagerFactory<KV<K, OutputT>> outputManagerFactory,
      String transformFullName,
      String transformId,
      IsBounded isBounded) {
    this.mainOutputTag = mainOutputTag;
    this.windowingStrategy = windowingStrategy;
    this.outputManagerFactory = outputManagerFactory;
    this.transformFullName = transformFullName;
    this.transformId = transformId;
    this.isBounded = isBounded;

    if (!(inputCoder instanceof KeyedWorkItemCoder)) {
      throw new IllegalArgumentException(
          String.format(
              "GroupByKeyOp requires input to use KeyedWorkItemCoder. Got: %s",
              inputCoder.getClass()));
    }
    this.inputCoder = (KeyedWorkItemCoder<K, InputT>) inputCoder;
    this.keyCoder = this.inputCoder.getKeyCoder();
    this.reduceFn = reduceFn;
  }

  @Override
  public void open(
      Config config,
      Context context,
      Scheduler<KeyedTimerData<K>> timerRegistry,
      OpEmitter<KV<K, OutputT>> emitter) {
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
            keyCoder,
            pipelineOptions.getStoreBatchGetSize());

    this.timerInternalsFactory =
        SamzaTimerInternalsFactory.createTimerInternalFactory(
            keyCoder,
            timerRegistry,
            TIMER_STATE_ID,
            nonKeyedStateInternalsFactory,
            windowingStrategy,
            isBounded,
            pipelineOptions);

    final DoFn<KeyedWorkItem<K, InputT>, KV<K, OutputT>> doFn =
        GroupAlsoByWindowViaWindowSetNewDoFn.create(
            windowingStrategy,
            stateInternalsFactory,
            timerInternalsFactory,
            NullSideInputReader.of(Collections.emptyList()),
            reduceFn,
            outputManager,
            mainOutputTag);

    final KeyedInternals<K> keyedInternals =
        new KeyedInternals<>(stateInternalsFactory, timerInternalsFactory);

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

    final DoFnRunner<KeyedWorkItem<K, InputT>, KV<K, OutputT>> doFnRunner =
        DoFnRunners.simpleRunner(
            PipelineOptionsFactory.create(),
            doFn,
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

    final SamzaExecutionContext executionContext =
        (SamzaExecutionContext) context.getApplicationContainerContext();
    this.fnRunner =
        DoFnRunnerWithMetrics.wrap(
            doFnRunner, executionContext.getMetricsContainer(), transformFullName);
  }

  @Override
  public void processElement(
      WindowedValue<KeyedWorkItem<K, InputT>> inputElement, OpEmitter<KV<K, OutputT>> emitter) {
    fnRunner.startBundle();
    fnRunner.processElement(inputElement);
    fnRunner.finishBundle();
  }

  @Override
  public void processWatermark(Instant watermark, OpEmitter<KV<K, OutputT>> emitter) {
    timerInternalsFactory.setInputWatermark(watermark);

    Collection<KeyedTimerData<K>> readyTimers = timerInternalsFactory.removeReadyTimers();
    if (!readyTimers.isEmpty()) {
      fnRunner.startBundle();
      for (KeyedTimerData<K> keyedTimerData : readyTimers) {
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
  public void processTimer(KeyedTimerData<K> keyedTimerData, OpEmitter<KV<K, OutputT>> emitter) {
    fnRunner.startBundle();
    fireTimer(keyedTimerData.getKey(), keyedTimerData.getTimerData());
    fnRunner.finishBundle();

    timerInternalsFactory.removeProcessingTimer(keyedTimerData);
  }

  private void fireTimer(K key, TimerData timer) {
    LOG.debug("Firing timer {} for key {}", timer, key);
    fnRunner.processElement(
        WindowedValue.valueInGlobalWindow(
            KeyedWorkItems.timersWorkItem(key, Collections.singletonList(timer))));
  }
}
