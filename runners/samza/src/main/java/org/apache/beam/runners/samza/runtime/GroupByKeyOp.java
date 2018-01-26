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

import java.util.Collections;
import org.apache.beam.runners.core.DoFnRunner;
import org.apache.beam.runners.core.DoFnRunners;
import org.apache.beam.runners.core.GroupAlsoByWindowViaWindowSetNewDoFn;
import org.apache.beam.runners.core.KeyedWorkItem;
import org.apache.beam.runners.core.KeyedWorkItemCoder;
import org.apache.beam.runners.core.KeyedWorkItems;
import org.apache.beam.runners.core.NullSideInputReader;
import org.apache.beam.runners.core.StateInternalsFactory;
import org.apache.beam.runners.core.SystemReduceFn;
import org.apache.beam.runners.core.TimerInternals;
import org.apache.beam.runners.samza.SamzaExecutionContext;
import org.apache.beam.runners.samza.metrics.DoFnRunnerWithMetrics;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.VoidCoder;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.apache.samza.config.Config;
import org.apache.samza.storage.kv.KeyValueStore;
import org.apache.samza.task.TaskContext;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Samza operator for {@link org.apache.beam.sdk.transforms.GroupByKey}.
 */
public class GroupByKeyOp<K, V> implements Op<KeyedWorkItem<K, V>, KV<K, Iterable<V>>> {
  private static final Logger LOG = LoggerFactory.getLogger(GroupByKeyOp.class);

  private final TupleTag<KV<K, Iterable<V>>> mainOutputTag;
  private final KeyedWorkItemCoder<K, V> inputCoder;
  private final WindowingStrategy<?, BoundedWindow> windowingStrategy;
  private final OutputManagerFactory<KV<K, Iterable<V>>> outputManagerFactory;
  private final Coder<K> keyCoder;
  private final String stepName;

  private transient StateInternalsFactory<Void> stateInternalsFactory;
  private transient SamzaTimerInternalsFactory<K> timerInternalsFactory;
  private transient DoFnRunner<KeyedWorkItem<K, V>, KV<K, Iterable<V>>> fnRunner;

  public GroupByKeyOp(TupleTag<KV<K, Iterable<V>>> mainOutputTag,
                      Coder<KeyedWorkItem<K, V>> inputCoder,
                      WindowingStrategy<?, BoundedWindow> windowingStrategy,
                      OutputManagerFactory<KV<K, Iterable<V>>> outputManagerFactory,
                      String stepName) {
    this.mainOutputTag = mainOutputTag;
    this.windowingStrategy = windowingStrategy;
    this.outputManagerFactory = outputManagerFactory;
    this.stepName = stepName;

    if (!(inputCoder instanceof KeyedWorkItemCoder)) {
      throw new IllegalArgumentException(
          String.format(
              "GroupByKeyOp requires input to use KeyedWorkItemCoder. Got: %s",
              inputCoder.getClass()));
    }
    this.inputCoder = (KeyedWorkItemCoder<K, V>) inputCoder;
    this.keyCoder = this.inputCoder.getKeyCoder();
  }

  @Override
  public void open(Config config,
                   TaskContext taskContext,
                   SamzaExecutionContext executionContext,
                   OpEmitter<KV<K, Iterable<V>>> emitter) {
    final DoFnRunners.OutputManager outputManager = outputManagerFactory.create(emitter);

    @SuppressWarnings("unchecked")
    final KeyValueStore<byte[], byte[]> store =
        (KeyValueStore<byte[], byte[]>) taskContext.getStore("beamStore");

    this.stateInternalsFactory =
        new SamzaStoreStateInternals.Factory<>(
            mainOutputTag.getId(),
            store,
            VoidCoder.of());

    this.timerInternalsFactory = new SamzaTimerInternalsFactory<>(inputCoder.getKeyCoder());

    final SystemReduceFn<K, V, Iterable<V>, Iterable<V>, BoundedWindow> reduceFn =
        SystemReduceFn.buffering(inputCoder.getElementCoder());

    final DoFn<KeyedWorkItem<K, V>, KV<K, Iterable<V>>> doFn =
        GroupAlsoByWindowViaWindowSetNewDoFn.create(
            windowingStrategy,
            new SamzaStoreStateInternals.Factory<>(
                mainOutputTag.getId(),
                store,
                keyCoder),
            timerInternalsFactory,
            NullSideInputReader.of(Collections.emptyList()),
            reduceFn,
            outputManager,
            mainOutputTag);

    final DoFnRunner<KeyedWorkItem<K, V>, KV<K, Iterable<V>>> doFnRunner = DoFnRunners.simpleRunner(
        PipelineOptionsFactory.create(),
        doFn,
        NullSideInputReader.of(Collections.emptyList()),
        outputManager,
        mainOutputTag,
        Collections.emptyList(),
        DoFnOp.createStepContext(stateInternalsFactory, timerInternalsFactory),
        windowingStrategy);

    this.fnRunner = DoFnRunnerWithMetrics.wrap(
        doFnRunner, executionContext.getMetricsContainer(), stepName);
  }

  @Override
  public void processElement(WindowedValue<KeyedWorkItem<K, V>> inputElement,
                             OpEmitter<KV<K, Iterable<V>>> emitter) {
    fnRunner.startBundle();
    fnRunner.processElement(inputElement);
    fnRunner.finishBundle();
  }

  @Override
  public void processWatermark(Instant watermark, OpEmitter<KV<K, Iterable<V>>> ctx) {
    timerInternalsFactory.setInputWatermark(watermark);

    fnRunner.startBundle();
    for (KeyedTimerData<K> keyedTimerData : timerInternalsFactory.removeReadyTimers()) {
      fireTimer(keyedTimerData.getKey(), keyedTimerData.getTimerData());
    }
    fnRunner.finishBundle();

    if (timerInternalsFactory.getOutputWatermark() == null
        || timerInternalsFactory.getOutputWatermark().isBefore(watermark)) {
      timerInternalsFactory.setOutputWatermark(watermark);
      ctx.emitWatermark(timerInternalsFactory.getOutputWatermark());
    }
  }

  private void fireTimer(K key, TimerInternals.TimerData timer) {
    LOG.debug("Firing timer {} for key {}", timer, key);
    fnRunner.processElement(
        WindowedValue.valueInGlobalWindow(
            KeyedWorkItems.timersWorkItem(key, Collections.singletonList(timer))));
  }
}
