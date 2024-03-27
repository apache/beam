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
package org.apache.beam.runners.twister2.translators.functions;

import edu.iu.dsc.tws.api.tset.TSetContext;
import edu.iu.dsc.tws.api.tset.fn.FlatMapFunc;
import edu.iu.dsc.tws.api.tset.fn.RecordCollector;
import java.io.IOException;
import java.io.ObjectStreamException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.logging.Logger;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.runners.core.InMemoryStateInternals;
import org.apache.beam.runners.core.InMemoryTimerInternals;
import org.apache.beam.runners.core.OutputWindowedValue;
import org.apache.beam.runners.core.ReduceFnRunner;
import org.apache.beam.runners.core.StateInternals;
import org.apache.beam.runners.core.SystemReduceFn;
import org.apache.beam.runners.core.TimerInternals;
import org.apache.beam.runners.core.UnsupportedSideInputReader;
import org.apache.beam.runners.core.triggers.ExecutableTriggerStateMachine;
import org.apache.beam.runners.core.triggers.TriggerStateMachines;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PortablePipelineOptions;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.util.construction.Environments;
import org.apache.beam.sdk.util.construction.RehydratedComponents;
import org.apache.beam.sdk.util.construction.SdkComponents;
import org.apache.beam.sdk.util.construction.TriggerTranslation;
import org.apache.beam.sdk.util.construction.WindowingStrategyTranslation;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.apache.beam.vendor.grpc.v1p60p1.com.google.protobuf.InvalidProtocolBufferException;
import org.joda.time.Instant;

/** GroupBy window function. */
@SuppressWarnings({
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
public class GroupByWindowFunction<K, V, W extends BoundedWindow>
    implements FlatMapFunc<WindowedValue<KV<K, Iterable<V>>>, KV<K, Iterable<WindowedValue<V>>>> {
  private static final Logger LOG = Logger.getLogger(GroupByWindowFunction.class.getName());
  private transient WindowingStrategy<?, W> windowingStrategy;
  private SystemReduceFn<K, V, Iterable<V>, Iterable<V>, W> reduceFn;
  // Options are kept as null this should be fixed when possible

  private transient boolean isInitialized = false;

  private transient RunnerApi.MessageWithComponents windowStrategyProto;
  private byte[] windowBytes;

  public GroupByWindowFunction() {
    // non arg constructor needed for kryo
    this.isInitialized = false;
  }

  public GroupByWindowFunction(
      WindowingStrategy<?, W> windowingStrategy,
      SystemReduceFn<K, V, Iterable<V>, Iterable<V>, W> reduceFn,
      PipelineOptions options) {
    this.windowingStrategy = windowingStrategy;
    SdkComponents components = SdkComponents.create();
    components.registerEnvironment(
        Environments.createOrGetDefaultEnvironment(options.as(PortablePipelineOptions.class)));

    try {
      windowStrategyProto =
          WindowingStrategyTranslation.toMessageProto(windowingStrategy, components);
      windowBytes = windowStrategyProto.toByteArray();
    } catch (IOException e) {
      LOG.info(e.getMessage());
    }
    this.reduceFn = reduceFn;
  }

  @Override
  public void flatMap(
      KV<K, Iterable<WindowedValue<V>>> kIteratorKV,
      RecordCollector<WindowedValue<KV<K, Iterable<V>>>> collector) {
    try {
      K key = kIteratorKV.getKey();
      Iterable<WindowedValue<V>> values = kIteratorKV.getValue();

      InMemoryTimerInternals timerInternals = new InMemoryTimerInternals();
      timerInternals.advanceProcessingTime(Instant.now());
      timerInternals.advanceSynchronizedProcessingTime(Instant.now());
      StateInternals stateInternals = InMemoryStateInternals.forKey(key);
      GABWOutputWindowedValue<K, V> outputter = new GABWOutputWindowedValue<>();

      ReduceFnRunner<K, V, Iterable<V>, W> reduceFnRunner =
          new ReduceFnRunner<>(
              key,
              windowingStrategy,
              ExecutableTriggerStateMachine.create(
                  TriggerStateMachines.stateMachineForTrigger(
                      TriggerTranslation.toProto(windowingStrategy.getTrigger()))),
              stateInternals,
              timerInternals,
              outputter,
              new UnsupportedSideInputReader("GroupAlsoByWindow"),
              reduceFn,
              null);

      // Process the grouped values.
      reduceFnRunner.processElements(values);

      // Finish any pending windows by advancing the input watermark to infinity.
      timerInternals.advanceInputWatermark(BoundedWindow.TIMESTAMP_MAX_VALUE);

      // Finally, advance the processing time to infinity to fire any timers.
      timerInternals.advanceProcessingTime(BoundedWindow.TIMESTAMP_MAX_VALUE);
      timerInternals.advanceSynchronizedProcessingTime(BoundedWindow.TIMESTAMP_MAX_VALUE);

      fireEligibleTimers(timerInternals, reduceFnRunner);

      reduceFnRunner.persist();
      Iterator<WindowedValue<KV<K, Iterable<V>>>> outputs = outputter.getOutputs().iterator();
      while (outputs.hasNext()) {
        collector.collect(outputs.next());
      }
    } catch (Exception e) {
      LOG.info(e.getMessage());
    }
  }

  private void fireEligibleTimers(
      InMemoryTimerInternals timerInternals, ReduceFnRunner<K, V, Iterable<V>, W> reduceFnRunner)
      throws Exception {
    List<TimerInternals.TimerData> timers = new ArrayList<>();
    while (true) {
      TimerInternals.TimerData timer;
      while ((timer = timerInternals.removeNextEventTimer()) != null) {
        timers.add(timer);
      }
      while ((timer = timerInternals.removeNextProcessingTimer()) != null) {
        timers.add(timer);
      }
      while ((timer = timerInternals.removeNextSynchronizedProcessingTimer()) != null) {
        timers.add(timer);
      }
      if (timers.isEmpty()) {
        break;
      }
      reduceFnRunner.onTimers(timers);
      timers.clear();
    }
  }

  @Override
  public void prepare(TSetContext context) {
    initTransient();
  }

  private static class GABWOutputWindowedValue<K, V>
      implements OutputWindowedValue<KV<K, Iterable<V>>> {
    private final List<WindowedValue<KV<K, Iterable<V>>>> outputs = new ArrayList<>();

    @Override
    public void outputWindowedValue(
        KV<K, Iterable<V>> output,
        Instant timestamp,
        Collection<? extends BoundedWindow> windows,
        PaneInfo pane) {
      outputs.add(WindowedValue.of(output, timestamp, windows, pane));
    }

    @Override
    public <AdditionalOutputT> void outputWindowedValue(
        TupleTag<AdditionalOutputT> tag,
        AdditionalOutputT output,
        Instant timestamp,
        Collection<? extends BoundedWindow> windows,
        PaneInfo pane) {
      throw new UnsupportedOperationException("GroupAlsoByWindow should not use tagged outputs.");
    }

    Iterable<WindowedValue<KV<K, Iterable<V>>>> getOutputs() {
      return outputs;
    }
  }

  /**
   * Method used to initialize the transient variables that were sent over as byte arrays or proto
   * buffers.
   */
  private void initTransient() {
    if (isInitialized) {
      return;
    }

    SdkComponents components = SdkComponents.create();

    try {
      windowStrategyProto = RunnerApi.MessageWithComponents.parseFrom(windowBytes);
      windowingStrategy =
          (WindowingStrategy<?, W>)
              WindowingStrategyTranslation.fromProto(
                  windowStrategyProto.getWindowingStrategy(),
                  RehydratedComponents.forComponents(components.toComponents()));
    } catch (InvalidProtocolBufferException e) {
      LOG.info(e.getMessage());
    }
    this.isInitialized = true;
  }

  protected Object readResolve() throws ObjectStreamException {
    return this;
  }
}
