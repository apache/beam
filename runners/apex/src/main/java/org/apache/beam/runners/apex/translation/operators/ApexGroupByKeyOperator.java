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
package org.apache.beam.runners.apex.translation.operators;

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkNotNull;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.Operator;
import com.datatorrent.api.StreamCodec;
import com.datatorrent.api.annotation.OutputPortFieldAnnotation;
import com.esotericsoftware.kryo.serializers.FieldSerializer.Bind;
import com.esotericsoftware.kryo.serializers.JavaSerializer;
import java.util.Collection;
import java.util.Collections;
import org.apache.beam.runners.apex.ApexPipelineOptions;
import org.apache.beam.runners.apex.translation.utils.ApexStateInternals.ApexStateBackend;
import org.apache.beam.runners.apex.translation.utils.ApexStreamTuple;
import org.apache.beam.runners.core.NullSideInputReader;
import org.apache.beam.runners.core.OutputWindowedValue;
import org.apache.beam.runners.core.ReduceFnRunner;
import org.apache.beam.runners.core.StateInternalsFactory;
import org.apache.beam.runners.core.SystemReduceFn;
import org.apache.beam.runners.core.TimerInternals;
import org.apache.beam.runners.core.TimerInternals.TimerData;
import org.apache.beam.runners.core.construction.SerializablePipelineOptions;
import org.apache.beam.runners.core.construction.TriggerTranslation;
import org.apache.beam.runners.core.triggers.ExecutableTriggerStateMachine;
import org.apache.beam.runners.core.triggers.TriggerStateMachines;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.state.TimeDomain;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Throwables;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Apex operator for Beam {@link GroupByKey}. This operator expects the input stream already
 * partitioned by K, which is determined by the {@link StreamCodec} on the input port.
 *
 * @param <K> key type
 * @param <V> value type
 */
public class ApexGroupByKeyOperator<K, V>
    implements Operator, ApexTimerInternals.TimerProcessor<K> {
  private static final Logger LOG = LoggerFactory.getLogger(ApexGroupByKeyOperator.class);
  private boolean traceTuples = true;

  @Bind(JavaSerializer.class)
  private WindowingStrategy<V, BoundedWindow> windowingStrategy;

  @Bind(JavaSerializer.class)
  private Coder<K> keyCoder;

  @Bind(JavaSerializer.class)
  private Coder<V> valueCoder;

  @Bind(JavaSerializer.class)
  private final SerializablePipelineOptions serializedOptions;

  @Bind(JavaSerializer.class)
  private final StateInternalsFactory<K> stateInternalsFactory;

  private final ApexTimerInternals<K> timerInternals;
  private Instant inputWatermark = BoundedWindow.TIMESTAMP_MIN_VALUE;

  public final transient DefaultInputPort<ApexStreamTuple<WindowedValue<KV<K, V>>>> input =
      new DefaultInputPort<ApexStreamTuple<WindowedValue<KV<K, V>>>>() {
        @Override
        public void process(ApexStreamTuple<WindowedValue<KV<K, V>>> t) {
          try {
            if (t instanceof ApexStreamTuple.WatermarkTuple) {
              ApexStreamTuple.WatermarkTuple<?> mark = (ApexStreamTuple.WatermarkTuple<?>) t;
              processWatermark(mark);
              if (traceTuples) {
                LOG.debug("\nemitting watermark {}\n", mark.getTimestamp());
              }
              output.emit(ApexStreamTuple.WatermarkTuple.of(mark.getTimestamp()));
              return;
            }
            if (traceTuples) {
              LOG.debug("\ninput {}\n", t.getValue());
            }
            processElement(t.getValue());
          } catch (Exception e) {
            Throwables.throwIfUnchecked(e);
            throw new RuntimeException(e);
          }
        }
      };

  @OutputPortFieldAnnotation(optional = true)
  public final transient DefaultOutputPort<ApexStreamTuple<WindowedValue<KV<K, Iterable<V>>>>>
      output = new DefaultOutputPort<>();

  @SuppressWarnings("unchecked")
  public ApexGroupByKeyOperator(
      ApexPipelineOptions pipelineOptions,
      PCollection<KV<K, V>> input,
      ApexStateBackend stateBackend) {
    checkNotNull(pipelineOptions);
    this.serializedOptions = new SerializablePipelineOptions(pipelineOptions);
    this.windowingStrategy = (WindowingStrategy<V, BoundedWindow>) input.getWindowingStrategy();
    this.keyCoder = ((KvCoder<K, V>) input.getCoder()).getKeyCoder();
    this.valueCoder = ((KvCoder<K, V>) input.getCoder()).getValueCoder();
    this.stateInternalsFactory = stateBackend.newStateInternalsFactory(keyCoder);
    TimerInternals.TimerDataCoderV2 timerCoder =
        TimerInternals.TimerDataCoderV2.of(windowingStrategy.getWindowFn().windowCoder());
    this.timerInternals = new ApexTimerInternals<>(timerCoder);
  }

  @SuppressWarnings("unused") // for Kryo
  private ApexGroupByKeyOperator() {
    this.serializedOptions = null;
    this.stateInternalsFactory = null;
    this.timerInternals = null;
  }

  @Override
  public void beginWindow(long l) {}

  @Override
  public void endWindow() {
    timerInternals.fireReadyTimers(
        timerInternals.currentProcessingTime().getMillis(), this, TimeDomain.PROCESSING_TIME);
  }

  @Override
  public void setup(OperatorContext context) {
    this.traceTuples =
        ApexStreamTuple.Logging.isDebugEnabled(
            serializedOptions.get().as(ApexPipelineOptions.class), this);
  }

  @Override
  public void teardown() {}

  private ReduceFnRunner<K, V, Iterable<V>, BoundedWindow> newReduceFnRunner(K key) {
    return new ReduceFnRunner<>(
        key,
        windowingStrategy,
        ExecutableTriggerStateMachine.create(
            TriggerStateMachines.stateMachineForTrigger(
                TriggerTranslation.toProto(windowingStrategy.getTrigger()))),
        stateInternalsFactory.stateInternalsForKey(key),
        timerInternals,
        new OutputWindowedValue<KV<K, Iterable<V>>>() {
          @Override
          public void outputWindowedValue(
              KV<K, Iterable<V>> output,
              Instant timestamp,
              Collection<? extends BoundedWindow> windows,
              PaneInfo pane) {
            if (traceTuples) {
              LOG.debug("\nemitting {} timestamp {}\n", output, timestamp);
            }
            ApexGroupByKeyOperator.this.output.emit(
                ApexStreamTuple.DataTuple.of(WindowedValue.of(output, timestamp, windows, pane)));
          }

          @Override
          public <AdditionalOutputT> void outputWindowedValue(
              TupleTag<AdditionalOutputT> tag,
              AdditionalOutputT output,
              Instant timestamp,
              Collection<? extends BoundedWindow> windows,
              PaneInfo pane) {
            throw new UnsupportedOperationException(
                "GroupAlsoByWindow should not use side outputs");
          }
        },
        NullSideInputReader.empty(),
        SystemReduceFn.buffering(this.valueCoder),
        serializedOptions.get());
  }

  private void processElement(WindowedValue<KV<K, V>> windowedValue) throws Exception {
    final KV<K, V> kv = windowedValue.getValue();
    final WindowedValue<V> updatedWindowedValue =
        WindowedValue.of(
            kv.getValue(),
            windowedValue.getTimestamp(),
            windowedValue.getWindows(),
            windowedValue.getPane());
    timerInternals.setContext(kv.getKey(), this.keyCoder, this.inputWatermark, null);
    ReduceFnRunner<K, V, Iterable<V>, BoundedWindow> reduceFnRunner =
        newReduceFnRunner(kv.getKey());
    reduceFnRunner.processElements(Collections.singletonList(updatedWindowedValue));
    reduceFnRunner.persist();
  }

  @Override
  public void fireTimer(K key, Collection<TimerData> timerData) {
    timerInternals.setContext(key, keyCoder, inputWatermark, null);
    ReduceFnRunner<K, V, Iterable<V>, BoundedWindow> reduceFnRunner = newReduceFnRunner(key);
    try {
      reduceFnRunner.onTimers(timerData);
    } catch (Exception e) {
      Throwables.throwIfUnchecked(e);
      throw new RuntimeException(e);
    }
    reduceFnRunner.persist();
  }

  private void processWatermark(ApexStreamTuple.WatermarkTuple<?> mark) {
    this.inputWatermark = new Instant(mark.getTimestamp());
    timerInternals.fireReadyTimers(this.inputWatermark.getMillis(), this, TimeDomain.EVENT_TIME);
  }
}
