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

import static com.google.common.base.Preconditions.checkNotNull;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.Operator;
import com.datatorrent.api.StreamCodec;
import com.datatorrent.api.annotation.OutputPortFieldAnnotation;
import com.esotericsoftware.kryo.serializers.FieldSerializer.Bind;
import com.esotericsoftware.kryo.serializers.JavaSerializer;
import com.google.common.base.Throwables;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.apache.beam.runners.apex.ApexPipelineOptions;
import org.apache.beam.runners.apex.translation.utils.ApexStreamTuple;
import org.apache.beam.runners.apex.translation.utils.SerializablePipelineOptions;
import org.apache.beam.runners.core.GroupAlsoByWindowViaWindowSetDoFn;
import org.apache.beam.runners.core.SystemReduceFn;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.Aggregator;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.OldDoFn;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.util.CoderUtils;
import org.apache.beam.sdk.util.KeyedWorkItem;
import org.apache.beam.sdk.util.KeyedWorkItems;
import org.apache.beam.sdk.util.TimeDomain;
import org.apache.beam.sdk.util.TimerInternals;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.util.WindowingInternals;
import org.apache.beam.sdk.util.WindowingStrategy;
import org.apache.beam.sdk.util.state.StateInternals;
import org.apache.beam.sdk.util.state.StateInternalsFactory;
import org.apache.beam.sdk.util.state.StateNamespace;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TupleTag;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Apex operator for Beam {@link GroupByKey}.
 * This operator expects the input stream already partitioned by K,
 * which is determined by the {@link StreamCodec} on the input port.
 *
 * @param <K> key type
 * @param <V> value type
 */
public class ApexGroupByKeyOperator<K, V> implements Operator {
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
  private Map<ByteBuffer, StateInternals<K>> perKeyStateInternals = new HashMap<>();
  private Map<ByteBuffer, Set<TimerInternals.TimerData>> activeTimers = new HashMap<>();

  private transient ProcessContext context;
  private transient OldDoFn<KeyedWorkItem<K, V>, KV<K, Iterable<V>>> fn;
  private transient ApexTimerInternals timerInternals = new ApexTimerInternals();
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
          output.emit(ApexStreamTuple.WatermarkTuple.<WindowedValue<KV<K, Iterable<V>>>>of(
              mark.getTimestamp()));
          return;
        }
        if (traceTuples) {
          LOG.debug("\ninput {}\n", t.getValue());
        }
        processElement(t.getValue());
      } catch (Exception e) {
        Throwables.propagateIfPossible(e);
        throw new RuntimeException(e);
      }
    }
  };

  @OutputPortFieldAnnotation(optional = true)
  public final transient DefaultOutputPort<ApexStreamTuple<WindowedValue<KV<K, Iterable<V>>>>>
      output = new DefaultOutputPort<>();

  @SuppressWarnings("unchecked")
  public ApexGroupByKeyOperator(ApexPipelineOptions pipelineOptions, PCollection<KV<K, V>> input,
      StateInternalsFactory<K> stateInternalsFactory) {
    checkNotNull(pipelineOptions);
    this.serializedOptions = new SerializablePipelineOptions(pipelineOptions);
    this.windowingStrategy = (WindowingStrategy<V, BoundedWindow>) input.getWindowingStrategy();
    this.keyCoder = ((KvCoder<K, V>) input.getCoder()).getKeyCoder();
    this.valueCoder = ((KvCoder<K, V>) input.getCoder()).getValueCoder();
    this.stateInternalsFactory = stateInternalsFactory;
  }

  @SuppressWarnings("unused") // for Kryo
  private ApexGroupByKeyOperator() {
    this.serializedOptions = null;
    this.stateInternalsFactory = null;
  }

  @Override
  public void beginWindow(long l) {
  }

  @Override
  public void endWindow() {
  }

  @Override
  public void setup(OperatorContext context) {
    this.traceTuples = ApexStreamTuple.Logging.isDebugEnabled(serializedOptions.get(), this);
    StateInternalsFactory<K> stateInternalsFactory = new GroupByKeyStateInternalsFactory();
    this.fn = GroupAlsoByWindowViaWindowSetDoFn.create(this.windowingStrategy,
        stateInternalsFactory, SystemReduceFn.<K, V, BoundedWindow>buffering(this.valueCoder));
    this.context = new ProcessContext(fn, this.timerInternals);
  }

  @Override
  public void teardown() {
  }

  /**
   * Returns the list of timers that are ready to fire. These are the timers
   * that are registered to be triggered at a time before the current watermark.
   * We keep these timers in a Set, so that they are deduplicated, as the same
   * timer can be registered multiple times.
   */
  private Multimap<ByteBuffer, TimerInternals.TimerData> getTimersReadyToProcess(
      long currentWatermark) {

    // we keep the timers to return in a different list and launch them later
    // because we cannot prevent a trigger from registering another trigger,
    // which would lead to concurrent modification exception.
    Multimap<ByteBuffer, TimerInternals.TimerData> toFire = HashMultimap.create();

    Iterator<Map.Entry<ByteBuffer, Set<TimerInternals.TimerData>>> it =
        activeTimers.entrySet().iterator();
    while (it.hasNext()) {
      Map.Entry<ByteBuffer, Set<TimerInternals.TimerData>> keyWithTimers = it.next();

      Iterator<TimerInternals.TimerData> timerIt = keyWithTimers.getValue().iterator();
      while (timerIt.hasNext()) {
        TimerInternals.TimerData timerData = timerIt.next();
        if (timerData.getTimestamp().isBefore(currentWatermark)) {
          toFire.put(keyWithTimers.getKey(), timerData);
          timerIt.remove();
        }
      }

      if (keyWithTimers.getValue().isEmpty()) {
        it.remove();
      }
    }
    return toFire;
  }

  private void processElement(WindowedValue<KV<K, V>> windowedValue) throws Exception {
    final KV<K, V> kv = windowedValue.getValue();
    final WindowedValue<V> updatedWindowedValue = WindowedValue.of(kv.getValue(),
        windowedValue.getTimestamp(),
        windowedValue.getWindows(),
        windowedValue.getPane());

    KeyedWorkItem<K, V> kwi = KeyedWorkItems.elementsWorkItem(
            kv.getKey(),
            Collections.singletonList(updatedWindowedValue));

    context.setElement(kwi, getStateInternalsForKey(kwi.key()));
    fn.processElement(context);
  }

  private StateInternals<K> getStateInternalsForKey(K key) {
    final ByteBuffer keyBytes;
    try {
      keyBytes = ByteBuffer.wrap(CoderUtils.encodeToByteArray(keyCoder, key));
    } catch (CoderException e) {
      throw new RuntimeException(e);
    }
    StateInternals<K> stateInternals = perKeyStateInternals.get(keyBytes);
    if (stateInternals == null) {
      stateInternals = stateInternalsFactory.stateInternalsForKey(key);
      perKeyStateInternals.put(keyBytes, stateInternals);
    }
    return stateInternals;
  }

  private void registerActiveTimer(K key, TimerInternals.TimerData timer) {
    final ByteBuffer keyBytes;
    try {
      keyBytes = ByteBuffer.wrap(CoderUtils.encodeToByteArray(keyCoder, key));
    } catch (CoderException e) {
      throw new RuntimeException(e);
    }
    Set<TimerInternals.TimerData> timersForKey = activeTimers.get(keyBytes);
    if (timersForKey == null) {
      timersForKey = new HashSet<>();
    }
    timersForKey.add(timer);
    activeTimers.put(keyBytes, timersForKey);
  }

  private void unregisterActiveTimer(K key, TimerInternals.TimerData timer) {
    final ByteBuffer keyBytes;
    try {
      keyBytes = ByteBuffer.wrap(CoderUtils.encodeToByteArray(keyCoder, key));
    } catch (CoderException e) {
      throw new RuntimeException(e);
    }
    Set<TimerInternals.TimerData> timersForKey = activeTimers.get(keyBytes);
    if (timersForKey != null) {
      timersForKey.remove(timer);
      if (timersForKey.isEmpty()) {
        activeTimers.remove(keyBytes);
      } else {
        activeTimers.put(keyBytes, timersForKey);
      }
    }
  }

  private void processWatermark(ApexStreamTuple.WatermarkTuple<?> mark) throws Exception {
    this.inputWatermark = new Instant(mark.getTimestamp());
    Multimap<ByteBuffer, TimerInternals.TimerData> timers = getTimersReadyToProcess(
        mark.getTimestamp());
    if (!timers.isEmpty()) {
      for (ByteBuffer keyBytes : timers.keySet()) {
        K key = CoderUtils.decodeFromByteArray(keyCoder, keyBytes.array());
        KeyedWorkItem<K, V> kwi = KeyedWorkItems.<K, V>timersWorkItem(key, timers.get(keyBytes));
        context.setElement(kwi, getStateInternalsForKey(kwi.key()));
        fn.processElement(context);
      }
    }
  }

  private class ProcessContext extends GroupAlsoByWindowViaWindowSetDoFn<K, V, Iterable<V>, ?,
      KeyedWorkItem<K, V>>.ProcessContext {

    private final ApexTimerInternals timerInternals;
    private StateInternals<K> stateInternals;
    private KeyedWorkItem<K, V> element;

    public ProcessContext(OldDoFn<KeyedWorkItem<K, V>, KV<K, Iterable<V>>> function,
                          ApexTimerInternals timerInternals) {
      function.super();
      this.timerInternals = checkNotNull(timerInternals);
    }

    public void setElement(KeyedWorkItem<K, V> element, StateInternals<K> stateForKey) {
      this.element = element;
      this.stateInternals = stateForKey;
    }

    @Override
    public KeyedWorkItem<K, V> element() {
      return this.element;
    }

    @Override
    public Instant timestamp() {
      throw new UnsupportedOperationException(
          "timestamp() is not available when processing KeyedWorkItems.");
    }

    @Override
    public PipelineOptions getPipelineOptions() {
      return serializedOptions.get();
    }

    @Override
    public void output(KV<K, Iterable<V>> output) {
      throw new UnsupportedOperationException(
          "output() is not available when processing KeyedWorkItems.");
    }

    @Override
    public void outputWithTimestamp(KV<K, Iterable<V>> output, Instant timestamp) {
      throw new UnsupportedOperationException(
          "outputWithTimestamp() is not available when processing KeyedWorkItems.");
    }

    @Override
    public PaneInfo pane() {
      throw new UnsupportedOperationException(
          "pane() is not available when processing KeyedWorkItems.");
    }

    @Override
    public BoundedWindow window() {
      throw new UnsupportedOperationException(
          "window() is not available when processing KeyedWorkItems.");
    }

    @Override
    public WindowingInternals<KeyedWorkItem<K, V>, KV<K, Iterable<V>>> windowingInternals() {
      return new WindowingInternals<KeyedWorkItem<K, V>, KV<K, Iterable<V>>>() {

        @Override
        public StateInternals<K> stateInternals() {
          return stateInternals;
        }

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
        public <SideOutputT> void sideOutputWindowedValue(
            TupleTag<SideOutputT> tag,
            SideOutputT output,
            Instant timestamp,
            Collection<? extends BoundedWindow> windows,
            PaneInfo pane) {
          throw new UnsupportedOperationException("GroupAlsoByWindow should not use side outputs");
        }

        @Override
        public TimerInternals timerInternals() {
          return timerInternals;
        }

        @Override
        public Collection<? extends BoundedWindow> windows() {
          throw new UnsupportedOperationException("windows() is not available in Streaming mode.");
        }

        @Override
        public PaneInfo pane() {
          throw new UnsupportedOperationException("pane() is not available in Streaming mode.");
        }

        @Override
        public <T> void writePCollectionViewData(
            TupleTag<?> tag, Iterable<WindowedValue<T>> data, Coder<T> elemCoder)
            throws IOException {
          throw new RuntimeException("writePCollectionViewData() not available in Streaming mode.");
        }

        @Override
        public <T> T sideInput(PCollectionView<T> view, BoundedWindow mainInputWindow) {
          throw new RuntimeException("sideInput() is not available in Streaming mode.");
        }
      };
    }

    @Override
    public <T> T sideInput(PCollectionView<T> view) {
      throw new RuntimeException("sideInput() is not supported in Streaming mode.");
    }

    @Override
    public <T> void sideOutput(TupleTag<T> tag, T output) {
      // ignore the side output, this can happen when a user does not register
      // side outputs but then outputs using a freshly created TupleTag.
      throw new RuntimeException("sideOutput() is not available when grouping by window.");
    }

    @Override
    public <T> void sideOutputWithTimestamp(TupleTag<T> tag, T output, Instant timestamp) {
      sideOutput(tag, output);
    }

    @Override
    protected <AggInputT, AggOutputT> Aggregator<AggInputT, AggOutputT> createAggregatorInternal(
        String name, Combine.CombineFn<AggInputT, ?, AggOutputT> combiner) {
      throw new UnsupportedOperationException();
    }
  }

  /**
   * An implementation of Beam's {@link TimerInternals}.
   *
   */
  public class ApexTimerInternals implements TimerInternals {

    @Override
    public void setTimer(TimerData timerKey) {
      registerActiveTimer(context.element().key(), timerKey);
    }

    @Override
    public void deleteTimer(TimerData timerKey) {
      unregisterActiveTimer(context.element().key(), timerKey);
    }

    @Override
    public Instant currentProcessingTime() {
      return Instant.now();
    }

    @Override
    public Instant currentSynchronizedProcessingTime() {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public Instant currentInputWatermarkTime() {
      return inputWatermark;
    }

    @Override
    public Instant currentOutputWatermarkTime() {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public void setTimer(StateNamespace namespace, String timerId, Instant target,
        TimeDomain timeDomain) {
      throw new UnsupportedOperationException("Setting timer by ID not yet supported.");
    }

    @Override
    public void deleteTimer(StateNamespace namespace, String timerId) {
      throw new UnsupportedOperationException("Canceling of timer by ID is not yet supported.");
    }

  }

  private class GroupByKeyStateInternalsFactory implements StateInternalsFactory<K> {
    @Override
    public StateInternals<K> stateInternalsForKey(K key) {
      return getStateInternalsForKey(key);
    }
  }
}
