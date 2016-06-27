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
package org.apache.beam.runners.flink.translation.wrappers.streaming;

import static com.google.common.base.Preconditions.checkNotNull;

import org.apache.beam.runners.core.GroupAlsoByWindowViaWindowSetDoFn;
import org.apache.beam.runners.flink.translation.types.CoderTypeInformation;
import org.apache.beam.runners.flink.translation.utils.SerializedPipelineOptions;
import org.apache.beam.runners.flink.translation.wrappers.SerializableFnAggregatorWrapper;
import org.apache.beam.runners.flink.translation.wrappers.streaming.state.AbstractFlinkTimerInternals;
import org.apache.beam.runners.flink.translation.wrappers.streaming.state.FlinkStateInternals;
import org.apache.beam.runners.flink.translation.wrappers.streaming.state.StateCheckpointReader;
import org.apache.beam.runners.flink.translation.wrappers.streaming.state.StateCheckpointUtils;
import org.apache.beam.runners.flink.translation.wrappers.streaming.state.StateCheckpointWriter;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.coders.IterableCoder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.Aggregator;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.OutputTimeFn;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.util.AppliedCombineFn;
import org.apache.beam.sdk.util.KeyedWorkItem;
import org.apache.beam.sdk.util.KeyedWorkItems;
import org.apache.beam.sdk.util.SystemReduceFn;
import org.apache.beam.sdk.util.TimerInternals;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.util.WindowingInternals;
import org.apache.beam.sdk.util.WindowingStrategy;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TupleTag;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;

import org.apache.flink.api.common.accumulators.Accumulator;
import org.apache.flink.api.common.accumulators.AccumulatorHelper;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.runtime.state.AbstractStateBackend;
import org.apache.flink.runtime.state.StateHandle;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.ChainingStrategy;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.TimestampedCollector;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.StreamTaskState;
import org.joda.time.Instant;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

/**
 * This class is the key class implementing all the windowing/triggering logic of Apache Beam.
 * To provide full compatibility and support for all the windowing/triggering combinations offered by
 * Beam, we opted for a strategy that uses the SDK's code for doing these operations. See the code in
 * ({@link org.apache.beam.runners.core.GroupAlsoByWindowsDoFn}.
 * <p/>
 * In a nutshell, when the execution arrives to this operator, we expect to have a stream <b>already
 * grouped by key</b>. Each of the elements that enter here, registers a timer
 * (see {@link TimerInternals#setTimer(TimerInternals.TimerData)} in the
 * {@link FlinkGroupAlsoByWindowWrapper#activeTimers}.
 * This is essentially a timestamp indicating when to trigger the computation over the window this
 * element belongs to.
 * <p/>
 * When a watermark arrives, all the registered timers are checked to see which ones are ready to
 * fire (see {@link FlinkGroupAlsoByWindowWrapper#processWatermark(Watermark)}). These are deregistered from
 * the {@link FlinkGroupAlsoByWindowWrapper#activeTimers}
 * list, and are fed into the {@link org.apache.beam.runners.core.GroupAlsoByWindowsDoFn}
 * for furhter processing.
 */
public class FlinkGroupAlsoByWindowWrapper<K, VIN, VACC, VOUT>
    extends AbstractStreamOperator<WindowedValue<KV<K, VOUT>>>
    implements OneInputStreamOperator<WindowedValue<KV<K, VIN>>, WindowedValue<KV<K, VOUT>>> {

  private static final long serialVersionUID = 1L;

  private SerializedPipelineOptions serializedOptions;

  private transient CoderRegistry coderRegistry;

  private DoFn<KeyedWorkItem<K, VIN>, KV<K, VOUT>> operator;

  private ProcessContext context;

  private final WindowingStrategy<KV<K, VIN>, BoundedWindow> windowingStrategy;

  private final Combine.KeyedCombineFn<K, VIN, VACC, VOUT> combineFn;

  private final KvCoder<K, VIN> inputKvCoder;

  /**
   * State is kept <b>per-key</b>. This data structure keeps this mapping between an active key, i.e. a
   * key whose elements are currently waiting to be processed, and its associated state.
   */
  private Map<K, FlinkStateInternals<K>> perKeyStateInternals = new HashMap<>();

  /**
   * Timers waiting to be processed.
   */
  private Map<K, Set<TimerInternals.TimerData>> activeTimers = new HashMap<>();

  private FlinkTimerInternals timerInternals = new FlinkTimerInternals();

  /**
   * Creates an DataStream where elements are grouped in windows based on the specified windowing strategy.
   * This method assumes that <b>elements are already grouped by key</b>.
   * <p/>
   * The difference with {@link #createForIterable(PipelineOptions, PCollection, KeyedStream)}
   * is that this method assumes that a combiner function is provided
   * (see {@link org.apache.beam.sdk.transforms.Combine.KeyedCombineFn}).
   * A combiner helps at increasing the speed and, in most of the cases, reduce the per-window state.
   *
   * @param options            the general job configuration options.
   * @param input              the input Dataflow {@link org.apache.beam.sdk.values.PCollection}.
   * @param groupedStreamByKey the input stream, it is assumed to already be grouped by key.
   * @param combiner           the combiner to be used.
   * @param outputKvCoder      the type of the output values.
   */
  public static <K, VIN, VACC, VOUT> DataStream<WindowedValue<KV<K, VOUT>>> create(
      PipelineOptions options,
      PCollection input,
      KeyedStream<WindowedValue<KV<K, VIN>>, K> groupedStreamByKey,
      Combine.KeyedCombineFn<K, VIN, VACC, VOUT> combiner,
      KvCoder<K, VOUT> outputKvCoder) {
    checkNotNull(options);

    KvCoder<K, VIN> inputKvCoder = (KvCoder<K, VIN>) input.getCoder();
    FlinkGroupAlsoByWindowWrapper windower = new FlinkGroupAlsoByWindowWrapper<>(options,
        input.getPipeline().getCoderRegistry(), input.getWindowingStrategy(), inputKvCoder, combiner);

    Coder<WindowedValue<KV<K, VOUT>>> windowedOutputElemCoder = WindowedValue.FullWindowedValueCoder.of(
        outputKvCoder,
        input.getWindowingStrategy().getWindowFn().windowCoder());

    CoderTypeInformation<WindowedValue<KV<K, VOUT>>> outputTypeInfo =
        new CoderTypeInformation<>(windowedOutputElemCoder);

    DataStream<WindowedValue<KV<K, VOUT>>> groupedByKeyAndWindow = groupedStreamByKey
        .transform("GroupByWindowWithCombiner",
            new CoderTypeInformation<>(outputKvCoder),
            windower)
        .returns(outputTypeInfo);

    return groupedByKeyAndWindow;
  }

  /**
   * Creates an DataStream where elements are grouped in windows based on the specified windowing strategy.
   * This method assumes that <b>elements are already grouped by key</b>.
   * <p/>
   * The difference with {@link #create(PipelineOptions, PCollection, KeyedStream, Combine.KeyedCombineFn, KvCoder)}
   * is that this method assumes no combiner function
   * (see {@link org.apache.beam.sdk.transforms.Combine.KeyedCombineFn}).
   *
   * @param options            the general job configuration options.
   * @param input              the input Dataflow {@link org.apache.beam.sdk.values.PCollection}.
   * @param groupedStreamByKey the input stream, it is assumed to already be grouped by key.
   */
  public static <K, VIN> DataStream<WindowedValue<KV<K, Iterable<VIN>>>> createForIterable(
      PipelineOptions options,
      PCollection input,
      KeyedStream<WindowedValue<KV<K, VIN>>, K> groupedStreamByKey) {
    checkNotNull(options);

    KvCoder<K, VIN> inputKvCoder = (KvCoder<K, VIN>) input.getCoder();
    Coder<K> keyCoder = inputKvCoder.getKeyCoder();
    Coder<VIN> inputValueCoder = inputKvCoder.getValueCoder();

    FlinkGroupAlsoByWindowWrapper windower = new FlinkGroupAlsoByWindowWrapper(options,
        input.getPipeline().getCoderRegistry(), input.getWindowingStrategy(), inputKvCoder, null);

    Coder<Iterable<VIN>> valueIterCoder = IterableCoder.of(inputValueCoder);
    KvCoder<K, Iterable<VIN>> outputElemCoder = KvCoder.of(keyCoder, valueIterCoder);

    Coder<WindowedValue<KV<K, Iterable<VIN>>>> windowedOutputElemCoder = WindowedValue.FullWindowedValueCoder.of(
        outputElemCoder,
        input.getWindowingStrategy().getWindowFn().windowCoder());

    CoderTypeInformation<WindowedValue<KV<K, Iterable<VIN>>>> outputTypeInfo =
        new CoderTypeInformation<>(windowedOutputElemCoder);

    DataStream<WindowedValue<KV<K, Iterable<VIN>>>> groupedByKeyAndWindow = groupedStreamByKey
        .transform("GroupByWindow",
            new CoderTypeInformation<>(windowedOutputElemCoder),
            windower)
        .returns(outputTypeInfo);

    return groupedByKeyAndWindow;
  }

  public static <K, VIN, VACC, VOUT> FlinkGroupAlsoByWindowWrapper
  createForTesting(PipelineOptions options,
                   CoderRegistry registry,
                   WindowingStrategy<KV<K, VIN>, BoundedWindow> windowingStrategy,
                   KvCoder<K, VIN> inputCoder,
                   Combine.KeyedCombineFn<K, VIN, VACC, VOUT> combiner) {
    checkNotNull(options);

    return new FlinkGroupAlsoByWindowWrapper(options, registry, windowingStrategy, inputCoder, combiner);
  }

  private FlinkGroupAlsoByWindowWrapper(PipelineOptions options,
                                        CoderRegistry registry,
                                        WindowingStrategy<KV<K, VIN>, BoundedWindow> windowingStrategy,
                                        KvCoder<K, VIN> inputCoder,
                                        Combine.KeyedCombineFn<K, VIN, VACC, VOUT> combiner) {
    checkNotNull(options);

    this.serializedOptions = new SerializedPipelineOptions(checkNotNull(options));
    this.coderRegistry = checkNotNull(registry);
    this.inputKvCoder = checkNotNull(inputCoder);//(KvCoder<K, VIN>) input.getCoder();
    this.windowingStrategy = checkNotNull(windowingStrategy);//input.getWindowingStrategy();
    this.combineFn = combiner;
    this.operator = createGroupAlsoByWindowOperator();
    this.chainingStrategy = ChainingStrategy.ALWAYS;
  }

  @Override
  public void open() throws Exception {
    super.open();
    this.context = new ProcessContext(operator, new TimestampedCollector<>(output), this.timerInternals);
    operator.startBundle(context);
  }

  /**
   * Create the adequate {@link org.apache.beam.runners.core.GroupAlsoByWindowsDoFn},
   * <b> if not already created</b>.
   * If a {@link org.apache.beam.sdk.transforms.Combine.KeyedCombineFn} was provided, then
   * a function with that combiner is created, so that elements are combined as they arrive. This is
   * done for speed and (in most of the cases) for reduction of the per-window state.
   */
  private <W extends BoundedWindow> DoFn<KeyedWorkItem<K, VIN>, KV<K, VOUT>> createGroupAlsoByWindowOperator() {
    if (this.operator == null) {
      if (this.combineFn == null) {
        // Thus VOUT == Iterable<VIN>
        Coder<VIN> inputValueCoder = inputKvCoder.getValueCoder();

        this.operator = (DoFn) GroupAlsoByWindowViaWindowSetDoFn.create(
            (WindowingStrategy<?, W>) this.windowingStrategy, SystemReduceFn.<K, VIN, W>buffering(inputValueCoder));
      } else {
        Coder<K> inputKeyCoder = inputKvCoder.getKeyCoder();

        AppliedCombineFn<K, VIN, VACC, VOUT> appliedCombineFn = AppliedCombineFn
            .withInputCoder(combineFn, coderRegistry, inputKvCoder);

        this.operator = GroupAlsoByWindowViaWindowSetDoFn.create(
            (WindowingStrategy<?, W>) this.windowingStrategy, SystemReduceFn.<K, VIN, VACC, VOUT, W>combining(inputKeyCoder, appliedCombineFn));
      }
    }
    return this.operator;
  }

  private void processKeyedWorkItem(KeyedWorkItem<K, VIN> workItem) throws Exception {
    context.setElement(workItem, getStateInternalsForKey(workItem.key()));
    operator.processElement(context);
  }

  @Override
  public void processElement(StreamRecord<WindowedValue<KV<K, VIN>>> element) throws Exception {
    final WindowedValue<KV<K, VIN>> windowedValue = element.getValue();
    final KV<K, VIN> kv = windowedValue.getValue();

    final WindowedValue<VIN> updatedWindowedValue = WindowedValue.of(kv.getValue(),
        windowedValue.getTimestamp(),
        windowedValue.getWindows(),
        windowedValue.getPane());

    processKeyedWorkItem(
        KeyedWorkItems.elementsWorkItem(
            kv.getKey(),
            Collections.singletonList(updatedWindowedValue)));
  }

  @Override
  public void processWatermark(Watermark mark) throws Exception {
    context.setCurrentInputWatermark(new Instant(mark.getTimestamp()));

    Multimap<K, TimerInternals.TimerData> timers = getTimersReadyToProcess(mark.getTimestamp());
    if (!timers.isEmpty()) {
      for (K key : timers.keySet()) {
        processKeyedWorkItem(KeyedWorkItems.<K, VIN>timersWorkItem(key, timers.get(key)));
      }
    }

    /**
     * This is to take into account the different semantics of the Watermark in Flink and
     * in Dataflow. To understand the reasoning behind the Dataflow semantics and its
     * watermark holding logic, see the documentation of
     * {@link WatermarkHold#addHold(ReduceFn.ProcessValueContext, boolean)}
     * */
    long millis = Long.MAX_VALUE;
    for (FlinkStateInternals state : perKeyStateInternals.values()) {
      Instant watermarkHold = state.getWatermarkHold();
      if (watermarkHold != null && watermarkHold.getMillis() < millis) {
        millis = watermarkHold.getMillis();
      }
    }

    if (mark.getTimestamp() < millis) {
      millis = mark.getTimestamp();
    }

    context.setCurrentOutputWatermark(new Instant(millis));

    // Don't forget to re-emit the watermark for further operators down the line.
    // This is critical for jobs with multiple aggregation steps.
    // Imagine a job with a groupByKey() on key K1, followed by a map() that changes
    // the key K1 to K2, and another groupByKey() on K2. In this case, if the watermark
    // is not re-emitted, the second aggregation would never be triggered, and no result
    // will be produced.
    output.emitWatermark(new Watermark(millis));
  }

  @Override
  public void close() throws Exception {
    operator.finishBundle(context);
    super.close();
  }

  private void registerActiveTimer(K key, TimerInternals.TimerData timer) {
    Set<TimerInternals.TimerData> timersForKey = activeTimers.get(key);
    if (timersForKey == null) {
      timersForKey = new HashSet<>();
    }
    timersForKey.add(timer);
    activeTimers.put(key, timersForKey);
  }

  private void unregisterActiveTimer(K key, TimerInternals.TimerData timer) {
    Set<TimerInternals.TimerData> timersForKey = activeTimers.get(key);
    if (timersForKey != null) {
      timersForKey.remove(timer);
      if (timersForKey.isEmpty()) {
        activeTimers.remove(key);
      } else {
        activeTimers.put(key, timersForKey);
      }
    }
  }

  /**
   * Returns the list of timers that are ready to fire. These are the timers
   * that are registered to be triggered at a time before the current watermark.
   * We keep these timers in a Set, so that they are deduplicated, as the same
   * timer can be registered multiple times.
   */
  private Multimap<K, TimerInternals.TimerData> getTimersReadyToProcess(long currentWatermark) {

    // we keep the timers to return in a different list and launch them later
    // because we cannot prevent a trigger from registering another trigger,
    // which would lead to concurrent modification exception.
    Multimap<K, TimerInternals.TimerData> toFire = HashMultimap.create();

    Iterator<Map.Entry<K, Set<TimerInternals.TimerData>>> it = activeTimers.entrySet().iterator();
    while (it.hasNext()) {
      Map.Entry<K, Set<TimerInternals.TimerData>> keyWithTimers = it.next();

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

  /**
   * Gets the state associated with the specified key.
   *
   * @param key the key whose state we want.
   * @return The {@link FlinkStateInternals}
   * associated with that key.
   */
  private FlinkStateInternals<K> getStateInternalsForKey(K key) {
    FlinkStateInternals<K> stateInternals = perKeyStateInternals.get(key);
    if (stateInternals == null) {
      Coder<? extends BoundedWindow> windowCoder = this.windowingStrategy.getWindowFn().windowCoder();
      OutputTimeFn<? super BoundedWindow> outputTimeFn = this.windowingStrategy.getOutputTimeFn();
      stateInternals = new FlinkStateInternals<>(key, inputKvCoder.getKeyCoder(), windowCoder, outputTimeFn);
      perKeyStateInternals.put(key, stateInternals);
    }
    return stateInternals;
  }

  private class FlinkTimerInternals extends AbstractFlinkTimerInternals<K, VIN> {
    @Override
    public void setTimer(TimerData timerKey) {
      registerActiveTimer(context.element().key(), timerKey);
    }

    @Override
    public void deleteTimer(TimerData timerKey) {
      unregisterActiveTimer(context.element().key(), timerKey);
    }
  }

  private class ProcessContext extends GroupAlsoByWindowViaWindowSetDoFn<K, VIN, VOUT, ?, KeyedWorkItem<K, VIN>>.ProcessContext {

    private final FlinkTimerInternals timerInternals;

    private final TimestampedCollector<WindowedValue<KV<K, VOUT>>> collector;

    private FlinkStateInternals<K> stateInternals;

    private KeyedWorkItem<K, VIN> element;

    public ProcessContext(DoFn<KeyedWorkItem<K, VIN>, KV<K, VOUT>> function,
                          TimestampedCollector<WindowedValue<KV<K, VOUT>>> outCollector,
                          FlinkTimerInternals timerInternals) {
      function.super();
      super.setupDelegateAggregators();

      this.collector = checkNotNull(outCollector);
      this.timerInternals = checkNotNull(timerInternals);
    }

    public void setElement(KeyedWorkItem<K, VIN> element,
                           FlinkStateInternals<K> stateForKey) {
      this.element = element;
      this.stateInternals = stateForKey;
    }

    public void setCurrentInputWatermark(Instant watermark) {
      this.timerInternals.setCurrentInputWatermark(watermark);
    }

    public void setCurrentOutputWatermark(Instant watermark) {
      this.timerInternals.setCurrentOutputWatermark(watermark);
    }

    @Override
    public KeyedWorkItem<K, VIN> element() {
      return this.element;
    }

    @Override
    public Instant timestamp() {
      throw new UnsupportedOperationException("timestamp() is not available when processing KeyedWorkItems.");
    }

    @Override
    public PipelineOptions getPipelineOptions() {
      return serializedOptions.getPipelineOptions();
    }

    @Override
    public void output(KV<K, VOUT> output) {
      throw new UnsupportedOperationException(
          "output() is not available when processing KeyedWorkItems.");
    }

    @Override
    public void outputWithTimestamp(KV<K, VOUT> output, Instant timestamp) {
      throw new UnsupportedOperationException(
          "outputWithTimestamp() is not available when processing KeyedWorkItems.");
    }

    @Override
    public PaneInfo pane() {
      throw new UnsupportedOperationException("pane() is not available when processing KeyedWorkItems.");
    }

    @Override
    public BoundedWindow window() {
      throw new UnsupportedOperationException(
          "window() is not available when processing KeyedWorkItems.");
    }

    @Override
    public WindowingInternals<KeyedWorkItem<K, VIN>, KV<K, VOUT>> windowingInternals() {
      return new WindowingInternals<KeyedWorkItem<K, VIN>, KV<K, VOUT>>() {

        @Override
        public org.apache.beam.sdk.util.state.StateInternals stateInternals() {
          return stateInternals;
        }

        @Override
        public void outputWindowedValue(KV<K, VOUT> output, Instant timestamp, Collection<? extends BoundedWindow> windows, PaneInfo pane) {
          // TODO: No need to represent timestamp twice.
          collector.setAbsoluteTimestamp(timestamp.getMillis());
          collector.collect(WindowedValue.of(output, timestamp, windows, pane));

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
        public <T> void writePCollectionViewData(TupleTag<?> tag, Iterable<WindowedValue<T>> data, Coder<T> elemCoder) throws IOException {
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
    protected <AggInputT, AggOutputT> Aggregator<AggInputT, AggOutputT> createAggregatorInternal(String name, Combine.CombineFn<AggInputT, ?, AggOutputT> combiner) {
      Accumulator acc = getRuntimeContext().getAccumulator(name);
      if (acc != null) {
        AccumulatorHelper.compareAccumulatorTypes(name,
            SerializableFnAggregatorWrapper.class, acc.getClass());
        return (Aggregator<AggInputT, AggOutputT>) acc;
      }

      SerializableFnAggregatorWrapper<AggInputT, AggOutputT> accumulator =
          new SerializableFnAggregatorWrapper<>(combiner);
      getRuntimeContext().addAccumulator(name, accumulator);
      return accumulator;
    }
  }

  //////////////        Checkpointing implementation        ////////////////

  @Override
  public StreamTaskState snapshotOperatorState(long checkpointId, long timestamp) throws Exception {
    StreamTaskState taskState = super.snapshotOperatorState(checkpointId, timestamp);
    AbstractStateBackend.CheckpointStateOutputView out = getStateBackend().createCheckpointStateOutputView(checkpointId, timestamp);
    StateCheckpointWriter writer = StateCheckpointWriter.create(out);
    Coder<K> keyCoder = inputKvCoder.getKeyCoder();

    // checkpoint the timers
    StateCheckpointUtils.encodeTimers(activeTimers, writer, keyCoder);

    // checkpoint the state
    StateCheckpointUtils.encodeState(perKeyStateInternals, writer, keyCoder);

    // checkpoint the timerInternals
    context.timerInternals.encodeTimerInternals(context, writer,
        inputKvCoder, windowingStrategy.getWindowFn().windowCoder());

    taskState.setOperatorState(out.closeAndGetHandle());
    return taskState;
  }

  @Override
  public void restoreState(StreamTaskState taskState, long recoveryTimestamp) throws Exception {
    super.restoreState(taskState, recoveryTimestamp);

    final ClassLoader userClassloader = getUserCodeClassloader();

    Coder<? extends BoundedWindow> windowCoder = this.windowingStrategy.getWindowFn().windowCoder();
    Coder<K> keyCoder = inputKvCoder.getKeyCoder();

    @SuppressWarnings("unchecked")
    StateHandle<DataInputView> inputState = (StateHandle<DataInputView>) taskState.getOperatorState();
    DataInputView in = inputState.getState(userClassloader);
    StateCheckpointReader reader = new StateCheckpointReader(in);

    // restore the timers
    this.activeTimers = StateCheckpointUtils.decodeTimers(reader, windowCoder, keyCoder);

    // restore the state
    this.perKeyStateInternals = StateCheckpointUtils.decodeState(
        reader, windowingStrategy.getOutputTimeFn(), keyCoder, windowCoder, userClassloader);

    // restore the timerInternals.
    this.timerInternals.restoreTimerInternals(reader, inputKvCoder, windowCoder);
  }
}
