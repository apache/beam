/*
 * Copyright 2015 Data Artisans GmbH
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.dataartisans.flink.dataflow.translation.wrappers.streaming;

import com.dataartisans.flink.dataflow.translation.types.CoderTypeInformation;
import com.dataartisans.flink.dataflow.translation.wrappers.SerializableFnAggregatorWrapper;
import com.dataartisans.flink.dataflow.translation.wrappers.streaming.state.*;
import com.google.cloud.dataflow.sdk.coders.*;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.cloud.dataflow.sdk.repackaged.com.google.common.base.Preconditions;
import com.google.cloud.dataflow.sdk.transforms.Aggregator;
import com.google.cloud.dataflow.sdk.transforms.Combine;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.windowing.BoundedWindow;
import com.google.cloud.dataflow.sdk.transforms.windowing.PaneInfo;
import com.google.cloud.dataflow.sdk.util.*;
import com.google.cloud.dataflow.sdk.values.*;
import org.apache.flink.api.common.accumulators.Accumulator;
import org.apache.flink.api.common.accumulators.AccumulatorHelper;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.StateHandle;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.operators.*;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.StreamTaskState;
import org.apache.flink.util.Collector;
import org.joda.time.Instant;

import java.io.IOException;
import java.util.*;

/**
 * This class is the key class implementing all the windowing/triggering logic of Apache Beam.
 * To provide full compatibility and support for all the windowing/triggering combinations offered by
 * Beam, we opted for a strategy that uses the SDK's code for doing these operations. See the code in
 * ({@link com.google.cloud.dataflow.sdk.util.StreamingGroupAlsoByWindowsDoFn}.
 * <p>
 * In a nutshell, when the execution arrives to this operator, we expect to have a stream <b>already
 * grouped by key</b>. Each of the elements that enter here, registers a timer
 * (see {@link TimerInternals#setTimer(TimerInternals.TimerData)} in the
 * {@link FlinkGroupAlsoByWindowWrapper#activeTimers}.
 * This is essentially a timestamp indicating when to trigger the computation over the window this
 * element belongs to.
 * <p>
 * When a watermark arrives, all the registered timers are checked to see which ones are ready to
 * fire (see {@link FlinkGroupAlsoByWindowWrapper#processWatermark(Watermark)}). These are deregistered from
 * the {@link FlinkGroupAlsoByWindowWrapper#activeTimers}
 * list, and are fed into the {@link com.google.cloud.dataflow.sdk.util.StreamingGroupAlsoByWindowsDoFn}
 * for furhter processing.
 */
public class FlinkGroupAlsoByWindowWrapper<K, VIN, VACC, VOUT>
		extends AbstractStreamOperator<WindowedValue<KV<K, VOUT>>>
		implements OneInputStreamOperator<WindowedValue<KV<K, VIN>>, WindowedValue<KV<K, VOUT>>> {

	private static final long serialVersionUID = 1L;

	private transient PipelineOptions options;

	private transient CoderRegistry coderRegistry;

	private StreamingGroupAlsoByWindowsDoFn operator;

	private ProcessContext context;

	private final WindowingStrategy<?, ?> windowingStrategy;

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
	 * <p>
	 * The difference with {@link #createForIterable(PipelineOptions, PCollection, KeyedStream)}
	 * is that this method assumes that a combiner function is provided
	 * (see {@link com.google.cloud.dataflow.sdk.transforms.Combine.KeyedCombineFn}).
	 * A combiner helps at increasing the speed and, in most of the cases, reduce the per-window state.
	 *
	 * @param options            the general job configuration options.
	 * @param input              the input Dataflow {@link com.google.cloud.dataflow.sdk.values.PCollection}.
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
	 * <p>
	 * The difference with {@link #create(PipelineOptions, PCollection, KeyedStream, Combine.KeyedCombineFn, KvCoder)}
	 * is that this method assumes no combiner function
	 * (see {@link com.google.cloud.dataflow.sdk.transforms.Combine.KeyedCombineFn}).
	 *
	 * @param options            the general job configuration options.
	 * @param input              the input Dataflow {@link com.google.cloud.dataflow.sdk.values.PCollection}.
	 * @param groupedStreamByKey the input stream, it is assumed to already be grouped by key.
	 */
	public static <K, VIN> DataStream<WindowedValue<KV<K, Iterable<VIN>>>> createForIterable(
			PipelineOptions options,
			PCollection input,
			KeyedStream<WindowedValue<KV<K, VIN>>, K> groupedStreamByKey) {

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

	public static <K, VIN, VACC, VOUT> FlinkGroupAlsoByWindowWrapper createForTesting(PipelineOptions options,
																					  CoderRegistry registry,
																					  WindowingStrategy<?, ?> windowingStrategy,
																					  KvCoder<K, VIN> inputCoder,
																					  Combine.KeyedCombineFn<K, VIN, VACC, VOUT> combiner) {
		return new FlinkGroupAlsoByWindowWrapper(options, registry, windowingStrategy, inputCoder, combiner);
	}

	private FlinkGroupAlsoByWindowWrapper(PipelineOptions options,
										  CoderRegistry registry,
										  WindowingStrategy<?, ?> windowingStrategy,
										  KvCoder<K, VIN> inputCoder,
										  Combine.KeyedCombineFn<K, VIN, VACC, VOUT> combiner) {

		this.options = Preconditions.checkNotNull(options);
		this.coderRegistry = Preconditions.checkNotNull(registry);
		this.inputKvCoder = Preconditions.checkNotNull(inputCoder);//(KvCoder<K, VIN>) input.getCoder();
		this.combineFn = combiner;
		this.windowingStrategy = Preconditions.checkNotNull(windowingStrategy);//input.getWindowingStrategy();
		this.operator = createGroupAlsoByWindowOperator();
		this.chainingStrategy = ChainingStrategy.ALWAYS;
	}

	@Override
	public void open() throws Exception {
		super.open();
		this.context = new ProcessContext(operator, new TimestampedCollector<>(output), this.timerInternals);

		// this is to cover the case that this is the state after a recovery.
		// In this case, the restoreState() has already initialized the timerInternals to a certain value.
		TimerOrElement<WindowedValue<KV<K, VIN>>> element = this.timerInternals.getElement();
		if (element != null) {
			if (element.isTimer()) {
				throw new RuntimeException("The recovered element cannot be a Timer.");
			}
			K key = element.element().getValue().getKey();
			FlinkStateInternals<K> stateForKey = getStateInternalsForKey(key);
			this.context.setElement(element, stateForKey);
		}
	}

	/**
	 * Create the adequate {@link com.google.cloud.dataflow.sdk.util.StreamingGroupAlsoByWindowsDoFn},
	 * <b> if not already created</b>.
	 * If a {@link com.google.cloud.dataflow.sdk.transforms.Combine.KeyedCombineFn} was provided, then
	 * a function with that combiner is created, so that elements are combined as they arrive. This is
	 * done for speed and (in most of the cases) for reduction of the per-window state.
	 */
	private StreamingGroupAlsoByWindowsDoFn createGroupAlsoByWindowOperator() {
		if (this.operator == null) {
			if (this.combineFn == null) {
				Coder<VIN> inputValueCoder = inputKvCoder.getValueCoder();

				this.operator = StreamingGroupAlsoByWindowsDoFn.createForIterable(
						this.windowingStrategy, inputValueCoder);
			} else {
				Coder<K> inputKeyCoder = inputKvCoder.getKeyCoder();

				AppliedCombineFn<K, VIN, VACC, VOUT> appliedCombineFn = AppliedCombineFn
						.withInputCoder(combineFn, coderRegistry, inputKvCoder);

				this.operator = StreamingGroupAlsoByWindowsDoFn.create(
						this.windowingStrategy, appliedCombineFn, inputKeyCoder);
			}
		}
		return this.operator;
	}


	@Override
	public void processElement(StreamRecord<WindowedValue<KV<K, VIN>>> element) throws Exception {
		WindowedValue<KV<K, VIN>> value = element.getValue();
		TimerOrElement<WindowedValue<KV<K, VIN>>> elem = TimerOrElement.element(value);
		processElementOrTimer(elem);
	}

	@Override
	public void processWatermark(Watermark mark) throws Exception {

		context.setCurrentWatermark(new Instant(mark.getTimestamp()));

		Set<TimerOrElement> toFire = getTimersReadyToProcess(mark.getTimestamp());
		if (!toFire.isEmpty()) {
			for (TimerOrElement timer : toFire) {
				processElementOrTimer(timer);
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

		// Don't forget to re-emit the watermark for further operators down the line.
		// This is critical for jobs with multiple aggregation steps.
		// Imagine a job with a groupByKey() on key K1, followed by a map() that changes
		// the key K1 to K2, and another groupByKey() on K2. In this case, if the watermark
		// is not re-emitted, the second aggregation would never be triggered, and no result
		// will be produced.
		output.emitWatermark(new Watermark(millis));
	}

	private void processElementOrTimer(TimerOrElement<WindowedValue<KV<K, VIN>>> timerOrElement) throws Exception {
		K key = timerOrElement.isTimer() ?
				(K) timerOrElement.key() :
				timerOrElement.element().getValue().getKey();

		context.setElement(timerOrElement, getStateInternalsForKey(key));

		operator.startBundle(context);
		operator.processElement(context);
		operator.finishBundle(context);
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
	private Set<TimerOrElement> getTimersReadyToProcess(long currentWatermark) {

		// we keep the timers to return in a different list and launch them later
		// because we cannot prevent a trigger from registering another trigger,
		// which would lead to concurrent modification exception.
		Set<TimerOrElement> toFire = new HashSet<>();

		Iterator<Map.Entry<K, Set<TimerInternals.TimerData>>> it = activeTimers.entrySet().iterator();
		while (it.hasNext()) {
			Map.Entry<K, Set<TimerInternals.TimerData>> keyWithTimers = it.next();

			Iterator<TimerInternals.TimerData> timerIt = keyWithTimers.getValue().iterator();
			while (timerIt.hasNext()) {
				TimerInternals.TimerData timerData = timerIt.next();
				if (timerData.getTimestamp().isBefore(currentWatermark)) {
					TimerOrElement timer = TimerOrElement.timer(keyWithTimers.getKey(), timerData);
					toFire.add(timer);
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
			stateInternals = new FlinkStateInternals<>(key, inputKvCoder.getKeyCoder(), windowCoder, combineFn);
			perKeyStateInternals.put(key, stateInternals);
		}
		return stateInternals;
	}

	private class FlinkTimerInternals extends AbstractFlinkTimerInternals<K, VIN> {

		@Override
		protected void registerTimer(K key, TimerData timerKey) {
			registerActiveTimer(key, timerKey);
		}

		@Override
		protected void unregisterTimer(K key, TimerData timerKey) {
			unregisterActiveTimer(key, timerKey);
		}
	}

	private class ProcessContext extends DoFn<TimerOrElement<WindowedValue<KV<K, VIN>>>, KV<K, VOUT>>.ProcessContext {

		private final FlinkTimerInternals timerInternals;

		private final DoFn<TimerOrElement<WindowedValue<KV<K, VIN>>>, KV<K, VOUT>> fn;

		private final Collector<WindowedValue<KV<K, VOUT>>> collector;

		private FlinkStateInternals<K> stateInternals;

		private TimerOrElement<WindowedValue<KV<K, VIN>>> element;

		public ProcessContext(DoFn<TimerOrElement<WindowedValue<KV<K, VIN>>>, KV<K, VOUT>> function,
							  Collector<WindowedValue<KV<K, VOUT>>> outCollector,
							  FlinkTimerInternals timerInternals) {
			function.super();
			super.setupDelegateAggregators();

			this.fn = Preconditions.checkNotNull(function);
			this.collector = Preconditions.checkNotNull(outCollector);
			this.timerInternals = Preconditions.checkNotNull(timerInternals);
		}

		public void setElement(TimerOrElement<WindowedValue<KV<K, VIN>>> value,
							   FlinkStateInternals<K> stateForKey) {
			this.element = value;
			this.stateInternals = stateForKey;
			this.timerInternals.setElement(value);
		}

		public void setCurrentWatermark(Instant watermark) {
			this.timerInternals.setCurrentWatermark(watermark);
		}

		@Override
		public TimerOrElement element() {
			if (element != null && !this.element.isTimer()) {
				return TimerOrElement.element(this.element.element().getValue());
			}
			return this.element;
		}

		@Override
		public Instant timestamp() {
			return this.element.isTimer() ?
					this.element.getTimer().getTimestamp() :
					this.element.element().getTimestamp();
		}

		@Override
		public PipelineOptions getPipelineOptions() {
			return options;
		}

		@Override
		public void output(KV<K, VOUT> output) {
			throw new UnsupportedOperationException(
					"output() is not available when grouping by window.");
		}

		@Override
		public void outputWithTimestamp(KV<K, VOUT> output, Instant timestamp) {
			throw new UnsupportedOperationException(
					"outputWithTimestamp() is not available when grouping by window.");
		}

		@Override
		public PaneInfo pane() {
			return this.element.element().getPane();
		}

		@Override
		public BoundedWindow window() {
			if (!(fn instanceof DoFn.RequiresWindowAccess)) {
				throw new UnsupportedOperationException(
						"window() is only available in the context of a DoFn marked as RequiresWindow.");
			}

			Collection<? extends BoundedWindow> windows = this.element.element().getWindows();
			if (windows.size() != 1) {
				throw new IllegalArgumentException("Each element is expected to belong to 1 window. " +
						"This belongs to " + windows.size() + ".");
			}
			return windows.iterator().next();
		}

		@Override
		public WindowingInternals<TimerOrElement<WindowedValue<KV<K, VIN>>>, KV<K, VOUT>> windowingInternals() {
			return new WindowingInternals<TimerOrElement<WindowedValue<KV<K, VIN>>>, KV<K, VOUT>>() {

				@Override
				public com.google.cloud.dataflow.sdk.util.state.StateInternals stateInternals() {
					return stateInternals;
				}

				@Override
				public void outputWindowedValue(KV<K, VOUT> output, Instant timestamp, Collection<? extends BoundedWindow> windows, PaneInfo pane) {
					collector.collect(WindowedValue.of(output, timestamp, windows, pane));
				}

				@Override
				public TimerInternals timerInternals() {
					return timerInternals;
				}

				@Override
				public Collection<? extends BoundedWindow> windows() {
					return element.element().getWindows();
				}

				@Override
				public PaneInfo pane() {
					return element.element().getPane();
				}

				@Override
				public <T> void writePCollectionViewData(TupleTag<?> tag, Iterable<WindowedValue<T>> data, Coder<T> elemCoder) throws IOException {
					throw new RuntimeException("writePCollectionViewData() not supported in Streaming mode.");
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

	//////////////				Checkpointing implementation				////////////////

	@Override
	public StreamTaskState snapshotOperatorState(long checkpointId, long timestamp) throws Exception {
		StreamTaskState taskState = super.snapshotOperatorState(checkpointId, timestamp);
		StateBackend.CheckpointStateOutputView out = getStateBackend().createCheckpointStateOutputView(checkpointId, timestamp);
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
	public void restoreState(StreamTaskState taskState) throws Exception {
		super.restoreState(taskState);

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
				reader, combineFn, keyCoder, windowCoder, userClassloader);

		// restore the timerInternals.
		this.timerInternals.restoreTimerInternals(reader, inputKvCoder, windowCoder);
	}
}