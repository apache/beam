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

import com.dataartisans.flink.dataflow.translation.wrappers.SerializableFnAggregatorWrapper;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.common.base.Preconditions;
import com.google.cloud.dataflow.sdk.transforms.Aggregator;
import com.google.cloud.dataflow.sdk.transforms.Combine;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.windowing.BoundedWindow;
import com.google.cloud.dataflow.sdk.transforms.windowing.PaneInfo;
import com.google.cloud.dataflow.sdk.transforms.windowing.WindowFn;
import com.google.cloud.dataflow.sdk.util.*;
import com.google.cloud.dataflow.sdk.values.PCollectionView;
import com.google.cloud.dataflow.sdk.values.TupleTag;
import com.google.common.base.Throwables;
import org.apache.flink.api.common.accumulators.Accumulator;
import org.apache.flink.api.common.accumulators.AccumulatorHelper;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.util.Collector;
import org.joda.time.Instant;
import org.joda.time.format.PeriodFormat;

import java.util.Collection;

/**
 * An abstract class that encapsulates the common code of the the {@link com.google.cloud.dataflow.sdk.transforms.ParDo.Bound}
 * and {@link com.google.cloud.dataflow.sdk.transforms.ParDo.BoundMulti} wrappers. See the {@link FlinkParDoBoundWrapper} and
 * {@link FlinkParDoBoundMultiWrapper} for the actual wrappers of the aforementioned transformations.
 * */
public abstract class FlinkAbstractParDoWrapper<IN, OUTDF, OUTFL> extends RichFlatMapFunction<WindowedValue<IN>, WindowedValue<OUTFL>> {

	private final DoFn<IN, OUTDF> doFn;
	private final WindowingStrategy<?, ?> windowingStrategy;
	private transient PipelineOptions options;

	private DoFnProcessContext context;

	public FlinkAbstractParDoWrapper(PipelineOptions options, WindowingStrategy<?, ?> windowingStrategy, DoFn<IN, OUTDF> doFn) {
		Preconditions.checkNotNull(options);
		Preconditions.checkNotNull(windowingStrategy);
		Preconditions.checkNotNull(doFn);

		this.doFn = doFn;
		this.options = options;
		this.windowingStrategy = windowingStrategy;
	}

	private void initContext(DoFn<IN, OUTDF> function, Collector<WindowedValue<OUTFL>> outCollector) {
		if (this.context == null) {
			this.context = new DoFnProcessContext(function, outCollector);
		}
	}

	@Override
	public void flatMap(WindowedValue<IN> value, Collector<WindowedValue<OUTFL>> out) throws Exception {
		this.initContext(doFn, out);

		// for each window the element belongs to, create a new copy here.
		Collection<? extends BoundedWindow> windows = value.getWindows();
		if (windows.size() <= 1) {
			processElement(value);
		} else {
			for (BoundedWindow window : windows) {
				processElement(WindowedValue.of(
						value.getValue(), value.getTimestamp(), window, value.getPane()));
			}
		}
	}

	private void processElement(WindowedValue<IN> value) throws Exception {
		this.context.setElement(value);
		this.doFn.startBundle(context);
		doFn.processElement(context);
		this.doFn.finishBundle(context);
	}

	private class DoFnProcessContext extends DoFn<IN, OUTDF>.ProcessContext {

		private final DoFn<IN, OUTDF> fn;

		protected final Collector<WindowedValue<OUTFL>> collector;

		private WindowedValue<IN> element;

		private DoFnProcessContext(DoFn<IN, OUTDF> function, Collector<WindowedValue<OUTFL>> outCollector) {
			function.super();
			super.setupDelegateAggregators();

			this.fn = function;
			this.collector = outCollector;
		}

		public void setElement(WindowedValue<IN> value) {
			this.element = value;
		}

		@Override
		public IN element() {
			return this.element.getValue();
		}

		@Override
		public Instant timestamp() {
			return this.element.getTimestamp();
		}

		@Override
		public BoundedWindow window() {
			if (!(fn instanceof DoFn.RequiresWindowAccess)) {
				throw new UnsupportedOperationException(
						"window() is only available in the context of a DoFn marked as RequiresWindow.");
			}

			Collection<? extends BoundedWindow> windows = this.element.getWindows();
			if (windows.size() != 1) {
				throw new IllegalArgumentException("Each element is expected to belong to 1 window. " +
						"This belongs to " + windows.size() + ".");
			}
			return windows.iterator().next();
		}

		@Override
		public PaneInfo pane() {
			return this.element.getPane();
		}

		@Override
		public WindowingInternals<IN, OUTDF> windowingInternals() {
			return windowingInternalsHelper(element, collector);
		}

		@Override
		public PipelineOptions getPipelineOptions() {
			return options;
		}

		@Override
		public <T> T sideInput(PCollectionView<T> view) {
			throw new RuntimeException("sideInput() is not supported in Streaming mode.");
		}

		@Override
		public void output(OUTDF output) {
			outputWithTimestamp(output, this.element.getTimestamp());
		}

		@Override
		public void outputWithTimestamp(OUTDF output, Instant timestamp) {
			outputWithTimestampHelper(element, output, timestamp, collector);
		}

		@Override
		public <T> void sideOutput(TupleTag<T> tag, T output) {
			sideOutputWithTimestamp(tag, output, this.element.getTimestamp());
		}

		@Override
		public <T> void sideOutputWithTimestamp(TupleTag<T> tag, T output, Instant timestamp) {
			sideOutputWithTimestampHelper(element, output, timestamp, collector, tag);
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

	protected void checkTimestamp(WindowedValue<IN> ref, Instant timestamp) {
		if (timestamp.isBefore(ref.getTimestamp().minus(doFn.getAllowedTimestampSkew()))) {
			throw new IllegalArgumentException(String.format(
					"Cannot output with timestamp %s. Output timestamps must be no earlier than the "
							+ "timestamp of the current input (%s) minus the allowed skew (%s). See the "
							+ "DoFn#getAllowedTimestmapSkew() Javadoc for details on changing the allowed skew.",
					timestamp, ref.getTimestamp(),
					PeriodFormat.getDefault().print(doFn.getAllowedTimestampSkew().toPeriod())));
		}
	}

	protected <T> WindowedValue<T> makeWindowedValue(
			T output, Instant timestamp, Collection<? extends BoundedWindow> windows, PaneInfo pane) {
		final Instant inputTimestamp = timestamp;
		final WindowFn windowFn = windowingStrategy.getWindowFn();

		if (timestamp == null) {
			timestamp = BoundedWindow.TIMESTAMP_MIN_VALUE;
		}

		if (windows == null) {
			try {
				windows = windowFn.assignWindows(windowFn.new AssignContext() {
					@Override
					public Object element() {
						throw new UnsupportedOperationException(
								"WindowFn attempted to access input element when none was available");
					}

					@Override
					public Instant timestamp() {
						if (inputTimestamp == null) {
							throw new UnsupportedOperationException(
									"WindowFn attempted to access input timestamp when none was available");
						}
						return inputTimestamp;
					}

					@Override
					public Collection<? extends BoundedWindow> windows() {
						throw new UnsupportedOperationException(
								"WindowFn attempted to access input windows when none were available");
					}
				});
			} catch (Exception e) {
				throw UserCodeException.wrap(e);
			}
		}

		return WindowedValue.of(output, timestamp, windows, pane);
	}

	///////////			ABSTRACT METHODS TO BE IMPLEMENTED BY SUBCLASSES			/////////////////

	public abstract void outputWithTimestampHelper(
			WindowedValue<IN> inElement,
			OUTDF output,
			Instant timestamp,
			Collector<WindowedValue<OUTFL>> outCollector);

	public abstract <T> void sideOutputWithTimestampHelper(
			WindowedValue<IN> inElement,
			T output,
			Instant timestamp,
			Collector<WindowedValue<OUTFL>> outCollector,
			TupleTag<T> tag);

	public abstract WindowingInternals<IN, OUTDF> windowingInternalsHelper(
			WindowedValue<IN> inElement,
			Collector<WindowedValue<OUTFL>> outCollector);

}