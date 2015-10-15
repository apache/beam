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
package com.dataartisans.flink.dataflow.translation.functions;

import com.dataartisans.flink.dataflow.translation.wrappers.CombineFnAggregatorWrapper;
import com.dataartisans.flink.dataflow.translation.wrappers.SerializableFnAggregatorWrapper;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.cloud.dataflow.sdk.transforms.Aggregator;
import com.google.cloud.dataflow.sdk.transforms.Combine;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.SerializableFunction;
import com.google.cloud.dataflow.sdk.transforms.windowing.BoundedWindow;
import com.google.cloud.dataflow.sdk.transforms.windowing.GlobalWindow;
import com.google.cloud.dataflow.sdk.transforms.windowing.PaneInfo;
import com.google.cloud.dataflow.sdk.util.WindowedValue;
import com.google.cloud.dataflow.sdk.util.WindowingInternals;
import com.google.cloud.dataflow.sdk.values.PCollectionView;
import com.google.cloud.dataflow.sdk.values.TupleTag;
import com.google.common.collect.ImmutableList;
import javafx.scene.layout.Pane;
import org.apache.flink.api.common.functions.RichMapPartitionFunction;
import org.apache.flink.util.Collector;
import org.joda.time.Instant;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * Encapsulates a {@link com.google.cloud.dataflow.sdk.transforms.DoFn}
 * inside a Flink {@link org.apache.flink.api.common.functions.RichMapPartitionFunction}.
 */
public class FlinkDoFnFunction<IN, OUT> extends RichMapPartitionFunction<IN, OUT> {

	private final DoFn<IN, OUT> doFn;
	private transient PipelineOptions options;

	public FlinkDoFnFunction(DoFn<IN, OUT> doFn, PipelineOptions options) {
		this.doFn = doFn;
		this.options = options;
	}

	private void writeObject(ObjectOutputStream out)
			throws IOException, ClassNotFoundException {
		out.defaultWriteObject();
		ObjectMapper mapper = new ObjectMapper();
		mapper.writeValue(out, options);
	}

	private void readObject(ObjectInputStream in)
			throws IOException, ClassNotFoundException {
		in.defaultReadObject();
		ObjectMapper mapper = new ObjectMapper();
		options = mapper.readValue(in, PipelineOptions.class);
	}

	@Override
	public void mapPartition(Iterable<IN> values, Collector<OUT> out) throws Exception {
		ProcessContext context = new ProcessContext(doFn, out);
		this.doFn.startBundle(context);
		for (IN value : values) {
			context.inValue = value;
			doFn.processElement(context);
		}
		this.doFn.finishBundle(context);
	}
	
	private class ProcessContext extends DoFn<IN, OUT>.ProcessContext {

		IN inValue;
		Collector<OUT> outCollector;

		public ProcessContext(DoFn<IN, OUT> fn, Collector<OUT> outCollector) {
			fn.super();
			super.setupDelegateAggregators();
			this.outCollector = outCollector;
		}

		@Override
		public IN element() {
			return this.inValue;
		}


		@Override
		public Instant timestamp() {
			return Instant.now();
		}

		@Override
		public BoundedWindow window() {
			return GlobalWindow.INSTANCE;
		}

		@Override
		public PaneInfo pane() {
			return PaneInfo.NO_FIRING;
		}

		@Override
		public WindowingInternals<IN, OUT> windowingInternals() {
			return null;
		}

		@Override
		public PipelineOptions getPipelineOptions() {
			return options;
		}

		@Override
		public <T> T sideInput(PCollectionView<T> view) {
			List<T> sideInput = getRuntimeContext().getBroadcastVariable(view.getTagInternal().getId());
			List<WindowedValue<?>> windowedValueList = new ArrayList<>(sideInput.size());
			for (T input : sideInput) {
				windowedValueList.add(WindowedValue.of(input, Instant.now(), ImmutableList.of(GlobalWindow.INSTANCE), pane()));
			}
			return view.fromIterableInternal(windowedValueList);
		}

		@Override
		public void output(OUT output) {
			outCollector.collect(output);
		}

		@Override
		public void outputWithTimestamp(OUT output, Instant timestamp) {
			// not FLink's way, just output normally
			output(output);
		}

		@Override
		public <T> void sideOutput(TupleTag<T> tag, T output) {
			// ignore the side output, this can happen when a user does not register
			// side outputs but then outputs using a freshly created TupleTag.
		}

		@Override
		public <T> void sideOutputWithTimestamp(TupleTag<T> tag, T output, Instant timestamp) {
			sideOutput(tag, output);
		}

		@Override
		protected <AggInputT, AggOutputT> Aggregator<AggInputT, AggOutputT> createAggregatorInternal(String name, Combine.CombineFn<AggInputT, ?, AggOutputT> combiner) {
			SerializableFnAggregatorWrapper<AggInputT, AggOutputT> wrapper = new SerializableFnAggregatorWrapper<AggInputT, AggOutputT>(combiner);
			getRuntimeContext().addAccumulator(name, wrapper);
			return wrapper;
		}


	}
}
