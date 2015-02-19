package com.dataartisans.flink.dataflow.translation;

import com.dataartisans.flink.dataflow.translation.wrappers.CombineFnAggregatorWrapper;
import com.dataartisans.flink.dataflow.translation.wrappers.SerializableFnAggregatorWrapper;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.cloud.dataflow.sdk.transforms.Aggregator;
import com.google.cloud.dataflow.sdk.transforms.Combine;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.SerializableFunction;
import com.google.cloud.dataflow.sdk.transforms.windowing.BoundedWindow;
import com.google.cloud.dataflow.sdk.transforms.windowing.GlobalWindow;
import com.google.cloud.dataflow.sdk.util.WindowedValue;
import com.google.cloud.dataflow.sdk.values.PCollectionView;
import com.google.cloud.dataflow.sdk.values.TupleTag;
import com.google.common.collect.ImmutableList;
import org.apache.flink.api.common.functions.RichMapPartitionFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.util.Collector;
import org.joda.time.Instant;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * Encapsulates a DoFn inside a Flink MapPartitionFunction
 */
public class FlinkDoFnFunction<IN, OUT> extends RichMapPartitionFunction<IN, OUT> {

	DoFn<IN, OUT> doFn;

	public FlinkDoFnFunction(DoFn<IN, OUT> doFn) {
		this.doFn = doFn;
	}

	@Override
	public void mapPartition(Iterable<IN> values, Collector<OUT> out) throws Exception {
		ProcessContext context = new ProcessContext(doFn, out, getRuntimeContext());
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
		RuntimeContext runContext;
		
		public ProcessContext(DoFn<IN, OUT> fn, Collector<OUT> outCollector, RuntimeContext runContext) {
			fn.super();
			this.outCollector = outCollector;
			this.runContext = runContext;
		}

		@Override
		public IN element() {
			return this.inValue;
		}

		@Override
		public DoFn.KeyedState keyedState() {
			throw new UnsupportedOperationException("Getting the keyed state is not supported!");
		}

		@Override
		public Instant timestamp() {
			return Instant.now();
		}

		@Override
		public Collection<? extends BoundedWindow> windows() {
			return ImmutableList.of();
		}

		@Override
		public PipelineOptions getPipelineOptions() {
			throw new UnsupportedOperationException("PipelineOptions are not yet supported!");
		}

		@Override
		public <T> T sideInput(PCollectionView<T, ?> view) {
			List<T> sideInput = runContext.getBroadcastVariable(view.getTagInternal().getId());
			List<WindowedValue<?>> windowedValueList = new ArrayList<>(sideInput.size());
			for (T input : sideInput) {
				windowedValueList.add(WindowedValue.of(input, Instant.now(), ImmutableList.of(GlobalWindow.INSTANCE)));
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
			throw new UnsupportedOperationException("Side outputs are not supported!");
		}

		@Override
		public <T> void sideOutputWithTimestamp(TupleTag<T> tag, T output, Instant timestamp) {
			sideOutput(tag, output);
		}

		@Override
		public <AI, AA, AO> Aggregator<AI> createAggregator(String name, Combine.CombineFn<? super AI, AA, AO> combiner) {
			CombineFnAggregatorWrapper<AI, AA, AO> wrapper = new CombineFnAggregatorWrapper<AI, AA, AO>(combiner);
			getRuntimeContext().addAccumulator(name, wrapper);
			return wrapper;
		}

		@Override
		public <AI, AO> Aggregator<AI> createAggregator(String name, SerializableFunction<Iterable<AI>, AO> serializableFunction) {
			SerializableFnAggregatorWrapper<AI, AO> wrapper = new SerializableFnAggregatorWrapper<AI, AO>(serializableFunction);
			getRuntimeContext().addAccumulator(name, wrapper);
			return wrapper;
		}
	}
}
