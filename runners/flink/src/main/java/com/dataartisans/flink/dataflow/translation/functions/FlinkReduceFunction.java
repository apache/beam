package com.dataartisans.flink.dataflow.translation.functions;

import com.google.cloud.dataflow.sdk.transforms.Combine;
import com.google.cloud.dataflow.sdk.values.KV;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.util.Collector;

import java.util.Iterator;

/**
 * Flink {@link org.apache.flink.api.common.functions.GroupReduceFunction} for executing a
 * {@link com.google.cloud.dataflow.sdk.transforms.GroupByKey} operation. This reads the input
 * {@link com.google.cloud.dataflow.sdk.values.KV} elements, extracts the key and collects
 * the values in a {@code List}.
 */
public class FlinkReduceFunction<K, VI, VO> implements GroupReduceFunction<KV<K, VI>, KV<K, VO>> {

	private final Combine.KeyedCombineFn<? super K, ? super VI, Object, VO> keyedCombineFn;

	public FlinkReduceFunction(Combine.KeyedCombineFn<? super K, ? super VI, ?, VO> keyedCombineFn) {
		this.keyedCombineFn = (Combine.KeyedCombineFn<? super K, ? super VI, Object, VO>) keyedCombineFn;
	}

	@Override
	public void reduce(Iterable<KV<K, VI>> values, Collector<KV<K, VO>> out) throws Exception {
		Iterator<KV<K, VI>> it = values.iterator();
		KV<K, VI> current = it.next();
		Object accumulator = keyedCombineFn.createAccumulator(current.getKey());
		keyedCombineFn.addInput(current.getKey(), accumulator, current.getValue());
		while (it.hasNext()) {
			current = it.next();
			keyedCombineFn.addInput(current.getKey(), accumulator, current.getValue());
		}
		out.collect(KV.of(current.getKey(), keyedCombineFn.extractOutput(current.getKey(),
				accumulator)));
	}
}
