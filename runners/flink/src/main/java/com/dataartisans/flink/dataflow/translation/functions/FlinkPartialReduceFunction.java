package com.dataartisans.flink.dataflow.translation.functions;

import com.google.cloud.dataflow.sdk.transforms.Combine;
import com.google.cloud.dataflow.sdk.values.KV;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.MapPartitionFunction;
import org.apache.flink.util.Collector;

import java.util.Iterator;

/**
 * Flink {@link org.apache.flink.api.common.functions.GroupReduceFunction} for executing a
 * {@link com.google.cloud.dataflow.sdk.transforms.GroupByKey} operation. This reads the input
 * {@link com.google.cloud.dataflow.sdk.values.KV} elements, extracts the key and collects
 * the values in a {@code List}.
 */
public class FlinkPartialReduceFunction<K, VI, VA, VO> implements MapPartitionFunction<KV<K, VI>, KV<K, VA>> {

	private final Combine.KeyedCombineFn<K, VI, VA, VO> keyedCombineFn;

	public FlinkPartialReduceFunction(Combine.KeyedCombineFn<K, VI, VA, VO>
			                                  keyedCombineFn) {
		this.keyedCombineFn = keyedCombineFn;
	}

	@Override
	public void mapPartition(Iterable<KV<K, VI>> values, Collector<KV<K, VA>> out) throws
			Exception {
		Iterator<KV<K, VI>> it = values.iterator();
		KV<K, VI> current = it.next();
		K currentKey = current.getKey();
		VA accumulator = keyedCombineFn.createAccumulator(currentKey);
		keyedCombineFn.addInput(currentKey, accumulator, current.getValue());

		while (it.hasNext()) {
			current = it.next();
			if (current.getKey().equals(currentKey)) {
				keyedCombineFn.addInput(currentKey, accumulator, current.getValue());
			} else {
				// output current accumulation value, start new accumulation
				out.collect(KV.of(currentKey, accumulator));
				currentKey = current.getKey();
				accumulator = keyedCombineFn.createAccumulator(currentKey);
			}
		}

		// also emit last accumulation
		out.collect(KV.of(currentKey, accumulator));
	}
}
