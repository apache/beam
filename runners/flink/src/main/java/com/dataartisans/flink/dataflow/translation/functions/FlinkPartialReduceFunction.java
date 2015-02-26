package com.dataartisans.flink.dataflow.translation.functions;

import com.google.cloud.dataflow.sdk.transforms.Combine;
import com.google.cloud.dataflow.sdk.values.KV;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.MapPartitionFunction;
import org.apache.flink.util.Collector;

import java.util.Iterator;

/**
 * Flink {@link org.apache.flink.api.common.functions.MapPartitionFunction} for executing a
 * {@link com.google.cloud.dataflow.sdk.transforms.Combine.PerKey} operation. This reads the input
 * {@link com.google.cloud.dataflow.sdk.values.KV} elements, extracts the key and emits accumulated
 * values.
 *
 * This assumes that the input values are sorted. We basically implement a GroupReduce operation
 * inside a partition here.
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
		if (!it.hasNext()) {
			return;
		}

		KV<K, VI> current = it.next();
		K currentKey = current.getKey();
		VA accumulator = keyedCombineFn.createAccumulator(currentKey);
		keyedCombineFn.addInput(currentKey, accumulator, current.getValue());

		while (it.hasNext()) {
			current = it.next();
			if (currentKey == null && current.getKey() == null || current.getKey().equals(currentKey)) {
				keyedCombineFn.addInput(currentKey, accumulator, current.getValue());
			} else {
				// output current accumulation value, start new accumulation
				out.collect(KV.of(currentKey, accumulator));
				currentKey = current.getKey();
				accumulator = keyedCombineFn.createAccumulator(currentKey);
				keyedCombineFn.addInput(currentKey, accumulator, current.getValue());
			}
		}

		// also emit last accumulation
		out.collect(KV.of(currentKey, accumulator));
	}
}
