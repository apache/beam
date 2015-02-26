package com.dataartisans.flink.dataflow.translation.functions;

import com.google.cloud.dataflow.sdk.transforms.Combine;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.common.collect.ImmutableList;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.util.Collector;

import java.util.Iterator;

/**
 * Flink {@link org.apache.flink.api.common.functions.GroupReduceFunction} for executing a
 * {@link com.google.cloud.dataflow.sdk.transforms.Combine.PerKey} operation. This reads the input
 * {@link com.google.cloud.dataflow.sdk.values.KV} elements, extracts the key and merges the
 * accumulators resulting from the PartialReduce in the MapPartition operation.
 */
public class FlinkReduceFunction<K, VI, VA, VO> implements GroupReduceFunction<KV<K, VA>, KV<K, VO>> {

	private final Combine.KeyedCombineFn<K, VI, VA, VO> keyedCombineFn;

	public FlinkReduceFunction(Combine.KeyedCombineFn<K, VI, VA, VO> keyedCombineFn) {
		this.keyedCombineFn = keyedCombineFn;
	}

	@Override
	public void reduce(Iterable<KV<K, VA>> values, Collector<KV<K, VO>> out) throws Exception {
		Iterator<KV<K, VA>> it = values.iterator();
		KV<K, VA> current = it.next();
		K k = current.getKey();
		VA accumulator = current.getValue();
		while (it.hasNext()) {
			current = it.next();
			keyedCombineFn.mergeAccumulators(k, ImmutableList.of(accumulator, current.getValue()) );
		}
		out.collect(KV.of(k, keyedCombineFn.extractOutput(k,
				accumulator)));
	}
}
