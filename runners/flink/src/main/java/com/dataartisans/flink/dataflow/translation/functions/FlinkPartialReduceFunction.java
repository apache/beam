package com.dataartisans.flink.dataflow.translation.functions;

import com.google.cloud.dataflow.sdk.transforms.Combine;
import com.google.cloud.dataflow.sdk.values.KV;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.util.Collector;

import java.util.Iterator;

/**
 * Flink {@link org.apache.flink.api.common.functions.GroupReduceFunction} for executing a
 * {@link com.google.cloud.dataflow.sdk.transforms.Combine.PerKey} operation. This reads the input
 * {@link com.google.cloud.dataflow.sdk.values.KV} elements VI, extracts the key and emits accumulated
 * values which have the intermediate format VA.
 */
public class FlinkPartialReduceFunction<K, VI, VA> implements GroupReduceFunction<KV<K, VI>, KV<K, VA>> {

	private final Combine.KeyedCombineFn<K, VI, VA, ?> keyedCombineFn;

	public FlinkPartialReduceFunction(Combine.KeyedCombineFn<K, VI, VA, ?>
			                                  keyedCombineFn) {
		this.keyedCombineFn = keyedCombineFn;
	}

	@Override
	public void reduce(Iterable<KV<K, VI>> elements, Collector<KV<K, VA>> out) throws Exception {

		final Iterator<KV<K, VI>> iterator = elements.iterator();
		// create accumulator using the first elements key
		KV<K, VI> first = iterator.next();
		K key = first.getKey();
		VI value = first.getValue();
		VA accumulator = keyedCombineFn.createAccumulator(key);
		// manually add for the first element
		keyedCombineFn.addInput(key, accumulator, value);

		while(iterator.hasNext()) {
			value = iterator.next().getValue();
			keyedCombineFn.addInput(key, accumulator, value);
		}

		out.collect(KV.of(key, accumulator));
	}
}
