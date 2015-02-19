package com.dataartisans.flink.dataflow.translation.functions;

import com.google.cloud.dataflow.sdk.values.KV;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;

/**
 * Flink {@link org.apache.flink.api.common.functions.GroupReduceFunction} for executing a
 * {@link com.google.cloud.dataflow.sdk.transforms.GroupByKey} operation. This reads the input
 * {@link com.google.cloud.dataflow.sdk.values.KV} elements, extracts the key and collects
 * the values in a {@code List}.
 */
public class FlinkKeyedListAggregationFunction<K,V> implements GroupReduceFunction<KV<K, V>, KV<K, Iterable<V>>> {

	@Override
	public void reduce(Iterable<KV<K, V>> values, Collector<KV<K, Iterable<V>>> out) throws Exception {
		K k = null;
		List<V> result = new ArrayList<V>();
		for (KV<K, V> kv : values) {
			k = kv.getKey();
			result.add(kv.getValue());
		}
		out.collect(KV.of(k, (Iterable<V>) result));
	}

}
