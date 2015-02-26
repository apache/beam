package com.dataartisans.flink.dataflow.translation.functions;

import com.google.cloud.dataflow.sdk.values.KV;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Iterator;
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
		Iterator<KV<K, V>> it = values.iterator();
		KV<K, V> first = it.next();
		Iterable<V> passThrough = new PassThroughIterable<>(first, it);
		out.collect(KV.of(first.getKey(), passThrough));
	}

	private static class PassThroughIterable<K, V> implements Iterable<V>, Iterator<V>  {
		private KV<K, V> first;
		private Iterator<KV<K, V>> iterator;

		public PassThroughIterable(KV<K, V> first, Iterator<KV<K, V>> iterator) {
			this.first = first;
			this.iterator = iterator;
		}

		@Override
		public Iterator<V> iterator() {
			return this;
		}

		@Override
		public boolean hasNext() {
			return first != null || iterator.hasNext();
		}

		@Override
		public V next() {
			if (first != null) {
				V result = first.getValue();
				first = null;
				return result;
			} else {
				return iterator.next().getValue();
			}
		}

		@Override
		public void remove() {
			throw new UnsupportedOperationException("Cannot remove elements from input.");
		}
	}
}
