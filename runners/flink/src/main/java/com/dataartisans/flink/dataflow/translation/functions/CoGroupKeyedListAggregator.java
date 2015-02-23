package com.dataartisans.flink.dataflow.translation.functions;

import com.google.cloud.dataflow.sdk.transforms.join.CoGbkResult;
import com.google.cloud.dataflow.sdk.values.KV;
import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.util.Collector;


public class CoGroupKeyedListAggregator <K,V1,V2> implements CoGroupFunction<KV<K,V1>, KV<K,V2>, KV<K, CoGbkResult>>{

//	private CoGbkResultSchema schema;
//	private TupleTag<?> tupleTag1;
//	private TupleTag<?> tupleTag2;

//	public CoGroupKeyedListAggregator(CoGbkResultSchema schema, TupleTag<?> tupleTag1, TupleTag<?> tupleTag2) {
//		this.schema = schema;
//		this.tupleTag1 = tupleTag1;
//		this.tupleTag2 = tupleTag2;
//	}

	@Override
	public void coGroup(Iterable<KV<K,V1>> first, Iterable<KV<K,V2>> second, Collector<KV<K, CoGbkResult>> out) throws Exception {
//		K k = null;
//		List<RawUnionValue> result = new ArrayList<>();
//		int index1 = schema.getIndex(tupleTag1);
//		for (KV<K,?> entry : first) {
//			k = entry.getKey();
//			result.add(new RawUnionValue(index1, entry.getValue()));
//		}
//		int index2 = schema.getIndex(tupleTag2);
//		for (KV<K,?> entry : second) {
//			k = entry.getKey();
//			result.add(new RawUnionValue(index2, entry.getValue()));
//		}
//		out.collect(KV.of(k, new CoGbkResult(schema, result)));
	}
}
