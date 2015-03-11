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

import com.google.cloud.dataflow.sdk.transforms.join.CoGbkResult;
import com.google.cloud.dataflow.sdk.transforms.join.CoGbkResultSchema;
import com.google.cloud.dataflow.sdk.transforms.join.RawUnionValue;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.TupleTag;
import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;


public class FlinkCoGroupKeyedListAggregator<K,V1,V2> implements CoGroupFunction<KV<K,V1>, KV<K,V2>, KV<K, CoGbkResult>>{

	private CoGbkResultSchema schema;
	private TupleTag<?> tupleTag1;
	private TupleTag<?> tupleTag2;

	public FlinkCoGroupKeyedListAggregator(CoGbkResultSchema schema, TupleTag<?> tupleTag1, TupleTag<?> tupleTag2) {
		this.schema = schema;
		this.tupleTag1 = tupleTag1;
		this.tupleTag2 = tupleTag2;
	}

	@Override
	public void coGroup(Iterable<KV<K,V1>> first, Iterable<KV<K,V2>> second, Collector<KV<K, CoGbkResult>> out) throws Exception {
		K k = null;
		List<RawUnionValue> result = new ArrayList<>();
		int index1 = schema.getIndex(tupleTag1);
		for (KV<K,?> entry : first) {
			k = entry.getKey();
			result.add(new RawUnionValue(index1, entry.getValue()));
		}
		int index2 = schema.getIndex(tupleTag2);
		for (KV<K,?> entry : second) {
			k = entry.getKey();
			result.add(new RawUnionValue(index2, entry.getValue()));
		}
		out.collect(KV.of(k, new CoGbkResult(schema, (List) result)));
	}
}
