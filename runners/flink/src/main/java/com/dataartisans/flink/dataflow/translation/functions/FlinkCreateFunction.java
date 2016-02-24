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

import com.dataartisans.flink.dataflow.translation.types.VoidCoderTypeSerializer;
import com.google.cloud.dataflow.sdk.coders.Coder;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;

import java.io.ByteArrayInputStream;
import java.util.List;

/**
 * This is a hack for transforming a {@link com.google.cloud.dataflow.sdk.transforms.Create}
 * operation. Flink does not allow {@code null} in it's equivalent operation:
 * {@link org.apache.flink.api.java.ExecutionEnvironment#fromElements(Object[])}. Therefore
 * we use a DataSource with one dummy element and output the elements of the Create operation
 * inside this FlatMap.
 */
public class FlinkCreateFunction<IN, OUT> implements FlatMapFunction<IN, OUT> {

	private final List<byte[]> elements;
	private final Coder<OUT> coder;

	public FlinkCreateFunction(List<byte[]> elements, Coder<OUT> coder) {
		this.elements = elements;
		this.coder = coder;
	}

	@Override
	@SuppressWarnings("unchecked")
	public void flatMap(IN value, Collector<OUT> out) throws Exception {

		for (byte[] element : elements) {
			ByteArrayInputStream bai = new ByteArrayInputStream(element);
			OUT outValue = coder.decode(bai, Coder.Context.OUTER);
			if (outValue == null) {
				// TODO Flink doesn't allow null values in records
				out.collect((OUT) VoidCoderTypeSerializer.VoidValue.INSTANCE);
			} else {
				out.collect(outValue);
			}
		}

		out.close();
	}
}
