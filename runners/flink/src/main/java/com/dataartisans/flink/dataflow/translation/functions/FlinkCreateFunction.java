package com.dataartisans.flink.dataflow.translation.functions;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;

import java.util.List;

/**
 * This is a hack for transforming a {@link com.google.cloud.dataflow.sdk.transforms.Create}
 * operation. Flink does not allow {@code null} in it's equivalent operation:
 * {@link org.apache.flink.api.java.ExecutionEnvironment#fromElements(Object[])}. Therefore
 * we use a DataSource with one dummy element and output the elements of the Create operation
 * inside this FlatMap.
 */
public class FlinkCreateFunction<IN, OUT> implements FlatMapFunction<IN, OUT> {

	private List<OUT> elements;

	public FlinkCreateFunction(List<OUT> elements) {
		this.elements = elements;
	}

	@Override
	public void flatMap(IN value, Collector<OUT> out) throws Exception {
		for (OUT actualValue : elements) {
			out.collect(actualValue);
		}
	}
}
