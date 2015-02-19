package com.dataartisans.flink.dataflow.translation.functions;

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
	public void flatMap(IN value, Collector<OUT> out) throws Exception {
		for (byte[] element : elements) {
			ByteArrayInputStream bai = new ByteArrayInputStream(element);
			out.collect(coder.decode(bai, Coder.Context.OUTER));
		}
	}
}
