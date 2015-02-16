package com.dataartisans.flink.dataflow.translation;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;

import java.util.List;

/**
 * Encapsulates a DoFn inside a Flink MapPartitionFunction
 */
public class FlinkCreateFlatMap<IN, OUT> implements FlatMapFunction<IN, OUT> {

	private List<OUT> actualElements;

	public FlinkCreateFlatMap(List<OUT> actualElements) {
		this.actualElements = actualElements;
	}

	@Override
	public void flatMap(IN value, Collector<OUT> out) throws Exception {
		for (OUT actualValue : actualElements) {
			out.collect(actualValue);
		}
	}
}
