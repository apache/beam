/*
 * Copyright 2015 Data Artisans GmbH
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.dataartisans.flink.dataflow.translation.wrappers;

import com.google.cloud.dataflow.sdk.transforms.Aggregator;
import com.google.cloud.dataflow.sdk.transforms.Combine;
import com.google.cloud.dataflow.sdk.util.SerializableUtils;
import com.google.common.collect.Lists;
import org.apache.flink.api.common.accumulators.Accumulator;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;

/**
 * Wrapper that wraps a {@link com.google.cloud.dataflow.sdk.transforms.Combine.CombineFn}
 * in a Flink {@link org.apache.flink.api.common.accumulators.Accumulator} for using
 * the combine function as an aggregator in a {@link com.google.cloud.dataflow.sdk.transforms.ParDo}
 * operation.
 */
public class CombineFnAggregatorWrapper<AI, AA, AR> implements Aggregator<AI, AR>, Accumulator<AI, Serializable> {
	
	private AA aa;
	private Combine.CombineFn<? super AI, AA, AR> combiner;

	public CombineFnAggregatorWrapper() {
	}

	public CombineFnAggregatorWrapper(Combine.CombineFn<? super AI, AA, AR> combiner) {
		this.combiner = combiner;
		this.aa = combiner.createAccumulator();
	}

	@Override
	public void add(AI value) {
		combiner.addInput(aa, value);
	}

	@Override
	public Serializable getLocalValue() {
		return (Serializable) combiner.extractOutput(aa);
	}

	@Override
	public void resetLocal() {
		aa = combiner.createAccumulator();
	}

	@Override
	@SuppressWarnings("unchecked")
	public void merge(Accumulator<AI, Serializable> other) {
		aa = combiner.mergeAccumulators(Lists.newArrayList(aa, ((CombineFnAggregatorWrapper<AI, AA, AR>)other).aa));
	}

	@Override
	public Accumulator<AI, Serializable> clone() {
		// copy it by merging
		AA aaCopy = combiner.mergeAccumulators(Lists.newArrayList(aa));
		CombineFnAggregatorWrapper<AI, AA, AR> result = new
				CombineFnAggregatorWrapper<>(combiner);
		result.aa = aaCopy;
		return result;
	}

	@Override
	public void addValue(AI value) {
		add(value);
	}

	@Override
	public String getName() {
		return "CombineFn: " + combiner.toString();
	}

	@Override
	public Combine.CombineFn getCombineFn() {
		return combiner;
	}

}
