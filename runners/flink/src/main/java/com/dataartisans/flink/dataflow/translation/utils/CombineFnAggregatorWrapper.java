package com.dataartisans.flink.dataflow.translation.utils;

import com.google.cloud.dataflow.sdk.transforms.Aggregator;
import com.google.cloud.dataflow.sdk.transforms.Combine;
import com.google.cloud.dataflow.sdk.util.SerializableUtils;
import com.google.common.collect.Lists;
import org.apache.flink.api.common.accumulators.Accumulator;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

import java.io.IOException;
import java.io.Serializable;


public class CombineFnAggregatorWrapper<AI,AA,AR> implements Aggregator<AI>, Accumulator<AI, AR> {
	
	private AA aa;
	private Combine.CombineFn<? super AI,AA,AR> combiner;

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
	public AR getLocalValue() {
		return combiner.extractOutput(aa);
	}

	@Override
	public void resetLocal() {
		aa = combiner.createAccumulator();
	}

	@Override
	@SuppressWarnings("unchecked")
	public void merge(Accumulator<AI, AR> other) {
		aa = combiner.mergeAccumulators(Lists.newArrayList(aa, ((CombineFnAggregatorWrapper<AI, AA, AR>)other).aa));
	}

	@Override
	public void addValue(AI value) {
		add(value);
	}

	@Override
	public void write(DataOutputView out) throws IOException {
		byte[] aaByte = SerializableUtils.serializeToByteArray((Serializable) aa);
		byte[] combinerByte = SerializableUtils.serializeToByteArray(combiner);
		out.write(aaByte.length);
		out.write(aaByte);
		out.write(combinerByte.length);
		out.write(combinerByte);
	}

	@Override
	@SuppressWarnings("unchecked")
	public void read(DataInputView in) throws IOException {
		byte[] aaByte = new byte[in.readInt()];
		in.read(aaByte);
		byte[] combinerByte = new byte[in.readInt()];
		in.read(combinerByte);
		this.aa = (AA) SerializableUtils.deserializeFromByteArray(aaByte, "AggreatorValue");
		this.combiner = (Combine.CombineFn<AI,AA,AR>) SerializableUtils.deserializeFromByteArray(combinerByte, "AggreatorCombiner");
	}
}
