package com.dataartisans.flink.dataflow.translation.utils;

import com.google.cloud.dataflow.sdk.transforms.Aggregator;
import com.google.cloud.dataflow.sdk.transforms.SerializableFunction;
import com.google.cloud.dataflow.sdk.util.SerializableUtils;
import com.google.common.collect.ImmutableList;
import org.apache.flink.api.common.accumulators.Accumulator;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

import java.io.IOException;
import java.io.Serializable;


public class SerializableFnAggregatorWrapper<AI, AO> implements Aggregator<AI>, Accumulator<AI, AO> {

	private AO result;
	private SerializableFunction<Iterable<AI>, AO> serFun;

	public SerializableFnAggregatorWrapper() {
	}

	public SerializableFnAggregatorWrapper(SerializableFunction<Iterable<AI>, AO> serFun) {
		this.serFun = serFun;
		resetLocal();
	}
	
	@Override
	@SuppressWarnings("unchecked")
	public void add(AI value) {
		this.result = serFun.apply(ImmutableList.of((AI) result, value));
	}

	@Override
	public AO getLocalValue() {
		return result;
	}

	@Override
	public void resetLocal() {
		this.result = serFun.apply(ImmutableList.<AI>of());
	}

	@Override
	@SuppressWarnings("unchecked")
	public void merge(Accumulator<AI, AO> other) {
		this.result = serFun.apply(ImmutableList.of((AI) result, (AI) other.getLocalValue()));
	}

	@Override
	public void addValue(AI value) {
		add(value);
	}

	@Override
	public void write(DataOutputView out) throws IOException {
		byte[] aaByte = SerializableUtils.serializeToByteArray((Serializable) result);
		byte[] combinerByte = SerializableUtils.serializeToByteArray(serFun);
		out.writeInt(aaByte.length);
		out.write(aaByte);
		out.writeInt(combinerByte.length);
		out.write(combinerByte);
	}

	@Override
	@SuppressWarnings("unchecked")
	public void read(DataInputView in) throws IOException {
		byte[] aaByte = new byte[in.readInt()];
		in.read(aaByte);
		byte[] serFunByte = new byte[in.readInt()];
		in.read(serFunByte);
		this.result = (AO) SerializableUtils.deserializeFromByteArray(aaByte, "AggreatorValue");
		this.serFun = (SerializableFunction<Iterable<AI>, AO>) SerializableUtils.deserializeFromByteArray(serFunByte, "AggreatorSerializableFunction");

	}
}
