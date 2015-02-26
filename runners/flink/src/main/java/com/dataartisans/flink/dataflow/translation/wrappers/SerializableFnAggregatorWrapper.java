package com.dataartisans.flink.dataflow.translation.wrappers;

import com.google.cloud.dataflow.sdk.transforms.Aggregator;
import com.google.cloud.dataflow.sdk.transforms.SerializableFunction;
import com.google.cloud.dataflow.sdk.util.SerializableUtils;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.apache.flink.api.common.accumulators.Accumulator;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;

/**
 * Wrapper that wraps a {@link com.google.cloud.dataflow.sdk.transforms.SerializableFunction}
 * in a Flink {@link org.apache.flink.api.common.accumulators.Accumulator} for using
 * the function as an aggregator in a {@link com.google.cloud.dataflow.sdk.transforms.ParDo}
 * operation.
 */
public class SerializableFnAggregatorWrapper<AI, AO> implements Aggregator<AI>, Accumulator<AI, Serializable> {

	private AO aa;
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
		this.aa = serFun.apply(ImmutableList.of((AI) aa, value));
	}

	@Override
	public Serializable getLocalValue() {
		return (Serializable) aa;
	}

	@Override
	public void resetLocal() {
		this.aa = serFun.apply(ImmutableList.<AI>of());
	}

	@Override
	@SuppressWarnings("unchecked")
	public void merge(Accumulator<AI, Serializable> other) {
		this.aa = serFun.apply(ImmutableList.of((AI) aa, (AI) other.getLocalValue()));
	}

	@Override
	public void addValue(AI value) {
		add(value);
	}

	@Override
	public void write(ObjectOutputStream out) throws IOException {
		byte[] aaByte = SerializableUtils.serializeToByteArray((Serializable) aa);
		byte[] combinerByte = SerializableUtils.serializeToByteArray(serFun);
		out.writeInt(aaByte.length);
		out.write(aaByte);
		out.writeInt(combinerByte.length);
		out.write(combinerByte);
	}

	@Override
	@SuppressWarnings("unchecked")
	public void read(ObjectInputStream in) throws IOException {
		byte[] aaByte = new byte[in.readInt()];
		in.read(aaByte);
		byte[] serFunByte = new byte[in.readInt()];
		in.read(serFunByte);
		this.aa = (AO) SerializableUtils.deserializeFromByteArray(aaByte, "AggreatorValue");
		this.serFun = (SerializableFunction<Iterable<AI>, AO>) SerializableUtils.deserializeFromByteArray(serFunByte, "AggreatorSerializableFunction");

	}

	@Override
	public Accumulator<AI, Serializable> clone() {
		// copy it by merging
		AO resultCopy = serFun.apply(Lists.newArrayList((AI) aa));
		SerializableFnAggregatorWrapper<AI, AO> result = new
				SerializableFnAggregatorWrapper<>(serFun);

		result.aa = resultCopy;
		return result;
	}
}
