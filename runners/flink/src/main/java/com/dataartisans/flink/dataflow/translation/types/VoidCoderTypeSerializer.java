package com.dataartisans.flink.dataflow.translation.types;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

import java.io.IOException;

/**
 * Special Flink {@link org.apache.flink.api.common.typeutils.TypeSerializer} for
 * {@link com.google.cloud.dataflow.sdk.coders.VoidCoder}. We need this because Flink does not
 * allow returning {@code null} from an input reader. We return a {@link VoidValue} instead
 * that behaves like a {@code null}, hopefully.
 */
public class VoidCoderTypeSerializer extends TypeSerializer<VoidCoderTypeSerializer.VoidValue> {

	@Override
	public boolean isImmutableType() {
		return false;
	}

	@Override
	public boolean isStateful() {
		return false;
	}

	@Override
	public VoidValue createInstance() {
		return VoidValue.INSTANCE;
	}

	@Override
	public VoidValue copy(VoidValue from) {
		return from;
	}

	@Override
	public VoidValue copy(VoidValue from, VoidValue reuse) {
		return from;
	}

	@Override
	public int getLength() {
		return 0;
	}

	@Override
	public void serialize(VoidValue record, DataOutputView target) throws IOException {
		target.writeByte(1);
	}

	@Override
	public VoidValue deserialize(DataInputView source) throws IOException {
		source.readByte();
		return VoidValue.INSTANCE;
	}

	@Override
	public VoidValue deserialize(VoidValue reuse, DataInputView source) throws IOException {
		return deserialize(source);
	}

	@Override
	public void copy(DataInputView source, DataOutputView target) throws IOException {
		source.readByte();
		target.writeByte(1);
	}

	public static class VoidValue {
		private VoidValue() {}
		
		public static VoidValue INSTANCE = new VoidValue();
	}


}
