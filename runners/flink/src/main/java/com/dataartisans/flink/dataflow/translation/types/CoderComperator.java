package com.dataartisans.flink.dataflow.translation.types;

import com.google.cloud.dataflow.sdk.coders.Coder;
import org.apache.flink.api.common.typeutils.TypeComparator;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.core.memory.MemorySegment;

import java.io.IOException;
import java.io.ObjectInputStream;

/**
 * Flink {@link org.apache.flink.api.common.typeutils.TypeComparator} for
 * {@link com.google.cloud.dataflow.sdk.coders.Coder}.
 */
public class CoderComperator<T> extends TypeComparator<T> {

	private T reference = null;
	private Coder<T> coder;

	// We use these for internal encoding/decoding for creating copies and comparing
	// serialized forms using a Coder
	private transient InspectableByteArrayOutputStream byteBuffer1;
	private transient InspectableByteArrayOutputStream byteBuffer2;

	public CoderComperator(Coder<T> coder) {
		this.coder = coder;
		byteBuffer1 = new InspectableByteArrayOutputStream();
		byteBuffer2 = new InspectableByteArrayOutputStream();
	}

	private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
		in.defaultReadObject();

		byteBuffer1 = new InspectableByteArrayOutputStream();
		byteBuffer2 = new InspectableByteArrayOutputStream();
	}

	@Override
	public int hash(T record) {
		return record.hashCode();
	}

	@Override
	public void setReference(T toCompare) {
		this.reference = toCompare;
	}

	@Override
	public boolean equalToReference(T candidate) {
		return reference.equals(candidate);
	}

	@Override
	public int compareToReference(TypeComparator<T> other) {
		return compare(this.reference, ((CoderComperator<T>) other).reference);
	}

	@Override
	public int compare(T first, T second) {
		try {
			byteBuffer1.reset();
			byteBuffer2.reset();
			coder.encode(first, byteBuffer1, Coder.Context.OUTER);
			coder.encode(second, byteBuffer2, Coder.Context.OUTER);
			byte[] arr = byteBuffer1.getBuffer();
			byte[] arrOther = byteBuffer2.getBuffer();
			if (byteBuffer1.size() != byteBuffer2.size()) {
				return byteBuffer1.size() - byteBuffer2.size();
			}
			int len = byteBuffer1.size();
			for(int i = 0; i < len; i++ ) {
				if (arr[i] != arrOther[i]) {
					return arr[i] - arrOther[i];
				}
			}
			return 0;
		} catch (IOException e) {
			throw new RuntimeException("Could not compare reference.", e);
		}
	}

	@Override
	public int compareSerialized(DataInputView firstSource, DataInputView secondSource) throws IOException {
		CoderTypeSerializer<T> serializer = new CoderTypeSerializer<T>(coder);
		T first = serializer.deserialize(firstSource);
		T second = serializer.deserialize(secondSource);
		return compare(first, second);
	}

	@Override
	public boolean supportsNormalizedKey() {
		return false;
	}

	@Override
	public boolean supportsSerializationWithKeyNormalization() {
		return false;
	}

	@Override
	public int getNormalizeKeyLen() {
		return 0;
	}

	@Override
	public boolean isNormalizedKeyPrefixOnly(int keyBytes) {
		return false;
	}

	@Override
	public void putNormalizedKey(T record, MemorySegment target, int offset, int numBytes) {

	}

	@Override
	public void writeWithKeyNormalization(T record, DataOutputView target) throws IOException {

	}

	@Override
	public T readWithKeyDenormalization(T reuse, DataInputView source) throws IOException {
		return null;
	}

	@Override
	public boolean invertNormalizedKey() {
		return false;
	}

	@Override
	public TypeComparator<T> duplicate() {
		return new CoderComperator<>(coder);
	}

	@Override
	public int extractKeys(Object record, Object[] target, int index) {
		target[index] = record;
		return 1;
	}

	@Override
	public TypeComparator[] getFlatComparators() {
		return new TypeComparator[] { this.duplicate() };
	}
}
