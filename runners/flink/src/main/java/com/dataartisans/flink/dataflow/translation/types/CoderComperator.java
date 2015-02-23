package com.dataartisans.flink.dataflow.translation.types;

import com.google.cloud.dataflow.sdk.coders.Coder;
import org.apache.flink.api.common.typeutils.TypeComparator;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.core.memory.MemorySegment;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

/**
 * Flink {@link org.apache.flink.api.common.typeutils.TypeComparator} for
 * {@link com.google.cloud.dataflow.sdk.coders.KvCoder}. We only need a comparator for KV
 * because the Dataflow API only allows comparisons on the key of a
 * {@link com.google.cloud.dataflow.sdk.values.KV}.
 */
public class CoderComperator<T> extends TypeComparator<T> {

	private T reference = null;
	private Coder<T> coder;

	public CoderComperator(Coder<T> coder) {
		this.coder = coder;
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
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		ByteArrayOutputStream baosOther = new ByteArrayOutputStream();
		try {
			coder.encode(first, baos, Coder.Context.OUTER);
			coder.encode(second, baosOther, Coder.Context.OUTER);
			byte[] arr = baos.toByteArray();
			byte[] arrOther = baosOther.toByteArray();
			int len = arr.length < arrOther.length ? arr.length : arrOther.length;
			for(int i = 0; i < len; i++ ) {
				if (arr[i] != arrOther[i]) {
					return arr[i] - arrOther[i];
				}
			}
			return arr.length - arrOther.length;
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
		return 0;
	}

	@Override
	public TypeComparator[] getFlatComparators() {
		return new TypeComparator[0];
	}
}
