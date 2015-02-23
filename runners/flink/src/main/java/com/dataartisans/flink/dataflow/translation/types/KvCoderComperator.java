package com.dataartisans.flink.dataflow.translation.types;

import com.dataartisans.flink.dataflow.translation.wrappers.DataInputViewWrapper;
import com.google.cloud.dataflow.sdk.coders.Coder;
import com.google.cloud.dataflow.sdk.coders.KvCoder;
import com.google.cloud.dataflow.sdk.values.KV;
import org.apache.flink.api.common.typeutils.TypeComparator;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.core.memory.MemorySegment;

import java.io.IOException;
import java.io.ObjectInputStream;

/**
 * Flink {@link org.apache.flink.api.common.typeutils.TypeComparator} for
 * {@link com.google.cloud.dataflow.sdk.coders.KvCoder}. We have a special comparator
 * for {@link KV} that always compares on the key only.
 */
public class KvCoderComperator <K, V> extends TypeComparator<KV<K, V>> {
	
	private KV<K, V> reference = null;
	private KvCoder<K, V> coder;

	// We use these for internal encoding/decoding for creating copies and comparing
	// serialized forms using a Coder
	private transient InspectableByteArrayOutputStream byteBuffer1;
	private transient InspectableByteArrayOutputStream byteBuffer2;

	// For deserializing the key
	private transient DataInputViewWrapper inputWrapper;


	public KvCoderComperator(KvCoder<K, V> coder) {
		this.coder = coder;

		byteBuffer1 = new InspectableByteArrayOutputStream();
		byteBuffer2 = new InspectableByteArrayOutputStream();

		inputWrapper = new DataInputViewWrapper(null);
	}

	private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
		in.defaultReadObject();

		byteBuffer1 = new InspectableByteArrayOutputStream();
		byteBuffer2 = new InspectableByteArrayOutputStream();

		inputWrapper = new DataInputViewWrapper(null);
	}

	public KV<K, V> getReference() {
		return reference;
	}

	@Override
	public int hash(KV<K, V> record) {
		K key = record.getKey();
		if (key != null) {
			return key.hashCode();
		} else {
			return 0;
		}
	}

	@Override
	public void setReference(KV<K, V> toCompare) {
		this.reference = toCompare;
	}

	@Override
	public boolean equalToReference(KV<K, V> candidate) {
		K key = reference.getKey();
		K otherKey = candidate.getKey();
		if (key == null && otherKey == null) {
			return true;
		} else if(key == null || otherKey == null) {
			return false;
		} else {
			return key.equals(otherKey);
		}
	}

	@Override
	public int compareToReference(TypeComparator<KV<K, V>> other) {
		return compare(this.reference, ((KvCoderComperator<K, V>) other).reference);
	}

	@Override
	public int compare(KV<K, V> first, KV<K, V> second) {
		try {
			byteBuffer1.reset();
			byteBuffer2.reset();
			coder.getKeyCoder().encode(first.getKey(), byteBuffer1, Coder.Context.OUTER);
			coder.getKeyCoder().encode(second.getKey(), byteBuffer2, Coder.Context.OUTER);
			byte[] arr = byteBuffer1.getBuffer();
			byte[] arrOther = byteBuffer2.getBuffer();
			int len = Math.min(byteBuffer1.size(), byteBuffer2.size());
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
		CoderTypeSerializer<KV<K, V>> serializer = new CoderTypeSerializer<KV<K, V>>(coder);
		inputWrapper.setInputView(firstSource);
		K firstKey = coder.getKeyCoder().decode(inputWrapper, Coder.Context.NESTED);
		inputWrapper.setInputView(secondSource);
		K secondKey = coder.getKeyCoder().decode(inputWrapper, Coder.Context.NESTED);

		try {
			byteBuffer1.reset();
			byteBuffer2.reset();
			coder.getKeyCoder().encode(firstKey, byteBuffer1, Coder.Context.OUTER);
			coder.getKeyCoder().encode(secondKey, byteBuffer2, Coder.Context.OUTER);
			byte[] arr = byteBuffer1.getBuffer();
			byte[] arrOther = byteBuffer2.getBuffer();
			int len = Math.min(byteBuffer1.size(), byteBuffer2.size());
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
	public void putNormalizedKey(KV<K, V> record, MemorySegment target, int offset, int numBytes) {

	}

	@Override
	public void writeWithKeyNormalization(KV<K, V> record, DataOutputView target) throws IOException {

	}

	@Override
	public KV<K, V> readWithKeyDenormalization(KV<K, V> reuse, DataInputView source) throws IOException {
		return null;
	}

	@Override
	public boolean invertNormalizedKey() {
		return false;
	}

	@Override
	public TypeComparator<KV<K, V>> duplicate() {
		return new KvCoderComperator<>(coder);
	}

	@Override
	public int extractKeys(Object record, Object[] target, int index) {
		KV<K, V> kv = (KV<K, V>) record;
		K k = kv.getKey();
		target[index] = k;
		return 1;
	}

	@Override
	public TypeComparator[] getFlatComparators() {
		return new TypeComparator[] { new CoderComperator<>(coder.getKeyCoder())};
	}
}
