package com.dataartisans.flink.dataflow.translation.types;

import com.google.cloud.dataflow.sdk.coders.Coder;
import com.google.cloud.dataflow.sdk.coders.KvCoder;
import com.google.cloud.dataflow.sdk.values.KV;
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
public class KvCoderComperator <K,V> extends TypeComparator<KV<K,V>> {
	
	private KV<K,V> reference = null;
	private KvCoder<K,V> coder;

	public KvCoderComperator(KvCoder<K,V> coder) {
		this.coder = coder;
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
		return compare(this.reference, ((KvCoderComperator<K,V>) other).reference);
	}

	@Override
	public int compare(KV<K, V> first, KV<K, V> second) {
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		ByteArrayOutputStream baosOther = new ByteArrayOutputStream();
		try {
			coder.getKeyCoder().encode(first.getKey(), baos, Coder.Context.OUTER);
			coder.getKeyCoder().encode(second.getKey(), baosOther, Coder.Context.OUTER);
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
		CoderTypeSerializer<KV<K, V>> serializer = new CoderTypeSerializer<KV<K, V>>(coder);
		KV<K, V> first = serializer.deserialize(firstSource);
		KV<K, V> second = serializer.deserialize(secondSource);
		//K keyFirst = first.getKey();
		//K keySecond = first.getKey();
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
		return 0;
	}

	@Override
	public TypeComparator[] getFlatComparators() {
		return new TypeComparator[0];
	}
}
