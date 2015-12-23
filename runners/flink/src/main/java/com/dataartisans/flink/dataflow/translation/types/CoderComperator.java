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

	private Coder<T> coder;

	// We use these for internal encoding/decoding for creating copies and comparing
	// serialized forms using a Coder
	private transient InspectableByteArrayOutputStream buffer1;
	private transient InspectableByteArrayOutputStream buffer2;

	// For storing the Reference in encoded form
	private transient InspectableByteArrayOutputStream referenceBuffer;

	public CoderComperator(Coder<T> coder) {
		this.coder = coder;
		buffer1 = new InspectableByteArrayOutputStream();
		buffer2 = new InspectableByteArrayOutputStream();
		referenceBuffer = new InspectableByteArrayOutputStream();
	}

	private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
		in.defaultReadObject();

		buffer1 = new InspectableByteArrayOutputStream();
		buffer2 = new InspectableByteArrayOutputStream();
		referenceBuffer = new InspectableByteArrayOutputStream();

	}

	@Override
	public int hash(T record) {
		return record.hashCode();
	}

	@Override
	public void setReference(T toCompare) {
		referenceBuffer.reset();
		try {
			coder.encode(toCompare, referenceBuffer, Coder.Context.OUTER);
		} catch (IOException e) {
			throw new RuntimeException("Could not set reference " + toCompare + ": " + e);
		}
	}

	@Override
	public boolean equalToReference(T candidate) {
		try {
			buffer2.reset();
			coder.encode(candidate, buffer2, Coder.Context.OUTER);
			byte[] arr = referenceBuffer.getBuffer();
			byte[] arrOther = buffer2.getBuffer();
			if (referenceBuffer.size() != buffer2.size()) {
				return false;
			}
			int len = buffer2.size();
			for(int i = 0; i < len; i++ ) {
				if (arr[i] != arrOther[i]) {
					return false;
				}
			}
			return true;
		} catch (IOException e) {
			throw new RuntimeException("Could not compare reference.", e);
		}
	}

	@Override
	public int compareToReference(TypeComparator<T> other) {
		InspectableByteArrayOutputStream otherReferenceBuffer = ((CoderComperator<T>) other).referenceBuffer;

		byte[] arr = referenceBuffer.getBuffer();
		byte[] arrOther = otherReferenceBuffer.getBuffer();
		if (referenceBuffer.size() != otherReferenceBuffer.size()) {
			return referenceBuffer.size() - otherReferenceBuffer.size();
		}
		int len = referenceBuffer.size();
		for (int i = 0; i < len; i++) {
			if (arr[i] != arrOther[i]) {
				return arr[i] - arrOther[i];
			}
		}
		return 0;
	}

	@Override
	public int compare(T first, T second) {
		try {
			buffer1.reset();
			buffer2.reset();
			coder.encode(first, buffer1, Coder.Context.OUTER);
			coder.encode(second, buffer2, Coder.Context.OUTER);
			byte[] arr = buffer1.getBuffer();
			byte[] arrOther = buffer2.getBuffer();
			if (buffer1.size() != buffer2.size()) {
				return buffer1.size() - buffer2.size();
			}
			int len = buffer1.size();
			for(int i = 0; i < len; i++ ) {
				if (arr[i] != arrOther[i]) {
					return arr[i] - arrOther[i];
				}
			}
			return 0;
		} catch (IOException e) {
			throw new RuntimeException("Could not compare: ", e);
		}
	}

	@Override
	public int compareSerialized(DataInputView firstSource, DataInputView secondSource) throws IOException {
		CoderTypeSerializer<T> serializer = new CoderTypeSerializer<>(coder);
		T first = serializer.deserialize(firstSource);
		T second = serializer.deserialize(secondSource);
		return compare(first, second);
	}

	@Override
	public boolean supportsNormalizedKey() {
		return true;
	}

	@Override
	public boolean supportsSerializationWithKeyNormalization() {
		return false;
	}

	@Override
	public int getNormalizeKeyLen() {
		return Integer.MAX_VALUE;
	}

	@Override
	public boolean isNormalizedKeyPrefixOnly(int keyBytes) {
		return true;
	}

	@Override
	public void putNormalizedKey(T record, MemorySegment target, int offset, int numBytes) {
		buffer1.reset();
		try {
			coder.encode(record, buffer1, Coder.Context.OUTER);
		} catch (IOException e) {
			throw new RuntimeException("Could not serializer " + record + " using coder " + coder + ": " + e);
		}
		final byte[] data = buffer1.getBuffer();
		final int limit = offset + numBytes;

		target.put(offset, data, 0, Math.min(numBytes, buffer1.size()));

		offset += buffer1.size();

		while (offset < limit) {
			target.put(offset++, (byte) 0);
		}
	}

	@Override
	public void writeWithKeyNormalization(T record, DataOutputView target) throws IOException {
		throw new UnsupportedOperationException();
	}

	@Override
	public T readWithKeyDenormalization(T reuse, DataInputView source) throws IOException {
		throw new UnsupportedOperationException();
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
