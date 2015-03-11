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
import com.google.cloud.dataflow.sdk.coders.KvCoder;
import com.google.cloud.dataflow.sdk.coders.VoidCoder;
import com.google.cloud.dataflow.sdk.values.KV;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.CompositeType;
import org.apache.flink.api.common.typeutils.TypeComparator;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.shaded.com.google.common.base.Preconditions;

import java.util.List;

/**
 * Flink {@link org.apache.flink.api.common.typeinfo.TypeInformation} for
 * Dataflow {@link com.google.cloud.dataflow.sdk.coders.KvCoder}.
 */
public class KvCoderTypeInformation<K, V> extends CompositeType<KV<K, V>> {

	private KvCoder<K, V> coder;

	@SuppressWarnings("unchecked")
	public KvCoderTypeInformation(KvCoder<K, V> coder) {
		// We don't have the Class, so we have to pass null here. What a shame...
		super(null);
		this.coder = coder;
		Preconditions.checkNotNull(coder);
	}

	@Override
	@SuppressWarnings("unchecked")
	public TypeComparator<KV<K, V>> createComparator(int[] logicalKeyFields, boolean[] orders, int logicalFieldOffset, ExecutionConfig config) {
		return new KvCoderComperator((KvCoder) coder);
	}

	@Override
	public boolean isBasicType() {
		return false;
	}

	@Override
	public boolean isTupleType() {
		return false;
	}

	@Override
	public int getArity() {
		return 2;
	}

	@Override
	@SuppressWarnings("unchecked")
	public Class<KV<K, V>> getTypeClass() {
		return privateGetTypeClass();
	}

	@SuppressWarnings("unchecked")
	private static <X> Class<X> privateGetTypeClass() {
		return (Class<X>) Object.class;
	}

	@Override
	public boolean isKeyType() {
		return true;
	}

	@Override
	@SuppressWarnings("unchecked")
	public TypeSerializer<KV<K, V>> createSerializer(ExecutionConfig config) {
		return new CoderTypeSerializer<>(coder);
	}

	@Override
	public int getTotalFields() {
		return 2;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;

		KvCoderTypeInformation that = (KvCoderTypeInformation) o;

		return coder.equals(that.coder);

	}

	@Override
	public int hashCode() {
		return coder.hashCode();
	}

	@Override
	public String toString() {
		return "CoderTypeInformation{" +
				"coder=" + coder +
				'}';
	}

	@Override
	public <X> TypeInformation<X> getTypeAt(int pos) {
		if (pos == 0) {
			return (TypeInformation<X>) new CoderTypeInformation<>(coder.getKeyCoder());
		} else if (pos == 1) {
			return (TypeInformation<X>) new CoderTypeInformation<>(coder.getValueCoder());
		} else {
			throw new RuntimeException("Invalid field position " + pos);
		}
	}

	@Override
	public <X> TypeInformation<X> getTypeAt(String fieldExpression) {
		if (fieldExpression.equals("key")) {
			return (TypeInformation<X>) new CoderTypeInformation<>(coder.getKeyCoder());
		} else if (fieldExpression.equals("value")) {
			return (TypeInformation<X>) new CoderTypeInformation<>(coder.getValueCoder());
		} else {
			throw new UnsupportedOperationException("Only KvCoder has fields.");
		}
	}

	@Override
	public String[] getFieldNames() {
		return new String[]{"key", "value"};
	}

	@Override
	public int getFieldIndex(String fieldName) {
		if (fieldName.equals("key")) {
			return 0;
		} else if (fieldName.equals("value")) {
			return 1;
		} else {
			return -1;
		}
	}

	// These three we only have because we support CompositeType, we create our own comparator
	// in createComparator.
	@Override
	protected void initializeNewComparator(int localKeyCount) {}
	@Override
	protected void addCompareField(int fieldId, TypeComparator<?> comparator) {}
	@Override
	protected TypeComparator<KV<K, V>> getNewComparator(ExecutionConfig config) { return null; }

	@Override
	public void getFlatFields(String fieldExpression, int offset, List<FlatFieldDescriptor> result) {
			CoderTypeInformation keyTypeInfo = new CoderTypeInformation<>(coder.getKeyCoder());
			result.add(new FlatFieldDescriptor(0, keyTypeInfo));
	}
}
