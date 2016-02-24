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
import com.google.cloud.dataflow.sdk.coders.VoidCoder;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.AtomicType;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeComparator;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import com.google.common.base.Preconditions;

/**
 * Flink {@link org.apache.flink.api.common.typeinfo.TypeInformation} for
 * Dataflow {@link com.google.cloud.dataflow.sdk.coders.Coder}s.
 */
public class CoderTypeInformation<T> extends TypeInformation<T> implements AtomicType<T> {

	private final Coder<T> coder;

	public CoderTypeInformation(Coder<T> coder) {
		Preconditions.checkNotNull(coder);
		this.coder = coder;
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
		return 1;
	}

	@Override
	@SuppressWarnings("unchecked")
	public Class<T> getTypeClass() {
		// We don't have the Class, so we have to pass null here. What a shame...
		return (Class<T>) Object.class;
	}

	@Override
	public boolean isKeyType() {
		return true;
	}

	@Override
	@SuppressWarnings("unchecked")
	public TypeSerializer<T> createSerializer(ExecutionConfig config) {
		if (coder instanceof VoidCoder) {
			return (TypeSerializer<T>) new VoidCoderTypeSerializer();
		}
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

		CoderTypeInformation that = (CoderTypeInformation) o;

		return coder.equals(that.coder);

	}

	@Override
	public int hashCode() {
		return coder.hashCode();
	}

	@Override
	public boolean canEqual(Object obj) {
		return obj instanceof CoderTypeInformation;
	}

	@Override
	public String toString() {
		return "CoderTypeInformation{" +
				"coder=" + coder +
				'}';
	}

	@Override
	public TypeComparator<T> createComparator(boolean sortOrderAscending, ExecutionConfig
			executionConfig) {
		return new CoderComparator<>(coder);
	}
}
