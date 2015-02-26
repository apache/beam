package com.dataartisans.flink.dataflow.translation.types;

import com.google.cloud.dataflow.sdk.coders.Coder;
import com.google.cloud.dataflow.sdk.coders.KvCoder;
import com.google.cloud.dataflow.sdk.coders.VoidCoder;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.AtomicType;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.CompositeType;
import org.apache.flink.api.common.typeutils.TypeComparator;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.shaded.com.google.common.base.Preconditions;

import java.util.List;

/**
 * Flink {@link org.apache.flink.api.common.typeinfo.TypeInformation} for
 * Dataflow {@link com.google.cloud.dataflow.sdk.coders.Coder}s.
 */
public class CoderTypeInformation<T> extends TypeInformation<T> implements AtomicType<T> {

	private Coder<T> coder;

	@SuppressWarnings("unchecked")
	public CoderTypeInformation(Coder<T> coder) {
		this.coder = coder;
		Preconditions.checkNotNull(coder);
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
	public String toString() {
		return "CoderTypeInformation{" +
				"coder=" + coder +
				'}';
	}

	@Override
	public TypeComparator<T> createComparator(boolean sortOrderAscending, ExecutionConfig
			executionConfig) {
		return new CoderComperator<>(coder);
	}
}
