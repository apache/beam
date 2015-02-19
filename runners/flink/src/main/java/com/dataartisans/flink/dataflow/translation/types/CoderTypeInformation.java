package com.dataartisans.flink.dataflow.translation.types;

import com.google.api.client.repackaged.com.google.common.base.Preconditions;
import com.google.cloud.dataflow.sdk.coders.Coder;
import com.google.cloud.dataflow.sdk.coders.KvCoder;
import com.google.cloud.dataflow.sdk.coders.VoidCoder;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.CompositeType;
import org.apache.flink.api.common.typeutils.TypeComparator;
import org.apache.flink.api.common.typeutils.TypeSerializer;

import java.util.List;

/**
 * Flink {@link org.apache.flink.api.common.typeinfo.TypeInformation} for
 * Dataflow {@link com.google.cloud.dataflow.sdk.coders.Coder}s
 */
public class CoderTypeInformation<T> extends CompositeType {

	private Coder<T> coder;

	@SuppressWarnings("unchecked")
	public CoderTypeInformation(Coder<T> coder) {
		// We don't have the Class, so we have to pass null here. What a shame...
		super(null);
		this.coder = coder;
		Preconditions.checkNotNull(coder);
	}

	@SuppressWarnings("unchecked")
	public TypeComparator<T> createComparator(int[] logicalKeyFields, boolean[] orders, int logicalFieldOffset) {
		if (!(coder instanceof KvCoder)) {
			throw new RuntimeException("Coder " + coder + " is not a KvCoder.");
		}
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
		return 0;
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
	public TypeSerializer<T> createSerializer() {
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
	@SuppressWarnings("unchecked")
	public void getKey(String fieldExpression, int offset, List result) {
		result.add(new FlatFieldDescriptor(0, BasicTypeInfo.INT_TYPE_INFO));
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

	// These methods we only have because we need to fulfill CompositeTypeInfo requirements.
	@Override
	public TypeInformation<?> getTypeAt(int pos) {
		return null;
	}

	@Override
	protected void initializeNewComparator(int localKeyCount) { }

	@Override
	protected TypeComparator getNewComparator() {
		return null;
	}

	@Override
	protected void addCompareField(int fieldId, TypeComparator comparator) { }
}
