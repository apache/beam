/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.beam.runners.flink.translation.types;

import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.KV;

import com.google.common.base.Preconditions;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.CompositeType;
import org.apache.flink.api.common.typeutils.TypeComparator;
import org.apache.flink.api.common.typeutils.TypeSerializer;

import java.util.List;

/**
 * Flink {@link TypeInformation} for {@link KvCoder}. This creates special comparator
 * for {@link KV} that always compares on the key only.
 */
public class KvCoderTypeInformation<K, V> extends CompositeType<WindowedValue<KV<K, V>>> {

  private final WindowedValue.WindowedValueCoder<KV<K, V>> coder;
//  private KvCoder<K, V> coder;

  // We don't have the Class, so we have to pass null here. What a shame...
  private static Object dummy = new Object();

  @SuppressWarnings("unchecked")
  public KvCoderTypeInformation(WindowedValue.WindowedValueCoder<KV<K, V>> coder) {
    super((Class) dummy.getClass());
    this.coder = coder;
    Preconditions.checkNotNull(coder);
  }

  @Override
  @SuppressWarnings("unchecked")
  public TypeComparator<WindowedValue<KV<K, V>>> createComparator(
      int[] logicalKeyFields,
      boolean[] orders,
      int logicalFieldOffset,
      ExecutionConfig config) {
    return new KvCoderComperator(coder);
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
  public Class<WindowedValue<KV<K, V>>> getTypeClass() {
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
  public TypeSerializer<WindowedValue<KV<K, V>>> createSerializer(ExecutionConfig config) {
    return new CoderTypeSerializer<>(coder);
  }

  @Override
  public int getTotalFields() {
    return 2;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

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
  @SuppressWarnings("unchecked")
  public <X> TypeInformation<X> getTypeAt(int pos) {
    KvCoder<K, V> kvCoder = (KvCoder<K, V>) coder.getValueCoder();
    if (pos == 0) {
      return (TypeInformation<X>) new CoderTypeInformation<>(kvCoder.getKeyCoder());
    } else if (pos == 1) {
      return (TypeInformation<X>) new CoderTypeInformation<>(kvCoder.getValueCoder());
    } else {
      throw new RuntimeException("Invalid field position " + pos);
    }
  }

  @Override
  @SuppressWarnings("unchecked")
  public <X> TypeInformation<X> getTypeAt(String fieldExpression) {
    KvCoder<K, V> kvCoder = (KvCoder<K, V>) coder.getValueCoder();
    switch (fieldExpression) {
      case "key":
        return (TypeInformation<X>) new CoderTypeInformation<>(kvCoder.getKeyCoder());
      case "value":
        return (TypeInformation<X>) new CoderTypeInformation<>(kvCoder.getValueCoder());
      default:
        throw new UnsupportedOperationException("Only KvCoder has fields.");
    }
  }

  @Override
  public String[] getFieldNames() {
    return new String[]{"key", "value"};
  }

  @Override
  public int getFieldIndex(String fieldName) {
    switch (fieldName) {
      case "key":
        return 0;
      case "value":
        return 1;
      default:
        return -1;
    }
  }

  @Override
  public void getFlatFields(
      String fieldExpression,
      int offset,
      List<FlatFieldDescriptor> result) {
    KvCoder<K, V> kvCoder = (KvCoder<K, V>) coder.getValueCoder();

    CoderTypeInformation keyTypeInfo =
        new CoderTypeInformation<>(kvCoder.getKeyCoder());
      result.add(new FlatFieldDescriptor(0, keyTypeInfo));
  }

  @Override
  protected TypeComparatorBuilder<WindowedValue<KV<K, V>>> createTypeComparatorBuilder() {
    return new KvCoderTypeComparatorBuilder();
  }

  private class KvCoderTypeComparatorBuilder
      implements TypeComparatorBuilder<WindowedValue<KV<K, V>>> {

    @Override
    public void initializeTypeComparatorBuilder(int size) {}

    @Override
    public void addComparatorField(int fieldId, TypeComparator<?> comparator) {}

    @Override
    public TypeComparator<WindowedValue<KV<K, V>>> createTypeComparator(ExecutionConfig config) {
      return new KvCoderComperator<>(coder);
    }
  }
}
