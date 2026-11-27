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
package org.apache.beam.sdk.schemas;

import static org.apache.beam.sdk.util.Preconditions.checkStateNotNull;
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkNotNull;
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkState;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.schemas.Schema.TypeName;
import org.apache.beam.sdk.schemas.logicaltypes.EnumerationType;
import org.apache.beam.sdk.schemas.logicaltypes.OneOfType;
import org.apache.beam.sdk.schemas.utils.ByteBuddyUtils;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.RowWithGetters;
import org.apache.beam.sdk.values.RowWithStorage;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Function;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Collections2;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Iterables;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Lists;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Maps;
import org.checkerframework.checker.nullness.qual.MonotonicNonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

/** Function to convert a {@link Row} to a user type using a creator factory. */
@SuppressWarnings({
  "nullness", // TODO(https://github.com/apache/beam/issues/20497)
  "rawtypes"
})
class FromRowUsingCreator<T> implements SerializableFunction<Row, T>, Function<Row, T> {
  private final TypeDescriptor<T> typeDescriptor;
  private final GetterBasedSchemaProvider schemaProvider;
  private final Factory<SchemaUserTypeCreator> schemaTypeCreatorFactory;

  private static final class ConverterPlan {
    final @Nullable Function[] fieldConverters;
    final boolean hasConversions;

    ConverterPlan(@Nullable Function[] fieldConverters, boolean hasConversions) {
      this.fieldConverters = fieldConverters;
      this.hasConversions = hasConversions;
    }
  }

  @SuppressFBWarnings("SE_TRANSIENT_FIELD_NOT_RESTORED")
  private transient volatile @MonotonicNonNull ConverterPlan converterPlan;

  @SuppressFBWarnings("SE_TRANSIENT_FIELD_NOT_RESTORED")
  private transient volatile @MonotonicNonNull SchemaUserTypeCreator cachedCreator;

  public FromRowUsingCreator(
      TypeDescriptor<T> typeDescriptor, GetterBasedSchemaProvider schemaProvider) {
    this(
        typeDescriptor,
        schemaProvider,
        new CachingFactory<>(schemaProvider::schemaTypeCreator),
        null);
  }

  private FromRowUsingCreator(
      TypeDescriptor<T> typeDescriptor,
      GetterBasedSchemaProvider schemaProvider,
      Factory<SchemaUserTypeCreator> schemaTypeCreatorFactory,
      @Nullable ConverterPlan converterPlan) {
    this.typeDescriptor = typeDescriptor;
    this.schemaProvider = schemaProvider;
    this.schemaTypeCreatorFactory = schemaTypeCreatorFactory;
    this.converterPlan = converterPlan;
  }

  @Override
  @SuppressWarnings({"unchecked", "ReferenceEquality"})
  public T apply(Row row) {
    if (row == null) {
      return null;
    }
    if (row instanceof RowWithGetters) {
      RowWithGetters rowWithGetters = (RowWithGetters) row;
      TypeDescriptor<?> targetType = rowWithGetters.getGetterTargetType();
      if (targetType == typeDescriptor || targetType.equals(typeDescriptor)) {
        // Efficient path: simply extract the underlying object instead of creating a new one.
        return (T) rowWithGetters.getGetterTarget();
      }
    }
    Schema rowSchema = row.getSchema();
    ConverterPlan plan = this.converterPlan;
    if (plan == null) {
      plan = initFieldConverters(rowSchema);
    }
    @Nullable Function[] converters = plan.fieldConverters;
    int fieldCount = row.getFieldCount();
    checkState(converters.length == fieldCount, "Unexpected field count");

    SchemaUserTypeCreator creator = this.cachedCreator;
    if (creator == null) {
      creator = schemaTypeCreatorFactory.create(typeDescriptor, rowSchema);
      this.cachedCreator = creator;
    }

    if (row instanceof RowWithStorage) {
      @Nullable Object[] rawArr = ((RowWithStorage) row).getRawArray();
      if (rawArr != null) {
        if (!plan.hasConversions) {
          return (T) creator.create(rawArr);
        }
        Object[] params = new Object[fieldCount];
        for (int i = 0; i < fieldCount; ++i) {
          Function converter = converters[i];
          Object val = rawArr[i];
          params[i] = converter != null ? converter.apply(val) : val;
        }
        return (T) creator.create(params);
      }
    }

    Object[] params = new Object[fieldCount];
    if (!plan.hasConversions) {
      for (int i = 0; i < fieldCount; ++i) {
        params[i] = row.getValue(i);
      }
    } else {
      for (int i = 0; i < fieldCount; ++i) {
        Function converter = converters[i];
        Object val = row.getValue(i);
        params[i] = converter != null ? converter.apply(val) : val;
      }
    }
    return (T) creator.create(params);
  }

  private synchronized ConverterPlan initFieldConverters(Schema schema) {
    ConverterPlan plan = this.converterPlan;
    if (plan == null) {
      CachingFactory<List<FieldValueTypeInformation>> typeFactory =
          new CachingFactory<>(schemaProvider::fieldValueTypeInformations);
      plan = fieldConverters(typeDescriptor, schema, typeFactory);
      this.converterPlan = plan;
    }
    return plan;
  }

  private ConverterPlan fieldConverters(
      TypeDescriptor<?> typeDescriptor,
      Schema schema,
      Factory<List<FieldValueTypeInformation>> typeFactory) {
    List<FieldValueTypeInformation> typeInfos = typeFactory.create(typeDescriptor, schema);
    checkState(
        typeInfos.size() == schema.getFieldCount(),
        "Did not have a matching number of type informations and fields.");
    @Nullable Function[] converters = new Function[schema.getFieldCount()];
    boolean hasConversions = false;
    for (int i = 0; i < converters.length; i++) {
      Function converter =
          fieldConverter(schema.getField(i).getType(), typeInfos.get(i), typeFactory);
      converters[i] = converter;
      if (converter != null) {
        hasConversions = true;
      }
    }
    return new ConverterPlan(converters, hasConversions);
  }

  private static boolean needsConversion(FieldType type) {
    TypeName typeName = type.getTypeName();
    return typeName.equals(TypeName.ROW)
        || typeName.isLogicalType()
        || ((typeName.equals(TypeName.ARRAY) || typeName.equals(TypeName.ITERABLE))
            && needsConversion(type.getCollectionElementType()))
        || (typeName.equals(TypeName.MAP)
            && (needsConversion(type.getMapKeyType()) || needsConversion(type.getMapValueType())));
  }

  /** Returns a converter for the given field type, or {@code null} when identity suffices. */
  private @Nullable Function fieldConverter(
      FieldType type,
      FieldValueTypeInformation typeInfo,
      Factory<List<FieldValueTypeInformation>> typeFactory) {
    if (!needsConversion(type)) {
      return null;
    } else if (TypeName.ROW.equals(type.getTypeName())) {
      ConverterPlan nestedPlan =
          fieldConverters(typeInfo.getType(), type.getRowSchema(), typeFactory);
      return new FromRowUsingCreator(
          typeInfo.getType(), schemaProvider, schemaTypeCreatorFactory, nestedPlan);
    } else if (TypeName.ARRAY.equals(type.getTypeName())) {
      Function elementConverter =
          fieldConverter(type.getCollectionElementType(), typeInfo.getElementType(), typeFactory);
      return elementConverter == null ? null : new ConvertCollection(elementConverter);
    } else if (TypeName.ITERABLE.equals(type.getTypeName())) {
      Function elementConverter =
          fieldConverter(type.getCollectionElementType(), typeInfo.getElementType(), typeFactory);
      return elementConverter == null ? null : new ConvertIterable(elementConverter);
    } else if (TypeName.MAP.equals(type.getTypeName())) {
      Function keyConverter =
          fieldConverter(type.getMapKeyType(), typeInfo.getMapKeyType(), typeFactory);
      Function valueConverter =
          fieldConverter(type.getMapValueType(), typeInfo.getMapValueType(), typeFactory);
      return (keyConverter == null && valueConverter == null)
          ? null
          : new ConvertMap(keyConverter, valueConverter);
    } else if (type.isLogicalType(OneOfType.IDENTIFIER)) {
      OneOfType oneOfType = type.getLogicalType(OneOfType.class);
      Schema schema = oneOfType.getOneOfSchema();
      Map<Integer, Function> readers = Maps.newHashMapWithExpectedSize(schema.getFieldCount());
      oneOfType
          .getCaseEnumType()
          .getValuesMap()
          .forEach(
              (name, id) -> {
                FieldType caseType = schema.getField(name).getType();
                FieldValueTypeInformation caseTypeInfo =
                    checkNotNull(typeInfo.getOneOfTypes().get(name));
                Function converter = fieldConverter(caseType, caseTypeInfo, typeFactory);
                readers.put(id, converter == null ? FieldConverter.IDENTITY : converter);
              });
      return new ConvertOneOf(oneOfType, readers);
    } else if (type.getTypeName().isLogicalType()) {
      Class<?> providerClass = schemaProvider.getClass();
      if ((providerClass == JavaFieldSchema.class
              || providerClass == JavaBeanSchema.class
              || providerClass == AutoValueSchema.class)
          && ByteBuddyUtils.matchesInferredLogicalType(
              type.getLogicalType(), typeInfo.getRawType())) {
        // Generated setters accept the Java input type directly via convertLogicalTypeForSetter,
        // avoiding the toBaseType -> toInputType round-trip allocation.
        return null;
      }
      return new ConvertLogicalType<>(type.getLogicalType());
    }
    return null;
  }

  private interface FieldConverter<FieldT, ValueT>
      extends SerializableFunction<FieldT, ValueT>, Function<FieldT, ValueT> {
    Function<Object, Object> IDENTITY = v -> v;

    ValueT convert(FieldT field);

    @Override
    default @Nullable ValueT apply(@Nullable FieldT fieldValue) {
      return fieldValue == null ? null : convert(fieldValue);
    }
  }

  private static class ConvertCollection implements FieldConverter<Collection, Collection> {
    final Function converter;

    ConvertCollection(Function converter) {
      this.converter = converter;
    }

    @Override
    public Collection convert(Collection collection) {
      if (collection instanceof List) {
        // For performance reasons if the input is a list, make sure that we produce a list.
        // Otherwise Row unwrapping is forced to physically copy the collection into a new List
        // object.
        return Lists.transform((List) collection, converter);
      } else {
        return Collections2.transform(collection, converter);
      }
    }
  }

  private static class ConvertIterable implements FieldConverter<Iterable, Iterable> {
    final Function converter;

    ConvertIterable(Function converter) {
      this.converter = converter;
    }

    @Override
    public Iterable convert(Iterable iterable) {
      return Iterables.transform(iterable, converter);
    }
  }

  private static class ConvertMap implements FieldConverter<Map, Map> {
    final @Nullable Function keyConverter;
    final @Nullable Function valueConverter;

    ConvertMap(@Nullable Function keyConverter, @Nullable Function valueConverter) {
      this.keyConverter = keyConverter;
      this.valueConverter = valueConverter;
    }

    @Override
    @SuppressWarnings("unchecked")
    public Map convert(Map field) {
      if (keyConverter == null && valueConverter == null) {
        return field;
      }
      Map result = Maps.newHashMapWithExpectedSize(field.size());
      for (Map.Entry<?, ?> entry : ((Map<?, ?>) field).entrySet()) {
        Object k = entry.getKey();
        Object v = entry.getValue();
        result.put(
            keyConverter != null ? keyConverter.apply(k) : k,
            valueConverter != null ? valueConverter.apply(v) : v);
      }
      return result;
    }
  }

  private static class ConvertOneOf implements FieldConverter<OneOfType.Value, OneOfType.Value> {
    private static final int MAX_DENSE_ENUM_ID = 256;
    final OneOfType oneOfType;
    final Map<Integer, Function> converters;
    final @Nullable Function @Nullable [] denseConverters;

    ConvertOneOf(OneOfType oneOfType, Map<Integer, Function> converters) {
      this.oneOfType = oneOfType;
      this.converters = converters;
      int min = Integer.MAX_VALUE;
      int max = Integer.MIN_VALUE;
      for (Integer id : converters.keySet()) {
        int v = id;
        if (v < min) {
          min = v;
        }
        if (v > max) {
          max = v;
        }
      }
      if (min >= 0 && max >= 0 && max < MAX_DENSE_ENUM_ID && !converters.isEmpty()) {
        Function[] dense = new Function[max + 1];
        for (Map.Entry<Integer, Function> entry : converters.entrySet()) {
          dense[entry.getKey()] = entry.getValue();
        }
        this.denseConverters = dense;
      } else {
        this.denseConverters = null;
      }
    }

    @Override
    public OneOfType.Value convert(OneOfType.Value field) {
      EnumerationType.Value caseType = field.getCaseType();
      int id = caseType.getValue();
      Function converter =
          denseConverters != null
              ? (id >= 0 && id < denseConverters.length ? denseConverters[id] : null)
              : converters.get(id);
      checkStateNotNull(converter, "Missing OneOf converter for case %s.", caseType);
      if (converter == FieldConverter.IDENTITY) {
        return field;
      }
      Object rawValue = field.getValue();
      Object converted = converter.apply(rawValue);
      if (converted == rawValue) {
        return field;
      }
      return oneOfType.createValue(caseType, converted);
    }
  }

  private static class ConvertLogicalType<FieldT, ValueT>
      implements FieldConverter<FieldT, ValueT> {
    final Schema.LogicalType<FieldT, ValueT> logicalType;

    ConvertLogicalType(Schema.LogicalType<FieldT, ValueT> logicalType) {
      this.logicalType = logicalType;
    }

    @Override
    public ValueT convert(FieldT field) {
      return logicalType.toBaseType(field);
    }
  }

  TypeDescriptor<T> getTypeDescriptor() {
    return typeDescriptor;
  }

  GetterBasedSchemaProvider getSchemaProvider() {
    return schemaProvider;
  }

  @Override
  public boolean equals(@Nullable Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof FromRowUsingCreator)) {
      return false;
    }
    FromRowUsingCreator<?> that = (FromRowUsingCreator<?>) o;
    return typeDescriptor.equals(that.typeDescriptor) && schemaProvider.equals(that.schemaProvider);
  }

  @Override
  public int hashCode() {
    return Objects.hash(typeDescriptor, schemaProvider);
  }
}
