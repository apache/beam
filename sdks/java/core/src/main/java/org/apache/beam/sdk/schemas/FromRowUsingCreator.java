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
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.RowWithGetters;
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

  @SuppressFBWarnings("SE_TRANSIENT_FIELD_NOT_RESTORED")
  private transient @MonotonicNonNull Function[] fieldConverters;

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
      @Nullable Function[] fieldConverters) {
    this.typeDescriptor = typeDescriptor;
    this.schemaProvider = schemaProvider;
    this.schemaTypeCreatorFactory = schemaTypeCreatorFactory;
    this.fieldConverters = fieldConverters;
  }

  @Override
  @SuppressWarnings("unchecked")
  public T apply(Row row) {
    if (row == null) {
      return null;
    }
    if (row instanceof RowWithGetters) {
      Object target = ((RowWithGetters) row).getGetterTarget();
      if (target.getClass().equals(typeDescriptor.getRawType())) {
        // Efficient path: simply extract the underlying object instead of creating a new one.
        return (T) target;
      }
    }
    if (fieldConverters == null) {
      initFieldConverters(row.getSchema());
    }
    checkState(fieldConverters.length == row.getFieldCount(), "Unexpected field count");

    Object[] params = new Object[row.getFieldCount()];
    for (int i = 0; i < row.getFieldCount(); ++i) {
      params[i] = fieldConverters[i].apply(row.getValue(i));
    }
    SchemaUserTypeCreator creator =
        schemaTypeCreatorFactory.create(typeDescriptor, row.getSchema());
    return (T) creator.create(params);
  }

  private synchronized void initFieldConverters(Schema schema) {
    if (fieldConverters == null) {
      CachingFactory<List<FieldValueTypeInformation>> typeFactory =
          new CachingFactory<>(schemaProvider::fieldValueTypeInformations);
      fieldConverters = fieldConverters(typeDescriptor, schema, typeFactory);
    }
  }

  private Function[] fieldConverters(
      TypeDescriptor<?> typeDescriptor,
      Schema schema,
      Factory<List<FieldValueTypeInformation>> typeFactory) {
    List<FieldValueTypeInformation> typeInfos = typeFactory.create(typeDescriptor, schema);
    checkState(
        typeInfos.size() == schema.getFieldCount(),
        "Did not have a matching number of type informations and fields.");
    Function[] converters = new Function[schema.getFieldCount()];
    for (int i = 0; i < converters.length; i++) {
      converters[i] = fieldConverter(schema.getField(i).getType(), typeInfos.get(i), typeFactory);
    }
    return converters;
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

  private Function fieldConverter(
      FieldType type,
      FieldValueTypeInformation typeInfo,
      Factory<List<FieldValueTypeInformation>> typeFactory) {
    if (!needsConversion(type)) {
      return FieldConverter.IDENTITY;
    } else if (TypeName.ROW.equals(type.getTypeName())) {
      Function[] converters = fieldConverters(typeInfo.getType(), type.getRowSchema(), typeFactory);
      return new FromRowUsingCreator(
          typeInfo.getType(), schemaProvider, schemaTypeCreatorFactory, converters);
    } else if (TypeName.ARRAY.equals(type.getTypeName())) {
      return new ConvertCollection(
          fieldConverter(type.getCollectionElementType(), typeInfo.getElementType(), typeFactory));
    } else if (TypeName.ITERABLE.equals(type.getTypeName())) {
      return new ConvertIterable(
          fieldConverter(type.getCollectionElementType(), typeInfo.getElementType(), typeFactory));
    } else if (TypeName.MAP.equals(type.getTypeName())) {
      return new ConvertMap(
          fieldConverter(type.getMapKeyType(), typeInfo.getMapKeyType(), typeFactory),
          fieldConverter(type.getMapValueType(), typeInfo.getMapValueType(), typeFactory));
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
                readers.put(id, fieldConverter(caseType, caseTypeInfo, typeFactory));
              });
      return new ConvertOneOf(oneOfType, readers);
    } else if (type.getTypeName().isLogicalType()) {
      return new ConvertLogicalType<>(type.getLogicalType());
    }
    return FieldConverter.IDENTITY;
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
    final Function keyConverter, valueConverter;

    ConvertMap(Function keyConverter, Function valueConverter) {
      this.keyConverter = keyConverter;
      this.valueConverter = valueConverter;
    }

    @Override
    public Map convert(Map field) {
      Map result = Maps.newHashMapWithExpectedSize(field.size());
      field.forEach((k, v) -> result.put(keyConverter.apply(k), valueConverter.apply(v)));
      return result;
    }
  }

  private static class ConvertOneOf implements FieldConverter<OneOfType.Value, OneOfType.Value> {
    final OneOfType oneOfType;
    final Map<Integer, Function> converters;

    ConvertOneOf(OneOfType oneOfType, Map<Integer, Function> converters) {
      this.oneOfType = oneOfType;
      this.converters = converters;
    }

    @Override
    public OneOfType.Value convert(OneOfType.Value field) {
      EnumerationType.Value caseType = field.getCaseType();
      Function converter =
          checkStateNotNull(
              converters.get(caseType.getValue()), "Missing OneOf converter for case %s.");
      return oneOfType.createValue(caseType, converter.apply(field.getValue()));
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

  @Override
  public boolean equals(@Nullable Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
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
