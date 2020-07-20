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

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkNotNull;
import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkState;

import java.lang.reflect.Type;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.annotations.Experimental.Kind;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.schemas.Schema.TypeName;
import org.apache.beam.sdk.schemas.logicaltypes.EnumerationType;
import org.apache.beam.sdk.schemas.logicaltypes.OneOfType;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.RowWithGetters;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Function;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Collections2;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Iterables;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Lists;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Maps;
import org.checkerframework.checker.nullness.qual.Nullable;

/** Function to convert a {@link Row} to a user type using a creator factory. */
@Experimental(Kind.SCHEMAS)
class FromRowUsingCreator<T> implements SerializableFunction<Row, T> {
  private final Class<T> clazz;
  private final GetterBasedSchemaProvider schemaProvider;
  private final Factory<SchemaUserTypeCreator> schemaTypeCreatorFactory;
  private final Factory<List<FieldValueTypeInformation>> fieldValueTypeInformationFactory;

  public FromRowUsingCreator(Class<T> clazz, GetterBasedSchemaProvider schemaProvider) {
    this.clazz = clazz;
    this.schemaProvider = schemaProvider;
    this.schemaTypeCreatorFactory = new CachingFactory<>(schemaProvider::schemaTypeCreator);
    this.fieldValueTypeInformationFactory =
        new CachingFactory<>(schemaProvider::fieldValueTypeInformations);
  }

  @Override
  public T apply(Row row) {
    return fromRow(row, clazz, fieldValueTypeInformationFactory);
  }

  @SuppressWarnings("unchecked")
  public <ValueT> ValueT fromRow(
      Row row, Class<ValueT> clazz, Factory<List<FieldValueTypeInformation>> typeFactory) {
    if (row instanceof RowWithGetters) {
      Object target = ((RowWithGetters) row).getGetterTarget();
      if (target.getClass().equals(clazz)) {
        // Efficient path: simply extract the underlying object instead of creating a new one.
        return (ValueT) target;
      }
    }

    Object[] params = new Object[row.getFieldCount()];
    Schema schema = row.getSchema();
    List<FieldValueTypeInformation> typeInformations = typeFactory.create(clazz, schema);
    checkState(
        typeInformations.size() == row.getFieldCount(),
        "Did not have a matching number of type informations and fields.");

    for (int i = 0; i < row.getFieldCount(); ++i) {
      FieldType type = schema.getField(i).getType();
      FieldValueTypeInformation typeInformation = checkNotNull(typeInformations.get(i));
      params[i] =
          fromValue(
              type, row.getValue(i), typeInformation.getRawType(), typeInformation, typeFactory);
    }

    SchemaUserTypeCreator creator = schemaTypeCreatorFactory.create(clazz, schema);
    return (ValueT) creator.create(params);
  }

  @SuppressWarnings("unchecked")
  private @Nullable <ValueT> ValueT fromValue(
      FieldType type,
      ValueT value,
      Type fieldType,
      FieldValueTypeInformation fieldValueTypeInformation,
      Factory<List<FieldValueTypeInformation>> typeFactory) {
    FieldValueTypeInformation elementType = fieldValueTypeInformation.getElementType();
    FieldValueTypeInformation keyType = fieldValueTypeInformation.getMapKeyType();
    FieldValueTypeInformation valueType = fieldValueTypeInformation.getMapValueType();
    if (value == null) {
      return null;
    }
    if (TypeName.ROW.equals(type.getTypeName())) {
      return (ValueT) fromRow((Row) value, (Class) fieldType, typeFactory);
    } else if (TypeName.ARRAY.equals(type.getTypeName())) {
      return (ValueT)
          fromCollectionValue(
              type.getCollectionElementType(), (Collection) value, elementType, typeFactory);
    } else if (TypeName.ITERABLE.equals(type.getTypeName())) {
      return (ValueT)
          fromIterableValue(
              type.getCollectionElementType(), (Iterable) value, elementType, typeFactory);
    }
    if (TypeName.MAP.equals(type.getTypeName())) {
      return (ValueT)
          fromMapValue(
              type.getMapKeyType(),
              type.getMapValueType(),
              (Map) value,
              keyType,
              valueType,
              typeFactory);
    } else {
      if (type.isLogicalType(OneOfType.IDENTIFIER)) {
        OneOfType oneOfType = type.getLogicalType(OneOfType.class);
        EnumerationType oneOfEnum = oneOfType.getCaseEnumType();
        OneOfType.Value oneOfValue = (OneOfType.Value) value;
        FieldValueTypeInformation oneOfFieldValueTypeInformation =
            checkNotNull(
                fieldValueTypeInformation
                    .getOneOfTypes()
                    .get(oneOfEnum.toString(oneOfValue.getCaseType())));
        Object fromValue =
            fromValue(
                oneOfType.getFieldType(oneOfValue),
                oneOfValue.getValue(),
                oneOfFieldValueTypeInformation.getRawType(),
                oneOfFieldValueTypeInformation,
                typeFactory);
        return (ValueT) oneOfType.createValue(oneOfValue.getCaseType(), fromValue);
      } else if (type.getTypeName().isLogicalType()) {
        return (ValueT) type.getLogicalType().toBaseType(value);
      }
      return value;
    }
  }

  private static <SourceT, DestT> Collection<DestT> transformCollection(
      Collection<SourceT> collection, Function<SourceT, DestT> function) {
    if (collection instanceof List) {
      // For performance reasons if the input is a list, make sure that we produce a list. Otherwise
      // Row unwrapping
      // is forced to physically copy the collection into a new List object.
      return Lists.transform((List) collection, function);
    } else {
      return Collections2.transform(collection, function);
    }
  }

  @SuppressWarnings("unchecked")
  private <ElementT> Collection fromCollectionValue(
      FieldType elementType,
      Collection<ElementT> rowCollection,
      FieldValueTypeInformation elementTypeInformation,
      Factory<List<FieldValueTypeInformation>> typeFactory) {
    return transformCollection(
        rowCollection,
        element ->
            fromValue(
                elementType,
                element,
                elementTypeInformation.getType().getType(),
                elementTypeInformation,
                typeFactory));
  }

  @SuppressWarnings("unchecked")
  private <ElementT> Iterable fromIterableValue(
      FieldType elementType,
      Iterable<ElementT> rowIterable,
      FieldValueTypeInformation elementTypeInformation,
      Factory<List<FieldValueTypeInformation>> typeFactory) {
    return Iterables.transform(
        rowIterable,
        element ->
            fromValue(
                elementType,
                element,
                elementTypeInformation.getType().getType(),
                elementTypeInformation,
                typeFactory));
  }

  @SuppressWarnings("unchecked")
  private Map<?, ?> fromMapValue(
      FieldType keyType,
      FieldType valueType,
      Map<?, ?> map,
      FieldValueTypeInformation keyTypeInformation,
      FieldValueTypeInformation valueTypeInformation,
      Factory<List<FieldValueTypeInformation>> typeFactory) {
    Map newMap = Maps.newHashMap();
    for (Map.Entry<?, ?> entry : map.entrySet()) {
      Object key =
          fromValue(
              keyType,
              entry.getKey(),
              keyTypeInformation.getType().getType(),
              keyTypeInformation,
              typeFactory);
      Object value =
          fromValue(
              valueType,
              entry.getValue(),
              valueTypeInformation.getType().getType(),
              valueTypeInformation,
              typeFactory);
      newMap.put(key, value);
    }
    return newMap;
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
    return clazz.equals(that.clazz) && schemaProvider.equals(that.schemaProvider);
  }

  @Override
  public int hashCode() {
    return Objects.hash(clazz, schemaProvider);
  }
}
