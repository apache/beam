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
package org.apache.beam.sdk.values;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.annotations.Experimental.Kind;
import org.apache.beam.sdk.schemas.Factory;
import org.apache.beam.sdk.schemas.FieldValueGetter;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.Field;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.schemas.Schema.TypeName;
import org.apache.beam.sdk.schemas.logicaltypes.OneOfType;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Collections2;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Iterables;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Lists;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Maps;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * A Concrete subclass of {@link Row} that delegates to a set of provided {@link FieldValueGetter}s.
 *
 * <p>This allows us to have {@link Row} objects for which the actual storage is in another object.
 * For example, the user's type may be a POJO, in which case the provided getters will simple read
 * the appropriate fields from the POJO.
 */
@Experimental(Kind.SCHEMAS)
public class RowWithGetters extends Row {
  private final Factory<List<FieldValueGetter>> fieldValueGetterFactory;
  private final Object getterTarget;
  private final List<FieldValueGetter> getters;

  private final Map<Integer, Collection> cachedCollections = Maps.newHashMap();
  private final Map<Integer, Iterable> cachedIterables = Maps.newHashMap();
  private final Map<Integer, Map> cachedMaps = Maps.newHashMap();

  RowWithGetters(
      Schema schema, Factory<List<FieldValueGetter>> getterFactory, Object getterTarget) {
    super(schema);
    this.fieldValueGetterFactory = getterFactory;
    this.getterTarget = getterTarget;
    this.getters = fieldValueGetterFactory.create(getterTarget.getClass(), schema);
  }

  @Nullable
  @Override
  @SuppressWarnings({"TypeParameterUnusedInFormals", "unchecked"})
  public <T> T getValue(int fieldIdx) {
    Field field = getSchema().getField(fieldIdx);
    FieldType type = field.getType();
    Object fieldValue = getters.get(fieldIdx).get(getterTarget);
    if (fieldValue == null && !field.getType().getNullable()) {
      throw new RuntimeException("Null value set on non-nullable field " + field);
    }
    return fieldValue != null ? getValue(type, fieldValue, fieldIdx) : null;
  }

  private Collection getCollectionValue(FieldType elementType, Object fieldValue) {
    Collection collection = (Collection) fieldValue;
    if (collection instanceof List) {
      // For performance reasons if the input is a list, make sure that we produce a list. Otherwise
      // Row forwarding
      // is forced to physically copy the collection into a new List object.
      return Lists.transform((List) collection, v -> getValue(elementType, v, null));
    } else {
      return Collections2.transform(collection, v -> getValue(elementType, v, null));
    }
  }

  private Iterable getIterableValue(FieldType elementType, Object fieldValue) {
    Iterable iterable = (Iterable) fieldValue;
    // Wrap the iterable to avoid having to materialize the entire collection.
    return Iterables.transform(iterable, v -> getValue(elementType, v, null));
  }

  private Map<?, ?> getMapValue(FieldType keyType, FieldType valueType, Map<?, ?> fieldValue) {
    Map returnMap = Maps.newHashMap();
    for (Map.Entry<?, ?> entry : fieldValue.entrySet()) {
      returnMap.put(
          getValue(keyType, entry.getKey(), null), getValue(valueType, entry.getValue(), null));
    }
    return returnMap;
  }

  @SuppressWarnings({"TypeParameterUnusedInFormals", "unchecked"})
  private <T> T getValue(FieldType type, Object fieldValue, @Nullable Integer cacheKey) {
    if (type.getTypeName().equals(TypeName.ROW)) {
      return (T) new RowWithGetters(type.getRowSchema(), fieldValueGetterFactory, fieldValue);
    } else if (type.getTypeName().equals(TypeName.ARRAY)) {
      return cacheKey != null
          ? (T)
              cachedCollections.computeIfAbsent(
                  cacheKey, i -> getCollectionValue(type.getCollectionElementType(), fieldValue))
          : (T) getCollectionValue(type.getCollectionElementType(), fieldValue);
    } else if (type.getTypeName().equals(TypeName.ITERABLE)) {
      return cacheKey != null
          ? (T)
              cachedIterables.computeIfAbsent(
                  cacheKey, i -> getIterableValue(type.getCollectionElementType(), fieldValue))
          : (T) getIterableValue(type.getCollectionElementType(), fieldValue);
    } else if (type.getTypeName().equals(TypeName.MAP)) {
      Map map = (Map) fieldValue;
      return cacheKey != null
          ? (T)
              cachedMaps.computeIfAbsent(
                  cacheKey, i -> getMapValue(type.getMapKeyType(), type.getMapValueType(), map))
          : (T) getMapValue(type.getMapKeyType(), type.getMapValueType(), map);
    } else {
      if (type.isLogicalType(OneOfType.IDENTIFIER)) {
        OneOfType oneOfType = type.getLogicalType(OneOfType.class);
        OneOfType.Value oneOfValue = (OneOfType.Value) fieldValue;
        Object convertedOneOfField =
            getValue(oneOfType.getFieldType(oneOfValue), oneOfValue.getValue(), null);
        return (T) oneOfType.createValue(oneOfValue.getCaseType(), convertedOneOfField);
      } else if (type.getTypeName().isLogicalType()) {
        // Getters are assumed to return the base type.
        return (T) type.getLogicalType().toInputType(fieldValue);
      }
      return (T) fieldValue;
    }
  }

  @Override
  public int getFieldCount() {
    return getters.size();
  }

  @Override
  public List<Object> getValues() {
    return getters.stream().map(g -> g.get(getterTarget)).collect(Collectors.toList());
  }

  public List<FieldValueGetter> getGetters() {
    return getters;
  }

  public Object getGetterTarget() {
    return getterTarget;
  }

  @Override
  public boolean equals(@Nullable Object o) {
    if (this == o) {
      return true;
    }
    if (o == null) {
      return false;
    }
    if (o instanceof RowWithGetters) {
      RowWithGetters other = (RowWithGetters) o;
      return Objects.equals(getSchema(), other.getSchema())
          && Objects.equals(getterTarget, other.getterTarget);
    } else if (o instanceof Row) {
      return super.equals(o);
    }
    return false;
  }

  @Override
  public int hashCode() {
    return Objects.hash(getSchema(), getterTarget);
  }
}
