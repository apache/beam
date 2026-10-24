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

import static org.apache.beam.sdk.util.Preconditions.checkStateNotNull;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.sdk.schemas.Factory;
import org.apache.beam.sdk.schemas.FieldValueGetter;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.Field;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * A Concrete subclass of {@link Row} that delegates to a set of provided {@link FieldValueGetter}s.
 *
 * <p>This allows us to have {@link Row} objects for which the actual storage is in another object.
 * For example, the user's type may be a POJO, in which case the provided getters will simple read
 * the appropriate fields from the POJO.
 */
@SuppressWarnings("rawtypes")
public class RowWithGetters<T extends @NonNull Object> extends Row {
  private static final Object NULL_VALUE = new Object();

  private final T getterTarget;
  private final TypeDescriptor<?> getterTargetType;
  private final List<FieldValueGetter<T, Object>> getters;
  private transient @Nullable Object @Nullable [] cache = null;

  RowWithGetters(
      Schema schema,
      Factory<List<FieldValueGetter<T, Object>>> getterFactory,
      T getterTarget,
      TypeDescriptor<?> getterTargetType) {
    super(schema);
    this.getterTarget = getterTarget;
    this.getterTargetType = getterTargetType;
    this.getters = getterFactory.create(getterTargetType, schema);
  }

  @Override
  @SuppressWarnings({"TypeParameterUnusedInFormals", "unchecked"})
  public <W> W getValue(int fieldIdx) {
    Field field = getSchema().getField(fieldIdx);
    @Nullable Object fieldValue;
    if (cacheFieldType(field)) {
      if (cache == null) {
        cache = new Object[getters.size()];
      }
      Object cached = cache[fieldIdx];
      if (cached == null) {
        FieldValueGetter<T, Object> getter = getters.get(fieldIdx);
        checkStateNotNull(getter);
        fieldValue = getter.get(getterTarget);
        cache[fieldIdx] = (fieldValue == null) ? NULL_VALUE : fieldValue;
      } else {
        fieldValue = (cached == NULL_VALUE) ? null : cached;
      }
    } else {
      fieldValue = getters.get(fieldIdx).get(getterTarget);
    }

    if (fieldValue == null && !field.getType().getNullable()) {
      throw new RuntimeException("Null value set on non-nullable field " + field);
    }
    return (W) fieldValue;
  }

  public TypeDescriptor<?> getGetterTargetType() {
    return getterTargetType;
  }

  private boolean cacheFieldType(Field field) {
    switch (field.getType().getTypeName()) {
      case MAP:
      case ARRAY:
      case ITERABLE:
      case ROW:
      case LOGICAL_TYPE:
        return true;
      default:
        return false;
    }
  }

  @Override
  public int getFieldCount() {
    return getters.size();
  }

  /** Return the list of raw unmodified data values to enable 0-copy code. */
  @Internal
  @Override
  public List<@Nullable Object> getValues() {
    List<@Nullable Object> rawValues = new ArrayList<>(getters.size());
    for (FieldValueGetter getter : getters) {
      rawValues.add(getter.getRaw(getterTarget));
    }
    return rawValues;
  }

  public List<FieldValueGetter<T, Object>> getGetters() {
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
      if (Objects.equals(getSchema(), other.getSchema())
          && Objects.equals(getterTarget, other.getterTarget)) {
        return true;
      }
    }
    if (o instanceof Row) {
      return super.equals(o);
    }
    return false;
  }

  @Override
  public int hashCode() {
    return super.hashCode();
  }
}
