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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReferenceArray;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.sdk.schemas.Factory;
import org.apache.beam.sdk.schemas.FieldValueGetter;
import org.apache.beam.sdk.schemas.Schema;
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
  private static final Object NULL_SENTINEL = new Object();

  private final T getterTarget;
  private final TypeDescriptor<?> getterTargetType;
  private final List<FieldValueGetter<T, Object>> getters;
  // Null slot means uncached; NULL_SENTINEL means cached null. AtomicReferenceArray ensures
  // safe JMM publication of cached collections across concurrent readers.
  private transient volatile @Nullable AtomicReferenceArray<@Nullable Object> cache = null;

  RowWithGetters(
      Schema schema,
      Factory<List<FieldValueGetter<T, Object>>> getterFactory,
      T getterTarget,
      TypeDescriptor<?> getterTargetType) {
    this(schema, getterFactory.create(getterTargetType, schema), getterTarget, getterTargetType);
  }

  RowWithGetters(
      Schema schema,
      List<FieldValueGetter<T, Object>> getters,
      T getterTarget,
      TypeDescriptor<?> getterTargetType) {
    super(schema);
    this.getterTarget = getterTarget;
    this.getterTargetType = getterTargetType;
    this.getters = getters;
  }

  @Override
  @SuppressWarnings({"TypeParameterUnusedInFormals", "unchecked"})
  public <W> W getValue(int fieldIdx) {
    Schema schema = getSchema();
    @Nullable Object fieldValue;
    int cacheSlot = schema.getContainerCacheSlot(fieldIdx);
    if (cacheSlot >= 0) {
      AtomicReferenceArray<@Nullable Object> localCache = cache;
      if (localCache == null) {
        localCache = new AtomicReferenceArray<>(schema.getCacheableContainerCount());
        cache = localCache;
      }
      @Nullable Object cached = localCache.get(cacheSlot);
      if (cached == null) {
        FieldValueGetter<T, Object> getter = getters.get(fieldIdx);
        fieldValue = getter.get(getterTarget);
        localCache.set(cacheSlot, fieldValue == null ? NULL_SENTINEL : fieldValue);
      } else {
        fieldValue = cached == NULL_SENTINEL ? null : cached;
      }
    } else {
      fieldValue = getters.get(fieldIdx).get(getterTarget);
    }

    if (fieldValue == null && !schema.isFieldNullable(fieldIdx)) {
      throw new RuntimeException(
          "Null value set on non-nullable field " + schema.getField(fieldIdx));
    }
    return (W) fieldValue;
  }

  public TypeDescriptor<?> getGetterTargetType() {
    return getterTargetType;
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
    for (int i = 0; i < getters.size(); i++) {
      rawValues.add(getters.get(i).getRaw(getterTarget));
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
      if (!getSchema().equals(other.getSchema())) {
        return false;
      }
      if (getterTarget == other.getterTarget) {
        return true;
      }
      return super.equals(o);
    } else if (o instanceof Row) {
      return super.equals(o);
    }
    return false;
  }

  @Override
  public int hashCode() {
    return super.hashCode();
  }
}
