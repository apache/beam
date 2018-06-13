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

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.schemas.Schema.TypeName;
import org.apache.beam.sdk.values.reflect.FieldValueGetter;
import org.apache.beam.sdk.values.reflect.FieldValueGetterFactory;

public class RowWithGetters extends Row {
  private FieldValueGetterFactory fieldValueGetterFactory;
  private Object getterTarget;
  private List<FieldValueGetter> getters;

  private Map<Integer, List> cachedLists = Maps.newHashMap();
  private Map<Integer, Map> cachedMaps = Maps.newHashMap();

  RowWithGetters(Schema schema, FieldValueGetterFactory getterFactory, Object getterTarget) {
    super(schema);
    this.fieldValueGetterFactory = getterFactory;
    this.getterTarget = getterTarget;
    this.getters = fieldValueGetterFactory.createGetters(getterTarget.getClass());
  }

  @Nullable
  @Override
  @SuppressWarnings({"TypeParameterUnusedInFormals", "unchecked"})
  public <T> T getValue(int fieldIdx) {
    FieldType type = getSchema().getField(fieldIdx).getType();
    Object fieldValue = getters.get(fieldIdx).get(getterTarget);

    return getValue(type, fieldValue, fieldIdx);
  }

  private List getListValue(FieldType elementType, Object fieldValue) {
    Iterable iterable = (Iterable) fieldValue;
    List<Object> list = Lists.newArrayList();
    for (Object o : iterable) {
      list.add(getValue(elementType, o, null));
    }
    return list;
  }

  private Map<?, ?> getMapValue(FieldType keyType, FieldType valueType, Map<?, ?> fieldValue) {
    Map returnMap = Maps.newHashMap();
    for (Map.Entry<?, ?> entry : fieldValue.entrySet()) {
      returnMap.put(getValue(keyType, entry.getKey(), null),
          getValue(valueType, entry.getValue(), null));
    }
    return  returnMap;
  }

  @SuppressWarnings({"TypeParameterUnusedInFormals", "unchecked"})
  private <T> T getValue(FieldType type, Object fieldValue, @Nullable Integer cacheKey) {
    if (type.getTypeName().equals(TypeName.ROW)) {
      return (T) new RowWithGetters(type.getRowSchema(), fieldValueGetterFactory, fieldValue);
    } else if (type.getTypeName().equals(TypeName.ARRAY)) {
      return cacheKey != null
          ? (T) cachedLists.computeIfAbsent(cacheKey,
          i -> getListValue(type.getCollectionElementType(), fieldValue))
          : (T) getListValue(type.getCollectionElementType(), fieldValue);
    }  else if (type.getTypeName().equals(TypeName.MAP)) {
      Map map = (Map) fieldValue;
      return cacheKey != null
          ? (T) cachedMaps.computeIfAbsent(cacheKey,
          i -> getMapValue(type.getMapKeyType(), type.getMapValueType(), (Map) fieldValue))
          : (T) getMapValue(type.getMapKeyType(), type.getMapValueType(), (Map) fieldValue);
    } else {
      return (T) fieldValue;
    }
  }

  @Override
  public int getFieldCount() {
    return getters.size();
  }

  @Override
  public List<Object> getValues() {
    return getters.stream()
        .map(g -> g.get(getterTarget))
        .collect(Collectors.toList());
  }

  public List<FieldValueGetter> getGetters() {
    return getters;
  }

  public Object getGetterTarget() {
    return getterTarget;
  }
}
