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

import static com.google.common.base.Preconditions.checkState;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Type;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.annotations.Experimental.Kind;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.schemas.Schema.TypeName;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.RowWithGetters;
import org.apache.beam.sdk.values.TypeDescriptor;

/**
 * A {@link SchemaProvider} base class that vends schemas and rows based on {@link
 * FieldValueGetter}s.
 */
@Experimental(Kind.SCHEMAS)
public abstract class GetterBasedSchemaProvider implements SchemaProvider {
  /** Implementing class should override to return a getter factory. */
  abstract FieldValueGetterFactory fieldValueGetterFactory();

  /** Implementing class should override to return a setter factory. */
  abstract FieldValueSetterFactory fieldValueSetterFactory();

  @Override
  public <T> SerializableFunction<T, Row> toRowFunction(TypeDescriptor<T> typeDescriptor) {
    // schemaFor is non deterministic - it might return fields in an arbitrary order. The reason
    // why is that Java reflection does not guarantee the order in which it returns fields and
    // methods, and these schemas are often based on reflective analysis of classes. Therefore it's
    // important to capture the schema once here, so all invocations of the toRowFunction see the
    // same version of the schema. If schemaFor were to be called inside the lambda below, different
    // workers would see different versions of the schema.
    Schema schema = schemaFor(typeDescriptor);

    // Since we know that this factory is always called from inside the lambda with the same class
    // and schema, return a caching factory that caches the first value seen. This prevents having
    // to lookup the getter list each time createGetters is called.
    FieldValueGetterFactory getterFactory =
        new FieldValueGetterFactory() {
          transient List<FieldValueGetter> getters;

          @Override
          public List<FieldValueGetter> createGetters(Class<?> targetClass, Schema schema) {
            if (getters != null) {
              return getters;
            }
            getters = fieldValueGetterFactory().createGetters(targetClass, schema);
            return getters;
          }
        };
    return o -> Row.withSchema(schema).withFieldValueGetters(getterFactory, o).build();
  }

  @Override
  @SuppressWarnings("unchecked")
  public <T> SerializableFunction<Row, T> fromRowFunction(TypeDescriptor<T> typeDescriptor) {
    return r -> {
      if (r instanceof RowWithGetters) {
        // Efficient path: simply extract the underlying POJO instead of creating a new one.
        return (T) ((RowWithGetters) r).getGetterTarget();
      } else {
        // Use the setters to copy values from the Row to a new instance of the class.
        return fromRow(r, (Class<T>) typeDescriptor.getType());
      }
    };
  }

  private <T> T fromRow(Row row, Class<T> clazz) {
    T object;
    try {
      object = clazz.getDeclaredConstructor().newInstance();
    } catch (NoSuchMethodException
        | IllegalAccessException
        | InvocationTargetException
        | InstantiationException e) {
      throw new RuntimeException("Failed to instantiate object ", e);
    }

    Schema schema = row.getSchema();
    List<FieldValueSetter> setters = fieldValueSetterFactory().createSetters(clazz, schema);
    checkState(
        setters.size() == row.getFieldCount(),
        "Did not have a matching number of setters and fields.");

    // Iterate over the row, and set (possibly recursively) each field in the underlying object
    // using the setter.
    for (int i = 0; i < row.getFieldCount(); ++i) {
      FieldType type = schema.getField(i).getType();
      FieldValueSetter setter = setters.get(i);
      if (setter == null) {
        throw new RuntimeException(
            "NULL SETTER FOR "
                + clazz.getSimpleName()
                + " field name "
                + schema.getField(i).getName()
                + " schema "
                + schema);
      }
      setter.set(
          object,
          fromValue(
              type,
              row.getValue(i),
              setter.type(),
              setter.elementType(),
              setter.mapKeyType(),
              setter.mapValueType()));
    }
    return object;
  }

  @SuppressWarnings("unchecked")
  @Nullable
  private <T> T fromValue(
      FieldType type, T value, Type fieldType, Type elemenentType, Type keyType, Type valueType) {
    if (value == null) {
      return null;
    }
    if (TypeName.ROW.equals(type.getTypeName())) {
      return (T) fromRow((Row) value, (Class) fieldType);
    } else if (TypeName.ARRAY.equals(type.getTypeName())) {
      return (T) fromListValue(type.getCollectionElementType(), (List) value, elemenentType);
    } else if (TypeName.MAP.equals(type.getTypeName())) {
      return (T)
          fromMapValue(
              type.getMapKeyType(), type.getMapValueType(), (Map) value, keyType, valueType);
    } else {
      return value;
    }
  }

  @SuppressWarnings("unchecked")
  private <T> List fromListValue(FieldType elementType, List<T> rowList, Type elementClass) {
    List list = Lists.newArrayList();
    for (T element : rowList) {
      list.add(fromValue(elementType, element, elementClass, null, null, null));
    }
    return list;
  }

  @SuppressWarnings("unchecked")
  private Map<?, ?> fromMapValue(
      FieldType keyType, FieldType valueType, Map<?, ?> map, Type keyClass, Type valueClass) {
    Map newMap = Maps.newHashMap();
    for (Map.Entry<?, ?> entry : map.entrySet()) {
      Object key = fromValue(keyType, entry.getKey(), keyClass, null, null, null);
      Object value = fromValue(valueType, entry.getValue(), valueClass, null, null, null);
      newMap.put(key, value);
    }
    return newMap;
  }
}
