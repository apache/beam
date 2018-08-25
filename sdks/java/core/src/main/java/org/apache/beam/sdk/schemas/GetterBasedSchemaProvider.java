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
import java.util.concurrent.ConcurrentHashMap;
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

    // Since we know that this factory is always called from inside the lambda with the same schema,
    // return a caching factory that caches the first value seen for each class. This prevents
    // having to lookup the getter list each time createGetters is called.
    FieldValueGetterFactory getterFactory =
        new FieldValueGetterFactory() {
          @Nullable
          private transient ConcurrentHashMap<Class, List<FieldValueGetter>> gettersMap = null;

          private final FieldValueGetterFactory innerFactory = fieldValueGetterFactory();

          @Override
          public List<FieldValueGetter> createGetters(Class<?> targetClass, Schema schema) {
            if (gettersMap == null) {
              gettersMap = new ConcurrentHashMap<>();
            }
            List<FieldValueGetter> getters = gettersMap.get(targetClass);
            if (getters != null) {
              return getters;
            }
            getters = innerFactory.createGetters(targetClass, schema);
            gettersMap.put(targetClass, getters);
            return getters;
          }
        };
    return o -> Row.withSchema(schema).withFieldValueGetters(getterFactory, o).build();
  }

  @Override
  @SuppressWarnings("unchecked")
  public <T> SerializableFunction<Row, T> fromRowFunction(TypeDescriptor<T> typeDescriptor) {
    FieldValueSetterFactory setterFactory =
        new FieldValueSetterFactory() {
          @Nullable
          private volatile ConcurrentHashMap<Class, List<FieldValueSetter>> settersMap = null;

          private final FieldValueSetterFactory innerFactory = fieldValueSetterFactory();

          @Override
          public List<FieldValueSetter> createSetters(Class<?> targetClass, Schema schema) {
            if (settersMap == null) {
              settersMap = new ConcurrentHashMap<>();
            }
            List<FieldValueSetter> setters = settersMap.get(targetClass);
            if (setters != null) {
              return setters;
            }
            setters = innerFactory.createSetters(targetClass, schema);
            settersMap.put(targetClass, setters);
            return setters;
          }
        };

    return r -> {
      if (r instanceof RowWithGetters) {
        // Efficient path: simply extract the underlying POJO instead of creating a new one.
        return (T) ((RowWithGetters) r).getGetterTarget();
      } else {
        // Use the setters to copy values from the Row to a new instance of the class.
        return fromRow(r, (Class<T>) typeDescriptor.getType(), setterFactory);
      }
    };
  }

  private <T> T fromRow(Row row, Class<T> clazz, FieldValueSetterFactory setterFactory) {
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
    List<FieldValueSetter> setters = setterFactory.createSetters(clazz, schema);
    checkState(
        setters.size() == row.getFieldCount(),
        "Did not have a matching number of setters and fields.");

    // Iterate over the row, and set (possibly recursively) each field in the underlying object
    // using the setter.
    for (int i = 0; i < row.getFieldCount(); ++i) {
      FieldType type = schema.getField(i).getType();
      FieldValueSetter setter = setters.get(i);
      setter.set(
          object,
          fromValue(
              type,
              row.getValue(i),
              setter.type(),
              setter.elementType(),
              setter.mapKeyType(),
              setter.mapValueType(),
              setterFactory));
    }
    return object;
  }

  @SuppressWarnings("unchecked")
  @Nullable
  private <T> T fromValue(
      FieldType type,
      T value,
      Type fieldType,
      Type elemenentType,
      Type keyType,
      Type valueType,
      FieldValueSetterFactory setterFactory) {
    if (value == null) {
      return null;
    }
    if (TypeName.ROW.equals(type.getTypeName())) {
      return (T) fromRow((Row) value, (Class) fieldType, setterFactory);
    } else if (TypeName.ARRAY.equals(type.getTypeName())) {
      return (T)
          fromListValue(
              type.getCollectionElementType(), (List) value, elemenentType, setterFactory);
    } else if (TypeName.MAP.equals(type.getTypeName())) {
      return (T)
          fromMapValue(
              type.getMapKeyType(),
              type.getMapValueType(),
              (Map) value,
              keyType,
              valueType,
              setterFactory);
    } else {
      return value;
    }
  }

  @SuppressWarnings("unchecked")
  private <T> List fromListValue(
      FieldType elementType,
      List<T> rowList,
      Type elementClass,
      FieldValueSetterFactory setterFactory) {
    List list = Lists.newArrayList();
    for (T element : rowList) {
      list.add(fromValue(elementType, element, elementClass, null, null, null, setterFactory));
    }
    return list;
  }

  @SuppressWarnings("unchecked")
  private Map<?, ?> fromMapValue(
      FieldType keyType,
      FieldType valueType,
      Map<?, ?> map,
      Type keyClass,
      Type valueClass,
      FieldValueSetterFactory setterFactory) {
    Map newMap = Maps.newHashMap();
    for (Map.Entry<?, ?> entry : map.entrySet()) {
      Object key = fromValue(keyType, entry.getKey(), keyClass, null, null, null, setterFactory);
      Object value =
          fromValue(valueType, entry.getValue(), valueClass, null, null, null, setterFactory);
      newMap.put(key, value);
    }
    return newMap;
  }
}
