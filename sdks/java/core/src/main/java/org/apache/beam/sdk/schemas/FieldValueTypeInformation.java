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

import static com.google.common.base.Preconditions.checkArgument;

import com.google.auto.value.AutoValue;
import java.io.Serializable;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.beam.sdk.schemas.utils.ReflectUtils;
import org.apache.beam.sdk.values.TypeDescriptor;

/** Represents type information for a Java type that will be used to infer a Schema type. */
@AutoValue
public abstract class FieldValueTypeInformation implements Serializable {
  /** Returns the field name. */
  public abstract String getName();

  /** Returns whether the field is nullable. */
  public abstract boolean isNullable();

  /** Returns the field type. */
  public abstract TypeDescriptor getType();

  @Nullable
  public abstract Field getField();

  @Nullable
  public abstract Method getMethod();

  /** If the field is a container type, returns the element type. */
  @Nullable
  public abstract Type getElementType();

  /** If the field is a map type, returns the key type. */
  @Nullable
  public abstract Type getMapKeyType();

  /** If the field is a map type, returns the key type. */
  @Nullable
  public abstract Type getMapValueType();

  abstract Builder toBuilder();

  @AutoValue.Builder
  abstract static class Builder {
    public abstract Builder setName(String name);

    public abstract Builder setNullable(boolean nullable);

    public abstract Builder setType(TypeDescriptor type);

    public abstract Builder setField(@Nullable Field field);

    public abstract Builder setMethod(@Nullable Method method);

    public abstract Builder setElementType(@Nullable Type elementType);

    public abstract Builder setMapKeyType(@Nullable Type mapKeyType);

    public abstract Builder setMapValueType(@Nullable Type mapValueType);

    abstract FieldValueTypeInformation build();
  }

  public static FieldValueTypeInformation forField(Field field) {
    return new AutoValue_FieldValueTypeInformation.Builder()
        .setName(field.getName())
        .setNullable(field.isAnnotationPresent(Nullable.class))
        .setType(TypeDescriptor.of(field.getGenericType()))
        .setField(field)
        .setElementType(getArrayComponentType(field))
        .setMapKeyType(getMapKeyType(field))
        .setMapValueType(getMapValueType(field))
        .build();
  }

  public static FieldValueTypeInformation forGetter(Method method) {
    String name;
    if (method.getName().startsWith("get")) {
      name = ReflectUtils.stripPrefix(method.getName(), "get");
    } else if (method.getName().startsWith("is")) {
      name = ReflectUtils.stripPrefix(method.getName(), "is");
    } else {
      throw new RuntimeException("Getter has wrong prefix " + method.getName());
    }

    TypeDescriptor type = TypeDescriptor.of(method.getGenericReturnType());
    boolean nullable = method.isAnnotationPresent(Nullable.class);
    return new AutoValue_FieldValueTypeInformation.Builder()
        .setName(name)
        .setNullable(nullable)
        .setType(type)
        .setMethod(method)
        .setElementType(getArrayComponentType(type))
        .setMapKeyType(getMapKeyType(type))
        .setMapValueType(getMapValueType(type))
        .build();
  }

  public static FieldValueTypeInformation forSetter(Method method) {
    String name;
    if (method.getName().startsWith("set")) {
      name = ReflectUtils.stripPrefix(method.getName(), "set");
    } else {
      throw new RuntimeException("Setter has wrong prefix " + method.getName());
    }
    if (method.getParameterCount() != 1) {
      throw new RuntimeException("Setter methods should take a single argument.");
    }
    TypeDescriptor type = TypeDescriptor.of(method.getGenericParameterTypes()[0]);
    boolean nullable =
        Arrays.stream(method.getParameterAnnotations()[0]).anyMatch(Nullable.class::isInstance);
    return new AutoValue_FieldValueTypeInformation.Builder()
        .setName(name)
        .setNullable(nullable)
        .setType(type)
        .setMethod(method)
        .setElementType(getArrayComponentType(type))
        .setMapKeyType(getMapKeyType(type))
        .setMapValueType(getMapValueType(type))
        .build();
  }

  public FieldValueTypeInformation withName(String name) {
    return toBuilder().setName(name).build();
  }

  private static Type getArrayComponentType(Field field) {
    return getArrayComponentType(TypeDescriptor.of(field.getGenericType()));
  }

  @Nullable
  private static Type getArrayComponentType(TypeDescriptor valueType) {
    if (valueType.isArray()) {
      Type component = valueType.getComponentType().getType();
      if (!component.equals(byte.class)) {
        return component;
      }
    } else if (valueType.isSubtypeOf(TypeDescriptor.of(Collection.class))) {
      TypeDescriptor<Collection<?>> collection = valueType.getSupertype(Collection.class);
      if (collection.getType() instanceof ParameterizedType) {
        ParameterizedType ptype = (ParameterizedType) collection.getType();
        java.lang.reflect.Type[] params = ptype.getActualTypeArguments();
        checkArgument(params.length == 1);
        return params[0];
      } else {
        throw new RuntimeException("Collection parameter is not parameterized!");
      }
    }
    return null;
  }

  public Class getRawType() {
    return getType().getRawType();
  }

  // If the Field is a map type, returns the key type, otherwise returns a null reference.
  @Nullable
  private static Type getMapKeyType(Field field) {
    return getMapKeyType(TypeDescriptor.of(field.getGenericType()));
  }

  @Nullable
  private static Type getMapKeyType(TypeDescriptor<?> typeDescriptor) {
    return getMapType(typeDescriptor, 0);
  }

  // If the Field is a map type, returns the value type, otherwise returns a null reference.
  @Nullable
  private static Type getMapValueType(Field field) {
    return getMapType(TypeDescriptor.of(field.getGenericType()), 1);
  }

  @Nullable
  private static Type getMapValueType(TypeDescriptor typeDescriptor) {
    return getMapType(typeDescriptor, 1);
  }

  // If the Field is a map type, returns the key or value type (0 is key type, 1 is value).
  // Otherwise returns a null reference.
  @SuppressWarnings("unchecked")
  @Nullable
  private static Type getMapType(TypeDescriptor valueType, int index) {
    if (valueType.isSubtypeOf(TypeDescriptor.of(Map.class))) {
      TypeDescriptor<Collection<?>> map = valueType.getSupertype(Map.class);
      if (map.getType() instanceof ParameterizedType) {
        ParameterizedType ptype = (ParameterizedType) map.getType();
        java.lang.reflect.Type[] params = ptype.getActualTypeArguments();
        return params[index];
      } else {
        throw new RuntimeException("Map type is not parameterized! " + map);
      }
    }
    return null;
  }
}
