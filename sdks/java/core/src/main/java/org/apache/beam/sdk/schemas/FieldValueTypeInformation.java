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

import java.io.Serializable;
import java.lang.reflect.Field;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.Collection;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.beam.sdk.schemas.utils.StaticSchemaInference.TypeInformation;
import org.apache.beam.sdk.values.TypeDescriptor;

/** Represents type information for a schema field. */
public class FieldValueTypeInformation implements Serializable {
  private final String name;
  private final Class type;
  @Nullable private final Type elementType;
  @Nullable private final Type mapKeyType;
  @Nullable private final Type mapValueType;

  public FieldValueTypeInformation(Field field) {
    this(
        field.getName(),
        field.getType(),
        getArrayComponentType(field),
        getMapKeyType(field),
        getMapValueType(field));
  }

  public FieldValueTypeInformation(TypeInformation typeInformation) {
    this(
        typeInformation.getName(),
        typeInformation.getType().getRawType(),
        getArrayComponentType(typeInformation),
        getMapKeyType(typeInformation),
        getMapValueType(typeInformation));
  }

  public FieldValueTypeInformation(
      String name,
      Class type,
      @Nullable Type elementType,
      @Nullable Type mapKeyType,
      @Nullable Type mapValueType) {
    this.name = name;
    this.type = type;
    this.elementType = elementType;
    this.mapKeyType = mapKeyType;
    this.mapValueType = mapValueType;
  }

  private static Type getArrayComponentType(TypeInformation typeInformation) {
    return getArrayComponentType(typeInformation.getType());
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

  // If the Field is a map type, returns the key type, otherwise returns a null reference.
  @Nullable
  private static Type getMapKeyType(Field field) {
    return getMapType(TypeDescriptor.of(field.getGenericType()), 0);
  }

  @Nullable
  private static Type getMapKeyType(TypeInformation typeInformation) {
    return getMapType(typeInformation.getType(), 0);
  }

  // If the Field is a map type, returns the value type, otherwise returns a null reference.
  @Nullable
  private static Type getMapValueType(Field field) {
    return getMapType(TypeDescriptor.of(field.getGenericType()), 1);
  }

  @Nullable
  private static Type getMapValueType(TypeInformation typeInformation) {
    return getMapType(typeInformation.getType(), 1);
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

  /** Returns the field name. */
  public String name() {
    return name;
  }

  /** Returns the field type. */
  public Class type() {
    return type;
  }

  /** If the field is a container type, returns the element type. */
  @Nullable
  public Type elementType() {
    return elementType;
  }

  /** If the field is a map type, returns the key type. */
  @Nullable
  public Type mapKeyType() {
    return mapKeyType;
  }

  /** If the field is a map type, returns the key type. */
  @Nullable
  public Type mapValueType() {
    return mapValueType;
  }
}
