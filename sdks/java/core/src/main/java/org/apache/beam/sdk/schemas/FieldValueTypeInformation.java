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

import com.google.auto.value.AutoValue;
import java.io.Serializable;
import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import org.apache.beam.sdk.schemas.logicaltypes.OneOfType;
import org.apache.beam.sdk.schemas.utils.ReflectUtils;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.checkerframework.checker.nullness.qual.Nullable;

/** Represents type information for a Java type that will be used to infer a Schema type. */
@AutoValue
public abstract class FieldValueTypeInformation implements Serializable {
  /** Returns the field name. */
  public abstract String getName();

  /** Returns whether the field is nullable. */
  public abstract boolean isNullable();

  /** Returns the field type. */
  public abstract TypeDescriptor getType();

  /** Returns the raw class type. */
  public abstract Class getRawType();

  public abstract @Nullable Field getField();

  public abstract @Nullable Method getMethod();

  public abstract Map<String, FieldValueTypeInformation> getOneOfTypes();

  /** If the field is a container type, returns the element type. */
  public abstract @Nullable FieldValueTypeInformation getElementType();

  /** If the field is a map type, returns the key type. */
  public abstract @Nullable FieldValueTypeInformation getMapKeyType();

  /** If the field is a map type, returns the key type. */
  public abstract @Nullable FieldValueTypeInformation getMapValueType();

  abstract Builder toBuilder();

  @AutoValue.Builder
  public abstract static class Builder {
    public abstract Builder setName(String name);

    public abstract Builder setNullable(boolean nullable);

    public abstract Builder setType(TypeDescriptor type);

    public abstract Builder setRawType(Class type);

    public abstract Builder setField(@Nullable Field field);

    public abstract Builder setMethod(@Nullable Method method);

    public abstract Builder setOneOfTypes(Map<String, FieldValueTypeInformation> oneOfTypes);

    public abstract Builder setElementType(@Nullable FieldValueTypeInformation elementType);

    public abstract Builder setMapKeyType(@Nullable FieldValueTypeInformation mapKeyType);

    public abstract Builder setMapValueType(@Nullable FieldValueTypeInformation mapValueType);

    abstract FieldValueTypeInformation build();
  }

  public static FieldValueTypeInformation forOneOf(
      String name, boolean nullable, Map<String, FieldValueTypeInformation> oneOfTypes) {
    final TypeDescriptor<OneOfType.Value> typeDescriptor = TypeDescriptor.of(OneOfType.Value.class);
    return new AutoValue_FieldValueTypeInformation.Builder()
        .setName(name)
        .setNullable(nullable)
        .setType(typeDescriptor)
        .setRawType(typeDescriptor.getRawType())
        .setField(null)
        .setElementType(null)
        .setMapKeyType(null)
        .setMapValueType(null)
        .setOneOfTypes(oneOfTypes)
        .build();
  }

  public static FieldValueTypeInformation forField(Field field) {
    TypeDescriptor type = TypeDescriptor.of(field.getGenericType());
    return new AutoValue_FieldValueTypeInformation.Builder()
        .setName(field.getName())
        .setNullable(hasNullableAnnotation(field))
        .setType(type)
        .setRawType(type.getRawType())
        .setField(field)
        .setElementType(getIterableComponentType(field))
        .setMapKeyType(getMapKeyType(field))
        .setMapValueType(getMapValueType(field))
        .setOneOfTypes(Collections.emptyMap())
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
    boolean nullable = hasNullableReturnType(method);
    return new AutoValue_FieldValueTypeInformation.Builder()
        .setName(name)
        .setNullable(nullable)
        .setType(type)
        .setRawType(type.getRawType())
        .setMethod(method)
        .setElementType(getIterableComponentType(type))
        .setMapKeyType(getMapKeyType(type))
        .setMapValueType(getMapValueType(type))
        .setOneOfTypes(Collections.emptyMap())
        .build();
  }

  private static boolean hasNullableAnnotation(Field field) {
    for (Annotation annotation : field.getAnnotations()) {
      if (isNullableAnnotation(annotation)) {
        return true;
      }
    }

    for (Annotation annotation : field.getAnnotatedType().getAnnotations()) {
      if (isNullableAnnotation(annotation)) {
        return true;
      }
    }

    return false;
  }

  /**
   * If the method or its return type are annotated with any variant of Nullable, then the schema
   * field is nullable.
   */
  private static boolean hasNullableReturnType(Method method) {
    for (Annotation annotation : method.getAnnotations()) {
      if (isNullableAnnotation(annotation)) {
        return true;
      }
    }

    for (Annotation annotation : method.getAnnotatedReturnType().getAnnotations()) {
      if (isNullableAnnotation(annotation)) {
        return true;
      }
    }

    return false;
  }

  /** Try to accept any Nullable annotation. */
  private static boolean isNullableAnnotation(Annotation annotation) {
    return annotation.annotationType().getSimpleName().equals("Nullable");
  }

  public static FieldValueTypeInformation forSetter(Method method) {
    return forSetter(method, "set");
  }

  public static FieldValueTypeInformation forSetter(Method method, String setterPrefix) {
    String name;
    if (method.getName().startsWith(setterPrefix)) {
      name = ReflectUtils.stripPrefix(method.getName(), setterPrefix);
    } else {
      throw new RuntimeException("Setter has wrong prefix " + method.getName());
    }
    if (method.getParameterCount() != 1) {
      throw new RuntimeException("Setter methods should take a single argument.");
    }
    TypeDescriptor type = TypeDescriptor.of(method.getGenericParameterTypes()[0]);
    boolean nullable =
        Arrays.stream(method.getParameters()[0].getAnnotatedType().getAnnotations())
            .anyMatch(annotation -> isNullableAnnotation(annotation));
    return new AutoValue_FieldValueTypeInformation.Builder()
        .setName(name)
        .setNullable(nullable)
        .setType(type)
        .setRawType(type.getRawType())
        .setMethod(method)
        .setElementType(getIterableComponentType(type))
        .setMapKeyType(getMapKeyType(type))
        .setMapValueType(getMapValueType(type))
        .setOneOfTypes(Collections.emptyMap())
        .build();
  }

  public FieldValueTypeInformation withName(String name) {
    return toBuilder().setName(name).build();
  }

  private static FieldValueTypeInformation getIterableComponentType(Field field) {
    return getIterableComponentType(TypeDescriptor.of(field.getGenericType()));
  }

  static @Nullable FieldValueTypeInformation getIterableComponentType(TypeDescriptor valueType) {
    // TODO: Figure out nullable elements.
    TypeDescriptor componentType = ReflectUtils.getIterableComponentType(valueType);
    if (componentType == null) {
      return null;
    }

    return new AutoValue_FieldValueTypeInformation.Builder()
        .setName("")
        .setNullable(false)
        .setType(componentType)
        .setRawType(componentType.getRawType())
        .setElementType(getIterableComponentType(componentType))
        .setMapKeyType(getMapKeyType(componentType))
        .setMapValueType(getMapValueType(componentType))
        .setOneOfTypes(Collections.emptyMap())
        .build();
  }

  // If the Field is a map type, returns the key type, otherwise returns a null reference.

  private static @Nullable FieldValueTypeInformation getMapKeyType(Field field) {
    return getMapKeyType(TypeDescriptor.of(field.getGenericType()));
  }

  private static @Nullable FieldValueTypeInformation getMapKeyType(
      TypeDescriptor<?> typeDescriptor) {
    return getMapType(typeDescriptor, 0);
  }

  // If the Field is a map type, returns the value type, otherwise returns a null reference.

  private static @Nullable FieldValueTypeInformation getMapValueType(Field field) {
    return getMapType(TypeDescriptor.of(field.getGenericType()), 1);
  }

  private static @Nullable FieldValueTypeInformation getMapValueType(
      TypeDescriptor typeDescriptor) {
    return getMapType(typeDescriptor, 1);
  }

  // If the Field is a map type, returns the key or value type (0 is key type, 1 is value).
  // Otherwise returns a null reference.
  @SuppressWarnings("unchecked")
  private static @Nullable FieldValueTypeInformation getMapType(
      TypeDescriptor valueType, int index) {
    TypeDescriptor mapType = ReflectUtils.getMapType(valueType, index);
    if (mapType == null) {
      return null;
    }
    return new AutoValue_FieldValueTypeInformation.Builder()
        .setName("")
        .setNullable(false)
        .setType(mapType)
        .setRawType(mapType.getRawType())
        .setElementType(getIterableComponentType(mapType))
        .setMapKeyType(getMapKeyType(mapType))
        .setMapValueType(getMapValueType(mapType))
        .setOneOfTypes(Collections.emptyMap())
        .build();
  }
}
