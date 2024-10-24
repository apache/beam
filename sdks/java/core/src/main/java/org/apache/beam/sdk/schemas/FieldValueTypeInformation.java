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
import java.lang.reflect.AnnotatedElement;
import java.lang.reflect.Field;
import java.lang.reflect.Member;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;
import org.apache.beam.sdk.schemas.annotations.SchemaCaseFormat;
import org.apache.beam.sdk.schemas.annotations.SchemaFieldDescription;
import org.apache.beam.sdk.schemas.annotations.SchemaFieldName;
import org.apache.beam.sdk.schemas.annotations.SchemaFieldNumber;
import org.apache.beam.sdk.schemas.logicaltypes.OneOfType;
import org.apache.beam.sdk.schemas.utils.ReflectUtils;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.CaseFormat;
import org.checkerframework.checker.nullness.qual.Nullable;

/** Represents type information for a Java type that will be used to infer a Schema type. */
@AutoValue
public abstract class FieldValueTypeInformation implements Serializable {
  /** Optionally returns the field index. */
  public abstract @Nullable Integer getNumber();

  /** Returns the field name. */
  public abstract String getName();

  /** Returns whether the field is nullable. */
  public abstract boolean isNullable();

  /** Returns the field type. */
  public abstract TypeDescriptor<?> getType();

  /** Returns the raw class type. */
  public abstract Class<?> getRawType();

  public abstract @Nullable Field getField();

  public abstract @Nullable Method getMethod();

  public abstract Map<String, FieldValueTypeInformation> getOneOfTypes();

  /** If the field is a container type, returns the element type. */
  public abstract @Nullable FieldValueTypeInformation getElementType();

  /** If the field is a map type, returns the key type. */
  public abstract @Nullable FieldValueTypeInformation getMapKeyType();

  /** If the field is a map type, returns the key type. */
  public abstract @Nullable FieldValueTypeInformation getMapValueType();

  /** If the field has a description, returns the description for the field. */
  public abstract @Nullable String getDescription();

  abstract Builder toBuilder();

  @AutoValue.Builder
  public abstract static class Builder {
    public abstract Builder setNumber(@Nullable Integer number);

    public abstract Builder setName(String name);

    public abstract Builder setNullable(boolean nullable);

    public abstract Builder setType(TypeDescriptor<?> type);

    public abstract Builder setRawType(Class<?> type);

    public abstract Builder setField(@Nullable Field field);

    public abstract Builder setMethod(@Nullable Method method);

    public abstract Builder setOneOfTypes(Map<String, FieldValueTypeInformation> oneOfTypes);

    public abstract Builder setElementType(@Nullable FieldValueTypeInformation elementType);

    public abstract Builder setMapKeyType(@Nullable FieldValueTypeInformation mapKeyType);

    public abstract Builder setMapValueType(@Nullable FieldValueTypeInformation mapValueType);

    public abstract Builder setDescription(@Nullable String fieldDescription);

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

  public static FieldValueTypeInformation forField(Field field, int index) {
    return forField(null, field, index);
  }

  public static FieldValueTypeInformation forField(
      @Nullable TypeDescriptor<?> typeDescriptor, Field field, int index) {
    TypeDescriptor<?> type =
        Optional.ofNullable(typeDescriptor)
            .map(td -> (TypeDescriptor) td.resolveType(field.getGenericType()))
            // fall back to previous behavior
            .orElseGet(() -> TypeDescriptor.of(field.getGenericType()));
    return new AutoValue_FieldValueTypeInformation.Builder()
        .setName(getNameOverride(field.getName(), field))
        .setNumber(getNumberOverride(index, field))
        .setNullable(hasNullableAnnotation(field))
        .setType(type)
        .setRawType(type.getRawType())
        .setField(field)
        .setElementType(getIterableComponentType(type))
        .setMapKeyType(getMapKeyType(type))
        .setMapValueType(getMapValueType(type))
        .setOneOfTypes(Collections.emptyMap())
        .setDescription(getFieldDescription(field))
        .build();
  }

  public static <T extends AnnotatedElement & Member> int getNumberOverride(int index, T member) {
    @Nullable SchemaFieldNumber fieldNumber = member.getAnnotation(SchemaFieldNumber.class);
    if (fieldNumber == null) {
      return index;
    }
    return Integer.parseInt(fieldNumber.value());
  }

  public static <T extends AnnotatedElement & Member> String getNameOverride(
      String original, T member) {
    @Nullable SchemaFieldName fieldName = member.getAnnotation(SchemaFieldName.class);
    @Nullable SchemaCaseFormat caseFormatAnnotation = member.getAnnotation(SchemaCaseFormat.class);
    @Nullable
    SchemaCaseFormat classCaseFormatAnnotation =
        member.getDeclaringClass().getAnnotation(SchemaCaseFormat.class);
    if (fieldName != null) {
      if (caseFormatAnnotation != null) {
        throw new RuntimeException(
            String.format(
                "Cannot define both @SchemaFieldName and @SchemaCaseFormat. From member '%s'.",
                member.getName()));
      }
      return fieldName.value();
    } else if (caseFormatAnnotation != null) {
      return CaseFormat.LOWER_CAMEL.to(caseFormatAnnotation.value(), original);
    } else if (classCaseFormatAnnotation != null) {
      return CaseFormat.LOWER_CAMEL.to(classCaseFormatAnnotation.value(), original);
    } else {
      return original;
    }
  }

  public static <T extends AnnotatedElement & Member> @Nullable String getFieldDescription(
      T member) {
    @Nullable
    SchemaFieldDescription fieldDescription = member.getAnnotation(SchemaFieldDescription.class);
    if (fieldDescription == null) {
      return null;
    }
    return fieldDescription.value();
  }

  public static FieldValueTypeInformation forGetter(Method method, int index) {
    return forGetter(null, method, index);
  }

  public static FieldValueTypeInformation forGetter(
      @Nullable TypeDescriptor<?> typeDescriptor, Method method, int index) {
    String name;
    if (method.getName().startsWith("get")) {
      name = ReflectUtils.stripPrefix(method.getName(), "get");
    } else if (method.getName().startsWith("is")) {
      name = ReflectUtils.stripPrefix(method.getName(), "is");
    } else {
      throw new RuntimeException("Getter has wrong prefix " + method.getName());
    }

    TypeDescriptor<?> type =
        Optional.ofNullable(typeDescriptor)
            .map(td -> (TypeDescriptor) td.resolveType(method.getGenericReturnType()))
            // fall back to previous behavior
            .orElseGet(() -> TypeDescriptor.of(method.getGenericReturnType()));

    boolean nullable = hasNullableReturnType(method);
    return new AutoValue_FieldValueTypeInformation.Builder()
        .setName(getNameOverride(name, method))
        .setNumber(getNumberOverride(index, method))
        .setNullable(nullable)
        .setType(type)
        .setRawType(type.getRawType())
        .setMethod(method)
        .setElementType(getIterableComponentType(type))
        .setMapKeyType(getMapKeyType(type))
        .setMapValueType(getMapValueType(type))
        .setOneOfTypes(Collections.emptyMap())
        .setDescription(getFieldDescription(method))
        .build();
  }

  private static boolean hasNullableAnnotation(Field field) {
    Stream<Annotation> annotations =
        Stream.concat(
            Stream.of(field.getAnnotations()),
            Stream.of(field.getAnnotatedType().getAnnotations()));

    return annotations.anyMatch(FieldValueTypeInformation::isNullableAnnotation);
  }

  /**
   * If the method or its return type are annotated with any variant of Nullable, then the schema
   * field is nullable.
   */
  private static boolean hasNullableReturnType(Method method) {
    Stream<Annotation> annotations =
        Stream.concat(
            Stream.of(method.getAnnotations()),
            Stream.of(method.getAnnotatedReturnType().getAnnotations()));

    return annotations.anyMatch(FieldValueTypeInformation::isNullableAnnotation);
  }

  private static boolean hasSingleNullableParameter(Method method) {
    if (method.getParameterCount() != 1) {
      throw new RuntimeException(
          "Setter methods should take a single argument " + method.getName());
    }

    Stream<Annotation> annotations =
        Stream.concat(
            Arrays.stream(method.getAnnotatedParameterTypes()[0].getAnnotations()),
            Arrays.stream(method.getParameterAnnotations()[0]));

    return annotations.anyMatch(FieldValueTypeInformation::isNullableAnnotation);
  }

  /** Try to accept any Nullable annotation. */
  private static boolean isNullableAnnotation(Annotation annotation) {
    return annotation.annotationType().getSimpleName().equals("Nullable");
  }

  public static FieldValueTypeInformation forSetter(Method method) {
    return forSetter(null, method);
  }

  public static FieldValueTypeInformation forSetter(Method method, String setterPrefix) {
    return forSetter(null, method, setterPrefix);
  }

  public static FieldValueTypeInformation forSetter(
      @Nullable TypeDescriptor<?> typeDescriptor, Method method) {
    return forSetter(typeDescriptor, method, "set");
  }

  public static FieldValueTypeInformation forSetter(
      @Nullable TypeDescriptor<?> typeDescriptor, Method method, String setterPrefix) {
    String name;
    if (method.getName().startsWith(setterPrefix)) {
      name = ReflectUtils.stripPrefix(method.getName(), setterPrefix);
    } else {
      throw new RuntimeException("Setter has wrong prefix " + method.getName());
    }

    TypeDescriptor<?> type =
        Optional.ofNullable(typeDescriptor)
            .map(td -> (TypeDescriptor) td.resolveType(method.getGenericParameterTypes()[0]))
            // fall back to previous behavior
            .orElseGet(() -> TypeDescriptor.of(method.getGenericParameterTypes()[0]));
    boolean nullable = hasSingleNullableParameter(method);
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

  static @Nullable FieldValueTypeInformation getIterableComponentType(TypeDescriptor<?> valueType) {
    // TODO: Figure out nullable elements.
    TypeDescriptor<?> componentType = ReflectUtils.getIterableComponentType(valueType);
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

  // If the type is a map type, returns the key type, otherwise returns a null reference.
  private static @Nullable FieldValueTypeInformation getMapKeyType(
      TypeDescriptor<?> typeDescriptor) {
    return getMapType(typeDescriptor, 0);
  }

  // If the type is a map type, returns the value type, otherwise returns a null reference.
  private static @Nullable FieldValueTypeInformation getMapValueType(
      TypeDescriptor<?> typeDescriptor) {
    return getMapType(typeDescriptor, 1);
  }

  // If the Field is a map type, returns the key or value type (0 is key type, 1 is value).
  // Otherwise returns a null reference.
  private static @Nullable FieldValueTypeInformation getMapType(
      TypeDescriptor<?> valueType, int index) {
    TypeDescriptor<?> mapType = ReflectUtils.getMapType(valueType, index);
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
