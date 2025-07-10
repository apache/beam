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
package org.apache.beam.sdk.schemas.utils;

import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkArgument;

import java.lang.reflect.ParameterizedType;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.beam.sdk.schemas.FieldValueTypeInformation;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.schemas.logicaltypes.EnumerationType;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.joda.time.ReadableInstant;

/** A set of utilities for inferring a Beam {@link Schema} from static Java types. */
@SuppressWarnings({
  "nullness", // TODO(https://github.com/apache/beam/issues/20497)
  "rawtypes"
})
public class StaticSchemaInference {
  public static List<FieldValueTypeInformation> sortBySchema(
      List<FieldValueTypeInformation> types, Schema schema) {
    Map<String, FieldValueTypeInformation> typeMap =
        types.stream()
            .collect(Collectors.toMap(FieldValueTypeInformation::getName, Function.identity()));
    return schema.getFields().stream()
        .map(f -> typeMap.get(f.getName()))
        .collect(Collectors.toList());
  }

  enum MethodType {
    GETTER,
    SETTER
  }

  private static final Map<Class, FieldType> PRIMITIVE_TYPES =
      ImmutableMap.<Class, FieldType>builder()
          .put(Byte.class, FieldType.BYTE)
          .put(byte.class, FieldType.BYTE)
          .put(Short.class, FieldType.INT16)
          .put(short.class, FieldType.INT16)
          .put(Integer.class, FieldType.INT32)
          .put(int.class, FieldType.INT32)
          .put(Long.class, FieldType.INT64)
          .put(long.class, FieldType.INT64)
          .put(Float.class, FieldType.FLOAT)
          .put(float.class, FieldType.FLOAT)
          .put(Double.class, FieldType.DOUBLE)
          .put(double.class, FieldType.DOUBLE)
          .put(Boolean.class, FieldType.BOOLEAN)
          .put(boolean.class, FieldType.BOOLEAN)
          .put(BigDecimal.class, FieldType.DECIMAL)
          .build();

  /**
   * Infer a schema from a Java class.
   *
   * <p>Takes in a function to extract a list of field types from a class. Different callers may
   * have different strategies for extracting this list: e.g. introspecting public member variables,
   * public getter methods, or special annotations on the class.
   */
  public static Schema schemaFromClass(
      TypeDescriptor<?> typeDescriptor, FieldValueTypeSupplier fieldValueTypeSupplier) {
    return schemaFromClass(typeDescriptor, fieldValueTypeSupplier, new HashMap<>());
  }

  private static Schema schemaFromClass(
      TypeDescriptor<?> typeDescriptor,
      FieldValueTypeSupplier fieldValueTypeSupplier,
      Map<TypeDescriptor<?>, Schema> alreadyVisitedSchemas) {
    if (alreadyVisitedSchemas.containsKey(typeDescriptor)) {
      Schema existingSchema = alreadyVisitedSchemas.get(typeDescriptor);
      if (existingSchema == null) {
        throw new IllegalArgumentException(
            "Cannot infer schema with a circular reference. Class: "
                + typeDescriptor.getRawType().getTypeName());
      }
      return existingSchema;
    }
    alreadyVisitedSchemas.put(typeDescriptor, null);
    Schema.Builder builder = Schema.builder();
    for (FieldValueTypeInformation type : fieldValueTypeSupplier.get(typeDescriptor)) {
      Schema.FieldType fieldType =
          fieldFromType(type.getType(), fieldValueTypeSupplier, alreadyVisitedSchemas);
      Schema.Field f =
          type.isNullable()
              ? Schema.Field.nullable(type.getName(), fieldType)
              : Schema.Field.of(type.getName(), fieldType);
      if (type.getDescription() != null) {
        f = f.withDescription(type.getDescription());
      }
      builder.addFields(f);
    }
    Schema generatedSchema = builder.build();
    alreadyVisitedSchemas.replace(typeDescriptor, generatedSchema);
    return generatedSchema;
  }

  /** Map a Java field type to a Beam Schema FieldType. */
  public static Schema.FieldType fieldFromType(
      TypeDescriptor type, FieldValueTypeSupplier fieldValueTypeSupplier) {
    return fieldFromType(type, fieldValueTypeSupplier, new HashMap<>());
  }

  // TODO(https://github.com/apache/beam/issues/21567): support type inference for logical types
  private static Schema.FieldType fieldFromType(
      TypeDescriptor type,
      FieldValueTypeSupplier fieldValueTypeSupplier,
      Map<TypeDescriptor<?>, Schema> alreadyVisitedSchemas) {
    FieldType primitiveType = PRIMITIVE_TYPES.get(type.getRawType());
    if (primitiveType != null) {
      return primitiveType;
    }

    if (type.getRawType().isEnum()) {
      Map<String, Integer> enumValues =
          Arrays.stream(type.getRawType().getEnumConstants())
              .map(Enum.class::cast)
              .collect(Collectors.toMap(Enum::toString, Enum::ordinal));
      return FieldType.logicalType(EnumerationType.create(enumValues));
    }
    if (type.isArray()) {
      // If the type is T[] where T is byte, this is a BYTES type.
      TypeDescriptor component = type.getComponentType();
      if (component.getRawType().equals(byte.class)) {
        return FieldType.BYTES;
      } else {
        // Otherwise this is an array type.
        return FieldType.array(
            fieldFromType(component, fieldValueTypeSupplier, alreadyVisitedSchemas));
      }
    } else if (type.isSubtypeOf(TypeDescriptor.of(Map.class))) {
      TypeDescriptor<Collection<?>> map = type.getSupertype(Map.class);
      if (map.getType() instanceof ParameterizedType) {
        ParameterizedType ptype = (ParameterizedType) map.getType();
        java.lang.reflect.Type[] params = ptype.getActualTypeArguments();
        checkArgument(params.length == 2);
        FieldType keyType =
            fieldFromType(
                TypeDescriptor.of(params[0]), fieldValueTypeSupplier, alreadyVisitedSchemas);
        FieldType valueType =
            fieldFromType(
                TypeDescriptor.of(params[1]), fieldValueTypeSupplier, alreadyVisitedSchemas);
        checkArgument(
            keyType.getTypeName().isPrimitiveType(),
            "Only primitive types can be map keys. type: " + keyType.getTypeName());
        return FieldType.map(keyType, valueType);
      } else {
        throw new RuntimeException("Cannot infer schema from unparameterized map.");
      }
    } else if (type.isSubtypeOf(TypeDescriptor.of(CharSequence.class))) {
      return FieldType.STRING;
    } else if (type.isSubtypeOf(TypeDescriptor.of(ReadableInstant.class))) {
      return FieldType.DATETIME;
    } else if (type.isSubtypeOf(TypeDescriptor.of(ByteBuffer.class))) {
      return FieldType.BYTES;
    } else if (type.isSubtypeOf(TypeDescriptor.of(Iterable.class))) {
      TypeDescriptor<Iterable<?>> iterable = type.getSupertype(Iterable.class);
      if (iterable.getType() instanceof ParameterizedType) {
        ParameterizedType ptype = (ParameterizedType) iterable.getType();
        java.lang.reflect.Type[] params = ptype.getActualTypeArguments();
        checkArgument(params.length == 1);
        // TODO: should this be AbstractCollection?
        if (type.isSubtypeOf(TypeDescriptor.of(Collection.class))) {
          return FieldType.array(
              fieldFromType(
                  TypeDescriptor.of(params[0]), fieldValueTypeSupplier, alreadyVisitedSchemas));
        } else {
          return FieldType.iterable(
              fieldFromType(
                  TypeDescriptor.of(params[0]), fieldValueTypeSupplier, alreadyVisitedSchemas));
        }
      } else {
        throw new RuntimeException("Cannot infer schema from unparameterized collection.");
      }
    } else {
      return FieldType.row(schemaFromClass(type, fieldValueTypeSupplier, alreadyVisitedSchemas));
    }
  }
}
