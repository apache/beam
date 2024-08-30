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

import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkArgument;

import java.lang.reflect.ParameterizedType;
import java.util.Collection;
import java.util.Map;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.schemas.Schema.TypeName;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.BiMap;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableBiMap;
import org.joda.time.Instant;

/**
 * Utilities for converting between {@link Schema} field types and {@link TypeDescriptor}s that
 * define Java objects which can represent these field types.
 */
@SuppressWarnings({
  "nullness", // TODO(https://github.com/apache/beam/issues/20497)
  "rawtypes"
})
public class FieldTypeDescriptors {
  private static final BiMap<TypeName, TypeDescriptor> PRIMITIVE_MAPPING =
      ImmutableBiMap.<TypeName, TypeDescriptor>builder()
          .put(TypeName.BYTE, TypeDescriptors.bytes())
          .put(TypeName.INT16, TypeDescriptors.shorts())
          .put(TypeName.INT32, TypeDescriptors.integers())
          .put(TypeName.INT64, TypeDescriptors.longs())
          .put(TypeName.DECIMAL, TypeDescriptors.bigdecimals())
          .put(TypeName.FLOAT, TypeDescriptors.floats())
          .put(TypeName.DOUBLE, TypeDescriptors.doubles())
          .put(TypeName.STRING, TypeDescriptors.strings())
          .put(TypeName.DATETIME, TypeDescriptor.of(Instant.class))
          .put(TypeName.BOOLEAN, TypeDescriptors.booleans())
          .put(TypeName.BYTES, TypeDescriptor.of(byte[].class))
          .build();
  /** Get a {@link TypeDescriptor} from a {@link FieldType}. */
  public static TypeDescriptor javaTypeForFieldType(FieldType fieldType) {
    switch (fieldType.getTypeName()) {
      case LOGICAL_TYPE:
        // TODO: shouldn't we handle this differently?
        return javaTypeForFieldType(fieldType.getLogicalType().getBaseType());
      case ARRAY:
        return TypeDescriptors.lists(javaTypeForFieldType(fieldType.getCollectionElementType()));
      case ITERABLE:
        return TypeDescriptors.iterables(
            javaTypeForFieldType(fieldType.getCollectionElementType()));
      case MAP:
        return TypeDescriptors.maps(
            javaTypeForFieldType(fieldType.getMapKeyType()),
            javaTypeForFieldType(fieldType.getMapValueType()));
      case ROW:
        return TypeDescriptors.rows();
      default:
        return PRIMITIVE_MAPPING.get(fieldType.getTypeName());
    }
  }
  /** Get a {@link FieldType} from a {@link TypeDescriptor}. */
  public static FieldType fieldTypeForJavaType(TypeDescriptor typeDescriptor) {
    // TODO: Convert for registered logical types.
    if (typeDescriptor.isArray()
        || typeDescriptor.isSubtypeOf(TypeDescriptor.of(Collection.class))) {
      return getArrayFieldType(typeDescriptor);
    } else if (typeDescriptor.isSubtypeOf(TypeDescriptor.of(Map.class))) {
      return getMapFieldType(typeDescriptor);
    } else if (typeDescriptor.isSubtypeOf(TypeDescriptor.of(Iterable.class))) {
      return getIterableFieldType(typeDescriptor);
    } else if (typeDescriptor.isSubtypeOf(TypeDescriptor.of(Row.class))) {
      throw new IllegalArgumentException(
          "Cannot automatically determine a field type from a Row class"
              + " as we cannot determine the schema. You should set a field type explicitly.");
    } else {
      TypeName typeName = PRIMITIVE_MAPPING.inverse().get(typeDescriptor);
      if (typeName == null) {
        throw new RuntimeException("Couldn't find field type for " + typeDescriptor);
      }
      return FieldType.of(typeName);
    }
  }

  private static FieldType getArrayFieldType(TypeDescriptor typeDescriptor) {
    if (typeDescriptor.isArray()) {
      if (typeDescriptor.getComponentType().getType().equals(byte.class)) {
        return FieldType.BYTES;
      } else {
        return FieldType.array(fieldTypeForJavaType(typeDescriptor.getComponentType()));
      }
    }
    if (typeDescriptor.isSubtypeOf(TypeDescriptor.of(Collection.class))) {
      TypeDescriptor<Collection<?>> collection = typeDescriptor.getSupertype(Collection.class);
      if (collection.getType() instanceof ParameterizedType) {
        ParameterizedType ptype = (ParameterizedType) collection.getType();
        java.lang.reflect.Type[] params = ptype.getActualTypeArguments();
        checkArgument(params.length == 1);
        return FieldType.array(fieldTypeForJavaType(TypeDescriptor.of(params[0])));
      }
    }
    throw new RuntimeException("Could not determine array parameter type for field.");
  }

  private static FieldType getIterableFieldType(TypeDescriptor typeDescriptor) {
    TypeDescriptor<Iterable<?>> iterable = typeDescriptor.getSupertype(Iterable.class);
    if (iterable.getType() instanceof ParameterizedType) {
      ParameterizedType ptype = (ParameterizedType) iterable.getType();
      java.lang.reflect.Type[] params = ptype.getActualTypeArguments();
      checkArgument(params.length == 1);
      return FieldType.iterable(fieldTypeForJavaType(TypeDescriptor.of(params[0])));
    }
    throw new RuntimeException("Could not determine array parameter type for field.");
  }

  private static FieldType getMapFieldType(TypeDescriptor typeDescriptor) {
    TypeDescriptor<Collection<?>> map = typeDescriptor.getSupertype(Map.class);
    if (map.getType() instanceof ParameterizedType) {
      ParameterizedType ptype = (ParameterizedType) map.getType();
      java.lang.reflect.Type[] params = ptype.getActualTypeArguments();
      return FieldType.map(
          fieldTypeForJavaType(TypeDescriptor.of(params[0])),
          fieldTypeForJavaType(TypeDescriptor.of(params[1])));
    }
    throw new RuntimeException("Cound not determine array parameter type for field.");
  }
}
