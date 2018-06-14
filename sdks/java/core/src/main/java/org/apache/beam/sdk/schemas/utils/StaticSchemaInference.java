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

import static com.google.common.base.Preconditions.checkArgument;

import com.google.common.collect.ImmutableMap;
import java.lang.reflect.GenericArrayType;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.joda.time.ReadableInstant;

/**
 * A set of utilities for inferring a Beam {@link Schema} from static Java types.
 */
public class StaticSchemaInference {
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
   * Relevant information about a Java type.
   */
  public static class TypeInformation {
    private String name;
    private Type type;
    private boolean nullable;

    public TypeInformation(String name, Type type, boolean nullable) {
      this.name = name;
      this.type = type;
      this.nullable = nullable;
    }

    public String getName() {
      return name;
    }

    public Type getType() {
      return type;
    }

    public boolean isNullable() {
      return nullable;
    }
  }

  /**
   * Infer a schema from a Java class.
   *
   * <p>Takes in a function to extract a list of field types from a class. Different callers may
   * have different strategies for extracting this list: e.g. introspecting public member variables,
   * public getter methods, or special annotations on the class.
   */
  public static Schema schemaFromClass(
      Class<?> clazz, Function<Class, List<TypeInformation>> getTypesForClass) {
    Schema.Builder builder = Schema.builder();
    for (TypeInformation type : getTypesForClass.apply(clazz)) {
      // TODO: look for nullable annotation.
      builder.addField(type.getName(), fieldFromType(type.getType(), getTypesForClass));
    }
    return builder.build();
  }

  // Map a Java field type to a Beam Schema FieldType.
  private static Schema.FieldType fieldFromType(
      Type type, Function<Class, List<TypeInformation>> getTypesForClass) {
    FieldType primitiveType = PRIMITIVE_TYPES.get(type);
    if (primitiveType != null) {
      return primitiveType;
    }

    if (type instanceof GenericArrayType) {
      // If the type is T[] where T is byte, this is a BYTES type.
      Type component = ((GenericArrayType)type).getGenericComponentType();
      if (component.equals(byte.class)) {
        return FieldType.BYTES;
      } else {
        // Otherwise this is an array type.
        return FieldType.array(fieldFromType(component, getTypesForClass));
      }
    } else if (type instanceof ParameterizedType) {
      ParameterizedType ptype = (ParameterizedType) type;
      Class raw = (Class) ptype.getRawType();
      java.lang.reflect.Type[] params = ptype.getActualTypeArguments();
      if (Collection.class.isAssignableFrom(raw)) {
        checkArgument(params.length == 1);
        if (params[0].equals(byte.class)) {
          return FieldType.BYTES;
        } else {
          return FieldType.array(fieldFromType(params[0], getTypesForClass));
        }
      } else if (Map.class.isAssignableFrom(raw)) {
        FieldType keyType = fieldFromType(params[0], getTypesForClass);
        FieldType valueType = fieldFromType(params[1], getTypesForClass);
        checkArgument(keyType.getTypeName().isPrimitiveType(),
            "Only primitive types can be map keys");
        return FieldType.map(keyType, valueType);
      }
    } else if (type instanceof Class) {
      Class clazz = (Class)type;
      if (clazz.isArray()) {
        Class componentType = clazz.getComponentType();
        if (componentType == Byte.TYPE) {
          return FieldType.BYTES;
        }
        return FieldType.array(fieldFromType(componentType, getTypesForClass));
      }
      if (CharSequence.class.isAssignableFrom(clazz)) {
        return FieldType.STRING;
      } else if (ReadableInstant.class.isAssignableFrom(clazz)) {
        return FieldType.DATETIME;
      } else if (ByteBuffer.class.isAssignableFrom(clazz)) {
        return FieldType.BYTES;
      }
      return FieldType.row(schemaFromClass(clazz, getTypesForClass));
    }
    return null;
  }
}
