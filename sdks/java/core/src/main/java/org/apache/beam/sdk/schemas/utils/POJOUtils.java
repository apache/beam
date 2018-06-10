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

import java.lang.reflect.GenericArrayType;
import java.lang.reflect.Modifier;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.annotations.Experimental.Kind;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.joda.time.ReadableInstant;

@Experimental(Kind.SCHEMAS)
public class POJOUtils {
  public static Schema schemaFromClass(Class<?> clazz) {
    Schema.Builder builder = Schema.builder();
    for (java.lang.reflect.Field field : getFields(clazz)) {
      // TODO: look for nullable annotation.
      builder.addField(field.getName(), fieldFromType(field.getGenericType()));
    }
    return builder.build();
  }

  private static java.lang.reflect.Field[] getFields(Class<?> clazz) {
    java.lang.reflect.Field[] fieldsList;
    Map<String, java.lang.reflect.Field> fields = new LinkedHashMap<>();
    do {
      if (clazz.getPackage() != null && clazz.getPackage().getName().startsWith("java."))
        break;                                   // skip java built-in classes
      for (java.lang.reflect.Field field : clazz.getDeclaredFields())
        if ((field.getModifiers() & (Modifier.TRANSIENT | Modifier.STATIC)) == 0)
          checkArgument(fields.put(field.getName(), field) == null,
            clazz.getSimpleName() + " contains two fields named: " + field);
      clazz = clazz.getSuperclass();
    } while (clazz != null);
    fieldsList = fields.values().toArray(new java.lang.reflect.Field[0]);
    return fieldsList;
  }

  private static Schema.FieldType fieldFromType(java.lang.reflect.Type type) {
    if (type.equals(Byte.class) || type.equals(byte.class)) {
      return FieldType.BYTE;
    }  else if (type.equals(Short.class) || type.equals(short.class)) {
      return FieldType.INT16;
    } else if (type.equals(Integer.class) || type.equals(int.class)) {
      return FieldType.INT32;
    } else if (type.equals(Long.class) || type.equals(long.class)) {
      return FieldType.INT64;
    } else if (type.equals(BigDecimal.class)) {
      return FieldType.DECIMAL;
    } else if (type.equals(Float.class) || type.equals(float.class)) {
      return FieldType.FLOAT;
    } else if (type.equals(Double.class) || type.equals(double.class)) {
      return FieldType.DOUBLE;
    } else if (type.equals(Boolean.class) || type.equals(boolean.class)) {
      return FieldType.BOOLEAN;
    } else if (type instanceof GenericArrayType) {
      Type component = ((GenericArrayType)type).getGenericComponentType();
      if (component.equals(Byte.class) || component.equals(byte.class)) {
        return FieldType.BYTES;
      }
      return FieldType.array(fieldFromType(component));
    } else if (type instanceof ParameterizedType) {
      ParameterizedType ptype = (ParameterizedType) type;
      Class raw = (Class) ptype.getRawType();
      java.lang.reflect.Type[] params = ptype.getActualTypeArguments();
      if (Collection.class.isAssignableFrom(raw)) {
        checkArgument(params.length == 1);
        if (params[0].equals(Byte.class) || params[0].equals(byte.class)) {
          return FieldType.BYTES;
        } else {
          return FieldType.array(fieldFromType(params[0]));
        }
      } else if (Map.class.isAssignableFrom(raw)) {
        java.lang.reflect.Type keyType = params[0];
        java.lang.reflect.Type valueType = params[1];
        // TODO: Assert that key is a primitive type.
        return FieldType.map(fieldFromType(keyType), fieldFromType(valueType));
      }
    } else if (type instanceof Class) {
      // TODO: memoize schemas for classes.
      Class clazz = (Class)type;
      if (clazz.isArray()) {
        Class componentType = clazz.getComponentType();
        if (componentType == Byte.TYPE) {
          return FieldType.BYTES;
        }
        return FieldType.array(fieldFromType(componentType));
      }
      if (CharSequence.class.isAssignableFrom(clazz)) {
        return FieldType.STRING;
      } else if (ReadableInstant.class.isAssignableFrom(clazz)) {
        return FieldType.DATETIME;
      } else if (ByteBuffer.class.isAssignableFrom(clazz)) {
        return FieldType.BYTES;
      }
      return FieldType.row(schemaFromClass(clazz));
    }
    return null;
  }
}
