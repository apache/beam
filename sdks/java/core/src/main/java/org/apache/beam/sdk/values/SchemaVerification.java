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
package org.apache.beam.sdk.values;

import java.io.Serializable;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.schemas.Schema.LogicalType;
import org.apache.beam.sdk.schemas.Schema.TypeName;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Lists;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Maps;
import org.joda.time.Instant;
import org.joda.time.base.AbstractInstant;

@Experimental
public abstract class SchemaVerification implements Serializable {

  static List<Object> verifyRowValues(Schema schema, List<Object> values) {
    List<Object> verifiedValues = Lists.newArrayListWithCapacity(values.size());
    if (schema.getFieldCount() != values.size()) {
      throw new IllegalArgumentException(
          String.format(
              "Field count in Schema (%s) (%d) and values (%s) (%d)  must match",
              schema.getFieldNames(), schema.getFieldCount(), values, values.size()));
    }
    for (int i = 0; i < values.size(); ++i) {
      Object value = values.get(i);
      Schema.Field field = schema.getField(i);
      if (value == null) {
        if (!field.getType().getNullable()) {
          throw new IllegalArgumentException(
              String.format("Field %s is not nullable", field.getName()));
        }
        verifiedValues.add(null);
      } else {
        verifiedValues.add(verifyFieldValue(value, field.getType(), field.getName()));
      }
    }
    return verifiedValues;
  }

  public static Object verifyFieldValue(Object value, FieldType type, String fieldName) {
    if (TypeName.ARRAY.equals(type.getTypeName())) {
      return verifyArray(value, type.getCollectionElementType(), fieldName);
    } else if (TypeName.ITERABLE.equals(type.getTypeName())) {
      return verifyIterable(value, type.getCollectionElementType(), fieldName);
    }
    if (TypeName.MAP.equals(type.getTypeName())) {
      return verifyMap(value, type.getMapKeyType(), type.getMapValueType(), fieldName);
    } else if (TypeName.ROW.equals(type.getTypeName())) {
      return verifyRow(value, fieldName);
    } else if (TypeName.LOGICAL_TYPE.equals(type.getTypeName())) {
      return verifyLogicalType(value, type.getLogicalType(), fieldName);
    } else {
      return verifyPrimitiveType(value, type.getTypeName(), fieldName);
    }
  }

  private static Object verifyLogicalType(Object value, LogicalType logicalType, String fieldName) {
    return verifyFieldValue(logicalType.toBaseType(value), logicalType.getBaseType(), fieldName);
  }

  private static List<Object> verifyArray(
      Object value, FieldType collectionElementType, String fieldName) {
    boolean collectionElementTypeNullable = collectionElementType.getNullable();
    if (!(value instanceof List)) {
      throw new IllegalArgumentException(
          String.format(
              "For field name %s and array type expected List class. Instead "
                  + "class type was %s.",
              fieldName, value.getClass()));
    }
    List<Object> valueList = (List<Object>) value;
    List<Object> verifiedList = Lists.newArrayListWithCapacity(valueList.size());
    for (Object listValue : valueList) {
      if (listValue == null) {
        if (!collectionElementTypeNullable) {
          throw new IllegalArgumentException(
              String.format(
                  "%s is not nullable in Array field %s", collectionElementType, fieldName));
        }
        verifiedList.add(null);
      } else {
        verifiedList.add(verifyFieldValue(listValue, collectionElementType, fieldName));
      }
    }
    return verifiedList;
  }

  private static Iterable<Object> verifyIterable(
      Object value, FieldType collectionElementType, String fieldName) {
    boolean collectionElementTypeNullable = collectionElementType.getNullable();
    if (!(value instanceof Iterable)) {
      throw new IllegalArgumentException(
          String.format(
              "For field name %s and iterable type expected class extending Iterable. Instead "
                  + "class type was %s.",
              fieldName, value.getClass()));
    }
    Iterable<Object> valueList = (Iterable<Object>) value;
    for (Object listValue : valueList) {
      if (listValue == null) {
        if (!collectionElementTypeNullable) {
          throw new IllegalArgumentException(
              String.format(
                  "%s is not nullable in Array field %s", collectionElementType, fieldName));
        }
      } else {
        verifyFieldValue(listValue, collectionElementType, fieldName);
      }
    }
    return valueList;
  }

  private static Map<Object, Object> verifyMap(
      Object value, FieldType keyType, FieldType valueType, String fieldName) {
    boolean valueTypeNullable = valueType.getNullable();
    if (!(value instanceof Map)) {
      throw new IllegalArgumentException(
          String.format(
              "For field name %s and map type expected Map class. Instead " + "class type was %s.",
              fieldName, value.getClass()));
    }
    Map<Object, Object> valueMap = (Map<Object, Object>) value;
    Map<Object, Object> verifiedMap = Maps.newHashMapWithExpectedSize(valueMap.size());
    for (Entry<Object, Object> kv : valueMap.entrySet()) {
      if (kv.getValue() == null) {
        if (!valueTypeNullable) {
          throw new IllegalArgumentException(
              String.format("%s is not nullable in Map field %s", valueType, fieldName));
        }
        verifiedMap.put(verifyFieldValue(kv.getKey(), keyType, fieldName), null);
      } else {
        verifiedMap.put(
            verifyFieldValue(kv.getKey(), keyType, fieldName),
            verifyFieldValue(kv.getValue(), valueType, fieldName));
      }
    }
    return verifiedMap;
  }

  private static Row verifyRow(Object value, String fieldName) {
    if (!(value instanceof Row)) {
      throw new IllegalArgumentException(
          String.format(
              "For field name %s expected Row type. " + "Instead class type was %s.",
              fieldName, value.getClass()));
    }
    // No need to recursively validate the nested Row, since there's no way to build the
    // Row object without it validating.
    return (Row) value;
  }

  private static Object verifyPrimitiveType(Object value, TypeName type, String fieldName) {
    if (type.isDateType()) {
      return verifyDateTime(value, fieldName);
    } else {
      switch (type) {
        case BYTE:
          if (value instanceof Byte) {
            return value;
          }
          break;
        case BYTES:
          if (value instanceof ByteBuffer) {
            return ((ByteBuffer) value).array();
          } else if (value instanceof byte[]) {
            return (byte[]) value;
          }
          break;
        case INT16:
          if (value instanceof Short) {
            return value;
          }
          break;
        case INT32:
          if (value instanceof Integer) {
            return value;
          }
          break;
        case INT64:
          if (value instanceof Long) {
            return value;
          }
          break;
        case DECIMAL:
          if (value instanceof BigDecimal) {
            return value;
          }
          break;
        case FLOAT:
          if (value instanceof Float) {
            return value;
          }
          break;
        case DOUBLE:
          if (value instanceof Double) {
            return value;
          }
          break;
        case STRING:
          if (value instanceof String) {
            return value;
          }
          break;
        case BOOLEAN:
          if (value instanceof Boolean) {
            return value;
          }
          break;
        default:
          // Shouldn't actually get here, but we need this case to satisfy linters.
          throw new IllegalArgumentException(
              String.format("Not a primitive type for field name %s: %s", fieldName, type));
      }
      throw new IllegalArgumentException(
          String.format(
              "For field name %s and type %s found incorrect class type %s",
              fieldName, type, value.getClass()));
    }
  }

  private static Instant verifyDateTime(Object value, String fieldName) {
    // We support the following classes for datetimes.
    if (value instanceof AbstractInstant) {
      return ((AbstractInstant) value).toInstant();
    } else {
      throw new IllegalArgumentException(
          String.format(
              "For field name %s and DATETIME type got unexpected class %s ",
              fieldName, value.getClass()));
    }
  }
}
