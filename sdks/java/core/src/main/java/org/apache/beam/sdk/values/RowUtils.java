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

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import org.apache.beam.sdk.schemas.FieldAccessDescriptor;
import org.apache.beam.sdk.schemas.FieldAccessDescriptor.FieldDescriptor;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.schemas.Schema.LogicalType;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Iterables;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Lists;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Maps;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.checkerframework.checker.nullness.qual.PolyNull;
import org.joda.time.base.AbstractInstant;

@SuppressWarnings({
  "nullness", // TODO(https://github.com/apache/beam/issues/20497)
  "rawtypes"
})
class RowUtils {
  static Row captureRow(Schema schema, List<@Nullable Object> rawValues) {
    int fieldCount = schema.getFieldCount();
    List<@Nullable Object> capturedValues = new ArrayList<>(fieldCount);
    for (int i = 0; i < fieldCount; i++) {
      Schema.Field field = schema.getField(i);
      capturedValues.add(captureValue(field.getType(), rawValues.get(i), field.getName()));
    }
    return new RowWithStorage(schema, capturedValues);
  }

  @SuppressWarnings("unchecked")
  static @PolyNull Object captureValue(
      FieldType fieldType, @PolyNull Object value, String fieldName) {
    if (value == null) {
      if (!fieldType.getNullable()) {
        throw new IllegalArgumentException(
            String.format("%s is not nullable in field %s", fieldType, fieldName));
      }
      return null;
    }
    Object processedValue;
    switch (fieldType.getTypeName()) {
      case ARRAY:
        processedValue =
            captureIterable(
                fieldType.getCollectionElementType(), (Collection<Object>) value, fieldName);
        break;
      case ITERABLE:
        processedValue =
            captureIterable(
                fieldType.getCollectionElementType(), (Iterable<Object>) value, fieldName);
        break;
      case MAP:
        processedValue =
            captureMap(
                fieldType.getMapKeyType(),
                fieldType.getMapValueType(),
                (Map<Object, Object>) value,
                fieldName);
        break;
      case ROW:
        Row rowValue = (Row) value;
        Schema expectedSchema = fieldType.getRowSchema();
        if (!expectedSchema.equals(rowValue.getSchema())) {
          throw new IllegalArgumentException(
              String.format(
                  "Expected schema %s for field %s, but got %s",
                  expectedSchema, fieldName, rowValue.getSchema()));
        }
        processedValue = rowValue;
        break;
      case LOGICAL_TYPE:
        LogicalType logicalType = fieldType.getLogicalType();
        processedValue = logicalType.toInputType(logicalType.toBaseType(value));
        break;
      case DATETIME:
        processedValue = ((AbstractInstant) value).toInstant();
        break;
      case BYTE:
        processedValue = (Byte) value;
        break;
      case BYTES:
        processedValue = toByteArray(value);
        break;
      case INT16:
        processedValue = (Short) value;
        break;
      case INT32:
        processedValue = (Integer) value;
        break;
      case INT64:
        processedValue = (Long) value;
        break;
      case DECIMAL:
        processedValue = (BigDecimal) value;
        break;
      case FLOAT:
        processedValue = (Float) value;
        break;
      case DOUBLE:
        processedValue = (Double) value;
        break;
      case STRING:
        processedValue = (String) value;
        break;
      case BOOLEAN:
        processedValue = (Boolean) value;
        break;
      default:
        throw new IllegalArgumentException(
            String.format("Not a primitive type for field name %s: %s", fieldName, fieldType));
    }
    if (processedValue == null && !fieldType.getNullable()) {
      throw new IllegalArgumentException(
          String.format("%s is not nullable in field %s", fieldType, fieldName));
    }
    return processedValue;
  }

  private static @Nullable Collection<Object> captureIterable(
      FieldType collectionElementType, @Nullable Iterable<Object> values, String fieldName) {
    if (values == null) {
      return null;
    }
    List<Object> captured =
        (values instanceof Collection)
            ? new ArrayList<>(((Collection<?>) values).size())
            : new ArrayList<>();
    for (Object listValue : values) {
      if (listValue == null) {
        if (!collectionElementType.getNullable()) {
          throw new IllegalArgumentException(
              String.format(
                  "%s is not nullable in Array field %s", collectionElementType, fieldName));
        }
        captured.add(null);
      } else {
        captured.add(captureValue(collectionElementType, listValue, fieldName));
      }
    }
    return captured;
  }

  private static @Nullable Map<Object, Object> captureMap(
      FieldType keyType,
      FieldType valueType,
      @Nullable Map<Object, Object> valueMap,
      String fieldName) {
    if (valueMap == null) {
      return null;
    }
    Map<Object, Object> retValue = Maps.newHashMapWithExpectedSize(valueMap.size());
    for (Entry<Object, Object> kv : valueMap.entrySet()) {
      Object capturedKey = captureValue(keyType, kv.getKey(), fieldName);
      if (kv.getValue() == null) {
        if (!valueType.getNullable()) {
          throw new IllegalArgumentException(
              String.format("%s is not nullable in Map field %s", valueType, fieldName));
        }
        retValue.put(capturedKey, null);
      } else {
        retValue.put(capturedKey, captureValue(valueType, kv.getValue(), fieldName));
      }
    }
    return retValue;
  }

  static class FieldOverride {
    FieldOverride(Object overrideValue) {
      this.overrideValue = overrideValue;
    }

    Object getOverrideValue() {
      return overrideValue;
    }

    final Object overrideValue;
  }

  static class FieldOverrides {
    private final FieldAccessNode topNode;
    private final Schema rootSchema;

    FieldOverrides(Schema rootSchema) {
      this.topNode = new FieldAccessNode(rootSchema);
      this.rootSchema = rootSchema;
    }

    void addOverride(FieldAccessDescriptor fieldAccessDescriptor, FieldOverride fieldOverride) {
      topNode.addOverride(fieldAccessDescriptor, fieldOverride, rootSchema);
    }

    Row apply(@Nullable Row sourceRow) {
      return topNode.apply(rootSchema, sourceRow);
    }

    private static class FieldAccessNode {
      final List<FieldOverride> fieldOverrides;
      final List<FieldAccessNode> nestedAccess;

      FieldAccessNode(Schema schema) {
        fieldOverrides = Lists.newArrayListWithExpectedSize(schema.getFieldCount());
        nestedAccess = Lists.newArrayList();
      }

      void addOverride(
          FieldAccessDescriptor fieldAccessDescriptor,
          FieldOverride fieldOverride,
          Schema currentSchema) {
        if (!fieldAccessDescriptor.getFieldsAccessed().isEmpty()) {
          FieldDescriptor fieldDescriptor =
              Iterables.getOnlyElement(fieldAccessDescriptor.getFieldsAccessed());
          int aheadPosition = fieldDescriptor.getFieldId() - fieldOverrides.size() + 1;
          if (aheadPosition > 0) {
            fieldOverrides.addAll(Collections.nCopies(aheadPosition, null));
          }
          fieldOverrides.set(fieldDescriptor.getFieldId(), fieldOverride);
        } else if (!fieldAccessDescriptor.getNestedFieldsAccessed().isEmpty()) {
          Map.Entry<FieldDescriptor, FieldAccessDescriptor> entry =
              Iterables.getOnlyElement(fieldAccessDescriptor.getNestedFieldsAccessed().entrySet());
          int aheadPosition = entry.getKey().getFieldId() - nestedAccess.size() + 1;
          if (aheadPosition > 0) {
            nestedAccess.addAll(Collections.nCopies(aheadPosition, null));
          }

          Schema nestedSchema =
              currentSchema.getField(entry.getKey().getFieldId()).getType().getRowSchema();
          FieldAccessNode node = nestedAccess.get(entry.getKey().getFieldId());
          if (node == null) {
            node = new FieldAccessNode(nestedSchema);
            nestedAccess.set(entry.getKey().getFieldId(), node);
          }
          node.addOverride(entry.getValue(), fieldOverride, nestedSchema);
        }
      }

      Row apply(Schema schema, @Nullable Row sourceRow) {
        int fieldCount = schema.getFieldCount();
        List<@Nullable Object> values = new ArrayList<>(fieldCount);
        for (int i = 0; i < fieldCount; i++) {
          Schema.Field field = schema.getField(i);
          FieldType fieldType = field.getType();
          FieldOverride override = (i < fieldOverrides.size()) ? fieldOverrides.get(i) : null;
          if (override != null) {
            values.add(captureValue(fieldType, override.getOverrideValue(), field.getName()));
          } else {
            FieldAccessNode nestedNode = (i < nestedAccess.size()) ? nestedAccess.get(i) : null;
            if (nestedNode != null) {
              Row nestedSourceRow = (sourceRow != null) ? sourceRow.getValue(i) : null;
              Row capturedRow = nestedNode.apply(fieldType.getRowSchema(), nestedSourceRow);
              values.add(captureValue(fieldType, capturedRow, field.getName()));
            } else {
              Object fieldValue = (sourceRow != null) ? sourceRow.getValue(i) : null;
              values.add(captureValue(fieldType, fieldValue, field.getName()));
            }
          }
        }
        return new RowWithStorage(schema, values);
      }
    }
  }

  static byte @Nullable [] toByteArray(@Nullable Object value) {
    if (value == null) {
      return null;
    }
    if (value instanceof ByteBuffer) {
      ByteBuffer buf = (ByteBuffer) value;
      if (buf.hasArray()
          && buf.arrayOffset() == 0
          && buf.position() == 0
          && buf.limit() == buf.capacity()) {
        return buf.array();
      }
      byte[] bytes = new byte[buf.remaining()];
      buf.duplicate().get(bytes);
      return bytes;
    }
    return (byte[]) value;
  }
}
