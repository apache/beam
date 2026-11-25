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

import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkArgument;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import org.apache.beam.sdk.schemas.FieldAccessDescriptor;
import org.apache.beam.sdk.schemas.FieldAccessDescriptor.FieldDescriptor;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.schemas.Schema.LogicalType;
import org.apache.beam.sdk.schemas.Schema.TypeName;
import org.apache.beam.sdk.schemas.parser.FieldAccessDescriptorParser;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Iterables;
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
        Schema actualSchema = rowValue.getSchema();
        if (!expectedSchema.equals(actualSchema)) {
          throw new IllegalArgumentException(
              String.format(
                  "Expected schema %s for field %s, but got %s",
                  expectedSchema, fieldName, actualSchema));
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

  private static Collection<@Nullable Object> captureIterable(
      FieldType collectionElementType, Iterable<@Nullable Object> values, String fieldName) {
    List<@Nullable Object> captured =
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

  private static Map<Object, @Nullable Object> captureMap(
      FieldType keyType,
      FieldType valueType,
      Map<Object, @Nullable Object> valueMap,
      String fieldName) {
    Map<Object, @Nullable Object> retValue = Maps.newHashMapWithExpectedSize(valueMap.size());
    for (Entry<Object, @Nullable Object> kv : valueMap.entrySet()) {
      Object capturedKey = captureValue(keyType, kv.getKey(), fieldName);
      Object rawValue = kv.getValue();
      if (rawValue == null) {
        if (!valueType.getNullable()) {
          throw new IllegalArgumentException(
              String.format("%s is not nullable in Map field %s", valueType, fieldName));
        }
        retValue.put(capturedKey, null);
      } else {
        retValue.put(capturedKey, captureValue(valueType, rawValue, fieldName));
      }
    }
    return retValue;
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

  /**
   * Compact array-backed override tree used by {@link Row.FieldValueBuilder}. Direct overrides are
   * stored inline in an {@code Object[]} (with {@link #UNSET} distinguishing an absent override
   * from an explicit {@code null} override) to avoid boxing into {@code FieldOverride} wrapper
   * objects. Child {@link FieldAccessNode} arrays are allocated lazily only when nested field
   * overrides are added.
   */
  static final class FieldOverrides {
    private static final Object UNSET = new Object();

    private final Schema rootSchema;
    private final @Nullable Object[] values;
    private @Nullable FieldAccessNode @Nullable [] nestedAccess;

    FieldOverrides(Schema rootSchema) {
      this.rootSchema = rootSchema;
      @Nullable Object[] arr = new Object[rootSchema.getFieldCount()];
      Arrays.fill(arr, UNSET);
      this.values = arr;
    }

    void addDirectOverride(int fieldId, @Nullable Object value) {
      values[fieldId] = value;
      if (nestedAccess != null) {
        nestedAccess[fieldId] = null;
      }
    }

    /**
     * Fast path for flat ({@code "foo"}) and dotted ({@code "nested.foo"}) identifier paths that
     * bypasses ANTLR4 parsing, {@link FieldAccessDescriptor} construction, and schema resolution.
     * Returns {@code false} without mutating state if the path contains qualifiers, wildcards,
     * whitespace, unknown fields, or non-{@code ROW} intermediates so the caller can fall back to
     * {@link FieldAccessDescriptor#withFieldNames}.
     */
    boolean tryAddPathOverride(@Nullable String fieldPath, @Nullable Object value) {
      if (fieldPath == null || fieldPath.isEmpty()) {
        return false;
      }
      if (FieldAccessDescriptorParser.isSimpleIdentifier(fieldPath)) {
        if (rootSchema.hasField(fieldPath)) {
          addDirectOverride(rootSchema.indexOf(fieldPath), value);
          return true;
        }
        return false;
      }
      return tryAddDottedPathOverride(fieldPath, value);
    }

    private boolean tryAddDottedPathOverride(String fieldPath, @Nullable Object value) {
      if (fieldPath.indexOf('.') <= 0) {
        return false;
      }
      // Validate the entire dotted path before allocating any child FieldAccessNode so that a
      // fallback to FieldAccessDescriptor never observes partially mutated state.
      Schema currentSchema = rootSchema;
      int start = 0;
      int len = fieldPath.length();
      while (true) {
        int nextDot = fieldPath.indexOf('.', start);
        int end = (nextDot < 0) ? len : nextDot;
        if (!FieldAccessDescriptorParser.isSimpleIdentifier(fieldPath, start, end)) {
          return false;
        }
        String seg = fieldPath.substring(start, end);
        if (!currentSchema.hasField(seg)) {
          return false;
        }
        if (nextDot < 0) {
          break;
        }
        FieldType fieldType = currentSchema.getField(currentSchema.indexOf(seg)).getType();
        if (fieldType.getTypeName() != TypeName.ROW || fieldType.getRowSchema() == null) {
          return false;
        }
        currentSchema = fieldType.getRowSchema();
        start = nextDot + 1;
      }
      // Validated: commit value into the nested FieldAccessNode tree.
      int firstDot = fieldPath.indexOf('.');
      int rootFieldId = rootSchema.indexOf(fieldPath.substring(0, firstDot));
      Schema nestedSchema = rootSchema.getField(rootFieldId).getType().getRowSchema();
      FieldAccessNode node = getOrCreateNested(rootFieldId, nestedSchema.getFieldCount());
      currentSchema = nestedSchema;
      start = firstDot + 1;
      while (true) {
        int nextDot = fieldPath.indexOf('.', start);
        int end = (nextDot < 0) ? len : nextDot;
        String seg = fieldPath.substring(start, end);
        int fieldId = currentSchema.indexOf(seg);
        if (nextDot < 0) {
          node.setDirect(fieldId, value);
          return true;
        }
        nestedSchema = currentSchema.getField(fieldId).getType().getRowSchema();
        node = node.getOrCreateNested(fieldId, nestedSchema.getFieldCount());
        currentSchema = nestedSchema;
        start = nextDot + 1;
      }
    }

    private FieldAccessNode getOrCreateNested(int fieldId, int nestedFieldCount) {
      @Nullable FieldAccessNode[] nested = this.nestedAccess;
      if (nested == null) {
        nested = new FieldAccessNode[values.length];
        this.nestedAccess = nested;
      }
      FieldAccessNode child = nested[fieldId];
      if (child == null) {
        child = new FieldAccessNode(nestedFieldCount);
        nested[fieldId] = child;
      }
      return child;
    }

    void addOverride(FieldAccessDescriptor fieldAccessDescriptor, @Nullable Object value) {
      if (!fieldAccessDescriptor.getFieldsAccessed().isEmpty()) {
        FieldDescriptor fieldDescriptor =
            Iterables.getOnlyElement(fieldAccessDescriptor.getFieldsAccessed());
        addDirectOverride(fieldDescriptor.getFieldId(), value);
      } else if (!fieldAccessDescriptor.getNestedFieldsAccessed().isEmpty()) {
        Map.Entry<FieldDescriptor, FieldAccessDescriptor> entry =
            Iterables.getOnlyElement(fieldAccessDescriptor.getNestedFieldsAccessed().entrySet());
        int fieldId = entry.getKey().getFieldId();
        Schema nestedSchema = rootSchema.getField(fieldId).getType().getRowSchema();
        checkArgument(
            nestedSchema != null,
            "Cannot apply nested field override to non-ROW field %s",
            rootSchema.getField(fieldId).getName());
        FieldAccessNode child = getOrCreateNested(fieldId, nestedSchema.getFieldCount());
        child.addOverride(entry.getValue(), value, nestedSchema);
      }
    }

    Row apply(@Nullable Row sourceRow) {
      return applyNode(rootSchema, sourceRow, values, nestedAccess);
    }

    private static Row applyNode(
        Schema schema,
        @Nullable Row sourceRow,
        @Nullable Object[] vals,
        @Nullable FieldAccessNode @Nullable [] nested) {
      int fieldCount = schema.getFieldCount();
      List<@Nullable Object> result = new ArrayList<>(fieldCount);
      if (nested == null) {
        for (int i = 0; i < fieldCount; i++) {
          Schema.Field field = schema.getField(i);
          Object override = vals[i];
          Object raw =
              (override != UNSET) ? override : (sourceRow != null ? sourceRow.getValue(i) : null);
          result.add(captureValue(field.getType(), raw, field.getName()));
        }
      } else {
        for (int i = 0; i < fieldCount; i++) {
          Schema.Field field = schema.getField(i);
          FieldType fieldType = field.getType();
          Object override = vals[i];
          if (override != UNSET) {
            result.add(captureValue(fieldType, override, field.getName()));
          } else {
            FieldAccessNode child = nested[i];
            if (child != null) {
              Row nestedSourceRow = (sourceRow != null) ? sourceRow.getValue(i) : null;
              result.add(
                  applyNode(
                      fieldType.getRowSchema(), nestedSourceRow, child.values, child.nestedAccess));
            } else {
              Object raw = (sourceRow != null) ? sourceRow.getValue(i) : null;
              result.add(captureValue(fieldType, raw, field.getName()));
            }
          }
        }
      }
      return new RowWithStorage(schema, result);
    }

    private static final class FieldAccessNode {
      private final @Nullable Object[] values;
      private @Nullable FieldAccessNode @Nullable [] nestedAccess;

      FieldAccessNode(int fieldCount) {
        @Nullable Object[] arr = new Object[fieldCount];
        Arrays.fill(arr, UNSET);
        this.values = arr;
      }

      void setDirect(int fieldId, @Nullable Object value) {
        values[fieldId] = value;
        if (nestedAccess != null) {
          nestedAccess[fieldId] = null;
        }
      }

      FieldAccessNode getOrCreateNested(int fieldId, int nestedFieldCount) {
        @Nullable FieldAccessNode[] nested = this.nestedAccess;
        if (nested == null) {
          nested = new FieldAccessNode[values.length];
          this.nestedAccess = nested;
        }
        FieldAccessNode child = nested[fieldId];
        if (child == null) {
          child = new FieldAccessNode(nestedFieldCount);
          nested[fieldId] = child;
        }
        return child;
      }

      void addOverride(
          FieldAccessDescriptor fieldAccessDescriptor,
          @Nullable Object value,
          Schema currentSchema) {
        if (!fieldAccessDescriptor.getFieldsAccessed().isEmpty()) {
          FieldDescriptor fieldDescriptor =
              Iterables.getOnlyElement(fieldAccessDescriptor.getFieldsAccessed());
          setDirect(fieldDescriptor.getFieldId(), value);
        } else if (!fieldAccessDescriptor.getNestedFieldsAccessed().isEmpty()) {
          Map.Entry<FieldDescriptor, FieldAccessDescriptor> entry =
              Iterables.getOnlyElement(fieldAccessDescriptor.getNestedFieldsAccessed().entrySet());
          int fieldId = entry.getKey().getFieldId();
          Schema nestedSchema = currentSchema.getField(fieldId).getType().getRowSchema();
          checkArgument(
              nestedSchema != null,
              "Cannot apply nested field override to non-ROW field %s",
              currentSchema.getField(fieldId).getName());
          FieldAccessNode child = getOrCreateNested(fieldId, nestedSchema.getFieldCount());
          child.addOverride(entry.getValue(), value, nestedSchema);
        }
      }
    }
  }
}
