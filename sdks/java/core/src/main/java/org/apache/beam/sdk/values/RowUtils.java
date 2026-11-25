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

import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkNotNull;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.RandomAccess;
import org.apache.beam.sdk.schemas.FieldAccessDescriptor;
import org.apache.beam.sdk.schemas.FieldAccessDescriptor.FieldDescriptor;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.schemas.Schema.LogicalType;
import org.apache.beam.sdk.schemas.parser.FieldAccessDescriptorParser;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Iterables;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Maps;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.base.AbstractInstant;

class RowUtils {

  /** Validates and copies {@code rawValues} in-place, transferring ownership to the new Row. */
  static Row captureRowInPlace(Schema schema, List<@Nullable Object> rawValues) {
    int fieldCount = schema.getFieldCount();
    for (int i = 0; i < fieldCount; i++) {
      Schema.Field field = schema.getField(i);
      @Nullable Object raw = rawValues.get(i);
      @Nullable Object captured = captureValue(field.getType(), raw, field.getName());
      if (captured != raw) {
        rawValues.set(i, captured);
      }
    }
    return new RowWithStorage(schema, rawValues);
  }

  /**
   * Validates and normalizes a single field value according to {@code fieldType}. Outlined throw
   * helpers and direct case returns keep bytecode size well below HotSpot C2's default {@code
   * FreqInlineSize} (325 bytes).
   */
  @SuppressWarnings("unchecked")
  static @Nullable Object captureValue(
      FieldType fieldType, @Nullable Object value, String fieldName) {
    if (value == null) {
      if (!fieldType.getNullable()) {
        throw notNullable(fieldType, fieldName);
      }
      return null;
    }
    switch (fieldType.getTypeName()) {
      case ARRAY:
        return captureIterable(
            checkNotNull(fieldType.getCollectionElementType()),
            (Collection<@Nullable Object>) value,
            fieldName);
      case ITERABLE:
        return captureIterable(
            checkNotNull(fieldType.getCollectionElementType()),
            (Iterable<@Nullable Object>) value,
            fieldName);
      case MAP:
        return captureMap(
            checkNotNull(fieldType.getMapKeyType()),
            checkNotNull(fieldType.getMapValueType()),
            (Map<@Nullable Object, @Nullable Object>) value,
            fieldName);
      case ROW:
        // Do not reject nested rows via Schema.equals here: callers such as Beam SQL
        // (CalciteUtils.toSchema) strip field descriptions and schema/field Options from
        // nested ROW schemas produced by BigQuery, Avro, Protobuf, and Debezium.
        return (Row) value;
      case LOGICAL_TYPE:
        return captureLogicalValue(fieldType, value, fieldName);
      case DATETIME:
        return ((AbstractInstant) value).toInstant();
      case BYTE:
        return (Byte) value;
      case BYTES:
        return toByteArray(value);
      case INT16:
        return (Short) value;
      case INT32:
        return (Integer) value;
      case INT64:
        return (Long) value;
      case DECIMAL:
        return (BigDecimal) value;
      case FLOAT:
        return (Float) value;
      case DOUBLE:
        return (Double) value;
      case STRING:
        return (String) value;
      case BOOLEAN:
        return (Boolean) value;
      default:
        throw unsupportedType(fieldType, fieldName);
    }
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  private static @Nullable Object captureLogicalValue(
      FieldType fieldType, Object value, String fieldName) {
    LogicalType logicalType = checkNotNull(fieldType.getLogicalType());
    @Nullable Object processed = logicalType.toInputType(logicalType.toBaseType(value));
    if (processed == null && !fieldType.getNullable()) {
      throw notNullable(fieldType, fieldName);
    }
    return processed;
  }

  private static IllegalArgumentException notNullable(FieldType fieldType, String fieldName) {
    return new IllegalArgumentException(
        String.format("%s is not nullable in field %s", fieldType, fieldName));
  }

  private static IllegalArgumentException unsupportedType(FieldType fieldType, String fieldName) {
    return new IllegalArgumentException(
        String.format("Unsupported field type for field name %s: %s", fieldName, fieldType));
  }

  private static Collection<@Nullable Object> captureIterable(
      FieldType collectionElementType, Iterable<@Nullable Object> values, String fieldName) {
    if (values instanceof RandomAccess && values instanceof List) {
      List<@Nullable Object> list = (List<@Nullable Object>) values;
      int size = list.size();
      List<@Nullable Object> captured = new ArrayList<>(size);
      for (int i = 0; i < size; i++) {
        @Nullable Object listValue = list.get(i);
        captured.add(captureCollectionElement(collectionElementType, listValue, fieldName));
      }
      return captured;
    }
    List<@Nullable Object> captured =
        (values instanceof Collection)
            ? new ArrayList<>(((Collection<?>) values).size())
            : new ArrayList<>();
    for (@Nullable Object listValue : values) {
      captured.add(captureCollectionElement(collectionElementType, listValue, fieldName));
    }
    return captured;
  }

  private static @Nullable Object captureCollectionElement(
      FieldType collectionElementType, @Nullable Object listValue, String fieldName) {
    if (listValue == null) {
      if (!collectionElementType.getNullable()) {
        throw new IllegalArgumentException(
            String.format(
                "%s is not nullable in Array field %s", collectionElementType, fieldName));
      }
      return null;
    }
    return captureValue(collectionElementType, listValue, fieldName);
  }

  private static Map<@Nullable Object, @Nullable Object> captureMap(
      FieldType keyType,
      FieldType valueType,
      Map<@Nullable Object, @Nullable Object> valueMap,
      String fieldName) {
    Map<@Nullable Object, @Nullable Object> retValue =
        Maps.newHashMapWithExpectedSize(valueMap.size());
    for (Entry<@Nullable Object, @Nullable Object> kv : valueMap.entrySet()) {
      @Nullable Object capturedKey = captureValue(keyType, kv.getKey(), fieldName);
      @Nullable Object rawValue = kv.getValue();
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

  private static byte[] toByteArray(Object value) {
    if (value instanceof byte[]) {
      return (byte[]) value;
    }
    if (value instanceof ByteBuffer) {
      ByteBuffer buf = (ByteBuffer) value;
      if (buf.hasArray()) {
        byte[] backing = buf.array();
        // Compare limit against backing.length (not buf.capacity()): a HeapByteBuffer.slice() at
        // offset 0 sets capacity() == limit() even when backing.length is larger. The fast path
        // aliases the caller's backing array, matching direct byte[] and legacy buf.array().
        if (buf.arrayOffset() == 0 && buf.position() == 0 && buf.limit() == backing.length) {
          return backing;
        }
        int start = buf.arrayOffset() + buf.position();
        return Arrays.copyOfRange(backing, start, start + buf.remaining());
      }
      byte[] bytes = new byte[buf.remaining()];
      buf.duplicate().get(bytes);
      return bytes;
    }
    return (byte[]) value;
  }

  /**
   * Compact node storing sparse field overrides inline without boxing into wrapper objects. Direct
   * value arrays and nested child-node arrays are both allocated lazily so a node with only nested
   * overrides does not allocate or fill a {@code values} array, and a flat node never allocates a
   * {@code nestedAccess} array. {@link #UNSET} distinguishes an unset slot from an explicit {@code
   * null} override.
   */
  static class FieldAccessNode {
    private static final Object UNSET = new Object();

    private final int fieldCount;
    private @Nullable Object @Nullable [] values;
    private @Nullable FieldAccessNode @Nullable [] nestedAccess;

    FieldAccessNode(int fieldCount) {
      this.fieldCount = fieldCount;
    }

    void setDirect(int fieldId, @Nullable Object value) {
      @Nullable Object[] vals = this.values;
      if (vals == null) {
        vals = new Object[fieldCount];
        Arrays.fill(vals, UNSET);
        this.values = vals;
      }
      vals[fieldId] = value;
      @Nullable FieldAccessNode[] nested = this.nestedAccess;
      if (nested != null) {
        // Direct override replaces any prior nested field overrides under this field.
        nested[fieldId] = null;
      }
    }

    FieldAccessNode getOrCreateNested(int fieldId, int nestedFieldCount) {
      @Nullable FieldAccessNode[] nested = this.nestedAccess;
      if (nested == null) {
        nested = new FieldAccessNode[fieldCount];
        this.nestedAccess = nested;
      }
      @Nullable FieldAccessNode child = nested[fieldId];
      if (child == null) {
        child = new FieldAccessNode(nestedFieldCount);
        nested[fieldId] = child;
      }
      return child;
    }

    void addOverride(
        FieldAccessDescriptor fieldAccessDescriptor, @Nullable Object value, Schema currentSchema) {
      if (!fieldAccessDescriptor.getFieldsAccessed().isEmpty()) {
        FieldDescriptor fieldDescriptor =
            Iterables.getOnlyElement(fieldAccessDescriptor.getFieldsAccessed());
        setDirect(checkNotNull(fieldDescriptor.getFieldId()), value);
      } else if (!fieldAccessDescriptor.getNestedFieldsAccessed().isEmpty()) {
        Entry<FieldDescriptor, FieldAccessDescriptor> entry =
            Iterables.getOnlyElement(fieldAccessDescriptor.getNestedFieldsAccessed().entrySet());
        int fieldId = checkNotNull(entry.getKey().getFieldId());
        Schema.Field field = currentSchema.getField(fieldId);
        @Nullable Schema nestedSchema = field.getType().getRowSchema();
        if (nestedSchema == null) {
          throw new IllegalArgumentException(
              String.format(
                  "Cannot apply nested field override to non-ROW field %s", field.getName()));
        }
        FieldAccessNode child = getOrCreateNested(fieldId, nestedSchema.getFieldCount());
        child.addOverride(entry.getValue(), value, nestedSchema);
      }
    }

    Row apply(Schema schema, @Nullable Row sourceRow) {
      int count = schema.getFieldCount();
      @Nullable Object @Nullable [] vals = this.values;
      @Nullable FieldAccessNode @Nullable [] nested = this.nestedAccess;
      List<@Nullable Object> result = new ArrayList<>(count);
      if (nested == null) {
        for (int i = 0; i < count; i++) {
          Schema.Field field = schema.getField(i);
          @Nullable Object raw;
          if (vals != null && vals[i] != UNSET) {
            raw = vals[i];
          } else {
            raw = (sourceRow != null) ? sourceRow.getValue(i) : null;
          }
          result.add(captureValue(field.getType(), raw, field.getName()));
        }
      } else {
        for (int i = 0; i < count; i++) {
          Schema.Field field = schema.getField(i);
          FieldType fieldType = field.getType();
          @Nullable FieldAccessNode child = nested[i];
          @Nullable Object directVal = (vals != null) ? vals[i] : UNSET;
          if (child != null) {
            // Compose nested overrides on top of any earlier direct override (or sourceRow field),
            // so both ("nested.x", v) then ("nested", r) and ("nested", r) then ("nested.x", v)
            // behave sequentially.
            @Nullable Row nestedSource =
                (directVal != UNSET)
                    ? (Row) directVal
                    : (sourceRow != null ? sourceRow.getValue(i) : null);
            result.add(child.apply(checkNotNull(fieldType.getRowSchema()), nestedSource));
          } else if (directVal != UNSET) {
            result.add(captureValue(fieldType, directVal, field.getName()));
          } else {
            @Nullable Object raw = (sourceRow != null) ? sourceRow.getValue(i) : null;
            result.add(captureValue(fieldType, raw, field.getName()));
          }
        }
      }
      return new RowWithStorage(schema, result);
    }
  }

  /**
   * Root override node used by {@link Row.FieldValueBuilder}. Extends {@link FieldAccessNode}
   * directly to share all storage/override/apply logic with zero extra wrapper object allocation.
   */
  static final class FieldOverrides extends FieldAccessNode {
    private final Schema rootSchema;

    FieldOverrides(Schema rootSchema) {
      super(rootSchema.getFieldCount());
      this.rootSchema = rootSchema;
    }

    void addDirectOverride(int fieldId, @Nullable Object value) {
      setDirect(fieldId, value);
    }

    /**
     * Fast path for flat ({@code "foo"}) and dotted ({@code "nested.foo"}) identifier paths that
     * bypasses ANTLR4 parsing, {@link FieldAccessDescriptor} construction, and schema resolution.
     * Returns {@code false} without mutating state if the path contains qualifiers, wildcards,
     * whitespace, unknown fields, or non-{@code ROW} intermediates so the caller can fall back to
     * {@link FieldAccessDescriptor#withFieldNames(String...)}.
     */
    boolean tryAddPathOverride(@Nullable String fieldPath, @Nullable Object value) {
      if (fieldPath == null || fieldPath.isEmpty()) {
        return false;
      }
      if (FieldAccessDescriptorParser.isSimpleIdentifier(fieldPath)) {
        if (rootSchema.hasField(fieldPath)) {
          setDirect(rootSchema.indexOf(fieldPath), value);
          return true;
        }
        return false;
      }
      return tryAddDottedPathOverride(fieldPath, value);
    }

    private boolean tryAddDottedPathOverride(String fieldPath, @Nullable Object value) {
      int len = fieldPath.length();
      int firstDot = fieldPath.indexOf('.');
      if (firstDot <= 0 || firstDot == len - 1) {
        return false;
      }
      if (!FieldAccessDescriptorParser.isSimpleIdentifier(fieldPath, 0, firstDot)) {
        return false;
      }
      String firstSeg = fieldPath.substring(0, firstDot);
      if (!rootSchema.hasField(firstSeg)) {
        return false;
      }
      int firstId = rootSchema.indexOf(firstSeg);
      @Nullable Schema secondSchema = rootSchema.getField(firstId).getType().getRowSchema();
      if (secondSchema == null) {
        return false;
      }
      int secondDot = fieldPath.indexOf('.', firstDot + 1);
      if (secondDot < 0) {
        // Common 2-segment dotted path ("nested.field"): zero auxiliary array allocation.
        if (!FieldAccessDescriptorParser.isSimpleIdentifier(fieldPath, firstDot + 1, len)) {
          return false;
        }
        String leafSeg = fieldPath.substring(firstDot + 1, len);
        if (!secondSchema.hasField(leafSeg)) {
          return false;
        }
        int leafId = secondSchema.indexOf(leafSeg);
        getOrCreateNested(firstId, secondSchema.getFieldCount()).setDirect(leafId, value);
        return true;
      }
      return tryAddDeepDottedPathOverride(fieldPath, value, firstId, secondSchema, firstDot + 1);
    }

    private boolean tryAddDeepDottedPathOverride(
        String fieldPath, @Nullable Object value, int firstId, Schema secondSchema, int start) {
      int len = fieldPath.length();
      int[] fieldIds = new int[4];
      fieldIds[0] = firstId;
      int depth = 1;
      Schema currentSchema = secondSchema;
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
        int id = currentSchema.indexOf(seg);
        if (depth == fieldIds.length) {
          fieldIds = Arrays.copyOf(fieldIds, fieldIds.length * 2);
        }
        fieldIds[depth++] = id;
        if (nextDot < 0) {
          break;
        }
        @Nullable Schema nextSchema = currentSchema.getField(id).getType().getRowSchema();
        if (nextSchema == null) {
          return false;
        }
        currentSchema = nextSchema;
        start = nextDot + 1;
      }
      // Commit using cached fieldIds with no second substring() / indexOf() pass.
      FieldAccessNode node = this;
      currentSchema = rootSchema;
      for (int i = 0; i < depth - 1; i++) {
        int id = fieldIds[i];
        Schema nextSchema = checkNotNull(currentSchema.getField(id).getType().getRowSchema());
        node = node.getOrCreateNested(id, nextSchema.getFieldCount());
        currentSchema = nextSchema;
      }
      node.setDirect(fieldIds[depth - 1], value);
      return true;
    }

    void addOverride(FieldAccessDescriptor fieldAccessDescriptor, @Nullable Object value) {
      validateOverridePath(fieldAccessDescriptor, rootSchema);
      addOverride(fieldAccessDescriptor, value, rootSchema);
    }

    private static void validateOverridePath(
        FieldAccessDescriptor descriptor, Schema currentSchema) {
      while (descriptor.getFieldsAccessed().isEmpty()
          && !descriptor.getNestedFieldsAccessed().isEmpty()) {
        Entry<FieldDescriptor, FieldAccessDescriptor> entry =
            Iterables.getOnlyElement(descriptor.getNestedFieldsAccessed().entrySet());
        int fieldId = checkNotNull(entry.getKey().getFieldId());
        Schema.Field field = currentSchema.getField(fieldId);
        @Nullable Schema nestedSchema = field.getType().getRowSchema();
        if (nestedSchema == null) {
          throw new IllegalArgumentException(
              String.format(
                  "Cannot apply nested field override to non-ROW field %s", field.getName()));
        }
        currentSchema = nestedSchema;
        descriptor = entry.getValue();
      }
    }

    Row apply(@Nullable Row sourceRow) {
      return apply(rootSchema, sourceRow);
    }
  }
}
