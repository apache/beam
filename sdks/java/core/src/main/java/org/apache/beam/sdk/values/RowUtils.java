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
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import org.apache.beam.sdk.schemas.FieldAccessDescriptor;
import org.apache.beam.sdk.schemas.FieldAccessDescriptor.FieldDescriptor;
import org.apache.beam.sdk.schemas.FieldAccessDescriptor.FieldDescriptor.ListQualifier;
import org.apache.beam.sdk.schemas.FieldAccessDescriptor.FieldDescriptor.MapQualifier;
import org.apache.beam.sdk.schemas.FieldAccessDescriptor.FieldDescriptor.Qualifier;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.schemas.Schema.LogicalType;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Iterables;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Lists;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Maps;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Instant;
import org.joda.time.base.AbstractInstant;

class RowUtils {
  static class RowPosition {
    FieldAccessDescriptor descriptor;
    List<FieldAccessDescriptor.FieldDescriptor.Qualifier> qualifiers;

    RowPosition(FieldAccessDescriptor descriptor) {
      this(descriptor, Collections.emptyList());
    }

    RowPosition(FieldAccessDescriptor descriptor, List<Qualifier> qualifiers) {
      this.descriptor = descriptor;
      this.qualifiers = qualifiers;
    }

    RowPosition withArrayQualifier() {
      List<Qualifier> newQualifiers = Lists.newArrayListWithCapacity(qualifiers.size() + 1);
      newQualifiers.addAll(qualifiers);
      newQualifiers.add(Qualifier.of(ListQualifier.ALL));
      return new RowPosition(descriptor, newQualifiers);
    }

    RowPosition withMapQualifier() {
      List<Qualifier> newQualifiers = Lists.newArrayListWithCapacity(qualifiers.size() + 1);
      newQualifiers.addAll(qualifiers);
      newQualifiers.add(Qualifier.of(MapQualifier.ALL));
      return new RowPosition(descriptor, newQualifiers);
    }
  }

  // Subclasses of this interface implement process methods for each schema type. Each process
  // method is invoked as
  // a RowFieldMatcher walks down the schema tree. The FieldAccessDescriptor passed into each method
  // identifies the
  // current element of the schema being processed.
  interface RowCases {
    Row processRow(RowPosition rowPosition, Schema schema, Row value, RowFieldMatcher matcher);

    Collection<Object> processArray(
        RowPosition rowPosition,
        FieldType collectionElementType,
        Collection<Object> values,
        RowFieldMatcher matcher);

    Iterable<Object> processIterable(
        RowPosition rowPosition,
        FieldType collectionElementType,
        Iterable<Object> values,
        RowFieldMatcher matcher);

    Map<Object, Object> processMap(
        RowPosition rowPosition,
        FieldType keyType,
        FieldType valueType,
        Map<Object, Object> valueMap,
        RowFieldMatcher matcher);

    Object processLogicalType(
        RowPosition rowPosition, LogicalType logicalType, Object baseType, RowFieldMatcher matcher);

    Instant processDateTime(
        RowPosition rowPosition, AbstractInstant instant, RowFieldMatcher matcher);

    Byte processByte(RowPosition rowPosition, Byte value, RowFieldMatcher matcher);

    Short processInt16(RowPosition rowPosition, Short value, RowFieldMatcher matcher);

    Integer processInt32(RowPosition rowPosition, Integer value, RowFieldMatcher matcher);

    Long processInt64(RowPosition rowPosition, Long value, RowFieldMatcher matcher);

    BigDecimal processDecimal(RowPosition rowPosition, BigDecimal value, RowFieldMatcher matcher);

    Float processFloat(RowPosition rowPosition, Float value, RowFieldMatcher matcher);

    Double processDouble(RowPosition rowPosition, Double value, RowFieldMatcher matcher);

    String processString(RowPosition rowPosition, String value, RowFieldMatcher matcher);

    Boolean processBoolean(RowPosition rowPosition, Boolean value, RowFieldMatcher matcher);

    byte[] processBytes(RowPosition rowPosition, byte[] value, RowFieldMatcher matcher);
  }

  // Given a Row field, delegates processing to the correct process method on the RowCases
  // parameter.
  static class RowFieldMatcher {
    public Object match(
        RowCases cases, FieldType fieldType, RowPosition rowPosition, Object value) {
      Object processedValue = null;
      switch (fieldType.getTypeName()) {
        case ARRAY:
          processedValue =
              cases.processArray(
                  rowPosition,
                  fieldType.getCollectionElementType(),
                  (Collection<Object>) value,
                  this);
          break;
        case ITERABLE:
          processedValue =
              cases.processIterable(
                  rowPosition,
                  fieldType.getCollectionElementType(),
                  (Iterable<Object>) value,
                  this);
          break;
        case MAP:
          processedValue =
              cases.processMap(
                  rowPosition,
                  fieldType.getMapKeyType(),
                  fieldType.getMapValueType(),
                  (Map<Object, Object>) value,
                  this);
          break;
        case ROW:
          processedValue =
              cases.processRow(rowPosition, fieldType.getRowSchema(), (Row) value, this);
          break;
        case LOGICAL_TYPE:
          LogicalType logicalType = fieldType.getLogicalType();
          processedValue = cases.processLogicalType(rowPosition, logicalType, value, this);
          break;
        case DATETIME:
          processedValue = cases.processDateTime(rowPosition, (AbstractInstant) value, this);
          break;
        case BYTE:
          processedValue = cases.processByte(rowPosition, (Byte) value, this);
          break;
        case BYTES:
          processedValue = cases.processBytes(rowPosition, (byte[]) value, this);
          break;
        case INT16:
          processedValue = cases.processInt16(rowPosition, (Short) value, this);
          break;
        case INT32:
          processedValue = cases.processInt32(rowPosition, (Integer) value, this);
          break;
        case INT64:
          processedValue = cases.processInt64(rowPosition, (Long) value, this);
          break;
        case DECIMAL:
          processedValue = cases.processDecimal(rowPosition, (BigDecimal) value, this);
          break;
        case FLOAT:
          processedValue = cases.processFloat(rowPosition, (Float) value, this);
          break;
        case DOUBLE:
          processedValue = cases.processDouble(rowPosition, (Double) value, this);
          break;
        case STRING:
          processedValue = cases.processString(rowPosition, (String) value, this);
          break;
        case BOOLEAN:
          processedValue = cases.processBoolean(rowPosition, (Boolean) value, this);
          break;
        default:
          // Shouldn't actually get here, but we need this case to satisfy linters.
          throw new IllegalArgumentException(
              String.format(
                  "Not a primitive type for field name %s: %s", rowPosition.descriptor, fieldType));
      }
      if (processedValue == null) {
        if (!fieldType.getNullable()) {
          throw new IllegalArgumentException(
              String.format("%s is not nullable in  field %s", fieldType, rowPosition.descriptor));
        }
      }
      return processedValue;
    }
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
    private FieldAccessNode topNode;
    private Schema rootSchema;

    FieldOverrides(Schema rootSchema) {
      this.topNode = new FieldAccessNode(rootSchema);
      this.rootSchema = rootSchema;
    }

    boolean isEmpty() {
      return topNode.isEmpty();
    }

    void addOverride(FieldAccessDescriptor fieldAccessDescriptor, FieldOverride fieldOverride) {
      topNode.addOverride(fieldAccessDescriptor, fieldOverride, rootSchema);
    }

    void setOverrides(List<Object> values) {
      List<FieldOverride> overrides = Lists.newArrayListWithExpectedSize(values.size());
      for (Object value : values) {
        overrides.add(new FieldOverride(value));
      }
      topNode.setOverrides(overrides);
    }

    @Nullable
    FieldOverride getOverride(FieldAccessDescriptor fieldAccessDescriptor) {
      return topNode.getOverride(fieldAccessDescriptor);
    }

    boolean hasOverrideBelow(FieldAccessDescriptor fieldAccessDescriptor) {
      return topNode.hasOverrideBelow(fieldAccessDescriptor);
    }

    private static class FieldAccessNode {
      List<FieldOverride> fieldOverrides;
      List<FieldAccessNode> nestedAccess;

      FieldAccessNode(Schema schema) {
        fieldOverrides = Lists.newArrayListWithExpectedSize(schema.getFieldCount());
        nestedAccess = Lists.newArrayList();
      }

      boolean isEmpty() {
        return fieldOverrides.isEmpty() && nestedAccess.isEmpty();
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

      void setOverrides(List<FieldOverride> overrides) {
        this.fieldOverrides = overrides;
      }

      @Nullable
      FieldOverride getOverride(FieldAccessDescriptor fieldAccessDescriptor) {
        FieldOverride override = null;
        if (!fieldAccessDescriptor.getFieldsAccessed().isEmpty()) {
          FieldDescriptor fieldDescriptor =
              Iterables.getOnlyElement(fieldAccessDescriptor.getFieldsAccessed());
          if (fieldDescriptor.getFieldId() < fieldOverrides.size()) {
            override = fieldOverrides.get(fieldDescriptor.getFieldId());
          }
        } else if (!fieldAccessDescriptor.getNestedFieldsAccessed().isEmpty()) {
          Map.Entry<FieldDescriptor, FieldAccessDescriptor> entry =
              Iterables.getOnlyElement(fieldAccessDescriptor.getNestedFieldsAccessed().entrySet());
          if (entry.getKey().getFieldId() < nestedAccess.size()) {
            FieldAccessNode node = nestedAccess.get(entry.getKey().getFieldId());
            if (node != null) {
              override = node.getOverride(entry.getValue());
            }
          }
        }
        return override;
      }

      boolean hasOverrideBelow(FieldAccessDescriptor fieldAccessDescriptor) {
        if (!fieldAccessDescriptor.getFieldsAccessed().isEmpty()) {
          FieldDescriptor fieldDescriptor =
              Iterables.getOnlyElement(fieldAccessDescriptor.getFieldsAccessed());
          return (((fieldDescriptor.getFieldId() < nestedAccess.size()))
              && nestedAccess.get(fieldDescriptor.getFieldId()) != null);
        } else if (!fieldAccessDescriptor.getNestedFieldsAccessed().isEmpty()) {
          Map.Entry<FieldDescriptor, FieldAccessDescriptor> entry =
              Iterables.getOnlyElement(fieldAccessDescriptor.getNestedFieldsAccessed().entrySet());
          if (entry.getKey().getFieldId() < nestedAccess.size()) {
            FieldAccessNode node = nestedAccess.get(entry.getKey().getFieldId());
            if (node != null) {
              return node.hasOverrideBelow(entry.getValue());
            }
          }
        } else {
          return true;
        }
        return false;
      }
    }
  }

  // This implementation of RowCases captures a Row into a new Row. It also has the effect of
  // validating all the
  // field parameters.
  // A Map of field values can also be passed in, and those field values will be used to override
  // the values in the
  // passed-in row.
  static class CapturingRowCases implements RowCases {
    private final Schema topSchema;
    private final FieldOverrides fieldOverrides;

    CapturingRowCases(Schema topSchema, FieldOverrides fieldOverrides) {
      this.topSchema = topSchema;
      this.fieldOverrides = fieldOverrides;
    }

    private @Nullable FieldOverride override(RowPosition rowPosition) {
      if (!rowPosition.qualifiers.isEmpty()) {
        // Currently we only support overriding named schema fields. Individual array/map elements
        // or nested collections
        // cannot be overriden without overriding the entire schema fields.
        return null;
      } else {
        return fieldOverrides.getOverride(rowPosition.descriptor);
      }
    }

    private <T> T overrideOrReturn(RowPosition rowPosition, T value) {
      FieldOverride fieldOverride = override(rowPosition);
      // null return means the item isn't in the map.
      return (fieldOverride != null) ? (T) fieldOverride.getOverrideValue() : value;
    }

    @Override
    public Row processRow(
        RowPosition rowPosition, Schema schema, Row value, RowFieldMatcher matcher) {
      FieldOverride override = override(rowPosition);
      Row retValue = value;
      if (override != null) {
        retValue = (Row) override.getOverrideValue();
      } else if (fieldOverrides.hasOverrideBelow(rowPosition.descriptor)) {
        List<Object> values = Lists.newArrayListWithCapacity(schema.getFieldCount());
        for (int i = 0; i < schema.getFieldCount(); ++i) {
          FieldAccessDescriptor nestedDescriptor =
              FieldAccessDescriptor.withFieldIds(rowPosition.descriptor, i).resolve(topSchema);
          Object fieldValue = (value != null) ? value.getValue(i) : null;
          values.add(
              matcher.match(
                  this,
                  schema.getField(i).getType(),
                  new RowPosition(nestedDescriptor),
                  fieldValue));
        }
        retValue = new RowWithStorage(schema, values);
      }
      return retValue;
    }

    @Override
    public Collection<Object> processArray(
        RowPosition rowPosition,
        FieldType collectionElementType,
        Collection<Object> values,
        RowFieldMatcher matcher) {
      Collection<Object> retValue = null;
      FieldOverride override = override(rowPosition);
      if (override != null) {
        retValue =
            captureIterable(
                rowPosition,
                collectionElementType,
                (Collection<Object>) override.getOverrideValue(),
                matcher);
      } else if (values != null) {
        retValue = captureIterable(rowPosition, collectionElementType, values, matcher);
      }
      return retValue;
    }

    @Override
    public Iterable<Object> processIterable(
        RowPosition rowPosition,
        FieldType collectionElementType,
        Iterable<Object> values,
        RowFieldMatcher matcher) {
      Iterable<Object> retValue = null;
      FieldOverride override = override(rowPosition);
      if (override != null) {
        retValue =
            captureIterable(
                rowPosition,
                collectionElementType,
                (Iterable<Object>) override.getOverrideValue(),
                matcher);
      } else if (values != null) {
        retValue = captureIterable(rowPosition, collectionElementType, values, matcher);
      }
      return retValue;
    }

    private Collection<Object> captureIterable(
        RowPosition rowPosition,
        FieldType collectionElementType,
        Iterable<Object> values,
        RowFieldMatcher matcher) {
      if (values == null) {
        return null;
      }

      List<Object> captured = Lists.newArrayListWithCapacity(Iterables.size(values));
      RowPosition elementPosition = rowPosition.withArrayQualifier();
      for (Object listValue : values) {
        if (listValue == null) {
          if (!collectionElementType.getNullable()) {
            throw new IllegalArgumentException(
                String.format(
                    "%s is not nullable in Array field %s",
                    collectionElementType, rowPosition.descriptor));
          }
          captured.add(null);
        } else {
          Object capturedElement =
              matcher.match(this, collectionElementType, elementPosition, listValue);
          captured.add(capturedElement);
        }
      }
      return captured;
    }

    @Override
    public Map<Object, Object> processMap(
        RowPosition rowPosition,
        FieldType keyType,
        FieldType valueType,
        Map<Object, Object> valueMap,
        RowFieldMatcher matcher) {
      Map<Object, Object> retValue = null;
      FieldOverride override = override(rowPosition);
      if (override != null) {
        valueMap = (Map<Object, Object>) override.getOverrideValue();
      }

      if (valueMap != null) {
        RowPosition elementPosition = rowPosition.withMapQualifier();

        retValue = Maps.newHashMapWithExpectedSize(valueMap.size());
        for (Entry<Object, Object> kv : valueMap.entrySet()) {
          if (kv.getValue() == null) {
            if (!valueType.getNullable()) {
              throw new IllegalArgumentException(
                  String.format(
                      "%s is not nullable in Map field %s", valueType, rowPosition.descriptor));
            }
            retValue.put(matcher.match(this, keyType, elementPosition, kv.getKey()), null);
          } else {
            retValue.put(
                matcher.match(this, keyType, elementPosition, kv.getKey()),
                matcher.match(this, valueType, elementPosition, kv.getValue()));
          }
        }
      }
      return retValue;
    }

    @Override
    public Object processLogicalType(
        RowPosition rowPosition, LogicalType logicalType, Object value, RowFieldMatcher matcher) {
      Object retValue = null;
      FieldOverride override = override(rowPosition);
      if (override != null && override.getOverrideValue() != null) {
        retValue = logicalType.toInputType(logicalType.toBaseType(override.getOverrideValue()));
      } else if (value != null) {
        retValue = logicalType.toInputType(logicalType.toBaseType(value));
      }
      return retValue;
    }

    @Override
    public Instant processDateTime(
        RowPosition rowPosition, AbstractInstant value, RowFieldMatcher matcher) {
      AbstractInstant instantValue = overrideOrReturn(rowPosition, value);
      return (instantValue != null) ? instantValue.toInstant() : null;
    }

    @Override
    public Byte processByte(RowPosition rowPosition, Byte value, RowFieldMatcher matcher) {
      return overrideOrReturn(rowPosition, value);
    }

    @Override
    public Short processInt16(RowPosition rowPosition, Short value, RowFieldMatcher matcher) {
      return overrideOrReturn(rowPosition, value);
    }

    @Override
    public Integer processInt32(RowPosition rowPosition, Integer value, RowFieldMatcher matcher) {
      return overrideOrReturn(rowPosition, value);
    }

    @Override
    public Long processInt64(RowPosition rowPosition, Long value, RowFieldMatcher matcher) {
      return overrideOrReturn(rowPosition, value);
    }

    @Override
    public BigDecimal processDecimal(
        RowPosition rowPosition, BigDecimal value, RowFieldMatcher matcher) {
      return overrideOrReturn(rowPosition, value);
    }

    @Override
    public Float processFloat(RowPosition rowPosition, Float value, RowFieldMatcher matcher) {
      return overrideOrReturn(rowPosition, value);
    }

    @Override
    public Double processDouble(RowPosition rowPosition, Double value, RowFieldMatcher matcher) {
      return overrideOrReturn(rowPosition, value);
    }

    @Override
    public String processString(RowPosition rowPosition, String value, RowFieldMatcher matcher) {
      return overrideOrReturn(rowPosition, value);
    }

    @Override
    public Boolean processBoolean(RowPosition rowPosition, Boolean value, RowFieldMatcher matcher) {
      return overrideOrReturn(rowPosition, value);
    }

    @Override
    public byte[] processBytes(RowPosition rowPosition, byte[] value, RowFieldMatcher matcher) {
      Object retValue = overrideOrReturn(rowPosition, value);
      return (retValue instanceof ByteBuffer) ? ((ByteBuffer) retValue).array() : (byte[]) retValue;
    }
  }
}
