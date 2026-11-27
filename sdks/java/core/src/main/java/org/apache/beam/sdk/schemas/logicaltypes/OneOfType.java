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
package org.apache.beam.sdk.schemas.logicaltypes;

import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkArgument;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.Field;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.schemas.Schema.LogicalType;
import org.apache.beam.sdk.schemas.SchemaTranslation;
import org.apache.beam.sdk.values.Row;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Instant;
import org.joda.time.ReadableInstant;

/**
 * A logical type representing a union of fields. This logical type is initialized with a set of
 * field and represents a union of those fields. This logical type is backed by a Row object
 * containing one nullable field matching each input field, and one additional {@link
 * EnumerationType} logical type field that indicates which field is set.
 */
public class OneOfType implements LogicalType<OneOfType.Value, Row> {
  public static final String IDENTIFIER = "OneOf";

  private static final int MAX_DENSE_CASE_VALUE = 256;

  private final Schema oneOfSchema;
  private final EnumerationType enumerationType;
  private final byte[] schemaProtoRepresentation;
  private transient volatile @Nullable LookupCache lookupCache;

  private static final class LookupCache {
    final FieldType baseType;
    final EnumerationType.Value[] fieldIndexToCase;
    final FieldType[] fieldIndexToType;
    final int @Nullable [] denseCaseToFieldIndex;
    final @Nullable Map<Integer, Integer> sparseCaseToFieldIndex;

    LookupCache(Schema oneOfSchema, EnumerationType enumerationType) {
      int fieldCount = oneOfSchema.getFieldCount();
      this.baseType = FieldType.row(oneOfSchema);
      this.fieldIndexToCase = new EnumerationType.Value[fieldCount];
      this.fieldIndexToType = new FieldType[fieldCount];
      int min = Integer.MAX_VALUE;
      int max = Integer.MIN_VALUE;
      for (int i = 0; i < fieldCount; i++) {
        Field field = oneOfSchema.getField(i);
        EnumerationType.Value caseVal = enumerationType.valueOf(field.getName());
        fieldIndexToCase[i] = caseVal;
        fieldIndexToType[i] = field.getType();
        int iv = caseVal.getValue();
        if (iv < min) {
          min = iv;
        }
        if (iv > max) {
          max = iv;
        }
      }
      boolean useDense =
          fieldCount > 0
              && min >= 0
              && max <= MAX_DENSE_CASE_VALUE
              && max <= Math.max(fieldCount * 2, 64);
      if (useDense) {
        int[] dense = new int[max + 1];
        Arrays.fill(dense, -1);
        for (int i = 0; i < fieldCount; i++) {
          dense[fieldIndexToCase[i].getValue()] = i;
        }
        this.denseCaseToFieldIndex = dense;
        this.sparseCaseToFieldIndex = null;
      } else {
        Map<Integer, Integer> sparse = new HashMap<>(fieldCount * 2);
        for (int i = 0; i < fieldCount; i++) {
          sparse.put(fieldIndexToCase[i].getValue(), i);
        }
        this.denseCaseToFieldIndex = null;
        this.sparseCaseToFieldIndex = sparse;
      }
    }

    int getFieldIndex(int caseValue) {
      if (denseCaseToFieldIndex != null) {
        if (caseValue >= 0 && caseValue < denseCaseToFieldIndex.length) {
          int idx = denseCaseToFieldIndex[caseValue];
          if (idx >= 0) {
            return idx;
          }
        }
      } else if (sparseCaseToFieldIndex != null) {
        Integer idx = sparseCaseToFieldIndex.get(caseValue);
        if (idx != null) {
          return idx;
        }
      }
      throw new IllegalArgumentException(String.format("Unknown enumeration value %s", caseValue));
    }
  }

  private LookupCache getLookupCache() {
    LookupCache cache = lookupCache;
    if (cache == null) {
      cache = new LookupCache(oneOfSchema, enumerationType);
      lookupCache = cache;
    }
    return cache;
  }

  private OneOfType(List<Field> fields) {
    this(fields, null);
  }

  private OneOfType(List<Field> fields, @Nullable Map<String, Integer> enumMap) {
    List<Field> nullableFields =
        fields.stream().map(f -> f.withNullable(true)).collect(Collectors.toList());
    if (enumMap != null) {
      nullableFields.stream().forEach(f -> checkArgument(enumMap.containsKey(f.getName())));
      enumerationType = EnumerationType.create(enumMap);
    } else {
      List<String> enumValues =
          nullableFields.stream().map(Field::getName).collect(Collectors.toList());
      enumerationType = EnumerationType.create(enumValues);
    }
    oneOfSchema = Schema.builder().addFields(nullableFields).build();
    schemaProtoRepresentation =
        SchemaTranslation.schemaToProto(oneOfSchema, false, false).toByteArray();
    lookupCache = new LookupCache(oneOfSchema, enumerationType);
  }

  /** Create an {@link OneOfType} logical type. */
  public static OneOfType create(Field... fields) {
    return create(Arrays.asList(fields));
  }

  /** Create an {@link OneOfType} logical type. */
  public static OneOfType create(List<Field> fields) {
    return new OneOfType(fields);
  }

  /**
   * Create an {@link OneOfType} logical type. This method allows control over the integer values in
   * the generated enum.
   */
  public static OneOfType create(List<Field> fields, Map<String, Integer> enumValues) {
    return new OneOfType(fields, enumValues);
  }

  /** Returns the schema of the underlying {@link Row} that is used to represent the union. */
  public Schema getOneOfSchema() {
    return oneOfSchema;
  }

  /** Returns the {@link EnumerationType} that is used to represent the case type. */
  public EnumerationType getCaseEnumType() {
    return enumerationType;
  }

  @Override
  public String getIdentifier() {
    return IDENTIFIER;
  }

  @Override
  public FieldType getArgumentType() {
    return FieldType.BYTES;
  }

  @Override
  public byte[] getArgument() {
    return schemaProtoRepresentation;
  }

  @Override
  public FieldType getBaseType() {
    return getLookupCache().baseType;
  }

  /** Create a {@link Value} specifying which field to set and the value to set. */
  public <T extends @NonNull Object> Value createValue(String caseValue, T value) {
    return createValue(getCaseEnumType().valueOf(caseValue), value);
  }

  /** Create a {@link Value} specifying which field to set and the value to set. */
  public <T extends @NonNull Object> Value createValue(int caseValue, T value) {
    return createValue(getCaseEnumType().valueOf(caseValue), value);
  }

  /** Create a {@link Value} specifying which field to set and the value to set. */
  public <T extends @NonNull Object> Value createValue(EnumerationType.Value caseType, T value) {
    return new Value(caseType, value);
  }

  public FieldType getFieldType(OneOfType.Value oneOneValue) {
    LookupCache cache = getLookupCache();
    return cache.fieldIndexToType[cache.getFieldIndex(oneOneValue.getCaseType().getValue())];
  }

  @Override
  public Row toBaseType(Value input) {
    LookupCache cache = getLookupCache();
    int setFieldIndex = cache.getFieldIndex(input.getCaseType().getValue());
    Object val = input.getValue();
    FieldType fieldType = cache.fieldIndexToType[setFieldIndex];
    @Nullable Object coerced = tryFastCoerceScalar(fieldType, val);
    @Nullable Object[] values = new @Nullable Object[oneOfSchema.getFieldCount()];
    if (coerced != null) {
      values[setFieldIndex] = coerced;
      return Row.attachValues(oneOfSchema, values);
    }
    values[setFieldIndex] = val;
    return Row.withSchema(oneOfSchema).addValues(Arrays.asList(values)).build();
  }

  private static @Nullable Object tryFastCoerceScalar(FieldType fieldType, @Nullable Object val) {
    if (val == null) {
      return null;
    }
    switch (fieldType.getTypeName()) {
      case STRING:
        return val instanceof String ? val : null;
      case INT64:
        return val instanceof Long ? val : null;
      case INT32:
        return val instanceof Integer ? val : null;
      case DOUBLE:
        return val instanceof Double ? val : null;
      case FLOAT:
        return val instanceof Float ? val : null;
      case BOOLEAN:
        return val instanceof Boolean ? val : null;
      case BYTE:
        return val instanceof Byte ? val : null;
      case INT16:
        return val instanceof Short ? val : null;
      case DECIMAL:
        return val instanceof BigDecimal ? val : null;
      case BYTES:
        if (val instanceof byte[]) {
          return val;
        }
        if (val instanceof ByteBuffer) {
          ByteBuffer bb = ((ByteBuffer) val).slice();
          byte[] bytes = new byte[bb.remaining()];
          bb.get(bytes);
          return bytes;
        }
        return null;
      case DATETIME:
        if (val instanceof Instant) {
          return val;
        }
        if (val instanceof ReadableInstant) {
          return ((ReadableInstant) val).toInstant();
        }
        return null;
      default:
        return null;
    }
  }

  @Override
  public Value toInputType(Row base) {
    LookupCache cache = getLookupCache();
    EnumerationType.Value caseType = null;
    Object oneOfValue = null;
    int fieldCount = base.getFieldCount();
    for (int i = 0; i < fieldCount; ++i) {
      Object value = base.getValue(i);
      if (value != null) {
        if (caseType != null) {
          throw new IllegalArgumentException(
              String.format("More than one field set in union %s", this));
        }
        caseType = cache.fieldIndexToCase[i];
        oneOfValue = value;
      }
    }
    if (caseType == null || oneOfValue == null) {
      throw new IllegalArgumentException(String.format("No value set in union %s", this));
    }
    return createValue(caseType, oneOfValue);
  }

  @Override
  public String toString() {
    return "OneOf: " + oneOfSchema;
  }

  /**
   * Represents a single OneOf value. Each object contains an {@link EnumerationType.Value}
   * specifying which field is set along with the value of that field.
   */
  public static class Value {
    private final EnumerationType.Value caseType;
    private final Object value;

    public Value(EnumerationType.Value caseType, Object value) {
      this.caseType = caseType;
      this.value = value;
    }

    /** Returns the enumeration that specified which OneOf field is set. */
    public EnumerationType.Value getCaseType() {
      return caseType;
    }

    /** Returns the current value of the OneOf as the destination type. */
    public <T> T getValue(Class<T> clazz) {
      return (T) value;
    }

    /** Returns the current value of the OneOf. */
    public Object getValue() {
      return value;
    }

    @Override
    public String toString() {
      return "caseType: " + caseType + " Value: " + value;
    }

    @Override
    public boolean equals(@Nullable Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof Value)) {
        return false;
      }
      Value value1 = (Value) o;
      return Objects.equals(caseType, value1.caseType) && Objects.equals(value, value1.value);
    }

    @Override
    public int hashCode() {
      return 31 * (31 + Objects.hashCode(caseType)) + Objects.hashCode(value);
    }
  }
}
