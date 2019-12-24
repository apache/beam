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

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkArgument;
import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkNotNull;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.Field;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.schemas.Schema.LogicalType;
import org.apache.beam.sdk.schemas.SchemaTranslation;
import org.apache.beam.sdk.values.Row;

/**
 * A logical type representing a union of fields. This logical type is initialized with a set of
 * field and represents a union of those fields. This logical type is backed by a Row object
 * containing one nullable field matching each input field, and one additional {@link
 * EnumerationType} logical type field that indicates which field is set.
 */
public class OneOfType implements LogicalType<OneOfType.Value, Row> {
  public static final String IDENTIFIER = "OneOf";

  private final Schema oneOfSchema;
  private final EnumerationType enumerationType;
  private final byte[] schemaProtoRepresentation;

  private OneOfType(List<Field> fields) {
    this(fields, null);
  }

  private OneOfType(List<Field> fields, @Nullable Map<String, Integer> enumMap) {
    List<Field> nullableFields =
        fields.stream()
            .map(f -> Field.nullable(f.getName(), f.getType()))
            .collect(Collectors.toList());
    if (enumMap != null) {
      nullableFields.stream().forEach(f -> checkArgument(enumMap.containsKey(f.getName())));
      enumerationType = EnumerationType.create(enumMap);
    } else {
      List<String> enumValues =
          nullableFields.stream().map(Field::getName).collect(Collectors.toList());
      enumerationType = EnumerationType.create(enumValues);
    }
    oneOfSchema = Schema.builder().addFields(nullableFields).build();
    schemaProtoRepresentation = SchemaTranslation.schemaToProto(oneOfSchema, false).toByteArray();
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
    return FieldType.row(oneOfSchema);
  }

  /** Create a {@link Value} specifying which field to set and the value to set. */
  public <T> Value createValue(String caseType, T value) {
    return createValue(getCaseEnumType().valueOf(caseType), value);
  }

  /** Create a {@link Value} specifying which field to set and the value to set. */
  public <T> Value createValue(EnumerationType.Value caseType, T value) {
    return new Value(caseType, oneOfSchema.getField(caseType.toString()).getType(), value);
  }

  @Override
  public Row toBaseType(Value input) {
    EnumerationType.Value caseType = input.getCaseType();
    int setFieldIndex = oneOfSchema.indexOf(caseType.toString());
    Row.Builder builder = Row.withSchema(oneOfSchema);
    for (int i = 0; i < oneOfSchema.getFieldCount(); ++i) {
      Object value = (i == setFieldIndex) ? input.getValue() : null;
      builder = builder.addValue(value);
    }
    return builder.build();
  }

  @Override
  public Value toInputType(Row base) {
    EnumerationType.Value caseType = null;
    Object oneOfValue = null;
    for (int i = 0; i < base.getFieldCount(); ++i) {
      Object value = base.getValue(i);
      if (value != null) {
        checkArgument(caseType == null, "More than one field set in union " + this);
        caseType = enumerationType.valueOf(oneOfSchema.getField(i).getName());
        oneOfValue = value;
      }
    }
    checkNotNull(oneOfValue, "No value set in union" + this);
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
    private final FieldType fieldType;
    private final Object value;

    public Value(EnumerationType.Value caseType, FieldType fieldType, Object value) {
      this.caseType = caseType;
      this.fieldType = fieldType;
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

    /** Return the type of this union field. */
    public FieldType getFieldType() {
      return fieldType;
    }

    @Override
    public String toString() {
      return "caseType: " + caseType + " Value: " + value;
    }
  }
}
