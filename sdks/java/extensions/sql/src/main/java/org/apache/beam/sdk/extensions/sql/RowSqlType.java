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
package org.apache.beam.sdk.extensions.sql;

import static org.apache.beam.sdk.schemas.Schema.toSchema;

import com.google.common.collect.ImmutableList;
import javax.annotation.Nullable;
import org.apache.beam.sdk.extensions.sql.impl.utils.CalciteUtils;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.Field;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.schemas.Schema.FieldTypeDescriptor;
import org.apache.beam.sdk.values.Row;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.type.SqlTypeName;


/**
 * Type builder for {@link Row} with SQL types.
 *
 * <p>Limited SQL types are supported now, visit
 * <a href="https://beam.apache.org/documentation/dsls/sql/#data-types">data types</a>
 * for more details.
 *
 */
public class RowSqlType {
  public static Builder builder() {
    return new Builder();
  }

  /**
   * Builder class to construct {@link Schema}.
   */
  public static class Builder {

    private ImmutableList.Builder<Schema.Field> fields;

    public Builder withField(String fieldName, Schema.FieldType fieldType,
                             @Nullable Schema.FieldTypeDescriptor componentType,
                             @Nullable Schema fieldSchema,
                             @Nullable byte[] metadata) {
      FieldTypeDescriptor fieldTypeDescriptor =
          FieldTypeDescriptor.of(fieldType)
              .withComponentType(componentType)
              .withRowSchema(fieldSchema)
              .withMetadata(metadata);

      // For now, we mark all fields as Nullable. Calcite supports nullable annotations, so
      // TODO: Support proper nullable annotations in SQL. Note that Join schemas still need to be
      // marked nullable (for the case of outer joins).
      fields.add(Field.of(fieldName, fieldTypeDescriptor)
          .withNullable(true));
      return this;
    }

    public Builder withTinyIntField(String fieldName) {
      return withField(fieldName, FieldType.BYTE, null, null, null);
    }

    public Builder withSmallIntField(String fieldName) {
      return withField(fieldName, FieldType.INT16, null, null, null);
    }

    public Builder withIntegerField(String fieldName) {
      return withField(fieldName, FieldType.INT32, null, null, null);
    }

    public Builder withBigIntField(String fieldName) {
      return withField(fieldName, FieldType.INT64, null, null, null);
    }

    public Builder withFloatField(String fieldName) {
      return withField(fieldName, FieldType.FLOAT, null, null, null);
    }

    public Builder withDoubleField(String fieldName) {
      return withField(fieldName, FieldType.DOUBLE, null, null, null);
    }

    public Builder withDecimalField(String fieldName) {
      return withField(fieldName, FieldType.DECIMAL, null, null, null);
    }

    public Builder withBooleanField(String fieldName) {
      return withField(fieldName, FieldType.BOOLEAN, null, null, null);
    }

    public Builder withCharField(String fieldName) {
      return withField(fieldName, FieldType.STRING, null, null,
          CalciteUtils.typeToMetadata(SqlTypeName.CHAR));
    }

    public Builder withVarcharField(String fieldName) {
      return withField(fieldName, FieldType.STRING, null, null,
          CalciteUtils.typeToMetadata(SqlTypeName.VARCHAR));
    }

    public Builder withTimeField(String fieldName) {
      return withField(fieldName, FieldType.DATETIME, null, null,
          CalciteUtils.typeToMetadata(SqlTypeName.TIME));
    }

    public Builder withDateField(String fieldName) {
      return withField(fieldName, FieldType.DATETIME, null, null,
          CalciteUtils.typeToMetadata(SqlTypeName.DATE));
    }

    public Builder withTimestampField(String fieldName) {
      return withField(fieldName, FieldType.DATETIME, null, null,
          CalciteUtils.typeToMetadata(SqlTypeName.TIMESTAMP));
    }

    /**
     * Adds an ARRAY field with elements of the give type.
     */
    public Builder withArrayField(String fieldName, RelDataType relDataType) {
      return withField(
          fieldName,
          FieldType.ARRAY,
          CalciteUtils.toFieldTypeDescriptor(relDataType),
          null,
          null);
    }

    /**
     * Adds an ARRAY field with elements of the give type.
     */
    public Builder withArrayField(String fieldName, FieldTypeDescriptor typeDescriptor) {
      return withField(
          fieldName,
          FieldType.ARRAY,
          typeDescriptor,
          null,
          null);
    }

    /**
     * Adds an ARRAY field with elements of the give primitive type.
     */
    public Builder withArrayField(String fieldName, FieldType fieldType) {
      return withField(
          fieldName,
          FieldType.ARRAY,
          FieldTypeDescriptor.of(fieldType),
          null,
          null);
    }

    /**
     * Adds an ARRAY field with elements of {@code rowType}.
     */
    public Builder withArrayField(String fieldName, Schema schema) {
      FieldTypeDescriptor componentType =
          FieldTypeDescriptor
              .of(FieldType.ROW)
              .withRowSchema(schema);
      return withField(
          fieldName,
          FieldType.ARRAY,
          componentType,
          null,
          null);
    }

    public Builder withRowField(String fieldName, Schema schema) {
      return withField(fieldName, FieldType.ROW, null, schema, null);
    }

    private Builder() {
      this.fields = ImmutableList.builder();
    }

    public Schema build() {
      return fields.build().stream().collect(toSchema());
    }
  }
}
