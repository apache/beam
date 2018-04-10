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

import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.extensions.sql.impl.utils.CalciteUtils;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.Field;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.schemas.Schema.TypeName;
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
 * <p>TODO: We should remove this class in favor of directly using Beam.Schema.Builder
 *
 */
@Experimental
public class RowSqlTypes {
  // The list of field type names used in SQL as Beam field types.
  public static final FieldType TINY_INT = CalciteUtils.toFieldType(SqlTypeName.TINYINT);
  public static final FieldType SMALL_INT = CalciteUtils.toFieldType(SqlTypeName.SMALLINT);
  public static final FieldType INTEGER = CalciteUtils.toFieldType(SqlTypeName.INTEGER);
  public static final FieldType BIG_INT = CalciteUtils.toFieldType(SqlTypeName.BIGINT);
  public static final FieldType FLOAT = CalciteUtils.toFieldType(SqlTypeName.FLOAT);
  public static final FieldType DOUBLE = CalciteUtils.toFieldType(SqlTypeName.DOUBLE);
  public static final FieldType DECIMAL = CalciteUtils.toFieldType(SqlTypeName.DECIMAL);
  public static final FieldType BOOLEAN = CalciteUtils.toFieldType(SqlTypeName.BOOLEAN);
  public static final FieldType CHAR = CalciteUtils.toFieldType(SqlTypeName.CHAR);
  public static final FieldType VARCHAR = CalciteUtils.toFieldType(SqlTypeName.VARCHAR);
  public static final FieldType TIME = CalciteUtils.toFieldType(SqlTypeName.TIME);
  public static final FieldType DATE = CalciteUtils.toFieldType(SqlTypeName.DATE);
  public static final FieldType TIMESTAMP = CalciteUtils.toFieldType(SqlTypeName.TIMESTAMP);

  public static Builder builder() {
    return new Builder();
  }

  /**
   * Builder class to construct {@link Schema}.
   */
  public static class Builder {
    Schema.Builder builder;

    public Builder withTinyIntField(String fieldName) {
      builder.addField(Field.of(fieldName, TINY_INT).withNullable(true));
      return this;
    }

    public Builder withSmallIntField(String fieldName) {
      builder.addField(Field.of(fieldName, SMALL_INT).withNullable(true));
      return this;
    }

    public Builder withIntegerField(String fieldName) {
      builder.addField(Field.of(fieldName, INTEGER).withNullable(true));
      return this;
    }

    public Builder withBigIntField(String fieldName) {
      builder.addField(Field.of(fieldName, BIG_INT).withNullable(true));
      return this;
    }

    public Builder withFloatField(String fieldName) {
      builder.addField(Field.of(fieldName, FLOAT).withNullable(true));
      return this;
    }

    public Builder withDoubleField(String fieldName) {
      builder.addField(Field.of(fieldName, DOUBLE).withNullable(true));
      return this;
    }

    public Builder withDecimalField(String fieldName) {
      builder.addField(Field.of(fieldName, DECIMAL).withNullable(true));
      return this;
    }

    public Builder withBooleanField(String fieldName) {
      builder.addField(Field.of(fieldName, BOOLEAN).withNullable(true));
      return this;
    }

    public Builder withCharField(String fieldName) {
           builder.addField(Field.of(fieldName, CHAR).withNullable(true));
       return this;
    }

    public Builder withVarcharField(String fieldName) {
          builder.addField(Field.of(fieldName, VARCHAR).withNullable(true));
      return this;
    }

    public Builder withTimeField(String fieldName) {
      builder.addField(Field.of(fieldName, TIME).withNullable(true));
      return this;
    }

    public Builder withDateField(String fieldName) {
      builder.addField(Field.of(fieldName, DATE).withNullable(true));
      return this;
    }

    public Builder withTimestampField(String fieldName) {
      builder.addField(Field.of(fieldName, TIMESTAMP).withNullable(true));
      return this;
    }

    /**
     * Adds an ARRAY field with elements of the given type.
     */
    public Builder withArrayField(String fieldName, RelDataType relDataType) {
      builder.addField(Field.of(fieldName, CalciteUtils.toArrayType(relDataType)));
      return this;
    }

    /**
     * Adds an ARRAY field with elements of the given type.
     */
    public Builder withArrayField(String fieldName, SqlTypeName typeName) {
      builder.addField(Field.of(fieldName, CalciteUtils.toArrayType(typeName)));
      return this;
    }

    /**
     * Adds a MAP field with elements of the given key/value type.
     */
    public Builder withMapField(String fieldName, RelDataType keyRelDataType,
        RelDataType valueRelDataType) {
      builder
          .addField(Field.of(fieldName, CalciteUtils.toMapType(keyRelDataType, valueRelDataType)));
      return this;
    }

    /**
     * Adds a MAP field with elements of the given key/value type.
     */
    public Builder withMapField(String fieldName, SqlTypeName keyTypeName,
        SqlTypeName valueTypeName) {
      builder.addField(Field.of(fieldName, CalciteUtils.toMapType(keyTypeName, valueTypeName)));
      return this;
    }

    /**
     * Adds an ARRAY field with elements of {@code rowType}.
     */
    public Builder withArrayField(String fieldName, Schema schema) {
      FieldType componentType =
          FieldType
              .of(TypeName.ROW)
              .withRowSchema(schema);
      builder.addField(Field.of(fieldName,
          TypeName.ARRAY.type().withComponentType(componentType)));
      return this;
    }

    public Builder withRowField(String fieldName, Schema schema) {
       builder.addRowField(fieldName, schema, true);
       return this;
    }

    private Builder() {
      this.builder = Schema.builder();
    }

    public Schema build() {
      return builder.build();
    }
  }
}
