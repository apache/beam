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

import static org.apache.beam.sdk.values.RowType.toRowType;

import com.google.common.collect.ImmutableList;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.RowType;


/**
 * Type builder for {@link Row} with SQL types.
 *
 * <p>Limited SQL types are supported now, visit
 * <a href="https://beam.apache.org/documentation/dsls/sql/#data-types">data types</a>
 * for more details.
 *
 * <p>SQL types are represented by instances of {@link SqlTypeCoder}, see {@link SqlTypeCoders}.
 */
public class RowSqlType {
  public static Builder builder() {
    return new Builder();
  }

  /**
   * Builder class to construct {@link RowType}.
   */
  public static class Builder {

    private ImmutableList.Builder<RowType.Field> fields;

    public Builder withField(String fieldName, SqlTypeCoder fieldCoder) {
      fields.add(RowType.newField(fieldName, fieldCoder));
      return this;
    }

    public Builder withTinyIntField(String fieldName) {
      return withField(fieldName, SqlTypeCoders.TINYINT);
    }

    public Builder withSmallIntField(String fieldName) {
      return withField(fieldName, SqlTypeCoders.SMALLINT);
    }

    public Builder withIntegerField(String fieldName) {
      return withField(fieldName, SqlTypeCoders.INTEGER);
    }

    public Builder withBigIntField(String fieldName) {
      return withField(fieldName, SqlTypeCoders.BIGINT);
    }

    public Builder withFloatField(String fieldName) {
      return withField(fieldName, SqlTypeCoders.FLOAT);
    }

    public Builder withDoubleField(String fieldName) {
      return withField(fieldName, SqlTypeCoders.DOUBLE);
    }

    public Builder withDecimalField(String fieldName) {
      return withField(fieldName, SqlTypeCoders.DECIMAL);
    }

    public Builder withBooleanField(String fieldName) {
      return withField(fieldName, SqlTypeCoders.BOOLEAN);
    }

    public Builder withCharField(String fieldName) {
      return withField(fieldName, SqlTypeCoders.CHAR);
    }

    public Builder withVarcharField(String fieldName) {
      return withField(fieldName, SqlTypeCoders.VARCHAR);
    }

    public Builder withTimeField(String fieldName) {
      return withField(fieldName, SqlTypeCoders.TIME);
    }

    public Builder withDateField(String fieldName) {
      return withField(fieldName, SqlTypeCoders.DATE);
    }

    public Builder withTimestampField(String fieldName) {
      return withField(fieldName, SqlTypeCoders.TIMESTAMP);
    }

    public Builder withArrayField(String fieldName, SqlTypeCoder elementCoder) {
      return withField(fieldName, SqlTypeCoders.arrayOf(elementCoder));
    }

    private Builder() {
      this.fields = ImmutableList.builder();
    }

    public RowType build() {
      return fields.build().stream().collect(toRowType());
    }
  }
}
