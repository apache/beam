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
package org.apache.beam.sdk.io.csv;

import com.google.auto.value.AutoValue;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.Field;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.Row;
import org.apache.commons.csv.CSVFormat;

/** Converts between {@link Row} and CSV string using a {@link CSVFormat}. */
@AutoValue
abstract class RowToCsv implements SerializableFunction<Row, String> {

  static Builder builder() {
    return new AutoValue_RowToCsv.Builder();
  }

  /** The expected {@link Schema} of the {@link Row} input. */
  abstract Schema getSchema();

  /** The {@link CSVFormat} of the converted {@link Row} input. */
  abstract CSVFormat getCSVFormat();

  /** The order and subset of {@link Schema} field names to drive the {@link Row} conversion. */
  abstract List<String> getSchemaFields();

  String buildHeader() {
    return buildHeaderFrom(getSchemaFields(), getCSVFormat());
  }

  /**
   * Converts a {@link Row} to a CSV string formatted using {@link #getCSVFormat()}. Null values are
   * converted to empty strings.
   */
  @Override
  public String apply(Row input) {
    List<String> schemaFields = getSchemaFields();
    Object[] values = new Object[schemaFields.size()];
    for (int i = 0; i < schemaFields.size(); i++) {
      String name = schemaFields.get(i);
      values[i] = input.getValue(name);
    }
    return getCSVFormat().format(values);
  }

  @AutoValue.Builder
  abstract static class Builder {

    /** The expected {@link Schema} of the {@link Row} input. */
    abstract Builder setSchema(Schema schema);

    abstract Schema getSchema();

    /** The {@link CSVFormat} of the converted {@link Row} input. */
    abstract Builder setCSVFormat(CSVFormat format);

    /** The order and subset of {@link Schema} field names to drive the {@link Row} conversion. */
    abstract Builder setSchemaFields(List<String> fields);

    abstract List<String> getSchemaFields();

    abstract RowToCsv autoBuild();

    final RowToCsv build() {

      validateSchema(getSchema());

      validateHeaderAgainstSchema(getSchemaFields(), getSchema());

      return autoBuild();
    }
  }

  /** Formats columns into a header String based on {@link CSVFormat}. */
  static String buildHeaderFrom(List<String> columns, CSVFormat csvFormat) {
    StringBuilder builder = new StringBuilder();
    try {
      boolean newRecord = true;
      for (String name : columns) {
        csvFormat.print(name, builder, newRecord);
        newRecord = false;
      }
    } catch (IOException e) {
      throw new IllegalStateException(e);
    }
    return builder.toString();
  }

  private static void validateHeaderAgainstSchema(List<String> schemaFields, Schema schema) {
    if (schemaFields.isEmpty()) {
      throw new IllegalArgumentException(
          "Columns is empty. An intent to not override columns, should assign null to the columns parameter.");
    }
    List<String> mismatch = new ArrayList<>();
    for (String name : schemaFields) {
      if (name.isEmpty()) {
        throw new IllegalArgumentException(
            String.format("empty schema field found in: %s", String.join(", ", schemaFields)));
      }
      if (!schema.hasField(name)) {
        mismatch.add(name);
      }
    }
    if (!mismatch.isEmpty()) {
      String mismatchString = String.join(", ", mismatch);
      String schemaString = String.join(", ", schema.getFieldNames());
      throw new IllegalArgumentException(
          String.format("schema: [%s] missing columns: [%s]", schemaString, mismatchString));
    }
  }

  private static void validateSchema(Schema schema) {
    if (schema.getFieldCount() == 0) {
      throw new IllegalArgumentException("schema is empty");
    }
    List<String> invalidFields = new ArrayList<>();
    for (Field field : schema.getFields()) {
      FieldType fieldType = field.getType().withNullable(false);
      if (!CsvIO.VALID_FIELD_TYPE_SET.contains(fieldType)) {
        invalidFields.add(field.toString());
      }
    }
    if (!invalidFields.isEmpty()) {
      String invalidFieldsMessage = String.join(", ", invalidFields);
      throw new IllegalArgumentException(
          String.format("CSV should only contain flat fields but found: %s", invalidFieldsMessage));
    }
  }
}
