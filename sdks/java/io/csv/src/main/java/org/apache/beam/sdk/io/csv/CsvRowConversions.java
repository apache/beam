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

import static org.apache.beam.sdk.io.csv.CsvIO.VALID_FIELD_TYPE_SET;
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkArgument;
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkNotNull;

import com.google.auto.value.AutoValue;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.schemas.Schema.TypeName;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.Row;
import org.apache.commons.csv.CSVFormat;
import org.checkerframework.checker.nullness.qual.NonNull;

/** Contains classes and methods to help with converting between {@link Row} and CSV strings. */
class CsvRowConversions {

  /** Converts between {@link Row} and CSV string using a {@link CSVFormat}. */
  @AutoValue
  abstract static class RowToCsv implements SerializableFunction<Row, String> {

    static Builder builder() {
      return new AutoValue_CsvRowConversions_RowToCsv.Builder();
    }

    /** The expected {@link Schema} of the {@link Row} input. */
    abstract Schema getSchema();

    /** The {@link CSVFormat} of the converted {@link Row} input. */
    abstract CSVFormat getCSVFormat();

    /** Converts a {@link Row} to a CSV string formatted using {@link #getCSVFormat}. */
    @Override
    public String apply(Row input) {
      Row safeInput = checkNotNull(input);
      String[] header = getHeader();
      Object[] values = new Object[header.length];
      for (int i = 0; i < header.length; i++) {
        values[i] = safeInput.getValue(header[i]);
      }
      return getCSVFormat().format(values);
    }

    @NonNull
    String[] getHeader() {
      return checkNotNull(getCSVFormat().getHeader());
    }

    @AutoValue.Builder
    abstract static class Builder {

      /** The expected {@link Schema} of the {@link Row} input. */
      abstract Builder setSchema(Schema schema);

      abstract Schema getSchema();

      /** The {@link CSVFormat} of the converted {@link Row} input. */
      abstract Builder setCSVFormat(CSVFormat format);

      abstract CSVFormat getCSVFormat();

      abstract RowToCsv autoBuild();

      final RowToCsv build() {
        checkArgument(getSchema().getFieldCount() > 0, "Schema has no fields");
        setCSVFormat(
            getCSVFormat()
                // CSVFormat was designed to write to a single file.
                // Therefore, we need to apply withSkipHeaderRecord to prevent CSVFormat to apply
                // its header to each converted Row in the context of RowToCsv.
                .withSkipHeaderRecord()
                // Delegate to TextIO.Write.withDelimiter instead.
                .withRecordSeparator(' ')
                .withHeaderComments());
        validateCSVFormat(getCSVFormat());
        validateHeaderAgainstSchema(getCSVFormat().getHeader(), getSchema());

        return autoBuild();
      }
    }
  }

  private static void validateCSVFormat(CSVFormat csvFormat) {
    String[] header = checkNotNull(csvFormat.getHeader(), "CSVFormat withHeader is required");

    checkArgument(header.length > 0, "CSVFormat withHeader requires at least one column");

    checkArgument(!csvFormat.getAutoFlush(), "withAutoFlush is an illegal CSVFormat setting");

    checkArgument(
        !csvFormat.getIgnoreHeaderCase(), "withIgnoreHeaderCase is an illegal CSVFormat setting");

    checkArgument(
        !csvFormat.getAllowMissingColumnNames(),
        "withAllowMissingColumnNames is an illegal CSVFormat setting");

    checkArgument(
        !csvFormat.getIgnoreSurroundingSpaces(),
        "withIgnoreSurroundingSpaces is an illegal CSVFormat setting");
  }

  private static void validateHeaderAgainstSchema(String[] csvHeader, Schema schema) {
    Set<String> distinctColumns = new HashSet<>(Arrays.asList(csvHeader));
    List<String> mismatchColumns = new ArrayList<>();
    List<String> invalidTypes = new ArrayList<>();
    Set<TypeName> validTypeNames =
        VALID_FIELD_TYPE_SET.stream().map(FieldType::getTypeName).collect(Collectors.toSet());
    for (String column : distinctColumns) {
      if (!schema.hasField(column)) {
        mismatchColumns.add(column);
        continue;
      }
      TypeName typeName = schema.getField(column).getType().getTypeName();
      if (!validTypeNames.contains(typeName)) {
        invalidTypes.add(column);
      }
    }

    checkArgument(
        mismatchColumns.isEmpty(),
        "columns in CSVFormat header do not exist in Schema: %s",
        String.join(",", mismatchColumns));
    checkArgument(
        invalidTypes.isEmpty(),
        "columns in header match fields in Schema with invalid types: %s. See CsvIO#VALID_FIELD_TYPE_SET for a list of valid field types.",
        String.join(",", invalidTypes));
  }
}
