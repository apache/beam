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

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkNotNull;

import com.google.auto.value.AutoValue;
import org.apache.beam.sdk.schemas.Schema;
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

    /** Converts a {@link Row} to a CSV string formatted using {@link #getCSVFormat()}. */
    @Override
    public String apply(Row input) {
      Row safeInput = checkNotNull(input);
      String[] header = getHeader();
      Object[] values = new Object[header.length];
      for (int i = 0; i < header.length; i++) {
        values[i] = safeInput.getValue(header[i]);
      }
      return getCSVFormat().withSkipHeaderRecord().format(values);
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

      private String[] buildHeaderFromSchema() {
        return getSchema().getFieldNames().toArray(new String[0]);
      }

      final RowToCsv build() {
        if (getCSVFormat().getHeader() == null) {
          setCSVFormat(getCSVFormat().withHeader(buildHeaderFromSchema()));
        }
        validateHeaderAgainstSchema(getCSVFormat().getHeader(), getSchema());

        return autoBuild();
      }
    }
  }

  private static void validateHeaderAgainstSchema(String[] csvHeader, Schema schema) {
    checkNotNull(csvHeader);
    checkNotNull(schema);
    if (schema.getFieldCount() == 0) {
      throw new IllegalArgumentException("schema is empty");
    }
    if (csvHeader.length == 0) {
      throw new IllegalArgumentException("csvHeader length is zero");
    }
  }
}
