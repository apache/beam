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

import java.math.BigDecimal;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Strings;
import org.apache.commons.csv.CSVFormat;

import static org.apache.beam.sdk.util.Preconditions.checkArgumentNotNull;
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkArgument;

/** A utility class containing shared methods for parsing CSV records. */
final class CsvIOParseHelpers {
  /**
   * Validate the {@link CSVFormat} for CSV record parsing requirements. See the public-facing
   * "Reading CSV Files" section of the {@link CsvIO} documentation for information regarding which
   * {@link CSVFormat} parameters are checked during validation.
   */
  static void validate(CSVFormat format) {
    String[] header =
        checkArgumentNotNull(format.getHeader(), "Illegal %s: header is required", CSVFormat.class);
    checkArgument(header.length > 0, "Illegal %s: header cannot be empty", CSVFormat.class);
    checkArgument(
        !format.getAllowMissingColumnNames(),
        "Illegal %s: cannot allow missing column names",
        CSVFormat.class);
    checkArgument(
        !format.getIgnoreHeaderCase(), "Illegal %s: cannot ignore header case", CSVFormat.class);

    for (String columnName : header) {
      checkArgument(
          !Strings.isNullOrEmpty(columnName),
          "Illegal %s: column name is required",
          CSVFormat.class);
    }
  }

  /**
   * Validate the {@link CSVFormat} in relation to the {@link Schema} for CSV record parsing
   * requirements.
   */
  // TODO(https://github.com/apache/beam/issues/31716): implement method.
  static void validate(CSVFormat format, Schema schema) {}

  /**
   * Build a {@link List} of {@link Schema.Field}s corresponding to the expected position of each
   * field within the CSV record.
   */
  // TODO(https://github.com/apache/beam/issues/31718): implement method.
  static List<Schema.Field> mapFieldPositions(CSVFormat format, Schema schema) {
    return new ArrayList<>();
  }

  /**
   * Parse the given {@link String} cell of the CSV record based on the given field's {@link
   * Schema.FieldType}.
   */
  static Object parseCell(String cell, Schema.Field field) {
    Schema.FieldType fieldType = field.getType();
    try {
      switch (fieldType.getTypeName()) {
        case STRING:
          return cell;
        case INT16:
          return Short.parseShort(cell);
        case INT32:
          return Integer.parseInt(cell);
        case INT64:
          return Long.parseLong(cell);
        case BOOLEAN:
          return Boolean.parseBoolean(cell);
        case BYTE:
          return Byte.parseByte(cell);
        case DECIMAL:
          return new BigDecimal(cell);
        case DOUBLE:
          return Double.parseDouble(cell);
        case FLOAT:
          return Float.parseFloat(cell);
        case DATETIME:
          return Instant.parse(cell);
        default:
          throw new UnsupportedOperationException(
              "Unsupported type: " + fieldType + ", consider using withCustomRecordParsing");
      }

    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException(
          e.getMessage() + " field " + field.getName() + " was received -- type mismatch");
    }
  }
}
