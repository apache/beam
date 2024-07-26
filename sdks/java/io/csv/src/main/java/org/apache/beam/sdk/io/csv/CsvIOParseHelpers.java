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

import static org.apache.beam.sdk.util.Preconditions.checkArgumentNotNull;
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkArgument;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Strings;
import org.apache.commons.csv.CSVFormat;
import org.joda.time.Instant;

/** A utility class containing shared methods for parsing CSV records. */
final class CsvIOParseHelpers {
  /**
   * Validate the {@link CSVFormat} for CSV record parsing requirements. See the public-facing
   * "Reading CSV Files" section of the {@link CsvIO} documentation for information regarding which
   * {@link CSVFormat} parameters are checked during validation.
   */
  static void validateCsvFormat(CSVFormat format) {
    String[] header =
        checkArgumentNotNull(format.getHeader(), "Illegal %s: header is required", CSVFormat.class);

    checkArgument(header.length > 0, "Illegal %s: header cannot be empty", CSVFormat.class);

    checkArgument(
        !format.getAllowMissingColumnNames(),
        "Illegal %s: cannot allow missing column names",
        CSVFormat.class);

    checkArgument(
        !format.getIgnoreHeaderCase(), "Illegal %s: cannot ignore header case", CSVFormat.class);

    checkArgument(
        !format.getAllowDuplicateHeaderNames(),
        "Illegal %s: cannot allow duplicate header names",
        CSVFormat.class);

    for (String columnName : header) {
      checkArgument(
          !Strings.isNullOrEmpty(columnName),
          "Illegal %s: column name is required",
          CSVFormat.class);
    }
    checkArgument(
        !format.getSkipHeaderRecord(),
        "Illegal %s: cannot skip header record because the header is already accounted for",
        CSVFormat.class);
  }

  /**
   * Validate the {@link CSVFormat} in relation to the {@link Schema} for CSV record parsing
   * requirements.
   */
  static void validateCsvFormatWithSchema(CSVFormat format, Schema schema) {
    List<String> header = Arrays.asList(format.getHeader());
    for (Schema.Field field : schema.getFields()) {
      String fieldName = field.getName();
      if (!field.getType().getNullable()) {
        checkArgument(
            header.contains(fieldName),
            "Illegal %s: required %s field '%s' not found in header",
            CSVFormat.class,
            Schema.class.getTypeName(),
            fieldName);
      }
    }
  }

  /**
   * Build a {@link List} of {@link Schema.Field}s corresponding to the expected position of each
   * field within the CSV record.
   */
  static Map<Integer, Schema.Field> mapFieldPositions(CSVFormat format, Schema schema) {
    List<String> header = Arrays.asList(format.getHeader());
    Map<Integer, Schema.Field> indexToFieldMap = new HashMap<>();
    for (Schema.Field field : schema.getFields()) {
      int index = getIndex(header, field);
      if (index >= 0) {
        indexToFieldMap.put(index, field);
      }
    }
    return indexToFieldMap;
  }

  /**
   * Attains expected index from {@link CSVFormat's} header matching a given {@link Schema.Field}.
   */
  private static int getIndex(List<String> header, Schema.Field field) {
    String fieldName = field.getName();
    boolean presentInHeader = header.contains(fieldName);
    boolean isNullable = field.getType().getNullable();
    if (presentInHeader) {
      return header.indexOf(fieldName);
    }
    if (isNullable) {
      return -1;
    }

    throw new IllegalArgumentException(
        String.format("header does not contain required %s field: %s", Schema.class, fieldName));
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
