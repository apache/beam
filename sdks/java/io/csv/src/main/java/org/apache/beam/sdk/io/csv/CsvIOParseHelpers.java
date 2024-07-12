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
import java.util.*;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.commons.csv.CSVFormat;

/** A utility class containing shared methods for parsing CSV records. */
final class CsvIOParseHelpers {
  /** Validate the {@link CSVFormat} for CSV record parsing requirements. */
  // TODO(https://github.com/apache/beam/issues/31712): implement method.
  static void validate(CSVFormat format) {}

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
