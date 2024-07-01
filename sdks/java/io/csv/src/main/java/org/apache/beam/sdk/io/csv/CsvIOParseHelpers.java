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

import java.util.ArrayList;
import java.util.List;
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
  // TODO(https://github.com/apache/beam/issues/31718): implement method.
  static List<Schema.Field> mapFieldPositions(CSVFormat format, Schema schema) {
    return new ArrayList<>();
  }

  /**
   * Parse the given {@link String} cell of the CSV record based on the given field's {@link
   * Schema.FieldType}.
   */
  // TODO(https://github.com/apache/beam/issues/31719): implement method.
  static Object parseCell(String cell, Schema.Field field) {
    return "";
  }
}
