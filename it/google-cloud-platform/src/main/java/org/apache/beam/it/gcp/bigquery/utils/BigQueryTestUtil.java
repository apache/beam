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
package org.apache.beam.it.gcp.bigquery.utils;

import com.google.cloud.Tuple;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.InsertAllRequest.RowToInsert;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.StandardSQLTypeName;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.RandomStringUtils;

/** Utilities for BigQuery tests. */
public final class BigQueryTestUtil {

  private BigQueryTestUtil() {}

  /**
   * Generate data to be persisted to a BigQuery table for testing.
   *
   * @param idColumn Column name containing the ID.
   * @param numRows Number of rows to generate.
   * @param numFields Number of fields in the schema.
   * @param maxEntryLength Maximum length for each field. Please note that maxEntryLength can not
   *     exceed 300 characters.
   * @return Tuple containing the schema and the row values.
   */
  public static Tuple<Schema, List<RowToInsert>> generateBigQueryTable(
      String idColumn, int numRows, int numFields, int maxEntryLength) {

    Schema schema = createSchema(idColumn, numFields, maxEntryLength);
    List<RowToInsert> bigQueryRows = generateRandomData(schema, idColumn, numRows, maxEntryLength);

    // Return tuple containing the randomly generated schema and table data
    return Tuple.of(schema, bigQueryRows);
  }

  /**
   * Utility method to create a schema based on the given constraints.
   *
   * @param idColumn Name of the id column to use.
   * @param numFields Number of fields.
   * @param maxEntryLength Maximum entry.
   * @return Generated Schema.
   */
  public static Schema createSchema(String idColumn, int numFields, int maxEntryLength) {
    // List to store BigQuery schema fields
    List<Field> bqSchemaFields = new ArrayList<>();

    // Add unique identifier field
    bqSchemaFields.add(Field.of(idColumn, StandardSQLTypeName.INT64));

    // Generate random fields
    for (int i = 1; i < numFields; i++) {
      StringBuilder randomField = new StringBuilder();

      // Field must start with letter
      String prependLetter = RandomStringUtils.randomAlphabetic(1);
      // Field uses unique number at end to keep name unique
      String appendNum = String.valueOf(i);
      // Remaining field name is generated randomly within bounds of maxEntryLength
      String randomString =
          RandomStringUtils.randomAlphanumeric(0, maxEntryLength - appendNum.length());

      randomField.append(prependLetter).append(randomString).append(appendNum);
      bqSchemaFields.add(Field.of(randomField.toString(), StandardSQLTypeName.STRING));
    }
    // Create schema and BigQuery table
    return Schema.of(bqSchemaFields);
  }

  /**
   * Utility method to generate random data for a given schema.
   *
   * @param schema Schema to generate the data for.
   * @param idColumn The name of the id column.
   * @param numRows Number of rows to create.
   * @param maxEntryLength Size of strings to use.
   * @return Generated data, a {@link RowToInsert} list.
   */
  public static List<RowToInsert> generateRandomData(
      Schema schema, String idColumn, int numRows, int maxEntryLength) {
    // Generate random data
    List<RowToInsert> bigQueryRows = new ArrayList<>();
    for (int i = 0; i < numRows; i++) {
      Map<String, Object> content = new HashMap<>();

      // Iterate unique identifier column
      content.put(idColumn, i);

      // Generate remaining cells in row
      for (int j = 1; j < schema.getFields().size(); j++) {
        content.put(
            schema.getFields().get(j).getName(),
            RandomStringUtils.randomAlphanumeric(1, maxEntryLength));
      }
      bigQueryRows.add(RowToInsert.of(content));
    }
    return bigQueryRows;
  }
}
