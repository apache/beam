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
package org.apache.beam.it.gcp.bigquery.matchers;

import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatRecords;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.cloud.bigquery.FieldValueList;
import com.google.cloud.bigquery.InsertAllRequest;
import com.google.cloud.bigquery.TableResult;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.beam.it.truthmatchers.RecordsSubject;

/** Assert utilities for BigQuery tests. */
public class BigQueryAsserts {

  private static final ObjectMapper objectMapper = new ObjectMapper();

  private static final TypeReference<Map<String, Object>> recordTypeReference =
      new TypeReference<Map<String, Object>>() {};

  /**
   * Convert BigQuery {@link TableResult} to a list of maps.
   *
   * @param tableResult Table Result to parse
   * @return List of maps to use in {@link RecordsSubject}
   */
  public static List<Map<String, Object>> tableResultToRecords(TableResult tableResult) {
    try {
      List<Map<String, Object>> records = new ArrayList<>();

      for (FieldValueList row : tableResult.iterateAll()) {
        String jsonRow = row.get(0).getStringValue();
        Map<String, Object> converted = objectMapper.readValue(jsonRow, recordTypeReference);
        records.add(converted);
      }

      return records;
    } catch (Exception e) {
      throw new RuntimeException("Error converting TableResult to Records", e);
    }
  }

  /**
   * Convert BigQuery {@link InsertAllRequest.RowToInsert} to a list of maps.
   *
   * @param rows BigQuery rows to parse.
   * @return List of maps to use in {@link RecordsSubject}
   */
  public static List<Map<String, Object>> bigQueryRowsToRecords(
      List<InsertAllRequest.RowToInsert> rows) {
    try {
      List<Map<String, Object>> records = new ArrayList<>();
      rows.forEach(row -> records.add(new HashMap<>(row.getContent())));

      return records;
    } catch (Exception e) {
      throw new RuntimeException("Error converting BigQuery Row to Map", e);
    }
  }

  /**
   * Convert BigQuery {@link InsertAllRequest.RowToInsert} to a list of maps.
   *
   * @param rows BigQuery rows to parse.
   * @param excludeCols BigQuery columns to filter out of result.
   * @return List of maps to use in {@link RecordsSubject}
   */
  public static List<Map<String, Object>> bigQueryRowsToRecords(
      List<InsertAllRequest.RowToInsert> rows, List<String> excludeCols) {
    List<Map<String, Object>> records = bigQueryRowsToRecords(rows);
    try {
      excludeCols.forEach(col -> records.forEach(row -> row.remove(col)));

      return records;
    } catch (Exception e) {
      throw new RuntimeException("Error converting BigQuery Row to Map", e);
    }
  }

  /**
   * Creates a {@link RecordsSubject} to assert information within a list of records.
   *
   * @param tableResult Records in BigQuery {@link TableResult} format to use in the comparison.
   * @return Truth Subject to chain assertions.
   */
  public static RecordsSubject assertThatBigQueryRecords(@Nullable TableResult tableResult) {
    return assertThatRecords(tableResultToRecords(tableResult));
  }
}
