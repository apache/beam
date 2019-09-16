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
package org.apache.beam.sdk.testutils.publishing;

import static java.lang.String.format;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQuery.TableOption;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.InsertAllRequest;
import com.google.cloud.bigquery.InsertAllResponse;
import com.google.cloud.bigquery.LegacySQLTypeName;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.StandardTableDefinition;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TableInfo;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Wraps {@link BigQuery} to provide high level useful methods for working with big query database.
 */
public class BigQueryClient {

  // Fetching all fields (default way) may result in NullPointerException from the client library:
  // https://issues.apache.org/jira/browse/BEAM-6076.
  // Specifying only TYPE field enforces getting only the required fields (not null).
  private static final TableOption FIELD_OPTIONS = TableOption.fields(BigQuery.TableField.TYPE);

  private BigQuery client;

  private String projectId;

  private String dataset;

  protected BigQueryClient(BigQuery client, String projectId, String dataset) {
    this.client = client;
    this.projectId = projectId;
    this.dataset = dataset;
  }

  /**
   * Creates the publisher with a application default credentials from the environment and default
   * project name.
   */
  public static BigQueryClient create(String dataset) {
    BigQueryOptions options = BigQueryOptions.newBuilder().build();

    return new BigQueryClient(options.getService(), options.getProjectId(), dataset);
  }

  private void createTable(TableId tableId, Schema schema) {
    TableInfo tableInfo =
        TableInfo.newBuilder(tableId, StandardTableDefinition.of(schema))
            .setFriendlyName(tableId.getTable())
            .build();

    client.create(tableInfo, FIELD_OPTIONS);
  }

  /**
   * Inserts one row to BigQuery table. Creates table using the given schema if the table does not
   * exist.
   *
   * @see #insertRow(Map, String)
   * @see #createTableIfNotExists(String, Map) for more details.
   */
  public void insertRow(Map<String, ?> row, Map<String, String> schema, String table) {
    createTableIfNotExists(table, schema);
    insertRow(row, table);
  }

  /**
   * Inserts one row to BigQuery table.
   *
   * @see #insertAll(Collection, String) for more details
   */
  public void insertRow(Map<String, ?> row, String table) {
    insertAll(Collections.singletonList(row), table);
  }

  /**
   * Inserts rows to BigQuery table. Creates table using the given schema if the table does not
   * exist.
   *
   * @see #insertRow(Map, String)
   * @see #createTableIfNotExists(String, Map) for more details.
   */
  public void insertAll(Collection<Map<String, ?>> rows, Map<String, String> schema, String table) {
    createTableIfNotExists(table, schema);
    insertAll(rows, table);
  }

  /** Inserts multiple rows of the same schema to a BigQuery table. */
  public void insertAll(Collection<Map<String, ?>> rows, String table) {
    TableId tableId = TableId.of(projectId, dataset, table);

    InsertAllRequest.Builder builder = InsertAllRequest.newBuilder(tableId);

    for (Map<String, ?> row : rows) {
      builder.addRow(row);
    }

    InsertAllResponse response = client.insertAll(builder.build());
    handleBigQueryResponseExceptions(response);
  }

  private void handleBigQueryResponseExceptions(InsertAllResponse response) {
    if (response.hasErrors()) {
      throw new RuntimeException(
          format(
              "The following errors occurred while inserting to BigQuery: %s",
              response.getInsertErrors()));
    }
  }

  /**
   * Creates a new table with given schema when it not exists.
   *
   * @param tableName name of the desired table
   * @param schema schema of consequent table fields (name, type pairs).
   */
  public void createTableIfNotExists(String tableName, Map<String, String> schema) {
    TableId tableId = TableId.of(projectId, dataset, tableName);

    if (client.getTable(tableId, FIELD_OPTIONS) == null) {
      List<Field> schemaFields =
          schema.entrySet().stream()
              .map(entry -> Field.of(entry.getKey(), LegacySQLTypeName.valueOf(entry.getValue())))
              .collect(Collectors.toList());

      createTable(tableId, Schema.of(schemaFields));
    }
  }
}
