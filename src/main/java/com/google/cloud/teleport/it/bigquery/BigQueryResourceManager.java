/*
 * Copyright (C) 2022 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.google.cloud.teleport.it.bigquery;

import com.google.cloud.bigquery.InsertAllRequest.RowToInsert;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TableResult;
import java.util.List;

/** Interface for managing BigQuery resources in integration tests. */
public interface BigQueryResourceManager {

  /**
   * Create a BigQuery dataset in which all tables will exist.
   *
   * @param region the region to store the dataset in.
   * @throws BigQueryResourceManagerException if there is an error creating the dataset in BigQuery.
   */
  void createDataset(String region);

  /**
   * Return the dataset ID this Resource Manager uses to create and manage tables in.
   *
   * @return the dataset ID.
   */
  String getDatasetId();

  /**
   * Creates a table within the current dataset given a table name and schema.
   *
   * <p>This table will automatically expire 1 hour after creation if not cleaned up manually or by
   * calling the {@link BigQueryResourceManager#cleanupAll()} method.
   *
   * <p>Note: Implementations may do dataset creation here, if one does not already exist.
   *
   * @param tableName The name of the table.
   * @param schema A schema object that defines the table.
   * @return The TableId (reference) to the table
   * @throws BigQueryResourceManagerException if there is an error creating the table in BigQuery.
   */
  TableId createTable(String tableName, Schema schema);

  /**
   * Creates a table within the current dataset given a table name and schema.
   *
   * <p>This table will automatically expire at the time specified by {@code expirationTime} if not
   * cleaned up manually or by calling the {@link BigQueryResourceManager#cleanupAll()} method.
   *
   * <p>Note: Implementations may do dataset creation here, if one does not already exist.
   *
   * @param tableName The name of the table.
   * @param schema A schema object that defines the table.
   * @param expirationTimeMillis Sets the time when this table expires, in milliseconds since the
   *     epoch.
   * @return The TableId (reference) to the table
   * @throws BigQueryResourceManagerException if there is an error creating the table in BigQuery.
   */
  TableId createTable(String tableName, Schema schema, Long expirationTimeMillis);

  /**
   * Writes a given row into a table. This method requires {@link
   * BigQueryResourceManager#createTable(String, Schema)} to be called for the target table
   * beforehand.
   *
   * @param tableRow A row object representing the table row.
   * @throws BigQueryResourceManagerException if method is called after resources have been cleaned
   *     up, if the manager object has no dataset, if the table does not exist or if there is an
   *     Exception when attempting to insert the rows.
   */
  void write(String tableName, RowToInsert tableRow);

  /**
   * Writes a collection of table rows into a single table. This method requires {@link
   * BigQueryResourceManager#createTable(String, Schema)} to be called for the target table
   * beforehand.
   *
   * @param tableName The name of the table to insert the given rows into.
   * @param tableRows A collection of table rows.
   * @throws BigQueryResourceManagerException if method is called after resources have been cleaned
   *     up, if the manager object has no dataset, if the table does not exist or if there is an
   *     Exception when attempting to insert the rows.
   */
  void write(String tableName, List<RowToInsert> tableRows);

  /**
   * Reads all the rows in a table and returns a TableResult containing a JSON string
   * representation. This method requires {@link BigQueryResourceManager#createTable(String,
   * Schema)} to be called for the target table beforehand.
   *
   * @param tableName The name of the table to read rows from.
   * @return A TableResult containing all the rows in the table in JSON.
   * @throws BigQueryResourceManagerException if method is called after resources have been cleaned
   *     up, if the manager object has no dataset, if the table does not exist or if there is an
   *     Exception when attempting to insert the rows.
   */
  TableResult readTable(String tableName);

  /**
   * Reads number of rows in a table and returns a TableResult containing a JSON string
   * representation. This method requires {@link BigQueryResourceManager#createTable(String,
   * Schema)} to be called for the target table beforehand.
   *
   * @param tableName The name of the table to read rows from.
   * @return A TableResult containing all the rows in the table in JSON.
   * @throws BigQueryResourceManagerException if method is called after resources have been cleaned
   *     up, if the manager object has no dataset, if the table does not exist or if there is an
   *     Exception when attempting to insert the rows.
   */
  TableResult readTable(String tableName, int numRows);

  /**
   * Deletes all created resources (dataset and tables) and cleans up the BigQuery client, making
   * the manager object unusable.
   *
   * @throws BigQueryResourceManagerException if there is an error deleting the tables or dataset in
   *     BigQuery.
   */
  void cleanupAll();

  TableResult runQuery(String query);

  Long getRowCount(String project, String dataset, String table);
}
