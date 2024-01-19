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
package org.apache.beam.it.gcp.bigquery;

import com.google.api.gax.paging.Page;
import com.google.auth.Credentials;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryError;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.Dataset;
import com.google.cloud.bigquery.DatasetInfo;
import com.google.cloud.bigquery.InsertAllRequest.RowToInsert;
import com.google.cloud.bigquery.InsertAllResponse;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.StandardTableDefinition;
import com.google.cloud.bigquery.Table;
import com.google.cloud.bigquery.TableDefinition;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TableInfo;
import com.google.cloud.bigquery.TableResult;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.apache.beam.it.common.ResourceManager;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Client for managing BigQuery resources.
 *
 * <p>The class supports one dataset, and multiple tables per dataset object.
 *
 * <p>The class is thread-safe.
 */
@SuppressWarnings({
  "nullness" // TODO(https://github.com/apache/beam/issues/27438)
})
public final class BigQueryResourceManager implements ResourceManager {

  private static final Logger LOG = LoggerFactory.getLogger(BigQueryResourceManager.class);
  private static final String DEFAULT_DATASET_REGION = "us-central1";

  private final String projectId;
  private final String datasetId;

  private final BigQuery bigQuery;
  private Dataset dataset;

  private BigQueryResourceManager(Builder builder) {
    // create bigQuery client
    BigQueryOptions.Builder bigQueryOptions =
        BigQueryOptions.newBuilder().setProjectId(builder.projectId);

    // set credentials, if provided
    if (builder.credentials != null) {
      bigQueryOptions.setCredentials(builder.credentials);
    }
    this.bigQuery = bigQueryOptions.build().getService();
    this.projectId = builder.projectId;

    // If datasetId is provided, get the dataset.
    if (builder.datasetId != null) {
      this.datasetId = builder.datasetId;
      dataset = getDatasetIfExists(this.datasetId);
    } else {
      this.datasetId = BigQueryResourceManagerUtils.generateDatasetId(builder.testId);
      this.dataset = null;
    }
  }

  @VisibleForTesting
  BigQueryResourceManager(String testId, String projectId, BigQuery bigQuery) {
    this.datasetId = BigQueryResourceManagerUtils.generateDatasetId(testId);
    this.projectId = projectId;
    this.bigQuery = bigQuery;
  }

  public static Builder builder(String testId, String projectId, Credentials credentials) {
    return new Builder(testId, projectId, credentials);
  }

  public String getProjectId() {
    return projectId;
  }

  /**
   * Return the dataset ID this Resource Manager uses to create and manage tables in.
   *
   * @return the dataset ID.
   */
  public String getDatasetId() {
    return datasetId;
  }

  /**
   * Helper method for determining if a dataset exists in the resource manager.
   *
   * @throws IllegalStateException if a dataset does not exist.
   */
  private void checkHasDataset() {
    if (dataset == null) {
      throw new IllegalStateException("There is no dataset for manager to perform operation on.");
    }
  }

  /**
   * Helper method for fetching a dataset stored in the project given the datasetId.
   *
   * @param datasetId the name of the dataset to fetch.
   * @return the dataset, if it exists.
   * @throws IllegalStateException if the given dataset does not exist in the project.
   */
  private synchronized Dataset getDatasetIfExists(String datasetId) throws IllegalStateException {
    Dataset dataset = bigQuery.getDataset(datasetId);
    if (dataset == null) {
      throw new IllegalStateException(
          "The dataset " + datasetId + " does not exist in project " + projectId + ".");
    }
    return dataset;
  }

  /**
   * Helper method for fetching a table stored in the dataset given a table name.
   *
   * @param tableId the name of the table to fetch.
   * @return the table, if it exists.
   * @throws IllegalStateException if the given table name does not exist in the dataset.
   */
  private synchronized Table getTableIfExists(String tableId) throws IllegalStateException {
    checkHasDataset();
    Table table = dataset.get(tableId);
    if (table == null) {
      throw new IllegalStateException(
          "The table " + tableId + " does not exist in dataset " + datasetId + ".");
    }
    return table;
  }

  /**
   * Helper method for logging individual errors thrown by inserting rows to a table. This method is
   * used to log errors thrown by inserting certain rows when other rows were successful.
   *
   * @param insertErrors the map of errors to log.
   */
  private void logInsertErrors(Map<Long, List<BigQueryError>> insertErrors) {
    for (Map.Entry<Long, List<BigQueryError>> entries : insertErrors.entrySet()) {
      long index = entries.getKey();
      for (BigQueryError error : entries.getValue()) {
        LOG.info("Error when inserting row with index {}: {}", index, error.getMessage());
      }
    }
  }

  /**
   * Create a BigQuery dataset in which all tables will exist.
   *
   * @param region the region to store the dataset in.
   * @return Dataset id that was created
   * @throws BigQueryResourceManagerException if there is an error creating the dataset in BigQuery.
   */
  public synchronized String createDataset(String region) throws BigQueryResourceManagerException {

    // Check to see if dataset already exists, and throw error if it does
    if (dataset != null) {
      throw new IllegalStateException(
          "Dataset " + datasetId + " already exists for project " + projectId + ".");
    }

    LOG.info("Creating dataset {} in project {}.", datasetId, projectId);

    // Send the dataset request to Google Cloud
    try {
      DatasetInfo datasetInfo = DatasetInfo.newBuilder(datasetId).setLocation(region).build();
      LOG.info("Dataset {} created successfully", datasetId);
      dataset = bigQuery.create(datasetInfo);
      return datasetId;
    } catch (Exception e) {
      throw new BigQueryResourceManagerException("Failed to create dataset.", e);
    }
  }

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
  public synchronized TableId createTable(String tableName, Schema schema)
      throws BigQueryResourceManagerException {
    return createTable(tableName, schema, System.currentTimeMillis() + 3600000); // 1h
  }

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
  public synchronized TableId createTable(
      String tableName, Schema schema, Long expirationTimeMillis)
      throws BigQueryResourceManagerException {
    // Check table ID
    BigQueryResourceManagerUtils.checkValidTableId(tableName);

    // Check schema
    if (schema == null) {
      throw new IllegalArgumentException("A valid schema must be provided to create a table.");
    }
    // Create a default dataset if this resource manager has not already created one
    if (dataset == null) {
      createDataset(DEFAULT_DATASET_REGION);
    }
    checkHasDataset();
    LOG.info("Creating table using tableName '{}'.", tableName);

    // Create the table if it does not already exist in the dataset
    try {
      TableId tableId = TableId.of(dataset.getDatasetId().getDataset(), tableName);
      if (bigQuery.getTable(tableId) == null) {
        TableDefinition tableDefinition = StandardTableDefinition.of(schema);
        TableInfo tableInfo =
            TableInfo.newBuilder(tableId, tableDefinition)
                .setExpirationTime(expirationTimeMillis)
                .build();
        bigQuery.create(tableInfo);
        LOG.info(
            "Successfully created table {}.{}", dataset.getDatasetId().getDataset(), tableName);

        return tableId;
      } else {
        throw new IllegalStateException(
            "Table " + tableId + " already exists for dataset " + datasetId + ".");
      }
    } catch (Exception e) {
      throw new BigQueryResourceManagerException("Failed to create table.", e);
    }
  }

  /**
   * Writes a given row into a table. This method requires {@link
   * BigQueryResourceManager#createTable(String, Schema)} to be called for the target table
   * beforehand.
   *
   * @param tableName The name of the table to insert the given row into.
   * @param row A row object representing the table row.
   * @throws BigQueryResourceManagerException if method is called after resources have been cleaned
   *     up, if the manager object has no dataset, if the table does not exist or if there is an
   *     Exception when attempting to insert the rows.
   */
  public synchronized void write(String tableName, RowToInsert row)
      throws BigQueryResourceManagerException {
    write(tableName, ImmutableList.of(row));
  }

  /**
   * Writes a collection of table rows into a single table. This method requires {@link
   * BigQueryResourceManager#createTable(String, Schema)} to be called for the target table
   * beforehand.
   *
   * @param tableName The name of the table to insert the given rows into.
   * @param rows A collection of table rows.
   * @throws BigQueryResourceManagerException if method is called after resources have been cleaned
   *     up, if the manager object has no dataset, if the table does not exist or if there is an
   *     Exception when attempting to insert the rows.
   */
  public synchronized void write(String tableName, List<RowToInsert> rows)
      throws BigQueryResourceManagerException {
    // Exit early if there are no mutations
    if (!rows.iterator().hasNext()) {
      return;
    }

    Table table = getTableIfExists(tableName);

    LOG.info(
        "Attempting to write {} records to {}.{}.",
        rows.size(),
        dataset.getDatasetId().getDataset(),
        tableName);

    // Send row mutations to the table
    int successfullyWrittenRecords = rows.size();
    try {
      InsertAllResponse insertResponse = table.insert(rows);
      successfullyWrittenRecords -= insertResponse.getInsertErrors().size();

      if (insertResponse.hasErrors()) {
        LOG.warn("Errors encountered when inserting rows: ");
        logInsertErrors(insertResponse.getInsertErrors());
      }

    } catch (Exception e) {
      throw new BigQueryResourceManagerException("Failed to write to table.", e);
    }

    LOG.info(
        "Successfully wrote {} records to {}.{}.",
        successfullyWrittenRecords,
        dataset.getDatasetId().getDataset(),
        tableName);
  }

  /**
   * Runs the specified query.
   *
   * @param query the query to execute
   */
  public TableResult runQuery(String query) {
    try {
      TableResult results = bigQuery.query(QueryJobConfiguration.newBuilder(query).build());
      LOG.info("Loaded {} rows from {}", results.getTotalRows(), query);
      return results;
    } catch (Exception e) {
      throw new BigQueryResourceManagerException("Failed to read query " + query, e);
    }
  }

  /**
   * Gets the number of rows in the table.
   *
   * @param table the name of the table
   */
  public Long getRowCount(String table) {
    TableResult r =
        runQuery(String.format("SELECT COUNT(*) FROM `%s.%s.%s`", projectId, datasetId, table));
    return StreamSupport.stream(r.getValues().spliterator(), false)
        .map(fieldValues -> fieldValues.get(0).getLongValue())
        .collect(Collectors.toList())
        .get(0);
  }

  /**
   * Reads all the rows in a table and returns a TableResult containing a JSON string
   * representation. This method requires {@link BigQueryResourceManager#createTable(String,
   * Schema)} to be called for the target table beforehand.
   *
   * @param table The table reference to read rows from.
   * @return A TableResult containing all the rows in the table in JSON.
   * @throws BigQueryResourceManagerException if method is called after resources have been cleaned
   *     up, if the manager object has no dataset, if the table does not exist or if there is an
   *     Exception when attempting to insert the rows.
   */
  public synchronized TableResult readTable(TableId table) throws BigQueryResourceManagerException {
    return readTable(table.getTable());
  }

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
  public synchronized TableResult readTable(String tableName)
      throws BigQueryResourceManagerException {
    return readTable(tableName, -1);
  }

  /**
   * Reads number of rows in a table and returns a TableResult containing a JSON string
   * representation. This method requires {@link BigQueryResourceManager#createTable(String,
   * Schema)} to be called for the target table beforehand.
   *
   * @param table The table reference to read rows from.
   * @return A TableResult containing all the rows in the table in JSON.
   * @throws BigQueryResourceManagerException if method is called after resources have been cleaned
   *     up, if the manager object has no dataset, if the table does not exist or if there is an
   *     Exception when attempting to insert the rows.
   */
  public synchronized TableResult readTable(TableId table, int numRows)
      throws BigQueryResourceManagerException {
    return readTable(table.getTable(), numRows);
  }

  /**
   * Reads number of rows in a table and returns a TableResult containing a JSON string
   * representation. This method requires {@link BigQueryResourceManager#createTable(String,
   * Schema)} to be called for the target table beforehand.
   *
   * @param tableName The name of the table to read rows from.
   * @param numRows number of rows to read from the table
   * @return A TableResult containing all the rows in the table in JSON.
   * @throws BigQueryResourceManagerException if method is called after resources have been cleaned
   *     up, if the manager object has no dataset, if the table does not exist or if there is an
   *     Exception when attempting to insert the rows.
   */
  public synchronized TableResult readTable(String tableName, int numRows)
      throws BigQueryResourceManagerException {
    getTableIfExists(tableName);

    LOG.info(
        "Reading {} rows from {}.{}",
        numRows == -1 ? "all" : Integer.toString(numRows),
        dataset.getDatasetId().getDataset(),
        tableName);

    // Read all the rows from the table given by tableId
    String query =
        "SELECT TO_JSON_STRING(t) FROM `"
            + String.join(".", projectId, datasetId, tableName)
            + "` AS t"
            + (numRows != -1 ? " LIMIT " + numRows + ";" : ";");
    return runQuery(query);
  }

  /**
   * Deletes all created resources (dataset and tables) and cleans up the BigQuery client, making
   * the manager object unusable.
   *
   * @throws BigQueryResourceManagerException if there is an error deleting the tables or dataset in
   *     BigQuery.
   */
  @Override
  public synchronized void cleanupAll() throws BigQueryResourceManagerException {
    LOG.info("Attempting to cleanup manager.");
    try {
      if (dataset != null) {
        Page<Table> tables = bigQuery.listTables(dataset.getDatasetId());
        for (Table table : tables.iterateAll()) {
          bigQuery.delete(
              TableId.of(
                  projectId, dataset.getDatasetId().getDataset(), table.getTableId().getTable()));
        }
        bigQuery.delete(dataset.getDatasetId());
        dataset = null;
      }
    } catch (Exception e) {
      throw new BigQueryResourceManagerException("Failed to delete resources.", e);
    }
    LOG.info("Manager successfully cleaned up.");
  }

  /** Builder for {@link BigQueryResourceManager}. */
  public static final class Builder {

    private final String testId;
    private final String projectId;
    private String datasetId;
    private Credentials credentials;

    private Builder(String testId, String projectId, Credentials credentials) {
      this.testId = testId;
      this.projectId = projectId;
      this.datasetId = null;
      this.credentials = credentials;
    }

    public Builder setDatasetId(String datasetId) {
      this.datasetId = datasetId;
      return this;
    }

    public Builder setCredentials(Credentials credentials) {
      this.credentials = credentials;
      return this;
    }

    public BigQueryResourceManager build() {
      return new BigQueryResourceManager(this);
    }
  }
}
