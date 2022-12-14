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

import static com.google.cloud.teleport.it.bigquery.BigQueryResourceManagerUtils.checkValidTableId;

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
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Default class for implementation of {@link BigQueryResourceManager} interface.
 *
 * <p>The class supports one dataset, and multiple tables per dataset object.
 *
 * <p>The class is thread-safe.
 */
public final class DefaultBigQueryResourceManager implements BigQueryResourceManager {
  private static final Logger LOG = LoggerFactory.getLogger(DefaultBigQueryResourceManager.class);
  private static final String DEFAULT_DATASET_REGION = "us-central1";

  private final String projectId;
  private final String datasetId;

  private final BigQuery bigQuery;
  private Dataset dataset;

  private DefaultBigQueryResourceManager(Builder builder) {
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
    }
  }

  @VisibleForTesting
  DefaultBigQueryResourceManager(String testId, String projectId, BigQuery bigQuery) {
    this.datasetId = BigQueryResourceManagerUtils.generateDatasetId(testId);
    this.projectId = projectId;
    this.bigQuery = bigQuery;
  }

  public static DefaultBigQueryResourceManager.Builder builder(String testId, String projectId) {
    return new DefaultBigQueryResourceManager.Builder(testId, projectId);
  }

  /**
   * Returns the project ID this Resource Manager is configured to operate on.
   *
   * @return the project ID.
   */
  public String getProjectId() {
    return projectId;
  }

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

  @Override
  public synchronized void createDataset(String region) {

    // Check to see if dataset already exists, and throw error if it does
    if (dataset != null) {
      throw new IllegalStateException(
          "Dataset " + datasetId + " already exists for project " + projectId + ".");
    }

    LOG.info("Creating dataset {} in project {}.", datasetId, projectId);

    // Send the dataset request to Google Cloud
    try {
      DatasetInfo datasetInfo = DatasetInfo.newBuilder(datasetId).setLocation(region).build();
      dataset = bigQuery.create(datasetInfo);

    } catch (Exception e) {
      throw new BigQueryResourceManagerException("Failed to create dataset.", e);
    }

    LOG.info("Dataset {} created successfully", datasetId);
  }

  @Override
  public synchronized TableId createTable(String tableName, Schema schema) {
    return createTable(tableName, schema, System.currentTimeMillis() + 3600000); // 1h
  }

  @Override
  public synchronized TableId createTable(String tableName, Schema schema, Long expirationTime) {
    // Check table ID
    checkValidTableId(tableName);

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
                .setExpirationTime(expirationTime)
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

  @Override
  public synchronized void write(String tableName, RowToInsert row) {
    write(tableName, ImmutableList.of(row));
  }

  @Override
  public synchronized void write(String tableName, List<RowToInsert> rows) {
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

  @Override
  public synchronized TableResult readTable(String tableName) {
    getTableIfExists(tableName);

    LOG.info("Reading all rows from {}.{}", dataset.getDatasetId().getDataset(), tableName);

    // Read all the rows from the table given by tableId
    TableResult results;
    try {
      String query =
          "SELECT TO_JSON_STRING(t) FROM `"
              + String.join(".", projectId, datasetId, tableName)
              + "` AS t;";
      QueryJobConfiguration queryConfig = QueryJobConfiguration.newBuilder(query).build();
      results = bigQuery.query(queryConfig);
    } catch (Exception e) {
      throw new BigQueryResourceManagerException("Failed to read from table " + tableName + ".", e);
    }

    LOG.info(
        "Loaded {} rows from {}.{}",
        results.getTotalRows(),
        dataset.getDatasetId().getDataset(),
        tableName);

    return results;
  }

  @Override
  public synchronized TableResult readTable(String tableName, int numRows) {
    getTableIfExists(tableName);

    LOG.info("Reading {} rows from {}.{}", numRows, dataset.getDatasetId().getDataset(), tableName);

    // Read all the rows from the table given by tableId
    TableResult results;
    try {
      String query =
          "SELECT TO_JSON_STRING(t) FROM `"
              + String.join(".", projectId, datasetId, tableName)
              + "` AS t"
              + " LIMIT "
              + numRows
              + ";";
      QueryJobConfiguration queryConfig = QueryJobConfiguration.newBuilder(query).build();
      results = bigQuery.query(queryConfig);
    } catch (Exception e) {
      throw new BigQueryResourceManagerException("Failed to read from table " + tableName + ".", e);
    }

    LOG.info(
        "Loaded {} rows from {}.{}",
        results.getTotalRows(),
        dataset.getDatasetId().getDataset(),
        tableName);

    return results;
  }

  @Override
  public synchronized void cleanupAll() {
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
      }
    } catch (Exception e) {
      throw new BigQueryResourceManagerException("Failed to delete resources.", e);
    }
    LOG.info("Manager successfully cleaned up.");
  }

  /** Builder for {@link DefaultBigQueryResourceManager}. */
  public static final class Builder {

    private final String testId;
    private final String projectId;
    private String datasetId;
    private Credentials credentials;

    private Builder(String testId, String projectId) {
      this.testId = testId;
      this.projectId = projectId;
    }

    public Builder setDatasetId(String datasetId) {
      this.datasetId = datasetId;
      return this;
    }

    public Builder setCredentials(Credentials credentials) {
      this.credentials = credentials;
      return this;
    }

    public DefaultBigQueryResourceManager build() {
      return new DefaultBigQueryResourceManager(this);
    }
  }
}
