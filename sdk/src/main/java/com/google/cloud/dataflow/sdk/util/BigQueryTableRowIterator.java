/*
 * Copyright (C) 2015 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.google.cloud.dataflow.sdk.util;

import com.google.api.client.googleapis.services.AbstractGoogleClientRequest;
import com.google.api.client.util.BackOff;
import com.google.api.client.util.BackOffUtils;
import com.google.api.client.util.Data;
import com.google.api.client.util.Preconditions;
import com.google.api.client.util.Sleeper;
import com.google.api.services.bigquery.Bigquery;
import com.google.api.services.bigquery.Bigquery.Jobs.Insert;
import com.google.api.services.bigquery.model.Dataset;
import com.google.api.services.bigquery.model.DatasetReference;
import com.google.api.services.bigquery.model.Job;
import com.google.api.services.bigquery.model.JobConfiguration;
import com.google.api.services.bigquery.model.JobConfigurationQuery;
import com.google.api.services.bigquery.model.JobReference;
import com.google.api.services.bigquery.model.Table;
import com.google.api.services.bigquery.model.TableDataList;
import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;

import org.joda.time.Duration;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Random;

/**
 * Iterates over all rows in a table.
 */
public class BigQueryTableRowIterator implements Iterator<TableRow>, Closeable {
  private static final Logger LOG = LoggerFactory.getLogger(BigQueryTableRowIterator.class);

  private final Bigquery client;
  private TableReference ref;
  private final String projectId;
  private TableSchema schema;
  private String pageToken;
  private Iterator<TableRow> rowIterator;
  // Set true when the final page is seen from the service.
  private boolean lastPage = false;

  // The maximum number of times a BigQuery request will be retried
  private static final int MAX_RETRIES = 3;
  // Initial wait time for the backoff implementation
  private static final Duration INITIAL_BACKOFF_TIME = Duration.standardSeconds(1);

  // After sending a query to BQ service we will be polling the BQ service to check the status with
  // following interval to check the status of query execution job
  private static final Duration QUERY_COMPLETION_POLL_TIME = Duration.standardSeconds(1);

  private final String query;
  // Temporary dataset used to store query results.
  private String temporaryDatasetId = null;
  // Temporary table used to store query results.
  private String temporaryTableId = null;

  public BigQueryTableRowIterator(Bigquery client, TableReference ref) {
    this.client = client;
    this.ref = ref;
    this.query = null;
    this.projectId = ref.getProjectId();
  }

  public BigQueryTableRowIterator(Bigquery client, String query, String projectId) {
    this.client = client;
    this.ref = null;
    this.query = query;
    this.projectId = projectId;

  }

  @Override
  public boolean hasNext() {
    try {
      if (rowIterator == null || (!rowIterator.hasNext() && !lastPage)) {
        readNext();
      }
    } catch (IOException | InterruptedException e) {
      throw new RuntimeException(e);
    }

    return rowIterator.hasNext();
  }

  /**
   * Adjusts a field returned from the API to
   * match the type that will be seen when run on the
   * backend service. The end result is:
   *
   * <p><ul>
   *   <li> Nulls are {@code null}.
   *   <li> Repeated fields are lists.
   *   <li> Record columns are {@link TableRow}s.
   *   <li> {@code BOOLEAN} columns are JSON booleans, hence Java {@link Boolean}s.
   *   <li> {@code FLOAT} columns are JSON floats, hence Java {@link Double}s.
   *   <li> {@code TIMESTAMP} columns are {@link String}s that are of the format
   *        {yyyy-MM-dd HH:mm:ss.SSS UTC}.
   *   <li> Every other atomic type is a {@link String}.
   * </ul></p>
   *
   * <p> Note that currently integers are encoded as strings to match
   * the behavior of the backend service.
   */
  private Object getTypedCellValue(TableFieldSchema fieldSchema, Object v) {
    // In the input from the BQ API, atomic types all come in as
    // strings, while on the Dataflow service they have more precise
    // types.

    if (Data.isNull(v)) {
      return null;
    }

    if (Objects.equals(fieldSchema.getMode(), "REPEATED")) {
      TableFieldSchema elementSchema = fieldSchema.clone().setMode("REQUIRED");
      @SuppressWarnings("unchecked")
      List<Map<String, Object>> rawValues = (List<Map<String, Object>>) v;
      List<Object> values = new ArrayList<Object>(rawValues.size());
      for (Map<String, Object> element : rawValues) {
        values.add(getTypedCellValue(elementSchema, element.get("v")));
      }
      return values;
    }

    if (fieldSchema.getType().equals("RECORD")) {
      @SuppressWarnings("unchecked")
      Map<String, Object> typedV = (Map<String, Object>) v;
      return getTypedTableRow(fieldSchema.getFields(), typedV);
    }

    if (fieldSchema.getType().equals("FLOAT")) {
      return Double.parseDouble((String) v);
    }

    if (fieldSchema.getType().equals("BOOLEAN")) {
      return Boolean.parseBoolean((String) v);
    }

    if (fieldSchema.getType().equals("TIMESTAMP")) {
      // Seconds to milliseconds
      long milliSecs = (new Double(Double.parseDouble((String) v) * 1000)).longValue();
      DateTimeFormatter formatter =
          DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss.SSS").withZoneUTC();
      return formatter.print(milliSecs) + " UTC";
    }

    return v;
  }

  private TableRow getTypedTableRow(List<TableFieldSchema> fields, Map<String, Object> rawRow) {
    @SuppressWarnings("unchecked")
    List<Map<String, Object>> cells = (List<Map<String, Object>>) rawRow.get("f");
    Preconditions.checkState(cells.size() == fields.size());

    Iterator<Map<String, Object>> cellIt = cells.iterator();
    Iterator<TableFieldSchema> fieldIt = fields.iterator();

    TableRow row = new TableRow();
    while (cellIt.hasNext()) {
      Map<String, Object> cell = cellIt.next();
      TableFieldSchema fieldSchema = fieldIt.next();
      row.set(fieldSchema.getName(), getTypedCellValue(fieldSchema, cell.get("v")));
    }
    return row;
  }

  @Override
  public TableRow next() {
    if (!hasNext()) {
      throw new NoSuchElementException();
    }

    // Embed schema information into the raw row, so that values have an
    // associated key.  This matches how rows are read when using the
    // DataflowPipelineRunner.
    return getTypedTableRow(schema.getFields(), rowIterator.next());
  }

  @Override
  public void remove() {
    throw new UnsupportedOperationException();
  }

  // Create a new BigQuery dataset
  private void createDataset(String datasetId) throws IOException, InterruptedException {
    Dataset dataset = new Dataset();
    DatasetReference reference = new DatasetReference();
    reference.setProjectId(projectId);
    reference.setDatasetId(datasetId);
    dataset.setDatasetReference(reference);

    String createDatasetError =
        "Error when trying to create the temporary dataset " + datasetId + " in project "
        + projectId;
    executeWithBackOff(
        client.datasets().insert(projectId, dataset), createDatasetError + " :{}");
  }

  // Delete the given table that is available in the given dataset.
  private void deleteTable(String datasetId, String tableId)
      throws IOException, InterruptedException {
    executeWithBackOff(
        client.tables().delete(projectId, datasetId, tableId),
        "Error when trying to delete the temporary table " + datasetId + " in dataset " + datasetId
        + " of project " + projectId + ". Manual deletion may be required. Error message : {}");
  }

  // Delete the given dataset. This will fail if the given dataset has any tables.
  private void deleteDataset(String datasetId) throws IOException, InterruptedException {
    executeWithBackOff(
        client.datasets().delete(projectId, datasetId),
        "Error when trying to delete the temporary dataset " + datasetId + " in project "
        + projectId + ". Manual deletion may be required. Error message : {}");
  }

  private TableReference executeQueryAndWaitForCompletion()
      throws IOException, InterruptedException {
    // Create a temporary dataset to store results.
    // Starting dataset name with an "_" so that it is hidden.
    Random rnd = new Random(System.currentTimeMillis());
    temporaryDatasetId = "_dataflow_temporary_dataset_" + rnd.nextInt(1000000);
    temporaryTableId = "dataflow_temporary_table_" + rnd.nextInt(1000000);

    createDataset(temporaryDatasetId);
    Job job = new Job();
    JobConfiguration config = new JobConfiguration();
    JobConfigurationQuery queryConfig = new JobConfigurationQuery();
    config.setQuery(queryConfig);
    job.setConfiguration(config);
    queryConfig.setQuery(query);
    queryConfig.setAllowLargeResults(true);

    TableReference destinationTable = new TableReference();
    destinationTable.setProjectId(projectId);
    destinationTable.setDatasetId(temporaryDatasetId);
    destinationTable.setTableId(temporaryTableId);
    queryConfig.setDestinationTable(destinationTable);

    Insert insert = client.jobs().insert(projectId, job);
    Job queryJob = executeWithBackOff(
        insert, "Error when trying to execute the job for query " + query + " :{}");
    JobReference jobId = queryJob.getJobReference();

    while (true) {
      Job pollJob = executeWithBackOff(
          client.jobs().get(projectId, jobId.getJobId()),
          "Error when trying to get status of the job for query " + query + " :{}");
      if (pollJob.getStatus().getState().equals("DONE")) {
        return pollJob.getConfiguration().getQuery().getDestinationTable();
      }
      try {
        Thread.sleep(QUERY_COMPLETION_POLL_TIME.getMillis());
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
  }

  // Execute a BQ request with exponential backoff and return the result.
  // client - BQ request to be executed
  // error - Formatted message to log if when a request fails. Takes exception message as a
  // formatter parameter.
  public static <T> T executeWithBackOff(AbstractGoogleClientRequest<T> client, String error,
      Object... errorArgs) throws IOException, InterruptedException {
    Sleeper sleeper = Sleeper.DEFAULT;
    BackOff backOff =
        new AttemptBoundedExponentialBackOff(MAX_RETRIES, INITIAL_BACKOFF_TIME.getMillis());

    T result = null;
    while (true) {
      try {
        result = client.execute();
        break;
      } catch (IOException e) {
        LOG.error(String.format(error, errorArgs), e.getMessage());
        if (!BackOffUtils.next(sleeper, backOff)) {
          LOG.error(
              String.format(error, errorArgs), "Failing after retrying " + MAX_RETRIES + " times.");
          throw e;
        }
      }
    }

    return result;
  }

  private void readNext() throws IOException, InterruptedException {
    if (query != null && ref == null) {
      ref = executeQueryAndWaitForCompletion();
    }
    if (!isOpen()) {
      open();
    }

    Bigquery.Tabledata.List list =
        client.tabledata().list(ref.getProjectId(), ref.getDatasetId(), ref.getTableId());
    if (pageToken != null) {
      list.setPageToken(pageToken);
    }

    TableDataList result =
        executeWithBackOff(list, "Error reading from BigQuery table %s of dataset %s : {}",
            ref.getTableId(), ref.getDatasetId());

    pageToken = result.getPageToken();
    rowIterator =
        result.getRows() != null
            ? result.getRows().iterator() : Collections.<TableRow>emptyIterator();

    // The server may return a page token indefinitely on a zero-length table.
    if (pageToken == null || result.getTotalRows() != null && result.getTotalRows() == 0) {
      lastPage = true;
    }
  }

  @Override
  public void close() throws IOException {
    // Prevent any further requests.
    lastPage = true;

    try {
      // Deleting temporary table and dataset that gets generated when executing a query.
      if (temporaryDatasetId != null) {
        deleteTable(temporaryDatasetId, temporaryTableId);
        deleteDataset(temporaryDatasetId);
      }
    } catch (InterruptedException e) {
      throw new IOException(e);
    }

  }

  private boolean isOpen() {
    return schema != null;
  }

  /**
   * Opens the table for read.
   * @throws IOException on failure
   */
  private void open() throws IOException, InterruptedException {
    // Get table schema.
    Bigquery.Tables.Get get =
        client.tables().get(ref.getProjectId(), ref.getDatasetId(), ref.getTableId());

    Table table = executeWithBackOff(get, "Error opening BigQuery table  %s of dataset %s  : {}",
        ref.getTableId(), ref.getDatasetId());
    schema = table.getSchema();
  }
}
