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
package org.apache.beam.sdk.io.gcp.bigquery;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

import com.google.api.client.googleapis.services.AbstractGoogleClientRequest;
import com.google.api.client.util.BackOff;
import com.google.api.client.util.BackOffUtils;
import com.google.api.client.util.ClassInfo;
import com.google.api.client.util.Data;
import com.google.api.client.util.Sleeper;
import com.google.api.services.bigquery.Bigquery;
import com.google.api.services.bigquery.model.Dataset;
import com.google.api.services.bigquery.model.DatasetReference;
import com.google.api.services.bigquery.model.ErrorProto;
import com.google.api.services.bigquery.model.Job;
import com.google.api.services.bigquery.model.JobConfiguration;
import com.google.api.services.bigquery.model.JobConfigurationQuery;
import com.google.api.services.bigquery.model.JobReference;
import com.google.api.services.bigquery.model.JobStatistics;
import com.google.api.services.bigquery.model.JobStatus;
import com.google.api.services.bigquery.model.Table;
import com.google.api.services.bigquery.model.TableCell;
import com.google.api.services.bigquery.model.TableDataList;
import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.Uninterruptibles;
import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;

import org.apache.beam.sdk.util.BackOffAdapter;
import org.apache.beam.sdk.util.FluentBackoff;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Iterates over all rows in a table.
 */
class BigQueryTableRowIterator implements AutoCloseable {
  private static final Logger LOG = LoggerFactory.getLogger(BigQueryTableRowIterator.class);

  @Nullable private TableReference ref;
  @Nullable private final String projectId;
  @Nullable private TableSchema schema;
  @Nullable private JobConfigurationQuery queryConfig;
  private final Bigquery client;
  private String pageToken;
  private Iterator<TableRow> iteratorOverCurrentBatch;
  private TableRow current;
  // Set true when the final page is seen from the service.
  private boolean lastPage = false;

  // The maximum number of times a BigQuery request will be retried
  private static final int MAX_RETRIES = 3;
  // Initial wait time for the backoff implementation
  private static final Duration INITIAL_BACKOFF_TIME = Duration.standardSeconds(1);

  // After sending a query to BQ service we will be polling the BQ service to check the status with
  // following interval to check the status of query execution job
  private static final Duration QUERY_COMPLETION_POLL_TIME = Duration.standardSeconds(1);

  // Temporary dataset used to store query results.
  private String temporaryDatasetId = null;
  // Temporary table used to store query results.
  private String temporaryTableId = null;

  private BigQueryTableRowIterator(
      @Nullable TableReference ref, @Nullable JobConfigurationQuery queryConfig,
      @Nullable String projectId, Bigquery client) {
    this.ref = ref;
    this.queryConfig = queryConfig;
    this.projectId = projectId;
    this.client = checkNotNull(client, "client");
  }

  /**
   * Constructs a {@code BigQueryTableRowIterator} that reads from the specified table.
   */
  static BigQueryTableRowIterator fromTable(TableReference ref, Bigquery client) {
    checkNotNull(ref, "ref");
    checkNotNull(client, "client");
    return new BigQueryTableRowIterator(ref, /* queryConfig */null, ref.getProjectId(), client);
  }

  /**
   * Constructs a {@code BigQueryTableRowIterator} that reads from the results of executing the
   * specified query in the specified project.
   */
  static BigQueryTableRowIterator fromQuery(
      JobConfigurationQuery queryConfig, String projectId, Bigquery client) {
    checkNotNull(queryConfig, "queryConfig");
    checkNotNull(projectId, "projectId");
    checkNotNull(client, "client");
    return new BigQueryTableRowIterator(/* ref */null, queryConfig, projectId, client);
  }

  /**
   * Opens the table for read.
   * @throws IOException on failure
   */
  void open() throws IOException, InterruptedException {
    if (queryConfig != null) {
      ref = executeQueryAndWaitForCompletion();
    }
    // Get table schema.
    schema = getTable(ref).getSchema();
  }

  boolean advance() throws IOException, InterruptedException {
    while (true) {
      if (iteratorOverCurrentBatch != null && iteratorOverCurrentBatch.hasNext()) {
        // Embed schema information into the raw row, so that values have an
        // associated key.
        current = getTypedTableRow(schema.getFields(), iteratorOverCurrentBatch.next());
        return true;
      }
      if (lastPage) {
        return false;
      }

      Bigquery.Tabledata.List list =
          client.tabledata().list(ref.getProjectId(), ref.getDatasetId(), ref.getTableId());
      if (pageToken != null) {
        list.setPageToken(pageToken);
      }

      TableDataList result = executeWithBackOff(
          list,
          String.format(
              "Error reading from BigQuery table %s of dataset %s.",
              ref.getTableId(), ref.getDatasetId()));

      pageToken = result.getPageToken();
      iteratorOverCurrentBatch =
          result.getRows() != null
              ? result.getRows().iterator()
              : Collections.<TableRow>emptyIterator();

      // The server may return a page token indefinitely on a zero-length table.
      if (pageToken == null || result.getTotalRows() != null && result.getTotalRows() == 0) {
        lastPage = true;
      }
    }
  }

  TableRow getCurrent() {
    if (current == null) {
      throw new NoSuchElementException();
    }
    return current;
  }

  /**
   * Adjusts a field returned from the BigQuery API to match what we will receive when running
   * BigQuery's export-to-GCS and parallel read, which is the efficient parallel implementation
   * used for batch jobs executed on the Beam Runners that perform initial splitting.
   *
   * <p>The following is the relationship between BigQuery schema and Java types:
   *
   * <ul>
   *   <li>Nulls are {@code null}.
   *   <li>Repeated fields are {@code List} of objects.
   *   <li>Record columns are {@link TableRow} objects.
   *   <li>{@code BOOLEAN} columns are JSON booleans, hence Java {@code Boolean} objects.
   *   <li>{@code FLOAT} columns are JSON floats, hence Java {@code Double} objects.
   *   <li>{@code TIMESTAMP} columns are {@code String} objects that are of the format
   *       {@code yyyy-MM-dd HH:mm:ss[.SSSSSS] UTC}, where the {@code .SSSSSS} has no trailing
   *       zeros and can be 1 to 6 digits long.
   *   <li>Every other atomic type is a {@code String}.
   * </ul>
   *
   * <p>Note that integers are encoded as strings to match BigQuery's exported JSON format.
   *
   * <p>Finally, values are stored in the {@link TableRow} as {"field name": value} pairs
   * and are not accessible through the {@link TableRow#getF} function.
   */
  @Nullable private Object getTypedCellValue(TableFieldSchema fieldSchema, Object v) {
    if (Data.isNull(v)) {
      return null;
    }

    if (Objects.equals(fieldSchema.getMode(), "REPEATED")) {
      TableFieldSchema elementSchema = fieldSchema.clone().setMode("REQUIRED");
      @SuppressWarnings("unchecked")
      List<Map<String, Object>> rawCells = (List<Map<String, Object>>) v;
      ImmutableList.Builder<Object> values = ImmutableList.builder();
      for (Map<String, Object> element : rawCells) {
        values.add(getTypedCellValue(elementSchema, element.get("v")));
      }
      return values.build();
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
      return BigQueryAvroUtils.formatTimestamp((String) v);
    }

    // Returns the original value for:
    // 1. String, 2. base64 encoded BYTES, 3. DATE, DATETIME, TIME strings.
    return v;
  }

  /**
   * A list of the field names that cannot be used in BigQuery tables processed by Apache Beam,
   * because they are reserved keywords in {@link TableRow}.
   */
  // TODO: This limitation is unfortunate. We need to give users a way to use BigQueryIO that does
  // not indirect through our broken use of {@link TableRow}.
  //     See discussion: https://github.com/GoogleCloudPlatform/DataflowJavaSDK/pull/41
  private static final Collection<String> RESERVED_FIELD_NAMES =
      ClassInfo.of(TableRow.class).getNames();

  /**
   * Converts a row returned from the BigQuery JSON API as a {@code Map<String, Object>} into a
   * Java {@link TableRow} with nested {@link TableCell TableCells}. The {@code Object} values in
   * the cells are converted to Java types according to the provided field schemas.
   *
   * <p>See {@link #getTypedCellValue(TableFieldSchema, Object)} for details on how BigQuery
   * types are mapped to Java types.
   */
  private TableRow getTypedTableRow(List<TableFieldSchema> fields, Map<String, Object> rawRow) {
    // If rawRow is a TableRow, use it. If not, create a new one.
    TableRow row;
    List<? extends Map<String, Object>> cells;
    if (rawRow instanceof TableRow) {
      // Since rawRow is a TableRow it already has TableCell objects in setF. We do not need to do
      // any type conversion, but extract the cells for cell-wise processing below.
      row = (TableRow) rawRow;
      cells = row.getF();
      // Clear the cells from the row, so that row.getF() will return null. This matches the
      // behavior of rows produced by the BigQuery export API used on the service.
      row.setF(null);
    } else {
      row = new TableRow();

      // Since rawRow is a Map<String, Object> we use Map.get("f") instead of TableRow.getF() to
      // get its cells. Similarly, when rawCell is a Map<String, Object> instead of a TableCell,
      // we will use Map.get("v") instead of TableCell.getV() get its value.
      @SuppressWarnings("unchecked")
      List<? extends Map<String, Object>> rawCells =
          (List<? extends Map<String, Object>>) rawRow.get("f");
      cells = rawCells;
    }

    checkState(cells.size() == fields.size(),
        "Expected that the row has the same number of cells %s as fields in the schema %s",
        cells.size(), fields.size());

    // Loop through all the fields in the row, normalizing their types with the TableFieldSchema
    // and storing the normalized values by field name in the Map<String, Object> that
    // underlies the TableRow.
    Iterator<? extends Map<String, Object>> cellIt = cells.iterator();
    Iterator<TableFieldSchema> fieldIt = fields.iterator();
    while (cellIt.hasNext()) {
      Map<String, Object> cell = cellIt.next();
      TableFieldSchema fieldSchema = fieldIt.next();

      // Convert the object in this cell to the Java type corresponding to its type in the schema.
      Object convertedValue = getTypedCellValue(fieldSchema, cell.get("v"));

      String fieldName = fieldSchema.getName();
      checkArgument(!RESERVED_FIELD_NAMES.contains(fieldName),
          "BigQueryIO does not support records with columns named %s", fieldName);

      if (convertedValue == null) {
        // BigQuery does not include null values when the export operation (to JSON) is used.
        // To match that behavior, BigQueryTableRowiterator, and the DirectRunner,
        // intentionally omits columns with null values.
        continue;
      }

      row.set(fieldName, convertedValue);
    }
    return row;
  }

  // Get the BiqQuery table.
  private Table getTable(TableReference ref) throws IOException, InterruptedException {
    Bigquery.Tables.Get get =
        client.tables().get(ref.getProjectId(), ref.getDatasetId(), ref.getTableId());

    return executeWithBackOff(
        get,
        String.format(
            "Error opening BigQuery table %s of dataset %s.",
            ref.getTableId(),
            ref.getDatasetId()));
  }

  // Create a new BigQuery dataset
  private void createDataset(String datasetId, @Nullable String location)
      throws IOException, InterruptedException {
    Dataset dataset = new Dataset();
    DatasetReference reference = new DatasetReference();
    reference.setProjectId(projectId);
    reference.setDatasetId(datasetId);
    dataset.setDatasetReference(reference);
    if (location != null) {
      dataset.setLocation(location);
    }

    executeWithBackOff(
        client.datasets().insert(projectId, dataset),
        String.format(
            "Error when trying to create the temporary dataset %s in project %s.",
            datasetId, projectId));
  }

  // Delete the given table that is available in the given dataset.
  private void deleteTable(String datasetId, String tableId)
      throws IOException, InterruptedException {
    executeWithBackOff(
        client.tables().delete(projectId, datasetId, tableId),
        String.format(
            "Error when trying to delete the temporary table %s in dataset %s of project %s. "
            + "Manual deletion may be required.",
            tableId, datasetId, projectId));
  }

  // Delete the given dataset. This will fail if the given dataset has any tables.
  private void deleteDataset(String datasetId) throws IOException, InterruptedException {
    executeWithBackOff(
        client.datasets().delete(projectId, datasetId),
        String.format(
            "Error when trying to delete the temporary dataset %s in project %s. "
            + "Manual deletion may be required.",
            datasetId, projectId));
  }

  /**
   * Executes the specified query and returns a reference to the temporary BigQuery table created
   * to hold the results.
   *
   * @throws IOException if the query fails.
   */
  private TableReference executeQueryAndWaitForCompletion()
      throws IOException, InterruptedException {
    checkState(projectId != null, "Unable to execute a query without a configured project id");
    checkState(queryConfig != null, "Unable to execute a query without a configured query");
    // Dry run query to get source table location
    Job dryRunJob = new Job()
        .setConfiguration(new JobConfiguration()
            .setQuery(queryConfig)
            .setDryRun(true));
    JobStatistics jobStats = executeWithBackOff(
        client.jobs().insert(projectId, dryRunJob),
        String.format("Error when trying to dry run query %s.",
            queryConfig.toPrettyString())).getStatistics();

    // Let BigQuery to pick default location if the query does not read any tables.
    String location = null;
    @Nullable List<TableReference> tables = jobStats.getQuery().getReferencedTables();
    if (tables != null && !tables.isEmpty()) {
      Table table = getTable(tables.get(0));
      location = table.getLocation();
    }

    // Create a temporary dataset to store results.
    // Starting dataset name with an "_" so that it is hidden.
    Random rnd = new Random(System.currentTimeMillis());
    temporaryDatasetId = "_beam_temporary_dataset_" + rnd.nextInt(1000000);
    temporaryTableId = "beam_temporary_table_" + rnd.nextInt(1000000);

    createDataset(temporaryDatasetId, location);
    Job job = new Job();
    JobConfiguration config = new JobConfiguration();
    config.setQuery(queryConfig);
    job.setConfiguration(config);

    TableReference destinationTable = new TableReference();
    destinationTable.setProjectId(projectId);
    destinationTable.setDatasetId(temporaryDatasetId);
    destinationTable.setTableId(temporaryTableId);
    queryConfig.setDestinationTable(destinationTable);
    queryConfig.setAllowLargeResults(true);

    Job queryJob = executeWithBackOff(
        client.jobs().insert(projectId, job),
        String.format("Error when trying to execute the job for query %s.",
            queryConfig.toPrettyString()));
    JobReference jobId = queryJob.getJobReference();

    while (true) {
      Job pollJob = executeWithBackOff(
          client.jobs().get(projectId, jobId.getJobId()),
          String.format("Error when trying to get status of the job for query %s.",
              queryConfig.toPrettyString()));
      JobStatus status = pollJob.getStatus();
      if (status.getState().equals("DONE")) {
        // Job is DONE, but did not necessarily succeed.
        ErrorProto error = status.getErrorResult();
        if (error == null) {
          return pollJob.getConfiguration().getQuery().getDestinationTable();
        } else {
          // There will be no temporary table to delete, so null out the reference.
          temporaryTableId = null;
          throw new IOException(String.format(
              "Executing query %s failed: %s", queryConfig.toPrettyString(), error.getMessage()));
        }
      }
      Uninterruptibles.sleepUninterruptibly(
          QUERY_COMPLETION_POLL_TIME.getMillis(), TimeUnit.MILLISECONDS);
    }
  }

  // Execute a BQ request with exponential backoff and return the result.
  // client - BQ request to be executed
  // error - Formatted message to log if when a request fails. Takes exception message as a
  // formatter parameter.
  private static <T> T executeWithBackOff(AbstractGoogleClientRequest<T> client, String error)
      throws IOException, InterruptedException {
    Sleeper sleeper = Sleeper.DEFAULT;
    BackOff backOff =
        BackOffAdapter.toGcpBackOff(
            FluentBackoff.DEFAULT
                .withMaxRetries(MAX_RETRIES).withInitialBackoff(INITIAL_BACKOFF_TIME).backoff());

    T result = null;
    while (true) {
      try {
        result = client.execute();
        break;
      } catch (IOException e) {
        LOG.error("{}", error, e);
        if (!BackOffUtils.next(sleeper, backOff)) {
          String errorMessage = String.format(
              "%s Failing to execute job after %d attempts.", error, MAX_RETRIES + 1);
          LOG.error("{}", errorMessage, e);
          throw new IOException(errorMessage, e);
        }
      }
    }
    return result;
  }

  @Override
  public void close() {
    // Prevent any further requests.
    lastPage = true;

    try {
      // Deleting temporary table and dataset that gets generated when executing a query.
      if (temporaryDatasetId != null) {
        if (temporaryTableId != null) {
          deleteTable(temporaryDatasetId, temporaryTableId);
        }
        deleteDataset(temporaryDatasetId);
      }
    } catch (IOException | InterruptedException e) {
      if (e instanceof InterruptedException) {
        Thread.currentThread().interrupt();
      }
      throw new RuntimeException(e);
    }

  }
}
