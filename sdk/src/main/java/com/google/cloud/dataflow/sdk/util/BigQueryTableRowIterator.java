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
import com.google.api.services.bigquery.Bigquery.Jobs.Insert;
import com.google.api.services.bigquery.model.Dataset;
import com.google.api.services.bigquery.model.DatasetReference;
import com.google.api.services.bigquery.model.Job;
import com.google.api.services.bigquery.model.JobConfiguration;
import com.google.api.services.bigquery.model.JobConfigurationQuery;
import com.google.api.services.bigquery.model.JobReference;
import com.google.api.services.bigquery.model.Table;
import com.google.api.services.bigquery.model.TableCell;
import com.google.api.services.bigquery.model.TableDataList;
import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.common.collect.ImmutableList;

import org.joda.time.Duration;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Random;

import javax.annotation.Nullable;

/**
 * Iterates over all rows in a table.
 */
public class BigQueryTableRowIterator implements Iterator<TableRow>, Closeable {
  private static final Logger LOG = LoggerFactory.getLogger(BigQueryTableRowIterator.class);

  @Nullable private TableReference ref;
  @Nullable private final String projectId;
  @Nullable private TableSchema schema;
  private final Bigquery client;
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

  private BigQueryTableRowIterator(
      @Nullable TableReference ref, @Nullable String query, @Nullable String projectId,
      Bigquery client) {
    this.ref = ref;
    this.query = query;
    this.projectId = projectId;
    this.client = checkNotNull(client, "client");
  }

  /**
   * Constructs a {@code BigQueryTableRowIterator} that reads from the specified table.
   */
  public static BigQueryTableRowIterator fromTable(TableReference ref, Bigquery client) {
    checkNotNull(ref, "ref");
    checkNotNull(client, "client");
    return new BigQueryTableRowIterator(ref, null, ref.getProjectId(), client);
  }

  /**
   * Constructs a {@code BigQueryTableRowIterator} that reads from the results of executing the
   * specified query in the specified project.
   */
  public static BigQueryTableRowIterator fromQuery(
      String query, String projectId, Bigquery client) {
    checkNotNull(query, "query");
    checkNotNull(projectId, "projectId");
    checkNotNull(client, "client");
    return new BigQueryTableRowIterator(null, query, projectId, client);
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
   * Formats BigQuery seconds-since-epoch into String matching JSON export. Thread-safe and
   * immutable.
   */
  private static final DateTimeFormatter DATE_AND_SECONDS_FORMATER =
      DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss").withZoneUTC();
  private static String formatTimestamp(String timestamp) {
    // timestamp is in "seconds since epoch" format, with scientific notation.
    // e.g., "1.45206229112345E9" to mean "2016-01-06 06:38:11.123456 UTC".
    // Separate into seconds and microseconds.
    double timestampDoubleMicros = Double.parseDouble(timestamp) * 1000000;
    long timestampMicros = (long) timestampDoubleMicros;
    long seconds = timestampMicros / 1000000;
    int micros = (int) (timestampMicros % 1000000);
    String dayAndTime = DATE_AND_SECONDS_FORMATER.print(seconds * 1000);

    // No sub-second component.
    if (micros == 0) {
      return String.format("%s UTC", dayAndTime);
    }

    // Sub-second component.
    int digits = 6;
    int subsecond = micros;
    while (subsecond % 10 == 0) {
      digits--;
      subsecond /= 10;
    }
    String formatString = String.format("%%0%dd", digits);
    String fractionalSeconds = String.format(formatString, subsecond);
    return String.format("%s.%s UTC", dayAndTime, fractionalSeconds);
  }

  /**
   * Adjusts a field returned from the BigQuery API to match what we will receive when running
   * BigQuery's export-to-GCS and parallel read, which is the efficient parallel implementation
   * used for batch jobs executed on the Cloud Dataflow service.
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
      return formatTimestamp((String) v);
    }

    return v;
  }

  /**
   * A list of the field names that cannot be used in BigQuery tables processed by Dataflow,
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
      row.set(fieldName, convertedValue);
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
