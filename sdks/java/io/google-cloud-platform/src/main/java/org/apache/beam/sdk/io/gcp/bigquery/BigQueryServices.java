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

import com.google.api.services.bigquery.model.Dataset;
import com.google.api.services.bigquery.model.Job;
import com.google.api.services.bigquery.model.JobConfigurationExtract;
import com.google.api.services.bigquery.model.JobConfigurationLoad;
import com.google.api.services.bigquery.model.JobConfigurationQuery;
import com.google.api.services.bigquery.model.JobConfigurationTableCopy;
import com.google.api.services.bigquery.model.JobReference;
import com.google.api.services.bigquery.model.JobStatistics;
import com.google.api.services.bigquery.model.Table;
import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.bigquery.storage.v1.CreateReadSessionRequest;
import com.google.cloud.bigquery.storage.v1.ReadRowsRequest;
import com.google.cloud.bigquery.storage.v1.ReadRowsResponse;
import com.google.cloud.bigquery.storage.v1.ReadSession;
import com.google.cloud.bigquery.storage.v1.SplitReadStreamRequest;
import com.google.cloud.bigquery.storage.v1.SplitReadStreamResponse;
import java.io.IOException;
import java.io.Serializable;
import java.util.List;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.annotations.Experimental.Kind;
import org.apache.beam.sdk.util.Histogram;
import org.apache.beam.sdk.values.ValueInSingleWindow;
import org.checkerframework.checker.nullness.qual.Nullable;

/** An interface for real, mock, or fake implementations of Cloud BigQuery services. */
public interface BigQueryServices extends Serializable {

  /** Returns a real, mock, or fake {@link JobService}. */
  JobService getJobService(BigQueryOptions bqOptions);

  /** Returns a real, mock, or fake {@link DatasetService}. */
  DatasetService getDatasetService(BigQueryOptions bqOptions);

  /** Returns a real, mock, or fake {@link DatasetService}. */
  DatasetService getDatasetService(BigQueryOptions bqOptions, Histogram requestLatencies);

  /** Returns a real, mock, or fake {@link StorageClient}. */
  @Experimental(Kind.SOURCE_SINK)
  StorageClient getStorageClient(BigQueryOptions bqOptions) throws IOException;

  /** An interface for the Cloud BigQuery load service. */
  public interface JobService {
    /** Start a BigQuery load job. */
    void startLoadJob(JobReference jobRef, JobConfigurationLoad loadConfig)
        throws InterruptedException, IOException;
    /** Start a BigQuery extract job. */
    void startExtractJob(JobReference jobRef, JobConfigurationExtract extractConfig)
        throws InterruptedException, IOException;

    /** Start a BigQuery query job. */
    void startQueryJob(JobReference jobRef, JobConfigurationQuery query)
        throws IOException, InterruptedException;

    /** Start a BigQuery copy job. */
    void startCopyJob(JobReference jobRef, JobConfigurationTableCopy copyConfig)
        throws IOException, InterruptedException;

    /**
     * Waits for the job is Done, and returns the job.
     *
     * <p>Returns null if the {@code maxAttempts} retries reached.
     */
    Job pollJob(JobReference jobRef, int maxAttempts) throws InterruptedException;

    /** Dry runs the query in the given project. */
    JobStatistics dryRunQuery(String projectId, JobConfigurationQuery queryConfig, String location)
        throws InterruptedException, IOException;

    /**
     * Gets the specified {@link Job} by the given {@link JobReference}.
     *
     * <p>Returns null if the job is not found.
     */
    Job getJob(JobReference jobRef) throws IOException, InterruptedException;
  }

  /** An interface to get, create and delete Cloud BigQuery datasets and tables. */
  public interface DatasetService {
    /**
     * Gets the specified {@link Table} resource by table ID.
     *
     * <p>Returns null if the table is not found.
     */
    @Nullable
    Table getTable(TableReference tableRef) throws InterruptedException, IOException;

    @Nullable
    Table getTable(TableReference tableRef, List<String> selectedFields)
        throws InterruptedException, IOException;

    /** Creates the specified table if it does not exist. */
    void createTable(Table table) throws InterruptedException, IOException;

    /**
     * Deletes the table specified by tableId from the dataset. If the table contains data, all the
     * data will be deleted.
     */
    void deleteTable(TableReference tableRef) throws IOException, InterruptedException;

    /**
     * Returns true if the table is empty.
     *
     * @throws IOException if the table is not found.
     */
    boolean isTableEmpty(TableReference tableRef) throws IOException, InterruptedException;

    /** Gets the specified {@link Dataset} resource by dataset ID. */
    Dataset getDataset(String projectId, String datasetId) throws IOException, InterruptedException;

    /**
     * Create a {@link Dataset} with the given {@code location}, {@code description} and default
     * expiration time for tables in the dataset (if {@code null}, tables don't expire).
     */
    void createDataset(
        String projectId,
        String datasetId,
        @Nullable String location,
        @Nullable String description,
        @Nullable Long defaultTableExpirationMs)
        throws IOException, InterruptedException;

    /**
     * Deletes the dataset specified by the datasetId value.
     *
     * <p>Before you can delete a dataset, you must delete all its tables.
     */
    void deleteDataset(String projectId, String datasetId) throws IOException, InterruptedException;

    /**
     * Inserts {@link TableRow TableRows} with the specified insertIds if not null.
     *
     * <p>If any insert fail permanently according to the retry policy, those rows are added to
     * failedInserts.
     *
     * <p>Returns the total bytes count of {@link TableRow TableRows}.
     */
    <T> long insertAll(
        TableReference ref,
        List<ValueInSingleWindow<TableRow>> rowList,
        @Nullable List<String> insertIdList,
        InsertRetryPolicy retryPolicy,
        List<ValueInSingleWindow<T>> failedInserts,
        ErrorContainer<T> errorContainer,
        boolean skipInvalidRows,
        boolean ignoreUnknownValues,
        boolean ignoreInsertIds)
        throws IOException, InterruptedException;

    /** Patch BigQuery {@link Table} description. */
    Table patchTableDescription(TableReference tableReference, @Nullable String tableDescription)
        throws IOException, InterruptedException;
  }

  /**
   * Container for reading data from streaming endpoints.
   *
   * <p>An implementation does not need to be thread-safe.
   */
  interface BigQueryServerStream<T> extends Iterable<T>, Serializable {
    /**
     * Cancels the stream, releasing any client- and server-side resources. This method may be
     * called multiple times and from any thread.
     */
    void cancel();
  }

  /** An interface representing a client object for making calls to the BigQuery Storage API. */
  @Experimental(Kind.SOURCE_SINK)
  interface StorageClient extends AutoCloseable {
    /** Create a new read session against an existing table. */
    ReadSession createReadSession(CreateReadSessionRequest request);

    /** Read rows in the context of a specific read stream. */
    BigQueryServerStream<ReadRowsResponse> readRows(ReadRowsRequest request);

    SplitReadStreamResponse splitReadStream(SplitReadStreamRequest request);

    /**
     * Close the client object.
     *
     * <p>The override is required since {@link AutoCloseable} allows the close method to raise an
     * exception.
     */
    @Override
    void close();
  }
}
