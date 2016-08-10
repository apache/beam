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

import static com.google.common.base.Preconditions.checkNotNull;

import org.apache.beam.sdk.options.BigQueryOptions;
import org.apache.beam.sdk.options.GcsOptions;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.util.AttemptBoundedExponentialBackOff;
import org.apache.beam.sdk.util.IntervalBoundedExponentialBackOff;
import org.apache.beam.sdk.util.Transport;

import com.google.api.client.googleapis.json.GoogleJsonResponseException;
import com.google.api.client.googleapis.services.AbstractGoogleClientRequest;
import com.google.api.client.util.BackOff;
import com.google.api.client.util.BackOffUtils;
import com.google.api.client.util.Sleeper;
import com.google.api.services.bigquery.Bigquery;
import com.google.api.services.bigquery.model.Dataset;
import com.google.api.services.bigquery.model.DatasetReference;
import com.google.api.services.bigquery.model.Job;
import com.google.api.services.bigquery.model.JobConfiguration;
import com.google.api.services.bigquery.model.JobConfigurationExtract;
import com.google.api.services.bigquery.model.JobConfigurationLoad;
import com.google.api.services.bigquery.model.JobConfigurationQuery;
import com.google.api.services.bigquery.model.JobReference;
import com.google.api.services.bigquery.model.JobStatistics;
import com.google.api.services.bigquery.model.JobStatus;
import com.google.api.services.bigquery.model.Table;
import com.google.api.services.bigquery.model.TableDataInsertAllRequest;
import com.google.api.services.bigquery.model.TableDataInsertAllResponse;
import com.google.api.services.bigquery.model.TableDataList;
import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.hadoop.util.ApiErrorExtractor;
import com.google.common.annotations.VisibleForTesting;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nullable;

/**
 * An implementation of {@link BigQueryServices} that actually communicates with the cloud BigQuery
 * service.
 */
class BigQueryServicesImpl implements BigQueryServices {

  private static final Logger LOG = LoggerFactory.getLogger(BigQueryServicesImpl.class);

  // The maximum number of attempts to execute a BigQuery RPC.
  private static final int MAX_RPC_ATTEMPTS = 10;

  // The initial backoff for executing a BigQuery RPC.
  private static final long INITIAL_RPC_BACKOFF_MILLIS = TimeUnit.SECONDS.toMillis(1);

  // The initial backoff for polling the status of a BigQuery job.
  private static final long INITIAL_JOB_STATUS_POLL_BACKOFF_MILLIS = TimeUnit.SECONDS.toMillis(1);

  @Override
  public JobService getJobService(BigQueryOptions options) {
    return new JobServiceImpl(options);
  }

  @Override
  public DatasetService getDatasetService(BigQueryOptions options) {
    return new DatasetServiceImpl(options);
  }

  @Override
  public BigQueryJsonReader getReaderFromTable(BigQueryOptions bqOptions, TableReference tableRef) {
    return BigQueryJsonReaderImpl.fromTable(bqOptions, tableRef);
  }

  @Override
  public BigQueryJsonReader getReaderFromQuery(
      BigQueryOptions bqOptions, String query, String projectId, @Nullable Boolean flatten) {
    return BigQueryJsonReaderImpl.fromQuery(bqOptions, query, projectId, flatten);
  }

  @VisibleForTesting
  static class JobServiceImpl implements BigQueryServices.JobService {
    private final ApiErrorExtractor errorExtractor;
    private final Bigquery client;

    @VisibleForTesting
    JobServiceImpl(Bigquery client) {
      this.errorExtractor = new ApiErrorExtractor();
      this.client = client;
    }

    private JobServiceImpl(BigQueryOptions options) {
      this.errorExtractor = new ApiErrorExtractor();
      this.client = Transport.newBigQueryClient(options).build();
    }

    /**
     * {@inheritDoc}
     *
     * <p> the RPC for at most {@code MAX_RPC_ATTEMPTS} times until it succeeds.
     *
     * @throws IOException if it exceeds max RPC .
     */
    @Override
    public void startLoadJob(
        JobReference jobRef,
        JobConfigurationLoad loadConfig) throws InterruptedException, IOException {
      Job job = new Job()
          .setJobReference(jobRef)
          .setConfiguration(new JobConfiguration().setLoad(loadConfig));

      startJob(job, errorExtractor, client);
    }

    /**
     * {@inheritDoc}
     *
     * <p> the RPC for at most {@code MAX_RPC_ATTEMPTS} times until it succeeds.
     *
     * @throws IOException if it exceeds max RPC .
     */
    @Override
    public void startExtractJob(JobReference jobRef, JobConfigurationExtract extractConfig)
        throws InterruptedException, IOException {
      Job job = new Job()
          .setJobReference(jobRef)
          .setConfiguration(
              new JobConfiguration().setExtract(extractConfig));

      startJob(job, errorExtractor, client);
    }

    /**
     * {@inheritDoc}
     *
     * <p> the RPC for at most {@code MAX_RPC_ATTEMPTS} times until it succeeds.
     *
     * @throws IOException if it exceeds max RPC .
     */
    @Override
    public void startQueryJob(JobReference jobRef, JobConfigurationQuery queryConfig)
        throws IOException, InterruptedException {
      Job job = new Job()
          .setJobReference(jobRef)
          .setConfiguration(
              new JobConfiguration().setQuery(queryConfig));

      startJob(job, errorExtractor, client);
    }

    private static void startJob(Job job,
      ApiErrorExtractor errorExtractor,
      Bigquery client) throws IOException, InterruptedException {
      BackOff backoff =
          new AttemptBoundedExponentialBackOff(MAX_RPC_ATTEMPTS, INITIAL_RPC_BACKOFF_MILLIS);
      startJob(job, errorExtractor, client, Sleeper.DEFAULT, backoff);
    }

    @VisibleForTesting
    static void startJob(
        Job job,
        ApiErrorExtractor errorExtractor,
        Bigquery client,
        Sleeper sleeper,
        BackOff backoff) throws IOException, InterruptedException {
      JobReference jobRef = job.getJobReference();
      Exception lastException = null;
      do {
        try {
          client.jobs().insert(jobRef.getProjectId(), job).execute();
          return; // SUCCEEDED
        } catch (GoogleJsonResponseException e) {
          if (errorExtractor.itemAlreadyExists(e)) {
            return; // SUCCEEDED
          }
          // ignore and retry
          LOG.warn("Ignore the error and retry inserting the job.", e);
          lastException = e;
        } catch (IOException e) {
          // ignore and retry
          LOG.warn("Ignore the error and retry inserting the job.", e);
          lastException = e;
        }
      } while (nextBackOff(sleeper, backoff));
      throw new IOException(
          String.format(
              "Unable to insert job: %s, aborting after %d .",
              jobRef.getJobId(), MAX_RPC_ATTEMPTS),
          lastException);
    }

    @Override
    public Job pollJob(JobReference jobRef, int maxAttempts)
        throws InterruptedException {
      BackOff backoff = new AttemptBoundedExponentialBackOff(
          maxAttempts, INITIAL_JOB_STATUS_POLL_BACKOFF_MILLIS);
      return pollJob(jobRef, Sleeper.DEFAULT, backoff);
    }

    @VisibleForTesting
    Job pollJob(
        JobReference jobRef,
        Sleeper sleeper,
        BackOff backoff) throws InterruptedException {
      do {
        try {
          Job job = client.jobs().get(jobRef.getProjectId(), jobRef.getJobId()).execute();
          JobStatus status = job.getStatus();
          if (status != null && status.getState() != null && status.getState().equals("DONE")) {
            return job;
          }
          // The job is not DONE, wait longer and retry.
        } catch (IOException e) {
          // ignore and retry
          LOG.warn("Ignore the error and retry polling job status.", e);
        }
      } while (nextBackOff(sleeper, backoff));
      LOG.warn("Unable to poll job status: {}, aborting after reached max .", jobRef.getJobId());
      return null;
    }

    @Override
    public JobStatistics dryRunQuery(String projectId, String query)
        throws InterruptedException, IOException {
      Job job = new Job()
          .setConfiguration(new JobConfiguration()
              .setQuery(new JobConfigurationQuery()
                  .setQuery(query))
              .setDryRun(true));
      BackOff backoff =
          new AttemptBoundedExponentialBackOff(MAX_RPC_ATTEMPTS, INITIAL_RPC_BACKOFF_MILLIS);
      return executeWithRetries(
          client.jobs().insert(projectId, job),
          String.format(
              "Unable to dry run query: %s, aborting after %d retries.",
              query, MAX_RPC_ATTEMPTS),
          Sleeper.DEFAULT,
          backoff).getStatistics();
    }
  }

  @VisibleForTesting
  static class DatasetServiceImpl implements DatasetService {
    // Approximate amount of table data to upload per InsertAll request.
    private static final long UPLOAD_BATCH_SIZE_BYTES = 64 * 1024;

    // The maximum number of rows to upload per InsertAll request.
    private static final long MAX_ROWS_PER_BATCH = 500;

    // The maximum number of times to retry inserting rows into BigQuery.
    private static final int MAX_INSERT_ATTEMPTS = 5;

    // The initial backoff after a failure inserting rows into BigQuery.
    private static final long INITIAL_INSERT_BACKOFF_INTERVAL_MS = 200L;

    // Backoff time bounds for rate limit exceeded errors.
    private static final long INITIAL_RATE_LIMIT_EXCEEDED_BACKOFF_MS = TimeUnit.SECONDS.toMillis(1);
    private static final long MAX_RATE_LIMIT_EXCEEDED_BACKOFF_MS = TimeUnit.MINUTES.toMillis(2);

    private final ApiErrorExtractor errorExtractor;
    private final Bigquery client;
    private final PipelineOptions options;
    private final long maxRowsPerBatch;

    private ExecutorService executor;

    @VisibleForTesting
    DatasetServiceImpl(Bigquery client, PipelineOptions options) {
      this.errorExtractor = new ApiErrorExtractor();
      this.client = client;
      this.options = options;
      this.maxRowsPerBatch = MAX_ROWS_PER_BATCH;
      this.executor = null;
    }

    @VisibleForTesting
    DatasetServiceImpl(Bigquery client, PipelineOptions options, long maxRowsPerBatch) {
      this.errorExtractor = new ApiErrorExtractor();
      this.client = client;
      this.options = options;
      this.maxRowsPerBatch = maxRowsPerBatch;
      this.executor = null;
    }

    private DatasetServiceImpl(BigQueryOptions bqOptions) {
      this.errorExtractor = new ApiErrorExtractor();
      this.client = Transport.newBigQueryClient(bqOptions).build();
      this.options = bqOptions;
      this.maxRowsPerBatch = MAX_ROWS_PER_BATCH;
      this.executor = null;
    }

    /**
     * {@inheritDoc}
     *
     * <p> the RPC for at most {@code MAX_RPC_ATTEMPTS} times until it succeeds.
     *
     * @throws IOException if it exceeds max RPC .
     */
    @Override
    public Table getTable(String projectId, String datasetId, String tableId)
        throws IOException, InterruptedException {
      BackOff backoff =
          new AttemptBoundedExponentialBackOff(MAX_RPC_ATTEMPTS, INITIAL_RPC_BACKOFF_MILLIS);
      return executeWithRetries(
          client.tables().get(projectId, datasetId, tableId),
          String.format(
              "Unable to get table: %s, aborting after %d retries.",
              tableId, MAX_RPC_ATTEMPTS),
          Sleeper.DEFAULT,
          backoff);
    }

    /**
     * {@inheritDoc}
     *
     * <p> the RPC for at most {@code MAX_RPC_ATTEMPTS} times until it succeeds.
     *
     * @throws IOException if it exceeds max RPC .
     */
    @Override
    public void deleteTable(String projectId, String datasetId, String tableId)
        throws IOException, InterruptedException {
      BackOff backoff =
          new AttemptBoundedExponentialBackOff(MAX_RPC_ATTEMPTS, INITIAL_RPC_BACKOFF_MILLIS);
      executeWithRetries(
          client.tables().delete(projectId, datasetId, tableId),
          String.format(
              "Unable to delete table: %s, aborting after %d retries.",
              tableId, MAX_RPC_ATTEMPTS),
          Sleeper.DEFAULT,
          backoff);
    }

    @Override
    public boolean isTableEmpty(String projectId, String datasetId, String tableId)
        throws IOException, InterruptedException {
      BackOff backoff =
          new AttemptBoundedExponentialBackOff(MAX_RPC_ATTEMPTS, INITIAL_RPC_BACKOFF_MILLIS);
      TableDataList dataList = executeWithRetries(
          client.tabledata().list(projectId, datasetId, tableId),
          String.format(
              "Unable to list table data: %s, aborting after %d retries.",
              tableId, MAX_RPC_ATTEMPTS),
          Sleeper.DEFAULT,
          backoff);
      return dataList.getRows() == null || dataList.getRows().isEmpty();
    }

    /**
     * {@inheritDoc}
     *
     * <p> the RPC for at most {@code MAX_RPC_ATTEMPTS} times until it succeeds.
     *
     * @throws IOException if it exceeds max RPC .
     */
    @Override
    public Dataset getDataset(String projectId, String datasetId)
        throws IOException, InterruptedException {
      BackOff backoff =
          new AttemptBoundedExponentialBackOff(MAX_RPC_ATTEMPTS, INITIAL_RPC_BACKOFF_MILLIS);
      return executeWithRetries(
          client.datasets().get(projectId, datasetId),
          String.format(
              "Unable to get dataset: %s, aborting after %d retries.",
              datasetId, MAX_RPC_ATTEMPTS),
          Sleeper.DEFAULT,
          backoff);
    }

    /**
     * {@inheritDoc}
     *
     * <p> the RPC for at most {@code MAX_RPC_ATTEMPTS} times until it succeeds.
     *
     * @throws IOException if it exceeds max RPC .
     */
    @Override
    public void createDataset(
        String projectId, String datasetId, String location, String description)
        throws IOException, InterruptedException {
      BackOff backoff =
          new AttemptBoundedExponentialBackOff(MAX_RPC_ATTEMPTS, INITIAL_RPC_BACKOFF_MILLIS);
      createDataset(projectId, datasetId, location, description, Sleeper.DEFAULT, backoff);
    }

    @VisibleForTesting
    void createDataset(
        String projectId,
        String datasetId,
        String location,
        String description,
        Sleeper sleeper,
        BackOff backoff) throws IOException, InterruptedException {
      DatasetReference datasetRef = new DatasetReference()
          .setProjectId(projectId)
          .setDatasetId(datasetId);

      Dataset dataset = new Dataset()
          .setDatasetReference(datasetRef)
          .setLocation(location)
          .setFriendlyName(location)
          .setDescription(description);

      Exception lastException;
      do {
        try {
          client.datasets().insert(projectId, dataset).execute();
          return; // SUCCEEDED
        } catch (GoogleJsonResponseException e) {
          if (errorExtractor.itemAlreadyExists(e)) {
            return; // SUCCEEDED
          }
          // ignore and retry
          LOG.warn("Ignore the error and retry creating the dataset.", e);
          lastException = e;
        } catch (IOException e) {
          LOG.warn("Ignore the error and retry creating the dataset.", e);
          lastException = e;
        }
      } while (nextBackOff(sleeper, backoff));
      throw new IOException(
          String.format(
              "Unable to create dataset: %s, aborting after %d .",
              datasetId, MAX_RPC_ATTEMPTS),
          lastException);
    }

    /**
     * {@inheritDoc}
     *
     * <p> the RPC for at most {@code MAX_RPC_ATTEMPTS} times until it succeeds.
     *
     * @throws IOException if it exceeds max RPC .
     */
    @Override
    public void deleteDataset(String projectId, String datasetId)
        throws IOException, InterruptedException {
      BackOff backoff =
          new AttemptBoundedExponentialBackOff(MAX_RPC_ATTEMPTS, INITIAL_RPC_BACKOFF_MILLIS);
      executeWithRetries(
          client.datasets().delete(projectId, datasetId),
          String.format(
              "Unable to delete table: %s, aborting after %d retries.",
              datasetId, MAX_RPC_ATTEMPTS),
          Sleeper.DEFAULT,
          backoff);
    }

    @Override
    public long insertAll(
        TableReference ref, List<TableRow> rowList, @Nullable List<String> insertIdList)
        throws IOException, InterruptedException {
      checkNotNull(ref, "ref");
      if (executor == null) {
        this.executor = options.as(GcsOptions.class).getExecutorService();
      }
      if (insertIdList != null && rowList.size() != insertIdList.size()) {
        throw new AssertionError("If insertIdList is not null it needs to have at least "
            + "as many elements as rowList");
      }

      AttemptBoundedExponentialBackOff backoff = new AttemptBoundedExponentialBackOff(
          MAX_INSERT_ATTEMPTS,
          INITIAL_INSERT_BACKOFF_INTERVAL_MS);

      long retTotalDataSize = 0;
      List<TableDataInsertAllResponse.InsertErrors> allErrors = new ArrayList<>();
      // These lists contain the rows to publish. Initially the contain the entire list.
      // If there are failures, they will contain only the failed rows to be retried.
      List<TableRow> rowsToPublish = rowList;
      List<String> idsToPublish = insertIdList;
      while (true) {
        List<TableRow> retryRows = new ArrayList<>();
        List<String> retryIds = (idsToPublish != null) ? new ArrayList<String>() : null;

        int strideIndex = 0;
        // Upload in batches.
        List<TableDataInsertAllRequest.Rows> rows = new LinkedList<>();
        int dataSize = 0;

        List<Future<List<TableDataInsertAllResponse.InsertErrors>>> futures = new ArrayList<>();
        List<Integer> strideIndices = new ArrayList<>();

        for (int i = 0; i < rowsToPublish.size(); ++i) {
          TableRow row = rowsToPublish.get(i);
          TableDataInsertAllRequest.Rows out = new TableDataInsertAllRequest.Rows();
          if (idsToPublish != null) {
            out.setInsertId(idsToPublish.get(i));
          }
          out.setJson(row.getUnknownKeys());
          rows.add(out);

          dataSize += row.toString().length();
          if (dataSize >= UPLOAD_BATCH_SIZE_BYTES || rows.size() >= maxRowsPerBatch
              || i == rowsToPublish.size() - 1) {
            TableDataInsertAllRequest content = new TableDataInsertAllRequest();
            content.setRows(rows);

            final Bigquery.Tabledata.InsertAll insert = client.tabledata()
                .insertAll(ref.getProjectId(), ref.getDatasetId(), ref.getTableId(),
                    content);

            futures.add(
                executor.submit(new Callable<List<TableDataInsertAllResponse.InsertErrors>>() {
                  @Override
                  public List<TableDataInsertAllResponse.InsertErrors> call() throws IOException {
                    BackOff backoff = new IntervalBoundedExponentialBackOff(
                        MAX_RATE_LIMIT_EXCEEDED_BACKOFF_MS, INITIAL_RATE_LIMIT_EXCEEDED_BACKOFF_MS);
                    while (true) {
                      try {
                        return insert.execute().getInsertErrors();
                      } catch (IOException e) {
                        if (new ApiErrorExtractor().rateLimited(e)) {
                          LOG.info("BigQuery insertAll exceeded rate limit, retrying");
                          try {
                            Thread.sleep(backoff.nextBackOffMillis());
                          } catch (InterruptedException interrupted) {
                            throw new IOException(
                                "Interrupted while waiting before retrying insertAll");
                          }
                        } else {
                          throw e;
                        }
                      }
                    }
                  }
                }));
            strideIndices.add(strideIndex);

            retTotalDataSize += dataSize;

            dataSize = 0;
            strideIndex = i + 1;
            rows = new LinkedList<>();
          }
        }

        try {
          for (int i = 0; i < futures.size(); i++) {
            List<TableDataInsertAllResponse.InsertErrors> errors = futures.get(i).get();
            if (errors != null) {
              for (TableDataInsertAllResponse.InsertErrors error : errors) {
                allErrors.add(error);
                if (error.getIndex() == null) {
                  throw new IOException("Insert failed: " + allErrors);
                }

                int errorIndex = error.getIndex().intValue() + strideIndices.get(i);
                retryRows.add(rowsToPublish.get(errorIndex));
                if (retryIds != null) {
                  retryIds.add(idsToPublish.get(errorIndex));
                }
              }
            }
          }
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          throw new IOException("Interrupted while inserting " + rowsToPublish);
        } catch (ExecutionException e) {
          throw new RuntimeException(e.getCause());
        }

        if (!allErrors.isEmpty() && !backoff.atMaxAttempts()) {
          try {
            Thread.sleep(backoff.nextBackOffMillis());
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException(
                "Interrupted while waiting before retrying insert of " + retryRows);
          }
          LOG.info("Retrying failed inserts to BigQuery");
          rowsToPublish = retryRows;
          idsToPublish = retryIds;
          allErrors.clear();
        } else {
          break;
        }
      }
      if (!allErrors.isEmpty()) {
        throw new IOException("Insert failed: " + allErrors);
      } else {
        return retTotalDataSize;
      }
    }
  }

  private static class BigQueryJsonReaderImpl implements BigQueryJsonReader {
    BigQueryTableRowIterator iterator;

    private BigQueryJsonReaderImpl(BigQueryTableRowIterator iterator) {
      this.iterator = iterator;
    }

    private static BigQueryJsonReader fromQuery(
        BigQueryOptions bqOptions,
        String query,
        String projectId,
        @Nullable Boolean flattenResults) {
      return new BigQueryJsonReaderImpl(
          BigQueryTableRowIterator.fromQuery(
              query, projectId, Transport.newBigQueryClient(bqOptions).build(), flattenResults));
    }

    private static BigQueryJsonReader fromTable(
        BigQueryOptions bqOptions,
        TableReference tableRef) {
      return new BigQueryJsonReaderImpl(BigQueryTableRowIterator.fromTable(
          tableRef, Transport.newBigQueryClient(bqOptions).build()));
    }

    @Override
    public boolean start() throws IOException {
      try {
        iterator.open();
        return iterator.advance();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new RuntimeException("Interrupted during start() operation", e);
      }
    }

    @Override
    public boolean advance() throws IOException {
      try {
        return iterator.advance();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new RuntimeException("Interrupted during advance() operation", e);
      }
    }

    @Override
    public TableRow getCurrent() throws NoSuchElementException {
      return iterator.getCurrent();
    }

    @Override
    public void close() throws IOException {
      iterator.close();
    }
  }

  @VisibleForTesting
  static <T> T executeWithRetries(
      AbstractGoogleClientRequest<T> request,
      String errorMessage,
      Sleeper sleeper,
      BackOff backoff)
      throws IOException, InterruptedException {
    Exception lastException = null;
    do {
      try {
        return request.execute();
      } catch (IOException e) {
        LOG.warn("Ignore the error and retry the request.", e);
        lastException = e;
      }
    } while (nextBackOff(sleeper, backoff));
    throw new IOException(
        errorMessage,
        lastException);
  }

  /**
   * Identical to {@link BackOffUtils#next} but without checked IOException.
   * @throws InterruptedException
   */
  private static boolean nextBackOff(Sleeper sleeper, BackOff backoff) throws InterruptedException {
    try {
      return BackOffUtils.next(sleeper, backoff);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
