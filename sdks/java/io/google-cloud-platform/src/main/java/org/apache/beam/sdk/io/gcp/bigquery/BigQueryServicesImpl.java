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

import com.google.api.client.googleapis.json.GoogleJsonResponseException;
import com.google.api.client.googleapis.services.AbstractGoogleClientRequest;
import com.google.api.client.util.BackOff;
import com.google.api.client.util.BackOffUtils;
import com.google.api.client.util.ExponentialBackOff;
import com.google.api.client.util.Sleeper;
import com.google.api.services.bigquery.Bigquery;
import com.google.api.services.bigquery.model.Dataset;
import com.google.api.services.bigquery.model.DatasetReference;
import com.google.api.services.bigquery.model.Job;
import com.google.api.services.bigquery.model.JobConfiguration;
import com.google.api.services.bigquery.model.JobConfigurationExtract;
import com.google.api.services.bigquery.model.JobConfigurationLoad;
import com.google.api.services.bigquery.model.JobConfigurationQuery;
import com.google.api.services.bigquery.model.JobConfigurationTableCopy;
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
import org.apache.beam.sdk.options.BigQueryOptions;
import org.apache.beam.sdk.options.GcsOptions;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.util.FluentBackoff;
import org.apache.beam.sdk.util.Transport;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An implementation of {@link BigQueryServices} that actually communicates with the cloud BigQuery
 * service.
 */
class BigQueryServicesImpl implements BigQueryServices {

  private static final Logger LOG = LoggerFactory.getLogger(BigQueryServicesImpl.class);

  // The maximum number of retries to execute a BigQuery RPC.
  private static final int MAX_RPC_RETRIES = 9;

  // The initial backoff for executing a BigQuery RPC.
  private static final Duration INITIAL_RPC_BACKOFF = Duration.standardSeconds(1);

  // The initial backoff for polling the status of a BigQuery job.
  private static final Duration INITIAL_JOB_STATUS_POLL_BACKOFF = Duration.standardSeconds(1);

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
      BigQueryOptions bqOptions, String projectId, JobConfigurationQuery queryConfig) {
    return BigQueryJsonReaderImpl.fromQuery(bqOptions, projectId, queryConfig);
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
     * <p>Tries executing the RPC for at most {@code MAX_RPC_RETRIES} times until it succeeds.
     *
     * @throws IOException if it exceeds {@code MAX_RPC_RETRIES} attempts.
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
     * <p>Tries executing the RPC for at most {@code MAX_RPC_RETRIES} times until it succeeds.
     *
     * @throws IOException if it exceeds {@code MAX_RPC_RETRIES} attempts.
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
     * <p>Tries executing the RPC for at most {@code MAX_RPC_RETRIES} times until it succeeds.
     *
     * @throws IOException if it exceeds {@code MAX_RPC_RETRIES} attempts.
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

    /**
     * {@inheritDoc}
     *
     * <p>Tries executing the RPC for at most {@code MAX_RPC_RETRIES} times until it succeeds.
     *
     * @throws IOException if it exceeds {@code MAX_RPC_RETRIES} attempts.
     */
    @Override
    public void startCopyJob(JobReference jobRef, JobConfigurationTableCopy copyConfig)
        throws IOException, InterruptedException {
      Job job = new Job()
          .setJobReference(jobRef)
          .setConfiguration(
              new JobConfiguration().setCopy(copyConfig));

      startJob(job, errorExtractor, client);
    }

    private static void startJob(Job job,
      ApiErrorExtractor errorExtractor,
      Bigquery client) throws IOException, InterruptedException {
      BackOff backoff =
          FluentBackoff.DEFAULT
              .withMaxRetries(MAX_RPC_RETRIES).withInitialBackoff(INITIAL_RPC_BACKOFF).backoff();
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
          LOG.info("Started BigQuery job: {}.", jobRef);
          return; // SUCCEEDED
        } catch (GoogleJsonResponseException e) {
          if (errorExtractor.itemAlreadyExists(e)) {
            return; // SUCCEEDED
          }
          // ignore and retry
          LOG.info("Ignore the error and retry inserting the job.", e);
          lastException = e;
        } catch (IOException e) {
          // ignore and retry
          LOG.info("Ignore the error and retry inserting the job.", e);
          lastException = e;
        }
      } while (nextBackOff(sleeper, backoff));
      throw new IOException(
          String.format(
              "Unable to insert job: %s, aborting after %d .",
              jobRef.getJobId(), MAX_RPC_RETRIES),
          lastException);
    }

    @Override
    public Job pollJob(JobReference jobRef, int maxAttempts) throws InterruptedException {
      BackOff backoff =
          FluentBackoff.DEFAULT
              .withMaxRetries(maxAttempts)
              .withInitialBackoff(INITIAL_JOB_STATUS_POLL_BACKOFF)
              .withMaxBackoff(Duration.standardMinutes(1))
              .backoff();
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
          LOG.info("Ignore the error and retry polling job status.", e);
        }
      } while (nextBackOff(sleeper, backoff));
      LOG.warn("Unable to poll job status: {}, aborting after reached max .", jobRef.getJobId());
      return null;
    }

    @Override
    public JobStatistics dryRunQuery(String projectId, JobConfigurationQuery queryConfig)
        throws InterruptedException, IOException {
      Job job = new Job()
          .setConfiguration(new JobConfiguration()
              .setQuery(queryConfig)
              .setDryRun(true));
      BackOff backoff =
          FluentBackoff.DEFAULT
              .withMaxRetries(MAX_RPC_RETRIES).withInitialBackoff(INITIAL_RPC_BACKOFF).backoff();
      return executeWithRetries(
          client.jobs().insert(projectId, job),
          String.format(
              "Unable to dry run query: %s, aborting after %d retries.",
              queryConfig, MAX_RPC_RETRIES),
          Sleeper.DEFAULT,
          backoff,
          ALWAYS_RETRY).getStatistics();
    }

    /**
     * {@inheritDoc}
     *
     * <p>Retries the RPC for at most {@code MAX_RPC_ATTEMPTS} times until it succeeds.
     *
     * @throws IOException if it exceeds max RPC retries.
     */
    @Override
    public Job getJob(JobReference jobRef) throws IOException, InterruptedException {
      BackOff backoff =
          FluentBackoff.DEFAULT
              .withMaxRetries(MAX_RPC_RETRIES).withInitialBackoff(INITIAL_RPC_BACKOFF).backoff();
     return getJob(jobRef, Sleeper.DEFAULT, backoff);
    }

    @VisibleForTesting
    public Job getJob(JobReference jobRef, Sleeper sleeper, BackOff backoff)
        throws IOException, InterruptedException {
      String jobId = jobRef.getJobId();
      Exception lastException;
      do {
        try {
          return client.jobs().get(jobRef.getProjectId(), jobId).execute();
        } catch (GoogleJsonResponseException e) {
          if (errorExtractor.itemNotFound(e)) {
            LOG.info("No BigQuery job with job id {} found.", jobId);
            return null;
          }
          LOG.info(
              "Ignoring the error encountered while trying to query the BigQuery job {}",
              jobId, e);
          lastException = e;
        } catch (IOException e) {
          LOG.info(
              "Ignoring the error encountered while trying to query the BigQuery job {}",
              jobId, e);
          lastException = e;
        }
      } while (nextBackOff(sleeper, backoff));
      throw new IOException(
          String.format(
              "Unable to find BigQuery job: %s, aborting after %d retries.",
              jobRef, MAX_RPC_RETRIES),
          lastException);
    }
  }

  @VisibleForTesting
  static class DatasetServiceImpl implements DatasetService {
    // Approximate amount of table data to upload per InsertAll request.
    private static final long UPLOAD_BATCH_SIZE_BYTES = 64 * 1024;

    // The maximum number of rows to upload per InsertAll request.
    private static final long MAX_ROWS_PER_BATCH = 500;

    private static final FluentBackoff INSERT_BACKOFF_FACTORY =
        FluentBackoff.DEFAULT.withInitialBackoff(Duration.millis(200)).withMaxRetries(5);

    // A backoff for rate limit exceeded errors. Retries forever.
    private static final FluentBackoff DEFAULT_BACKOFF_FACTORY =
        FluentBackoff.DEFAULT
            .withInitialBackoff(Duration.standardSeconds(1))
            .withMaxBackoff(Duration.standardMinutes(2));

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
     * <p>Tries executing the RPC for at most {@code MAX_RPC_RETRIES} times until it succeeds.
     *
     * @throws IOException if it exceeds {@code MAX_RPC_RETRIES} attempts.
     */
    @Override
    @Nullable
    public Table getTable(TableReference tableRef)
        throws IOException, InterruptedException {
      BackOff backoff =
          FluentBackoff.DEFAULT
              .withMaxRetries(MAX_RPC_RETRIES).withInitialBackoff(INITIAL_RPC_BACKOFF).backoff();
      return getTable(tableRef, backoff, Sleeper.DEFAULT);
    }

    @VisibleForTesting
    @Nullable
    Table getTable(TableReference ref, BackOff backoff, Sleeper sleeper)
        throws IOException, InterruptedException {
      try {
        return executeWithRetries(
            client.tables().get(ref.getProjectId(), ref.getDatasetId(), ref.getTableId()),
            String.format(
                "Unable to get table: %s, aborting after %d retries.",
                ref.getTableId(), MAX_RPC_RETRIES),
            sleeper,
            backoff,
            DONT_RETRY_NOT_FOUND);
      } catch (IOException e) {
        if (errorExtractor.itemNotFound(e)) {
          return null;
        }
        throw e;
      }
    }

    /**
     * Retry table creation up to 5 minutes (with exponential backoff) when this user is near the
     * quota for table creation. This relatively innocuous behavior can happen when BigQueryIO is
     * configured with a table spec function to use different tables for each window.
     */
    private static final int RETRY_CREATE_TABLE_DURATION_MILLIS =
        (int) TimeUnit.MINUTES.toMillis(5);

    /**
     * {@inheritDoc}
     *
     * <p>If a table with the same name already exists in the dataset, the function simply
     * returns. In such a case,
     * the existing table doesn't necessarily have the same schema as specified
     * by the parameter.
     *
     * @throws IOException if other error than already existing table occurs.
     */
    @Override
    public void createTable(Table table) throws InterruptedException, IOException {
      LOG.info("Trying to create BigQuery table: {}",
          BigQueryHelpers.toTableSpec(table.getTableReference()));
      BackOff backoff =
              new ExponentialBackOff.Builder()
                      .setMaxElapsedTimeMillis(RETRY_CREATE_TABLE_DURATION_MILLIS)
                      .build();

      tryCreateTable(table, backoff, Sleeper.DEFAULT);
    }

    @VisibleForTesting
    @Nullable
    Table tryCreateTable(Table table, BackOff backoff, Sleeper sleeper)
            throws IOException {
      boolean retry = false;
      while (true) {
        try {
          return client.tables().insert(
              table.getTableReference().getProjectId(),
              table.getTableReference().getDatasetId(),
              table).execute();
        } catch (IOException e) {
          ApiErrorExtractor extractor = new ApiErrorExtractor();
          if (extractor.itemAlreadyExists(e)) {
            // The table already exists, nothing to return.
            return null;
          } else if (extractor.rateLimited(e)) {
            // The request failed because we hit a temporary quota. Back off and try again.
            try {
              if (BackOffUtils.next(sleeper, backoff)) {
                if (!retry) {
                  LOG.info(
                      "Quota limit reached when creating table {}:{}.{}, retrying up to {} minutes",
                      table.getTableReference().getProjectId(),
                      table.getTableReference().getDatasetId(),
                      table.getTableReference().getTableId(),
                      TimeUnit.MILLISECONDS.toSeconds(RETRY_CREATE_TABLE_DURATION_MILLIS) / 60.0);
                  retry = true;
                }
                continue;
              }
            } catch (InterruptedException e1) {
              // Restore interrupted state and throw the last failure.
              Thread.currentThread().interrupt();
              throw e;
            }
          }
          throw e;
        }
      }
    }

    /**
     * {@inheritDoc}
     *
     * <p>Tries executing the RPC for at most {@code MAX_RPC_RETRIES} times until it succeeds.
     *
     * @throws IOException if it exceeds {@code MAX_RPC_RETRIES} attempts.
     */
    @Override
    public void deleteTable(TableReference tableRef) throws IOException, InterruptedException {
      BackOff backoff =
          FluentBackoff.DEFAULT
              .withMaxRetries(MAX_RPC_RETRIES).withInitialBackoff(INITIAL_RPC_BACKOFF).backoff();
      executeWithRetries(
          client.tables().delete(
              tableRef.getProjectId(), tableRef.getDatasetId(), tableRef.getTableId()),
          String.format(
              "Unable to delete table: %s, aborting after %d retries.",
              tableRef.getTableId(), MAX_RPC_RETRIES),
          Sleeper.DEFAULT,
          backoff,
          ALWAYS_RETRY);
    }

    @Override
    public boolean isTableEmpty(TableReference tableRef) throws IOException, InterruptedException {
      BackOff backoff =
          FluentBackoff.DEFAULT
              .withMaxRetries(MAX_RPC_RETRIES).withInitialBackoff(INITIAL_RPC_BACKOFF).backoff();
      return isTableEmpty(tableRef, backoff, Sleeper.DEFAULT);
    }

    @VisibleForTesting
    boolean isTableEmpty(TableReference tableRef, BackOff backoff, Sleeper sleeper)
        throws IOException, InterruptedException {
      TableDataList dataList = executeWithRetries(
          client.tabledata().list(
              tableRef.getProjectId(), tableRef.getDatasetId(), tableRef.getTableId()),
          String.format(
              "Unable to list table data: %s, aborting after %d retries.",
              tableRef.getTableId(), MAX_RPC_RETRIES),
          sleeper,
          backoff,
          DONT_RETRY_NOT_FOUND);
      return dataList.getRows() == null || dataList.getRows().isEmpty();
    }

    /**
     * {@inheritDoc}
     *
     * <p>Tries executing the RPC for at most {@code MAX_RPC_RETRIES} times until it succeeds.
     *
     * @throws IOException if it exceeds {@code MAX_RPC_RETRIES} attempts.
     */
    @Override
    public Dataset getDataset(String projectId, String datasetId)
        throws IOException, InterruptedException {
      BackOff backoff =
          FluentBackoff.DEFAULT
              .withMaxRetries(MAX_RPC_RETRIES).withInitialBackoff(INITIAL_RPC_BACKOFF).backoff();
      return executeWithRetries(
          client.datasets().get(projectId, datasetId),
          String.format(
              "Unable to get dataset: %s, aborting after %d retries.",
              datasetId, MAX_RPC_RETRIES),
          Sleeper.DEFAULT,
          backoff,
          DONT_RETRY_NOT_FOUND);
    }

    /**
     * {@inheritDoc}
     *
     * <p>Tries executing the RPC for at most {@code MAX_RPC_RETRIES} times until it succeeds.
     *
     * @throws IOException if it exceeds {@code MAX_RPC_RETRIES} attempts.
     */
    @Override
    public void createDataset(
        String projectId, String datasetId, @Nullable String location, @Nullable String description)
        throws IOException, InterruptedException {
      BackOff backoff =
          FluentBackoff.DEFAULT
              .withMaxRetries(MAX_RPC_RETRIES).withInitialBackoff(INITIAL_RPC_BACKOFF).backoff();
      createDataset(projectId, datasetId, location, description, Sleeper.DEFAULT, backoff);
    }

    private void createDataset(
        String projectId,
        String datasetId,
        @Nullable String location,
        @Nullable String description,
        Sleeper sleeper,
        BackOff backoff) throws IOException, InterruptedException {
      DatasetReference datasetRef = new DatasetReference()
          .setProjectId(projectId)
          .setDatasetId(datasetId);

      Dataset dataset = new Dataset().setDatasetReference(datasetRef);
      if (location != null) {
        dataset.setLocation(location);
      }
      if (description != null) {
        dataset.setFriendlyName(description);
        dataset.setDescription(description);
      }

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
          LOG.info("Ignore the error and retry creating the dataset.", e);
          lastException = e;
        } catch (IOException e) {
          LOG.info("Ignore the error and retry creating the dataset.", e);
          lastException = e;
        }
      } while (nextBackOff(sleeper, backoff));
      throw new IOException(
          String.format(
              "Unable to create dataset: %s, aborting after %d .",
              datasetId, MAX_RPC_RETRIES),
          lastException);
    }

    /**
     * {@inheritDoc}
     *
     * <p>Tries executing the RPC for at most {@code MAX_RPC_RETRIES} times until it succeeds.
     *
     * @throws IOException if it exceeds {@code MAX_RPC_RETRIES} attempts.
     */
    @Override
    public void deleteDataset(String projectId, String datasetId)
        throws IOException, InterruptedException {
      BackOff backoff =
          FluentBackoff.DEFAULT
              .withMaxRetries(MAX_RPC_RETRIES).withInitialBackoff(INITIAL_RPC_BACKOFF).backoff();
      executeWithRetries(
          client.datasets().delete(projectId, datasetId),
          String.format(
              "Unable to delete table: %s, aborting after %d retries.",
              datasetId, MAX_RPC_RETRIES),
          Sleeper.DEFAULT,
          backoff,
          ALWAYS_RETRY);
    }

    @VisibleForTesting
    long insertAll(TableReference ref, List<TableRow> rowList, @Nullable List<String> insertIdList,
        BackOff backoff, final Sleeper sleeper) throws IOException, InterruptedException {
      checkNotNull(ref, "ref");
      if (executor == null) {
        this.executor = options.as(GcsOptions.class).getExecutorService();
      }
      if (insertIdList != null && rowList.size() != insertIdList.size()) {
        throw new AssertionError("If insertIdList is not null it needs to have at least "
            + "as many elements as rowList");
      }

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
                    BackOff backoff = DEFAULT_BACKOFF_FACTORY.backoff();
                    while (true) {
                      try {
                        return insert.execute().getInsertErrors();
                      } catch (IOException e) {
                        if (new ApiErrorExtractor().rateLimited(e)) {
                          LOG.info("BigQuery insertAll exceeded rate limit, retrying");
                          try {
                            sleeper.sleep(backoff.nextBackOffMillis());
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

        if (allErrors.isEmpty()) {
          break;
        }
        long nextBackoffMillis = backoff.nextBackOffMillis();
        if (nextBackoffMillis == BackOff.STOP) {
          break;
        }
        try {
          sleeper.sleep(nextBackoffMillis);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          throw new IOException(
              "Interrupted while waiting before retrying insert of " + retryRows);
        }
        rowsToPublish = retryRows;
        idsToPublish = retryIds;
        allErrors.clear();
        LOG.info("Retrying {} failed inserts to BigQuery", rowsToPublish.size());
      }
      if (!allErrors.isEmpty()) {
        throw new IOException("Insert failed: " + allErrors);
      } else {
        return retTotalDataSize;
      }
    }

    @Override
    public long insertAll(
        TableReference ref, List<TableRow> rowList, @Nullable List<String> insertIdList)
        throws IOException, InterruptedException {
      return insertAll(
          ref, rowList, insertIdList, INSERT_BACKOFF_FACTORY.backoff(), Sleeper.DEFAULT);
    }


    @Override
    public Table patchTableDescription(TableReference tableReference,
                                       @Nullable String tableDescription)
        throws IOException, InterruptedException {
      Table table = new Table();
      table.setDescription(tableDescription);

      BackOff backoff =
          FluentBackoff.DEFAULT
              .withMaxRetries(MAX_RPC_RETRIES).withInitialBackoff(INITIAL_RPC_BACKOFF).backoff();
      return executeWithRetries(
          client.tables().patch(
              tableReference.getProjectId(),
              tableReference.getDatasetId(),
              tableReference.getTableId(),
              table),
          String.format(
              "Unable to patch table description: %s, aborting after %d retries.",
              tableReference, MAX_RPC_RETRIES),
          Sleeper.DEFAULT,
          backoff,
          ALWAYS_RETRY);
    }
  }

  private static class BigQueryJsonReaderImpl implements BigQueryJsonReader {
    private BigQueryTableRowIterator iterator;

    private BigQueryJsonReaderImpl(BigQueryTableRowIterator iterator) {
      this.iterator = iterator;
    }

    private static BigQueryJsonReader fromQuery(
        BigQueryOptions bqOptions, String projectId, JobConfigurationQuery queryConfig) {
      return new BigQueryJsonReaderImpl(
          BigQueryTableRowIterator.fromQuery(
              queryConfig, projectId, Transport.newBigQueryClient(bqOptions).build()));
    }

    private static BigQueryJsonReader fromTable(
        BigQueryOptions bqOptions, TableReference tableRef) {
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

  static final SerializableFunction<IOException, Boolean> DONT_RETRY_NOT_FOUND =
      new SerializableFunction<IOException, Boolean>() {
        @Override
        public Boolean apply(IOException input) {
          ApiErrorExtractor errorExtractor = new ApiErrorExtractor();
          return !errorExtractor.itemNotFound(input);
        }
      };

  static final SerializableFunction<IOException, Boolean> ALWAYS_RETRY =
      new SerializableFunction<IOException, Boolean>() {
        @Override
        public Boolean apply(IOException input) {
          return true;
        }
      };


  @VisibleForTesting
  static <T> T executeWithRetries(
      AbstractGoogleClientRequest<T> request,
      String errorMessage,
      Sleeper sleeper,
      BackOff backoff,
      SerializableFunction<IOException, Boolean> shouldRetry)
      throws IOException, InterruptedException {
    Exception lastException = null;
    do {
      try {
        return request.execute();
      } catch (IOException e) {
        lastException = e;
        if (!shouldRetry.apply(e)) {
          break;
        }
        LOG.info("Ignore the error and retry the request.", e);
      }
    } while (nextBackOff(sleeper, backoff));
    throw new IOException(
        errorMessage,
        lastException);
  }

  /**
   * Identical to {@link BackOffUtils#next} but without checked IOException.
   */
  private static boolean nextBackOff(Sleeper sleeper, BackOff backoff) throws InterruptedException {
    try {
      return BackOffUtils.next(sleeper, backoff);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
