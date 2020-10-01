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

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkNotNull;

import com.google.api.client.googleapis.json.GoogleJsonError;
import com.google.api.client.googleapis.json.GoogleJsonResponseException;
import com.google.api.client.googleapis.services.AbstractGoogleClientRequest;
import com.google.api.client.http.HttpRequestInitializer;
import com.google.api.client.util.BackOff;
import com.google.api.client.util.BackOffUtils;
import com.google.api.client.util.ExponentialBackOff;
import com.google.api.client.util.Sleeper;
import com.google.api.gax.core.FixedCredentialsProvider;
import com.google.api.gax.rpc.FixedHeaderProvider;
import com.google.api.gax.rpc.HeaderProvider;
import com.google.api.gax.rpc.ServerStream;
import com.google.api.services.bigquery.Bigquery;
import com.google.api.services.bigquery.Bigquery.Tables;
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
import com.google.auth.Credentials;
import com.google.auth.http.HttpCredentialsAdapter;
import com.google.cloud.bigquery.storage.v1.BigQueryReadClient;
import com.google.cloud.bigquery.storage.v1.BigQueryReadSettings;
import com.google.cloud.bigquery.storage.v1.CreateReadSessionRequest;
import com.google.cloud.bigquery.storage.v1.ReadRowsRequest;
import com.google.cloud.bigquery.storage.v1.ReadRowsResponse;
import com.google.cloud.bigquery.storage.v1.ReadSession;
import com.google.cloud.bigquery.storage.v1.SplitReadStreamRequest;
import com.google.cloud.bigquery.storage.v1.SplitReadStreamResponse;
import com.google.cloud.hadoop.util.ApiErrorExtractor;
import com.google.cloud.hadoop.util.ChainingHttpRequestInitializer;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import org.apache.beam.sdk.extensions.gcp.auth.NullCredentialInitializer;
import org.apache.beam.sdk.extensions.gcp.options.GcsOptions;
import org.apache.beam.sdk.extensions.gcp.util.BackOffAdapter;
import org.apache.beam.sdk.extensions.gcp.util.CustomHttpErrors;
import org.apache.beam.sdk.extensions.gcp.util.LatencyRecordingHttpRequestInitializer;
import org.apache.beam.sdk.extensions.gcp.util.RetryHttpRequestInitializer;
import org.apache.beam.sdk.extensions.gcp.util.Transport;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.util.FluentBackoff;
import org.apache.beam.sdk.util.Histogram;
import org.apache.beam.sdk.util.ReleaseInfo;
import org.apache.beam.sdk.values.ValueInSingleWindow;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Iterables;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An implementation of {@link BigQueryServices} that actually communicates with the cloud BigQuery
 * service.
 */
class BigQueryServicesImpl implements BigQueryServices {

  private static final Logger LOG = LoggerFactory.getLogger(BigQueryServicesImpl.class);

  // How frequently to log while polling.
  private static final Duration POLLING_LOG_GAP = Duration.standardMinutes(10);

  // The maximum number of retries to execute a BigQuery RPC.
  private static final int MAX_RPC_RETRIES = 9;

  // The initial backoff for executing a BigQuery RPC.
  private static final Duration INITIAL_RPC_BACKOFF = Duration.standardSeconds(1);

  // The initial backoff for polling the status of a BigQuery job.
  private static final Duration INITIAL_JOB_STATUS_POLL_BACKOFF = Duration.standardSeconds(1);

  private static final FluentBackoff DEFAULT_BACKOFF_FACTORY =
      FluentBackoff.DEFAULT.withMaxRetries(MAX_RPC_RETRIES).withInitialBackoff(INITIAL_RPC_BACKOFF);

  // The error code for quota exceeded error (https://cloud.google.com/bigquery/docs/error-messages)
  private static final String QUOTA_EXCEEDED = "quotaExceeded";

  @Override
  public JobService getJobService(BigQueryOptions options) {
    return new JobServiceImpl(options);
  }

  @Override
  public DatasetService getDatasetService(BigQueryOptions options) {
    return new DatasetServiceImpl(options, null);
  }

  @Override
  public DatasetService getDatasetService(BigQueryOptions options, Histogram requestLatencies) {
    return new DatasetServiceImpl(options, requestLatencies);
  }

  @Override
  public StorageClient getStorageClient(BigQueryOptions options) throws IOException {
    return new StorageClientImpl(options);
  }

  private static BackOff createDefaultBackoff() {
    return BackOffAdapter.toGcpBackOff(DEFAULT_BACKOFF_FACTORY.backoff());
  }

  @VisibleForTesting
  static class JobServiceImpl implements BigQueryServices.JobService {
    private final ApiErrorExtractor errorExtractor;
    private final Bigquery client;
    private final BigQueryIOMetadata bqIOMetadata;

    @VisibleForTesting
    JobServiceImpl(Bigquery client) {
      this.errorExtractor = new ApiErrorExtractor();
      this.client = client;
      this.bqIOMetadata = BigQueryIOMetadata.create();
    }

    private JobServiceImpl(BigQueryOptions options) {
      this.errorExtractor = new ApiErrorExtractor();
      this.client = newBigQueryClient(options, null).build();
      this.bqIOMetadata = BigQueryIOMetadata.create();
    }

    /**
     * {@inheritDoc}
     *
     * <p>Tries executing the RPC for at most {@code MAX_RPC_RETRIES} times until it succeeds.
     *
     * @throws IOException if it exceeds {@code MAX_RPC_RETRIES} attempts.
     */
    @Override
    public void startLoadJob(JobReference jobRef, JobConfigurationLoad loadConfig)
        throws InterruptedException, IOException {
      Map<String, String> labelMap = new HashMap<>();
      Job job =
          new Job()
              .setJobReference(jobRef)
              .setConfiguration(
                  new JobConfiguration()
                      .setLoad(loadConfig)
                      .setLabels(this.bqIOMetadata.addAdditionalJobLabels(labelMap)));
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
      Map<String, String> labelMap = new HashMap<>();
      Job job =
          new Job()
              .setJobReference(jobRef)
              .setConfiguration(
                  new JobConfiguration()
                      .setExtract(extractConfig)
                      .setLabels(this.bqIOMetadata.addAdditionalJobLabels(labelMap)));
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
      Map<String, String> labelMap = new HashMap<>();
      Job job =
          new Job()
              .setJobReference(jobRef)
              .setConfiguration(
                  new JobConfiguration()
                      .setQuery(queryConfig)
                      .setLabels(this.bqIOMetadata.addAdditionalJobLabels(labelMap)));
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
      Map<String, String> labelMap = new HashMap<>();
      Job job =
          new Job()
              .setJobReference(jobRef)
              .setConfiguration(
                  new JobConfiguration()
                      .setCopy(copyConfig)
                      .setLabels(this.bqIOMetadata.addAdditionalJobLabels(labelMap)));
      startJob(job, errorExtractor, client);
    }

    private static void startJob(Job job, ApiErrorExtractor errorExtractor, Bigquery client)
        throws IOException, InterruptedException {
      startJob(job, errorExtractor, client, Sleeper.DEFAULT, createDefaultBackoff());
    }

    @VisibleForTesting
    static void startJob(
        Job job,
        ApiErrorExtractor errorExtractor,
        Bigquery client,
        Sleeper sleeper,
        BackOff backoff)
        throws IOException, InterruptedException {
      JobReference jobRef = job.getJobReference();
      Exception lastException;
      do {
        try {
          client.jobs().insert(jobRef.getProjectId(), job).setPrettyPrint(false).execute();
          LOG.info(
              "Started BigQuery job: {}.\n{}",
              jobRef,
              formatBqStatusCommand(jobRef.getProjectId(), jobRef.getJobId()));
          return; // SUCCEEDED
        } catch (IOException e) {
          if (errorExtractor.itemAlreadyExists(e)) {
            LOG.info("BigQuery job " + jobRef + " already exists, will not retry inserting it:", e);
            return; // SUCCEEDED
          }
          // ignore and retry
          LOG.info("Failed to insert job " + jobRef + ", will retry:", e);
          lastException = e;
        }
      } while (nextBackOff(sleeper, backoff));
      throw new IOException(
          String.format(
              "Unable to insert job: %s, aborting after %d .", jobRef.getJobId(), MAX_RPC_RETRIES),
          lastException);
    }

    @Override
    public Job pollJob(JobReference jobRef, int maxAttempts) throws InterruptedException {
      BackOff backoff =
          BackOffAdapter.toGcpBackOff(
              FluentBackoff.DEFAULT
                  .withMaxRetries(maxAttempts)
                  .withInitialBackoff(INITIAL_JOB_STATUS_POLL_BACKOFF)
                  .withMaxBackoff(Duration.standardMinutes(1))
                  .backoff());
      return pollJob(jobRef, Sleeper.DEFAULT, backoff);
    }

    @VisibleForTesting
    Job pollJob(JobReference jobRef, Sleeper sleeper, BackOff backoff) throws InterruptedException {
      do {
        try {
          Job job =
              client
                  .jobs()
                  .get(jobRef.getProjectId(), jobRef.getJobId())
                  .setLocation(jobRef.getLocation())
                  .setPrettyPrint(false)
                  .execute();
          if (job == null) {
            LOG.info("Still waiting for BigQuery job {} to start", jobRef);
            continue;
          }
          JobStatus status = job.getStatus();
          if (status == null) {
            LOG.info("Still waiting for BigQuery job {} to enter pending state", jobRef);
            continue;
          }
          if ("DONE".equals(status.getState())) {
            LOG.info("BigQuery job {} completed in state DONE", jobRef);
            return job;
          }
          // The job is not DONE, wait longer and retry.
          LOG.info(
              "Still waiting for BigQuery job {}, currently in status {}\n{}",
              jobRef.getJobId(),
              status,
              formatBqStatusCommand(jobRef.getProjectId(), jobRef.getJobId()));
        } catch (IOException e) {
          // ignore and retry
          LOG.info("Ignore the error and retry polling job status.", e);
        }
      } while (nextBackOff(sleeper, backoff));
      LOG.warn("Unable to poll job status: {}, aborting after reached max .", jobRef.getJobId());
      return null;
    }

    private static String formatBqStatusCommand(String projectId, String jobId) {
      return String.format("bq show -j --format=prettyjson --project_id=%s %s", projectId, jobId);
    }

    @Override
    public JobStatistics dryRunQuery(
        String projectId, JobConfigurationQuery queryConfig, String location)
        throws InterruptedException, IOException {
      JobReference jobRef = new JobReference().setLocation(location).setProjectId(projectId);
      Job job =
          new Job()
              .setJobReference(jobRef)
              .setConfiguration(new JobConfiguration().setQuery(queryConfig).setDryRun(true));
      return executeWithRetries(
              client.jobs().insert(projectId, job).setPrettyPrint(false),
              String.format(
                  "Unable to dry run query: %s, aborting after %d retries.",
                  queryConfig, MAX_RPC_RETRIES),
              Sleeper.DEFAULT,
              createDefaultBackoff(),
              ALWAYS_RETRY)
          .getStatistics();
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
      return getJob(jobRef, Sleeper.DEFAULT, createDefaultBackoff());
    }

    @VisibleForTesting
    public Job getJob(JobReference jobRef, Sleeper sleeper, BackOff backoff)
        throws IOException, InterruptedException {
      String jobId = jobRef.getJobId();
      Exception lastException;
      do {
        try {
          return client
              .jobs()
              .get(jobRef.getProjectId(), jobId)
              .setLocation(jobRef.getLocation())
              .setPrettyPrint(false)
              .execute();
        } catch (GoogleJsonResponseException e) {
          if (errorExtractor.itemNotFound(e)) {
            LOG.info(
                "No BigQuery job with job id {} found in location {}.",
                jobId,
                jobRef.getLocation());
            return null;
          }
          LOG.info(
              "Ignoring the error encountered while trying to query the BigQuery job {}", jobId, e);
          lastException = e;
        } catch (IOException e) {
          LOG.info(
              "Ignoring the error encountered while trying to query the BigQuery job {}", jobId, e);
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
    private static final FluentBackoff INSERT_BACKOFF_FACTORY =
        FluentBackoff.DEFAULT.withInitialBackoff(Duration.millis(200)).withMaxRetries(5);

    // A backoff for rate limit exceeded errors. Only retry upto approximately 2 minutes
    // and propagate errors afterward. Otherwise, Dataflow UI cannot display rate limit
    // errors since they are silently retried in Callable threads.
    private static final FluentBackoff RATE_LIMIT_BACKOFF_FACTORY =
        FluentBackoff.DEFAULT.withInitialBackoff(Duration.standardSeconds(1)).withMaxRetries(13);

    private final ApiErrorExtractor errorExtractor;
    private final Bigquery client;
    private final PipelineOptions options;
    private final long maxRowsPerBatch;
    private final long maxRowBatchSize;
    // aggregate the total time spent in exponential backoff
    private final Counter throttlingMsecs =
        Metrics.counter(DatasetServiceImpl.class, "throttling-msecs");

    private ExecutorService executor;

    @VisibleForTesting
    DatasetServiceImpl(Bigquery client, PipelineOptions options) {
      BigQueryOptions bqOptions = options.as(BigQueryOptions.class);
      this.errorExtractor = new ApiErrorExtractor();
      this.client = client;
      this.options = options;
      this.maxRowsPerBatch = bqOptions.getMaxStreamingRowsToBatch();
      this.maxRowBatchSize = bqOptions.getMaxStreamingBatchSize();
      this.executor = null;
    }

    @VisibleForTesting
    DatasetServiceImpl(Bigquery client, PipelineOptions options, long maxRowsPerBatch) {
      BigQueryOptions bqOptions = options.as(BigQueryOptions.class);
      this.errorExtractor = new ApiErrorExtractor();
      this.client = client;
      this.options = options;
      this.maxRowsPerBatch = maxRowsPerBatch;
      this.maxRowBatchSize = bqOptions.getMaxStreamingBatchSize();
      this.executor = null;
    }

    private DatasetServiceImpl(BigQueryOptions bqOptions, @Nullable Histogram requestLatencies) {
      this.errorExtractor = new ApiErrorExtractor();
      this.client = newBigQueryClient(bqOptions, requestLatencies).build();
      this.options = bqOptions;
      this.maxRowsPerBatch = bqOptions.getMaxStreamingRowsToBatch();
      this.maxRowBatchSize = bqOptions.getMaxStreamingBatchSize();
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
    public @Nullable Table getTable(TableReference tableRef)
        throws IOException, InterruptedException {
      return getTable(tableRef, null);
    }

    @Override
    public @Nullable Table getTable(TableReference tableRef, List<String> selectedFields)
        throws IOException, InterruptedException {
      return getTable(tableRef, selectedFields, createDefaultBackoff(), Sleeper.DEFAULT);
    }

    @VisibleForTesting
    @Nullable
    Table getTable(
        TableReference ref, @Nullable List<String> selectedFields, BackOff backoff, Sleeper sleeper)
        throws IOException, InterruptedException {
      Tables.Get get =
          client
              .tables()
              .get(ref.getProjectId(), ref.getDatasetId(), ref.getTableId())
              .setPrettyPrint(false);
      if (selectedFields != null && !selectedFields.isEmpty()) {
        get.setSelectedFields(String.join(",", selectedFields));
      }
      try {
        return executeWithRetries(
            get,
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
     * <p>If a table with the same name already exists in the dataset, the function simply returns.
     * In such a case, the existing table doesn't necessarily have the same schema as specified by
     * the parameter.
     *
     * @throws IOException if other error than already existing table occurs.
     */
    @Override
    public void createTable(Table table) throws InterruptedException, IOException {
      LOG.info(
          "Trying to create BigQuery table: {}",
          BigQueryHelpers.toTableSpec(table.getTableReference()));
      BackOff backoff =
          new ExponentialBackOff.Builder()
              .setMaxElapsedTimeMillis(RETRY_CREATE_TABLE_DURATION_MILLIS)
              .build();

      tryCreateTable(table, backoff, Sleeper.DEFAULT);
    }

    @VisibleForTesting
    @Nullable
    Table tryCreateTable(Table table, BackOff backoff, Sleeper sleeper) throws IOException {
      boolean retry = false;
      while (true) {
        try {
          return client
              .tables()
              .insert(
                  table.getTableReference().getProjectId(),
                  table.getTableReference().getDatasetId(),
                  table)
              .setPrettyPrint(false)
              .execute();
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
      executeWithRetries(
          client
              .tables()
              .delete(tableRef.getProjectId(), tableRef.getDatasetId(), tableRef.getTableId()),
          String.format(
              "Unable to delete table: %s, aborting after %d retries.",
              tableRef.getTableId(), MAX_RPC_RETRIES),
          Sleeper.DEFAULT,
          createDefaultBackoff(),
          ALWAYS_RETRY);
    }

    @Override
    public boolean isTableEmpty(TableReference tableRef) throws IOException, InterruptedException {
      return isTableEmpty(tableRef, createDefaultBackoff(), Sleeper.DEFAULT);
    }

    @VisibleForTesting
    boolean isTableEmpty(TableReference tableRef, BackOff backoff, Sleeper sleeper)
        throws IOException, InterruptedException {
      TableDataList dataList =
          executeWithRetries(
              client
                  .tabledata()
                  .list(tableRef.getProjectId(), tableRef.getDatasetId(), tableRef.getTableId())
                  .setPrettyPrint(false),
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
      return executeWithRetries(
          client.datasets().get(projectId, datasetId).setPrettyPrint(false),
          String.format(
              "Unable to get dataset: %s, aborting after %d retries.", datasetId, MAX_RPC_RETRIES),
          Sleeper.DEFAULT,
          createDefaultBackoff(),
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
        String projectId,
        String datasetId,
        @Nullable String location,
        @Nullable String description,
        @Nullable Long defaultTableExpirationMs)
        throws IOException, InterruptedException {
      createDataset(
          projectId,
          datasetId,
          location,
          description,
          defaultTableExpirationMs,
          Sleeper.DEFAULT,
          createDefaultBackoff());
    }

    private void createDataset(
        String projectId,
        String datasetId,
        @Nullable String location,
        @Nullable String description,
        @Nullable Long defaultTableExpirationMs,
        Sleeper sleeper,
        BackOff backoff)
        throws IOException, InterruptedException {
      DatasetReference datasetRef =
          new DatasetReference().setProjectId(projectId).setDatasetId(datasetId);

      Dataset dataset = new Dataset().setDatasetReference(datasetRef);
      if (location != null) {
        dataset.setLocation(location);
      }
      if (description != null) {
        dataset.setFriendlyName(description);
        dataset.setDescription(description);
      }
      if (defaultTableExpirationMs != null) {
        dataset.setDefaultTableExpirationMs(defaultTableExpirationMs);
      }

      Exception lastException;
      do {
        try {
          client.datasets().insert(projectId, dataset).setPrettyPrint(false).execute();
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
              "Unable to create dataset: %s, aborting after %d .", datasetId, MAX_RPC_RETRIES),
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
      executeWithRetries(
          client.datasets().delete(projectId, datasetId),
          String.format(
              "Unable to delete table: %s, aborting after %d retries.", datasetId, MAX_RPC_RETRIES),
          Sleeper.DEFAULT,
          createDefaultBackoff(),
          ALWAYS_RETRY);
    }

    @VisibleForTesting
    <T> long insertAll(
        TableReference ref,
        List<ValueInSingleWindow<TableRow>> rowList,
        @Nullable List<String> insertIdList,
        BackOff backoff,
        FluentBackoff rateLimitBackoffFactory,
        final Sleeper sleeper,
        InsertRetryPolicy retryPolicy,
        List<ValueInSingleWindow<T>> failedInserts,
        ErrorContainer<T> errorContainer,
        boolean skipInvalidRows,
        boolean ignoreUnkownValues,
        boolean ignoreInsertIds)
        throws IOException, InterruptedException {
      checkNotNull(ref, "ref");
      if (executor == null) {
        this.executor =
            new BoundedExecutorService(
                options.as(GcsOptions.class).getExecutorService(),
                options.as(BigQueryOptions.class).getInsertBundleParallelism());
      }
      if (insertIdList != null && rowList.size() != insertIdList.size()) {
        throw new AssertionError(
            "If insertIdList is not null it needs to have at least "
                + "as many elements as rowList");
      }

      long retTotalDataSize = 0;
      List<TableDataInsertAllResponse.InsertErrors> allErrors = new ArrayList<>();
      // These lists contain the rows to publish. Initially the contain the entire list.
      // If there are failures, they will contain only the failed rows to be retried.
      List<ValueInSingleWindow<TableRow>> rowsToPublish = rowList;
      List<String> idsToPublish = null;
      if (!ignoreInsertIds) {
        idsToPublish = insertIdList;
      }
      while (true) {
        List<ValueInSingleWindow<TableRow>> retryRows = new ArrayList<>();
        List<String> retryIds = (idsToPublish != null) ? new ArrayList<>() : null;

        int strideIndex = 0;
        // Upload in batches.
        List<TableDataInsertAllRequest.Rows> rows = new ArrayList<>();
        long dataSize = 0L;

        List<Future<List<TableDataInsertAllResponse.InsertErrors>>> futures = new ArrayList<>();
        List<Integer> strideIndices = new ArrayList<>();
        // Store the longest throttled time across all parallel threads
        final AtomicLong maxThrottlingMsec = new AtomicLong();

        for (int i = 0; i < rowsToPublish.size(); ++i) {
          TableRow row = rowsToPublish.get(i).getValue();
          TableDataInsertAllRequest.Rows out = new TableDataInsertAllRequest.Rows();
          if (idsToPublish != null) {
            out.setInsertId(idsToPublish.get(i));
          }
          out.setJson(row.getUnknownKeys());
          rows.add(out);

          try {
            dataSize += TableRowJsonCoder.of().getEncodedElementByteSize(row);
          } catch (Exception ex) {
            throw new RuntimeException("Failed to convert the row to JSON", ex);
          }

          if (dataSize >= maxRowBatchSize
              || rows.size() >= maxRowsPerBatch
              || i == rowsToPublish.size() - 1) {
            TableDataInsertAllRequest content = new TableDataInsertAllRequest();
            content.setRows(rows);
            content.setSkipInvalidRows(skipInvalidRows);
            content.setIgnoreUnknownValues(ignoreUnkownValues);

            final Bigquery.Tabledata.InsertAll insert =
                client
                    .tabledata()
                    .insertAll(ref.getProjectId(), ref.getDatasetId(), ref.getTableId(), content)
                    .setPrettyPrint(false);

            futures.add(
                executor.submit(
                    () -> {
                      // A backoff for rate limit exceeded errors.
                      BackOff backoff1 =
                          BackOffAdapter.toGcpBackOff(rateLimitBackoffFactory.backoff());
                      long totalBackoffMillis = 0L;
                      while (true) {
                        try {
                          return insert.execute().getInsertErrors();
                        } catch (IOException e) {
                          GoogleJsonError.ErrorInfo errorInfo = getErrorInfo(e);
                          if (errorInfo == null) {
                            throw e;
                          }
                          /**
                           * TODO(BEAM-10584): Check for QUOTA_EXCEEDED error will be replaced by
                           * ApiErrorExtractor.INSTANCE.quotaExceeded(e) after the next release of
                           * GoogleCloudDataproc/hadoop-connectors
                           */
                          if (!ApiErrorExtractor.INSTANCE.rateLimited(e)
                              && !errorInfo.getReason().equals(QUOTA_EXCEEDED)) {
                            throw e;
                          }
                          LOG.info(
                              String.format(
                                  "BigQuery insertAll error, retrying: %s",
                                  ApiErrorExtractor.INSTANCE.getErrorMessage(e)));
                          try {
                            long nextBackOffMillis = backoff1.nextBackOffMillis();
                            if (nextBackOffMillis == BackOff.STOP) {
                              throw e;
                            }
                            sleeper.sleep(nextBackOffMillis);
                            totalBackoffMillis += nextBackOffMillis;
                            final long totalBackoffMillisSoFar = totalBackoffMillis;
                            maxThrottlingMsec.getAndUpdate(
                                current -> Math.max(current, totalBackoffMillisSoFar));
                          } catch (InterruptedException interrupted) {
                            throw new IOException(
                                "Interrupted while waiting before retrying insertAll");
                          }
                        }
                      }
                    }));
            strideIndices.add(strideIndex);

            retTotalDataSize += dataSize;

            dataSize = 0L;
            strideIndex = i + 1;
            rows = new ArrayList<>();
          }
        }

        try {
          for (int i = 0; i < futures.size(); i++) {
            List<TableDataInsertAllResponse.InsertErrors> errors = futures.get(i).get();
            if (errors == null) {
              continue;
            }
            for (TableDataInsertAllResponse.InsertErrors error : errors) {
              if (error.getIndex() == null) {
                throw new IOException("Insert failed: " + error + ", other errors: " + allErrors);
              }

              int errorIndex = error.getIndex().intValue() + strideIndices.get(i);
              if (retryPolicy.shouldRetry(new InsertRetryPolicy.Context(error))) {
                allErrors.add(error);
                retryRows.add(rowsToPublish.get(errorIndex));
                if (retryIds != null) {
                  retryIds.add(idsToPublish.get(errorIndex));
                }
              } else {
                errorContainer.add(failedInserts, error, ref, rowsToPublish.get(errorIndex));
              }
            }
          }
          // Accumulate the longest throttled time across all parallel threads
          throttlingMsecs.inc(maxThrottlingMsec.get());
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
          throw new IOException("Interrupted while waiting before retrying insert of " + retryRows);
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
    public <T> long insertAll(
        TableReference ref,
        List<ValueInSingleWindow<TableRow>> rowList,
        @Nullable List<String> insertIdList,
        InsertRetryPolicy retryPolicy,
        List<ValueInSingleWindow<T>> failedInserts,
        ErrorContainer<T> errorContainer,
        boolean skipInvalidRows,
        boolean ignoreUnknownValues,
        boolean ignoreInsertIds)
        throws IOException, InterruptedException {
      return insertAll(
          ref,
          rowList,
          insertIdList,
          BackOffAdapter.toGcpBackOff(INSERT_BACKOFF_FACTORY.backoff()),
          RATE_LIMIT_BACKOFF_FACTORY,
          Sleeper.DEFAULT,
          retryPolicy,
          failedInserts,
          errorContainer,
          skipInvalidRows,
          ignoreUnknownValues,
          ignoreInsertIds);
    }

    protected GoogleJsonError.ErrorInfo getErrorInfo(IOException e) {
      if (!(e instanceof GoogleJsonResponseException)) {
        return null;
      }
      GoogleJsonError jsonError = ((GoogleJsonResponseException) e).getDetails();
      GoogleJsonError.ErrorInfo errorInfo = Iterables.getFirst(jsonError.getErrors(), null);
      return errorInfo;
    }

    @Override
    public Table patchTableDescription(
        TableReference tableReference, @Nullable String tableDescription)
        throws IOException, InterruptedException {
      Table table = new Table();
      table.setDescription(tableDescription);

      return executeWithRetries(
          client
              .tables()
              .patch(
                  tableReference.getProjectId(),
                  tableReference.getDatasetId(),
                  tableReference.getTableId(),
                  table)
              .setPrettyPrint(false),
          String.format(
              "Unable to patch table description: %s, aborting after %d retries.",
              tableReference, MAX_RPC_RETRIES),
          Sleeper.DEFAULT,
          createDefaultBackoff(),
          ALWAYS_RETRY);
    }
  }

  static final SerializableFunction<IOException, Boolean> DONT_RETRY_NOT_FOUND =
      input -> {
        ApiErrorExtractor errorExtractor = new ApiErrorExtractor();
        return !errorExtractor.itemNotFound(input);
      };

  static final SerializableFunction<IOException, Boolean> ALWAYS_RETRY = input -> true;

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
    throw new IOException(errorMessage, lastException);
  }

  /** Identical to {@link BackOffUtils#next} but without checked IOException. */
  private static boolean nextBackOff(Sleeper sleeper, BackOff backoff) throws InterruptedException {
    try {
      return BackOffUtils.next(sleeper, backoff);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  /** Returns a BigQuery client builder using the specified {@link BigQueryOptions}. */
  private static Bigquery.Builder newBigQueryClient(
      BigQueryOptions options, @Nullable Histogram requestLatencies) {
    RetryHttpRequestInitializer httpRequestInitializer =
        new RetryHttpRequestInitializer(ImmutableList.of(404));
    httpRequestInitializer.setCustomErrors(createBigQueryClientCustomErrors());
    httpRequestInitializer.setWriteTimeout(options.getHTTPWriteTimeout());
    ImmutableList.Builder<HttpRequestInitializer> initBuilder = ImmutableList.builder();
    Credentials credential = options.getGcpCredential();
    initBuilder.add(
        credential == null
            ? new NullCredentialInitializer()
            : new HttpCredentialsAdapter(credential));
    // Do not log 404. It clutters the output and is possibly even required by the
    // caller.
    initBuilder.add(httpRequestInitializer);
    if (requestLatencies != null) {
      initBuilder.add(new LatencyRecordingHttpRequestInitializer(requestLatencies));
    }
    HttpRequestInitializer chainInitializer =
        new ChainingHttpRequestInitializer(
            Iterables.toArray(initBuilder.build(), HttpRequestInitializer.class));
    return new Bigquery.Builder(
            Transport.getTransport(), Transport.getJsonFactory(), chainInitializer)
        .setApplicationName(options.getAppName())
        .setGoogleClientRequestInitializer(options.getGoogleApiTrace());
  }

  public static CustomHttpErrors createBigQueryClientCustomErrors() {
    CustomHttpErrors.Builder builder = new CustomHttpErrors.Builder();
    // 403 errors, to list tables, matching this URL:
    // http://www.googleapis.com/bigquery/v2/projects/myproject/datasets/
    //     mydataset/tables?maxResults=1000
    builder.addErrorForCodeAndUrlContains(
        403,
        "/tables?",
        "The GCP project is most likely exceeding the rate limit on "
            + "bigquery.tables.list, please find the instructions to increase this limit at: "
            + "https://cloud.google.com/service-infrastructure/docs/rate-limiting#configure");
    return builder.build();
  }

  static class BigQueryServerStreamImpl<T> implements BigQueryServerStream<T> {

    private final ServerStream<T> serverStream;

    public BigQueryServerStreamImpl(ServerStream<T> serverStream) {
      this.serverStream = serverStream;
    }

    @Override
    public Iterator<T> iterator() {
      return serverStream.iterator();
    }

    @Override
    public void cancel() {
      serverStream.cancel();
    }
  }

  static class StorageClientImpl implements StorageClient {

    private static final HeaderProvider USER_AGENT_HEADER_PROVIDER =
        FixedHeaderProvider.create(
            "user-agent", "Apache_Beam_Java/" + ReleaseInfo.getReleaseInfo().getVersion());

    private final BigQueryReadClient client;

    private StorageClientImpl(BigQueryOptions options) throws IOException {
      BigQueryReadSettings settings =
          BigQueryReadSettings.newBuilder()
              .setCredentialsProvider(FixedCredentialsProvider.create(options.getGcpCredential()))
              .setTransportChannelProvider(
                  BigQueryReadSettings.defaultGrpcTransportProviderBuilder()
                      .setHeaderProvider(USER_AGENT_HEADER_PROVIDER)
                      .build())
              .build();
      this.client = BigQueryReadClient.create(settings);
    }

    @Override
    public ReadSession createReadSession(CreateReadSessionRequest request) {
      return client.createReadSession(request);
    }

    @Override
    public BigQueryServerStream<ReadRowsResponse> readRows(ReadRowsRequest request) {
      return new BigQueryServerStreamImpl<>(client.readRowsCallable().call(request));
    }

    @Override
    public SplitReadStreamResponse splitReadStream(SplitReadStreamRequest request) {
      return client.splitReadStream(request);
    }

    @Override
    public void close() {
      client.close();
    }
  }

  private static class BoundedExecutorService implements ExecutorService {
    private final ExecutorService executor;
    private final Semaphore semaphore;
    private final int parallelism;

    BoundedExecutorService(ExecutorService executor, int parallelism) {
      this.executor = executor;
      this.parallelism = parallelism;
      this.semaphore = new Semaphore(parallelism);
    }

    @Override
    public void shutdown() {
      executor.shutdown();
    }

    @Override
    public List<Runnable> shutdownNow() {
      List<Runnable> runnables = executor.shutdownNow();
      // try to release permits as many as possible before returning semaphored runnables.
      synchronized (this) {
        if (semaphore.availablePermits() <= parallelism) {
          semaphore.release(Integer.MAX_VALUE - parallelism);
        }
      }
      return runnables;
    }

    @Override
    public boolean isShutdown() {
      return executor.isShutdown();
    }

    @Override
    public boolean isTerminated() {
      return executor.isTerminated();
    }

    @Override
    public boolean awaitTermination(long l, TimeUnit timeUnit) throws InterruptedException {
      return executor.awaitTermination(l, timeUnit);
    }

    @Override
    public <T> Future<T> submit(Callable<T> callable) {
      return executor.submit(new SemaphoreCallable<>(callable));
    }

    @Override
    public <T> Future<T> submit(Runnable runnable, T t) {
      return executor.submit(new SemaphoreRunnable(runnable), t);
    }

    @Override
    public Future<?> submit(Runnable runnable) {
      return executor.submit(new SemaphoreRunnable(runnable));
    }

    @Override
    public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> collection)
        throws InterruptedException {
      return executor.invokeAll(
          collection.stream().map(SemaphoreCallable::new).collect(Collectors.toList()));
    }

    @Override
    public <T> List<Future<T>> invokeAll(
        Collection<? extends Callable<T>> collection, long l, TimeUnit timeUnit)
        throws InterruptedException {
      return executor.invokeAll(
          collection.stream().map(SemaphoreCallable::new).collect(Collectors.toList()),
          l,
          timeUnit);
    }

    @Override
    public <T> T invokeAny(Collection<? extends Callable<T>> collection)
        throws InterruptedException, ExecutionException {
      return executor.invokeAny(
          collection.stream().map(SemaphoreCallable::new).collect(Collectors.toList()));
    }

    @Override
    public <T> T invokeAny(Collection<? extends Callable<T>> collection, long l, TimeUnit timeUnit)
        throws InterruptedException, ExecutionException, TimeoutException {
      return executor.invokeAny(
          collection.stream().map(SemaphoreCallable::new).collect(Collectors.toList()),
          l,
          timeUnit);
    }

    @Override
    public void execute(Runnable runnable) {
      executor.execute(new SemaphoreRunnable(runnable));
    }

    private class SemaphoreRunnable implements Runnable {
      private final Runnable runnable;

      SemaphoreRunnable(Runnable runnable) {
        this.runnable = runnable;
      }

      @Override
      public void run() {
        try {
          semaphore.acquire();
        } catch (InterruptedException e) {
          throw new RuntimeException("semaphore acquisition interrupted. task canceled.");
        }
        try {
          runnable.run();
        } finally {
          semaphore.release();
        }
      }
    }

    private class SemaphoreCallable<V> implements Callable<V> {
      private final Callable<V> callable;

      SemaphoreCallable(Callable<V> callable) {
        this.callable = callable;
      }

      @Override
      public V call() throws Exception {
        semaphore.acquire();
        try {
          return callable.call();
        } finally {
          semaphore.release();
        }
      }
    }
  }
}
