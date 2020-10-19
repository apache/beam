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

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkState;

import com.google.api.client.util.BackOff;
import com.google.api.client.util.BackOffUtils;
import com.google.api.client.util.Sleeper;
import com.google.api.services.bigquery.model.Dataset;
import com.google.api.services.bigquery.model.Job;
import com.google.api.services.bigquery.model.JobStatus;
import com.google.api.services.bigquery.model.Table;
import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.api.services.bigquery.model.TimePartitioning;
import com.google.cloud.hadoop.util.ApiErrorExtractor;
import java.io.IOException;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.regex.Matcher;
import org.apache.beam.sdk.extensions.gcp.util.BackOffAdapter;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.fs.ResolveOptions;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryServices.DatasetService;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.options.ValueProvider.NestedValueProvider;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.util.FluentBackoff;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Lists;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** A set of helper functions and classes used by {@link BigQueryIO}. */
public class BigQueryHelpers {
  private static final String RESOURCE_NOT_FOUND_ERROR =
      "BigQuery %1$s not found for table \"%2$s\" . Please create the %1$s before pipeline"
          + " execution. If the %1$s is created by an earlier stage of the pipeline, this"
          + " validation can be disabled using #withoutValidation.";

  private static final String UNABLE_TO_CONFIRM_PRESENCE_OF_RESOURCE_ERROR =
      "Unable to confirm BigQuery %1$s presence for table \"%2$s\". If the %1$s is created by"
          + " an earlier stage of the pipeline, this validation can be disabled using"
          + " #withoutValidation.";

  private static final Logger LOG = LoggerFactory.getLogger(BigQueryHelpers.class);

  // Given a potential failure and a current job-id, return the next job-id to be used on retry.
  // Algorithm is as follows (given input of job_id_prefix-N)
  //   If BigQuery has no status for job_id_prefix-n, we should retry with the same id.
  //   If job-id-prefix-n is in the PENDING or successful states, no retry is needed.
  //   Otherwise (job-id-prefix-n completed with errors), try again with job-id-prefix-(n+1)
  //
  // We continue to loop through these job ids until we find one that has either succeed, or that
  // has not been issued yet.
  static class RetryJobIdResult {
    public final RetryJobId jobId;
    public final boolean shouldRetry;

    public RetryJobIdResult(RetryJobId jobId, boolean shouldRetry) {
      this.jobId = jobId;
      this.shouldRetry = shouldRetry;
    }
  }

  // A class that waits for pending jobs, retrying them according to policy if they fail.
  static class PendingJobManager {
    private static class JobInfo {
      private final PendingJob pendingJob;
      private final @Nullable SerializableFunction<PendingJob, Exception> onSuccess;

      public JobInfo(PendingJob pendingJob, SerializableFunction<PendingJob, Exception> onSuccess) {
        this.pendingJob = pendingJob;
        this.onSuccess = onSuccess;
      }
    }

    private List<JobInfo> pendingJobs = Lists.newArrayList();
    private final BackOff backOff;

    PendingJobManager() {
      this(
          BackOffAdapter.toGcpBackOff(
              FluentBackoff.DEFAULT
                  .withMaxRetries(Integer.MAX_VALUE)
                  .withInitialBackoff(Duration.standardSeconds(1))
                  .withMaxBackoff(Duration.standardMinutes(1))
                  .backoff()));
    }

    PendingJobManager(BackOff backOff) {
      this.backOff = backOff;
    }

    // Add a pending job and a function to call when the job has completed successfully.
    PendingJobManager addPendingJob(
        PendingJob pendingJob, @Nullable SerializableFunction<PendingJob, Exception> onSuccess) {
      this.pendingJobs.add(new JobInfo(pendingJob, onSuccess));
      return this;
    }

    void waitForDone() throws Exception {
      LOG.info("Waiting for jobs to complete.");
      Sleeper sleeper = Sleeper.DEFAULT;
      while (!pendingJobs.isEmpty()) {
        List<JobInfo> retryJobs = Lists.newArrayList();
        for (JobInfo jobInfo : pendingJobs) {
          if (jobInfo.pendingJob.pollJob()) {
            // Job has completed successfully.
            LOG.info("Job {} completed successfully.", jobInfo.pendingJob.currentJobId);
            Exception e = jobInfo.onSuccess.apply(jobInfo.pendingJob);
            if (e != null) {
              throw e;
            }
          } else {
            // Job not yet complete, schedule it again.
            LOG.info("Job {} pending. retrying.", jobInfo.pendingJob.currentJobId);
            retryJobs.add(jobInfo);
          }
        }
        pendingJobs = retryJobs;
        if (!pendingJobs.isEmpty()) {
          // Sleep before retrying.
          nextBackOff(sleeper, backOff);
          // Run the jobs to retry. If a job has hit the maximum number of retries then runJob
          // will raise an exception.
          for (JobInfo job : pendingJobs) {
            job.pendingJob.runJob();
          }
        }
      }
    }

    /** Identical to {@link BackOffUtils#next} but without checked IOException. */
    private static boolean nextBackOff(Sleeper sleeper, BackOff backOff)
        throws InterruptedException {
      try {
        return BackOffUtils.next(sleeper, backOff);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }

  static class PendingJob {
    private final SerializableFunction<RetryJobId, Void> executeJob;
    private final SerializableFunction<RetryJobId, Job> pollJob;
    private final SerializableFunction<RetryJobId, Job> lookupJob;
    private final int maxRetries;
    private int currentAttempt;
    RetryJobId currentJobId;
    Job lastJobAttempted;
    boolean started;

    PendingJob(
        SerializableFunction<RetryJobId, Void> executeJob,
        SerializableFunction<RetryJobId, Job> pollJob,
        SerializableFunction<RetryJobId, Job> lookupJob,
        int maxRetries,
        String jobIdPrefix) {
      this.executeJob = executeJob;
      this.pollJob = pollJob;
      this.lookupJob = lookupJob;
      this.maxRetries = maxRetries;
      this.currentAttempt = 0;
      currentJobId = new RetryJobId(jobIdPrefix, 0);
      this.started = false;
    }

    // Run the job.
    void runJob() throws IOException {
      ++currentAttempt;
      if (!shouldRetry()) {
        throw new RuntimeException(
            String.format(
                "Failed to create job with prefix %s, "
                    + "reached max retries: %d, last failed job: %s.",
                currentJobId.getJobIdPrefix(),
                maxRetries,
                BigQueryHelpers.jobToPrettyString(lastJobAttempted)));
      }

      try {
        this.started = false;
        executeJob.apply(currentJobId);
      } catch (RuntimeException e) {
        LOG.warn("Job {} failed.", currentJobId.getJobId(), e);
        // It's possible that the job actually made it to BQ even though we got a failure here.
        // For example, the response from BQ may have timed out returning. getRetryJobId will
        // return the correct job id to use on retry, or a job id to continue polling (if it turns
        // out that the job has not actually failed yet).
        RetryJobIdResult result = getRetryJobId(currentJobId, lookupJob);
        currentJobId = result.jobId;
        if (result.shouldRetry) {
          // Otherwise the jobs either never started or started and failed. Try the job again with
          // the job id returned by getRetryJobId.
          LOG.info("Will retry with job id {}", currentJobId.getJobId());
          return;
        }
      }
      LOG.info("job {} started", currentJobId.getJobId());
      // The job has reached BigQuery and is in either the PENDING state or has completed
      // successfully.
      this.started = true;
    }

    // Poll the status of the job. Returns true if the job has completed successfully and false
    // otherwise.
    boolean pollJob() throws IOException {
      if (started) {
        Job job = pollJob.apply(currentJobId);
        this.lastJobAttempted = job;
        Status jobStatus = parseStatus(job);
        switch (jobStatus) {
          case SUCCEEDED:
            LOG.info("Load job {} succeeded. Statistics: {}", currentJobId, job.getStatistics());
            return true;
          case UNKNOWN:
            // This might happen if BigQuery's job listing is slow. Retry with the same
            // job id.
            LOG.info(
                "Load job {} finished in unknown state: {}: {}",
                currentJobId,
                job.getStatus(),
                shouldRetry() ? "will retry" : "will not retry");
            return false;
          case FAILED:
            String oldJobId = currentJobId.getJobId();
            currentJobId = BigQueryHelpers.getRetryJobId(currentJobId, lookupJob).jobId;
            LOG.info(
                "Load job {} failed, {}: {}. Next job id {}",
                oldJobId,
                shouldRetry() ? "will retry" : "will not retry",
                job.getStatus(),
                currentJobId);
            return false;
          default:
            throw new IllegalStateException(
                String.format(
                    "Unexpected status [%s] of load job: %s.",
                    job.getStatus(), BigQueryHelpers.jobToPrettyString(job)));
        }
      }
      return false;
    }

    boolean shouldRetry() {
      return currentAttempt < maxRetries + 1;
    }
  }

  static class RetryJobId {
    private final String jobIdPrefix;
    private final int retryIndex;

    RetryJobId(String jobIdPrefix, int retryIndex) {
      this.jobIdPrefix = jobIdPrefix;
      this.retryIndex = retryIndex;
    }

    String getJobIdPrefix() {
      return jobIdPrefix;
    }

    int getRetryIndex() {
      return retryIndex;
    }

    String getJobId() {
      return jobIdPrefix + "-" + retryIndex;
    }

    @Override
    public String toString() {
      return getJobId();
    }
  }

  static RetryJobIdResult getRetryJobId(
      RetryJobId currentJobId, SerializableFunction<RetryJobId, Job> lookupJob) {
    for (int retryIndex = currentJobId.getRetryIndex(); ; retryIndex++) {
      RetryJobId jobId = new RetryJobId(currentJobId.getJobIdPrefix(), retryIndex);
      try {
        Job loadJob = lookupJob.apply(jobId);
        if (loadJob == null) {
          LOG.info("job id {} not found, so retrying with that id", jobId);
          // This either means that the original job was never properly issued (on the first
          // iteration of the loop) or that we've found a retry id that has not been used yet. Try
          // again with this job id.
          return new RetryJobIdResult(jobId, true);
        }
        JobStatus jobStatus = loadJob.getStatus();
        if (jobStatus == null) {
          LOG.info("job status for {} not found, so retrying with that job id", jobId);
          return new RetryJobIdResult(jobId, true);
        }
        if ("PENDING".equals(jobStatus.getState()) || "RUNNING".equals(jobStatus.getState())) {
          // The job id has been issued and is currently pending. This can happen after receiving
          // an error from the load or copy job creation (e.g. that error might come because the
          // job already exists). Return to the caller which job id is pending (it might not be the
          // one passed in) so the caller can then wait for this job to finish.
          LOG.info("job {} in pending or running state, so continuing with that job id", jobId);
          return new RetryJobIdResult(jobId, false);
        }
        if (jobStatus.getErrorResult() == null
            && (jobStatus.getErrors() == null || jobStatus.getErrors().isEmpty())) {
          // Import succeeded. No retry needed.
          LOG.info("job {} succeeded, so not retrying ", jobId);
          return new RetryJobIdResult(jobId, false);
        }
        // This job has failed, so we assume the data cannot enter BigQuery. We will check the next
        // job in the sequence (with the same unique prefix) to see if is either pending/succeeded
        // or can be used to generate a retry job.
        LOG.info("job {} is failed. Checking the next job id", jobId);
      } catch (RuntimeException e) {
        LOG.info("caught exception while querying job {}", jobId);
        return new RetryJobIdResult(jobId, true);
      }
    }
  }

  /** Status of a BigQuery job or request. */
  enum Status {
    SUCCEEDED,
    FAILED,
    UNKNOWN,
  }

  /** Project resource name formatted according to https://google.aip.dev/122. */
  static String toProjectResourceName(String projectName) {
    return "projects/" + projectName;
  }

  /** Table resource name formatted according to https://google.aip.dev/122. */
  static String toTableResourceName(TableReference tableReference) {
    return "projects/"
        + tableReference.getProjectId()
        + "/datasets/"
        + tableReference.getDatasetId()
        + "/tables/"
        + tableReference.getTableId();
  }

  /** Return a displayable string representation for a {@link TableReference}. */
  static @Nullable ValueProvider<String> displayTable(
      @Nullable ValueProvider<TableReference> table) {
    if (table == null) {
      return null;
    }
    return NestedValueProvider.of(table, new TableRefToTableSpec());
  }

  /** Returns a canonical string representation of the {@link TableReference}. */
  public static String toTableSpec(TableReference ref) {
    StringBuilder sb = new StringBuilder();
    if (ref.getProjectId() != null) {
      sb.append(ref.getProjectId());
      sb.append(":");
    }

    sb.append(ref.getDatasetId()).append('.').append(ref.getTableId());
    return sb.toString();
  }

  static <K, V> List<V> getOrCreateMapListValue(Map<K, List<V>> map, K key) {
    return map.computeIfAbsent(key, k -> new ArrayList<>());
  }

  /**
   * Parse a table specification in the form {@code "[project_id]:[dataset_id].[table_id]"} or
   * {@code "[dataset_id].[table_id]"}.
   *
   * <p>If the project id is omitted, the default project id is used.
   */
  public static TableReference parseTableSpec(String tableSpec) {
    Matcher match = BigQueryIO.TABLE_SPEC.matcher(tableSpec);
    if (!match.matches()) {
      throw new IllegalArgumentException(
          "Table reference is not in [project_id]:[dataset_id].[table_id] "
              + "format: "
              + tableSpec);
    }

    TableReference ref = new TableReference();
    ref.setProjectId(match.group("PROJECT"));

    return ref.setDatasetId(match.group("DATASET")).setTableId(match.group("TABLE"));
  }

  /** Strip off any partition decorator information from a tablespec. */
  public static String stripPartitionDecorator(String tableSpec) {
    int index = tableSpec.lastIndexOf('$');
    return (index == -1) ? tableSpec : tableSpec.substring(0, index);
  }

  static String jobToPrettyString(@Nullable Job job) throws IOException {
    if (job != null && job.getConfiguration().getLoad() != null) {
      // Removing schema and sourceUris from error messages for load jobs since these fields can be
      // quite long and error message might not be displayed properly in runner specific logs.
      job = job.clone();
      job.getConfiguration().getLoad().setSchema(null);
      job.getConfiguration().getLoad().setSourceUris(null);
    }

    return job == null ? "null" : job.toPrettyString();
  }

  static String statusToPrettyString(@Nullable JobStatus status) throws IOException {
    return status == null ? "Unknown status: null." : status.toPrettyString();
  }

  static Status parseStatus(@Nullable Job job) {
    if (job == null) {
      return Status.UNKNOWN;
    }
    JobStatus status = job.getStatus();
    if (status.getErrorResult() != null) {
      return Status.FAILED;
    } else if (status.getErrors() != null && !status.getErrors().isEmpty()) {
      return Status.FAILED;
    } else {
      return Status.SUCCEEDED;
    }
  }

  public static String toJsonString(Object item) {
    if (item == null) {
      return null;
    }
    try {
      return BigQueryIO.JSON_FACTORY.toString(item);
    } catch (IOException e) {
      throw new RuntimeException(
          String.format("Cannot serialize %s to a JSON string.", item.getClass().getSimpleName()),
          e);
    }
  }

  public static <T> T fromJsonString(String json, Class<T> clazz) {
    if (json == null) {
      return null;
    }
    try {
      return BigQueryIO.JSON_FACTORY.fromString(json, clazz);
    } catch (IOException e) {
      throw new RuntimeException(
          String.format("Cannot deserialize %s from a JSON string: %s.", clazz, json), e);
    }
  }

  /**
   * Returns a randomUUID string.
   *
   * <p>{@code '-'} is removed because BigQuery doesn't allow it in dataset id.
   */
  static String randomUUIDString() {
    return UUID.randomUUID().toString().replaceAll("-", "");
  }

  static void verifyTableNotExistOrEmpty(DatasetService datasetService, TableReference tableRef) {
    try {
      if (datasetService.getTable(tableRef) != null) {
        checkState(
            datasetService.isTableEmpty(tableRef),
            "BigQuery table is not empty: %s.",
            toTableSpec(tableRef));
      }
    } catch (IOException | InterruptedException e) {
      if (e instanceof InterruptedException) {
        Thread.currentThread().interrupt();
      }
      throw new RuntimeException(
          "unable to confirm BigQuery table emptiness for table " + toTableSpec(tableRef), e);
    }
  }

  static void verifyDatasetPresence(DatasetService datasetService, TableReference table) {
    try {
      datasetService.getDataset(table.getProjectId(), table.getDatasetId());
    } catch (Exception e) {
      ApiErrorExtractor errorExtractor = new ApiErrorExtractor();
      if ((e instanceof IOException) && errorExtractor.itemNotFound((IOException) e)) {
        throw new IllegalArgumentException(
            String.format(RESOURCE_NOT_FOUND_ERROR, "dataset", toTableSpec(table)), e);
      } else if (e instanceof RuntimeException) {
        throw (RuntimeException) e;
      } else {
        throw new RuntimeException(
            String.format(
                UNABLE_TO_CONFIRM_PRESENCE_OF_RESOURCE_ERROR, "dataset", toTableSpec(table)),
            e);
      }
    }
  }

  /**
   * It returns the number of rows for a given table.
   *
   * @return The number of rows in the table or null if it cannot get any estimate.
   */
  public static @Nullable BigInteger getNumRows(BigQueryOptions options, TableReference tableRef)
      throws InterruptedException, IOException {

    DatasetService datasetService = new BigQueryServicesImpl().getDatasetService(options);
    Table table = datasetService.getTable(tableRef);
    if (table == null) {
      return null;
    }
    return table.getNumRows();
  }

  static String getDatasetLocation(
      DatasetService datasetService, String projectId, String datasetId) {
    Dataset dataset;
    try {
      dataset = datasetService.getDataset(projectId, datasetId);
    } catch (Exception e) {
      if (e instanceof InterruptedException) {
        Thread.currentThread().interrupt();
      }
      throw new RuntimeException(
          String.format(
              "unable to obtain dataset for dataset %s in project %s", datasetId, projectId),
          e);
    }
    return dataset.getLocation();
  }

  static void verifyTablePresence(DatasetService datasetService, TableReference table) {
    try {
      datasetService.getTable(table);
    } catch (Exception e) {
      ApiErrorExtractor errorExtractor = new ApiErrorExtractor();
      if ((e instanceof IOException) && errorExtractor.itemNotFound((IOException) e)) {
        throw new IllegalArgumentException(
            String.format(RESOURCE_NOT_FOUND_ERROR, "table", toTableSpec(table)), e);
      } else if (e instanceof RuntimeException) {
        throw (RuntimeException) e;
      } else {
        throw new RuntimeException(
            String.format(
                UNABLE_TO_CONFIRM_PRESENCE_OF_RESOURCE_ERROR, "table", toTableSpec(table)),
            e);
      }
    }
  }

  @VisibleForTesting
  static class JsonSchemaToTableSchema implements SerializableFunction<String, TableSchema> {
    @Override
    public TableSchema apply(String from) {
      return fromJsonString(from, TableSchema.class);
    }
  }

  static class TableSchemaToJsonSchema implements SerializableFunction<TableSchema, String> {
    @Override
    public String apply(TableSchema from) {
      return toJsonString(from);
    }
  }

  static class JsonTableRefToTableRef implements SerializableFunction<String, TableReference> {
    @Override
    public TableReference apply(String from) {
      return fromJsonString(from, TableReference.class);
    }
  }

  static class JsonTableRefToTableSpec implements SerializableFunction<String, String> {
    @Override
    public String apply(String from) {
      return toTableSpec(fromJsonString(from, TableReference.class));
    }
  }

  static class TableRefToJson implements SerializableFunction<TableReference, String> {
    @Override
    public String apply(TableReference from) {
      return toJsonString(from);
    }
  }

  static class TableRefToTableSpec implements SerializableFunction<TableReference, String> {
    @Override
    public String apply(TableReference from) {
      return toTableSpec(from);
    }
  }

  @VisibleForTesting
  static class TableSpecToTableRef implements SerializableFunction<String, TableReference> {
    @Override
    public TableReference apply(String from) {
      return parseTableSpec(from);
    }
  }

  static class TimePartitioningToJson implements SerializableFunction<TimePartitioning, String> {
    @Override
    public String apply(TimePartitioning partitioning) {
      return toJsonString(partitioning);
    }
  }

  static String resolveTempLocation(
      String tempLocationDir, String bigQueryOperationName, String stepUuid) {
    return FileSystems.matchNewResource(tempLocationDir, true)
        .resolve(bigQueryOperationName, ResolveOptions.StandardResolveOptions.RESOLVE_DIRECTORY)
        .resolve(stepUuid, ResolveOptions.StandardResolveOptions.RESOLVE_DIRECTORY)
        .toString();
  }
}
