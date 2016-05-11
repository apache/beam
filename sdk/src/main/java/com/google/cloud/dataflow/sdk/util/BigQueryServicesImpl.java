/*
 * Copyright (C) 2016 Google Inc.
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

import com.google.api.client.googleapis.json.GoogleJsonResponseException;
import com.google.api.client.util.BackOff;
import com.google.api.client.util.BackOffUtils;
import com.google.api.client.util.Sleeper;
import com.google.api.services.bigquery.Bigquery;
import com.google.api.services.bigquery.model.Job;
import com.google.api.services.bigquery.model.JobConfiguration;
import com.google.api.services.bigquery.model.JobConfigurationExtract;
import com.google.api.services.bigquery.model.JobConfigurationLoad;
import com.google.api.services.bigquery.model.JobConfigurationQuery;
import com.google.api.services.bigquery.model.JobReference;
import com.google.api.services.bigquery.model.JobStatus;
import com.google.cloud.dataflow.sdk.options.BigQueryOptions;
import com.google.cloud.hadoop.util.ApiErrorExtractor;
import com.google.common.annotations.VisibleForTesting;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.TimeUnit;


/**
 * An implementation of {@link BigQueryServices} that actually communicates with the cloud BigQuery
 * service.
 */
public class BigQueryServicesImpl implements BigQueryServices {

  // The maximum number of attempts to execute a BigQuery RPC.
  private static final int MAX_RPC_ATTEMPTS = 10;

  // The initial backoff for executing a BigQuery RPC.
  private static final long INITIAL_RPC_BACKOFF_MILLIS = TimeUnit.SECONDS.toMillis(1);

  // The initial backoff for polling the status of a BigQuery job.
  private static final long INITIAL_JOB_STATUS_POLL_BACKOFF_MILLIS = TimeUnit.SECONDS.toMillis(60);

  @Override
  public JobService getJobService(BigQueryOptions options) {
    return new JobServiceImpl(options);
  }

  @VisibleForTesting
  static class JobServiceImpl implements BigQueryServices.JobService {
    private static final Logger LOG = LoggerFactory.getLogger(JobServiceImpl.class);

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
     * <p>Retries the RPC for at most {@code MAX_RPC_ATTEMPTS} times until it succeeds.
     *
     * @throws IOException if it exceeds max RPC retries.
     */
    @Override
    public void startLoadJob(
        String jobId,
        JobConfigurationLoad loadConfig) throws InterruptedException, IOException {
      Job job = new Job();
      JobReference jobRef = new JobReference();
      jobRef.setProjectId(loadConfig.getDestinationTable().getProjectId());
      jobRef.setJobId(jobId);
      job.setJobReference(jobRef);
      JobConfiguration jobConfig = new JobConfiguration();
      jobConfig.setLoad(loadConfig);
      job.setConfiguration(jobConfig);

      startJob(job, errorExtractor, client);
    }

    /**
     * {@inheritDoc}
     *
     * <p>Retries the RPC for at most {@code MAX_RPC_ATTEMPTS} times until it succeeds.
     *
     * @throws IOException if it exceeds max RPC retries.
     */
    @Override
    public void startExtractJob(String jobId, JobConfigurationExtract extractConfig)
        throws InterruptedException, IOException {
      Job job = new Job();
      JobReference jobRef = new JobReference();
      jobRef.setProjectId(extractConfig.getSourceTable().getProjectId());
      jobRef.setJobId(jobId);
      job.setJobReference(jobRef);
      JobConfiguration jobConfig = new JobConfiguration();
      jobConfig.setExtract(extractConfig);
      job.setConfiguration(jobConfig);

      startJob(job, errorExtractor, client);
    }

    /**
     * {@inheritDoc}
     *
     * <p>Retries the RPC for at most {@code MAX_RPC_ATTEMPTS} times until it succeeds.
     *
     * @throws IOException if it exceeds max RPC retries.
     */
    @Override
    public void startQueryJob(String jobId, JobConfigurationQuery queryConfig, boolean dryRun)
        throws IOException, InterruptedException {
      Job job = new Job();
      JobReference jobRef = new JobReference();
      jobRef.setProjectId(queryConfig.getDestinationTable().getProjectId());
      jobRef.setJobId(jobId);
      job.setJobReference(jobRef);
      JobConfiguration jobConfig = new JobConfiguration();
      jobConfig.setQuery(queryConfig);
      jobConfig.setDryRun(dryRun);
      job.setConfiguration(jobConfig);

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
        BackOff backoff)
        throws InterruptedException, IOException {
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
              "Unable to insert job: %s, aborting after %d retries.",
              jobRef.getJobId(), MAX_RPC_ATTEMPTS),
          lastException);
    }

    @Override
    public Job pollJob(String projectId, String jobId, int maxAttempts)
        throws InterruptedException {
      BackOff backoff = new AttemptBoundedExponentialBackOff(
          maxAttempts, INITIAL_JOB_STATUS_POLL_BACKOFF_MILLIS);
      return pollJob(projectId, jobId, Sleeper.DEFAULT, backoff);
    }

    @VisibleForTesting
    Job pollJob(
        String projectId,
        String jobId,
        Sleeper sleeper,
        BackOff backoff) throws InterruptedException {
      do {
        try {
          Job job = client.jobs().get(projectId, jobId).execute();
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
      LOG.warn("Unable to poll job status: {}, aborting after reached max retries.", jobId);
      return null;
    }

    /**
     * Identical to {@link BackOffUtils#next} but without checked IOException.
     * @throws InterruptedException
     */
    private static boolean nextBackOff(Sleeper sleeper, BackOff backoff)
        throws InterruptedException {
      try {
        return BackOffUtils.next(sleeper, backoff);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }
}

