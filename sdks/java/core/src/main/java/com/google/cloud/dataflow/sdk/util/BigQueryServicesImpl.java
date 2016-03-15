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
package com.google.cloud.dataflow.sdk.util;

import com.google.api.client.googleapis.json.GoogleJsonResponseException;
import com.google.api.client.util.BackOff;
import com.google.api.client.util.BackOffUtils;
import com.google.api.client.util.Sleeper;
import com.google.api.services.bigquery.Bigquery;
import com.google.api.services.bigquery.model.Job;
import com.google.api.services.bigquery.model.JobConfiguration;
import com.google.api.services.bigquery.model.JobConfigurationLoad;
import com.google.api.services.bigquery.model.JobReference;
import com.google.api.services.bigquery.model.JobStatus;
import com.google.api.services.bigquery.model.TableReference;
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

  // The maximum number of attempts to execute a load job RPC.
  private static final int MAX_LOAD_JOB_RPC_ATTEMPTS = 10;

  // The initial backoff for executing a load job RPC.
  private static final long INITIAL_LOAD_JOB_RPC_BACKOFF_MILLIS = TimeUnit.SECONDS.toMillis(1);
  // The maximum number of retries to poll the status of a load job.
  // It sets to {@code Integer.MAX_VALUE} to block until the BigQuery load job finishes.
  private static final int MAX_LOAD_JOB_POLL_RETRIES = Integer.MAX_VALUE;

  // The initial backoff for polling the status of a load job.
  private static final long INITIAL_LOAD_JOB_POLL_BACKOFF_MILLIS = TimeUnit.SECONDS.toMillis(60);

  @Override
  public LoadService getLoadService(BigQueryOptions options) {
    return new LoadServiceImpl(options);
  }

  @VisibleForTesting
  static class LoadServiceImpl implements BigQueryServices.LoadService {
    private static final Logger LOG = LoggerFactory.getLogger(LoadServiceImpl.class);

    private final ApiErrorExtractor errorExtractor;
    private final Bigquery client;

    @VisibleForTesting
    LoadServiceImpl(Bigquery client) {
      this.errorExtractor = new ApiErrorExtractor();
      this.client = client;
    }

    private LoadServiceImpl(BigQueryOptions options) {
      this.errorExtractor = new ApiErrorExtractor();
      this.client = Transport.newBigQueryClient(options).build();
    }

    /**
     * {@inheritDoc}
     *
     * <p>Retries the RPC for at most {@code MAX_LOAD_JOB_RPC_ATTEMPTS} times until it succeeds.
     *
     * @throws IOException if it exceeds max RPC retries.
     */
    @Override
    public void startLoadJob(
        String jobId,
        JobConfigurationLoad loadConfig) throws InterruptedException, IOException {
      BackOff backoff = new AttemptBoundedExponentialBackOff(
          MAX_LOAD_JOB_RPC_ATTEMPTS, INITIAL_LOAD_JOB_RPC_BACKOFF_MILLIS);
      startLoadJob(jobId, loadConfig, Sleeper.DEFAULT, backoff);
    }

    /**
     * {@inheritDoc}
     *
     * <p>Retries the poll request for at most {@code MAX_LOAD_JOB_POLL_RETRIES} times
     * until the job is DONE.
     */
    @Override
    public Status pollJobStatus(String projectId, String jobId) throws InterruptedException {
      BackOff backoff = new AttemptBoundedExponentialBackOff(
          MAX_LOAD_JOB_POLL_RETRIES, INITIAL_LOAD_JOB_POLL_BACKOFF_MILLIS);
      return pollJobStatus(projectId, jobId, Sleeper.DEFAULT, backoff);
    }

    @VisibleForTesting
    void startLoadJob(
        String jobId,
        JobConfigurationLoad loadConfig,
        Sleeper sleeper,
        BackOff backoff)
        throws InterruptedException, IOException {
      TableReference ref = loadConfig.getDestinationTable();
      String projectId = ref.getProjectId();

      Job job = new Job();
      JobReference jobRef = new JobReference();
      jobRef.setProjectId(projectId);
      jobRef.setJobId(jobId);
      job.setJobReference(jobRef);
      JobConfiguration config = new JobConfiguration();
      config.setLoad(loadConfig);
      job.setConfiguration(config);

      Exception lastException = null;
      do {
        try {
          client.jobs().insert(projectId, job).execute();
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
              jobId, MAX_LOAD_JOB_RPC_ATTEMPTS),
          lastException);
    }

    @VisibleForTesting
    Status pollJobStatus(
        String projectId,
        String jobId,
        Sleeper sleeper,
        BackOff backoff) throws InterruptedException {
      do {
        try {
          JobStatus status = client.jobs().get(projectId, jobId).execute().getStatus();
          if (status != null && status.getState() != null && status.getState().equals("DONE")) {
            if (status.getErrorResult() != null) {
              return Status.FAILED;
            } else if (status.getErrors() != null && !status.getErrors().isEmpty()) {
              return Status.FAILED;
            } else {
              return Status.SUCCEEDED;
            }
          }
          // The job is not DONE, wait longer and retry.
        } catch (IOException e) {
          // ignore and retry
          LOG.warn("Ignore the error and retry polling job status.", e);
        }
      } while (nextBackOff(sleeper, backoff));
      LOG.warn("Unable to poll job status: {}, aborting after {} retries.",
          jobId, MAX_LOAD_JOB_POLL_RETRIES);
      return Status.UNKNOWN;
    }

    /**
     * Identical to {@link BackOffUtils#next} but without checked IOException.
     * @throws InterruptedException
     */
    private boolean nextBackOff(Sleeper sleeper, BackOff backoff) throws InterruptedException {
      try {
        return BackOffUtils.next(sleeper, backoff);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }
}
