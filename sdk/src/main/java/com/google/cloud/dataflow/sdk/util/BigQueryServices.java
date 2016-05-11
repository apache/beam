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

import com.google.api.services.bigquery.model.Job;
import com.google.api.services.bigquery.model.JobConfigurationExtract;
import com.google.api.services.bigquery.model.JobConfigurationLoad;
import com.google.api.services.bigquery.model.JobConfigurationQuery;
import com.google.cloud.dataflow.sdk.options.BigQueryOptions;

import java.io.IOException;
import java.io.Serializable;

/**
 * An interface for real, mock, or fake implementations of Cloud BigQuery services.
 */
public interface BigQueryServices extends Serializable {

  /**
   * Returns a real, mock, or fake {@link JobService}.
   */
  public JobService getJobService(BigQueryOptions bqOptions);

  /**
   * An interface for the Cloud BigQuery load service.
   */
  public interface JobService {
    /**
     * Starts a BigQuery load job.
     */
    void startLoadJob(String jobId, JobConfigurationLoad loadConfig)
        throws InterruptedException, IOException;

    /**
     * Start a BigQuery extract job.
     */
    void startExtractJob(String jobId, JobConfigurationExtract extractConfig)
        throws InterruptedException, IOException;

    /**
     * Start a BigQuery extract job.
     */
    void startQueryJob(String jobId, JobConfigurationQuery query, boolean dryRun)
        throws IOException, InterruptedException;

    /**
     * Waits for the job is Done, and returns the job.
     *
     * <p>Returns null if the {@code maxAttempts} retries reached.
     */
    Job pollJob(String projectId, String jobId, int maxAttempts)
        throws InterruptedException, IOException;
  }
}
