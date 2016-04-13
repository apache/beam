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

import com.google.api.services.bigquery.model.JobConfigurationLoad;
import com.google.cloud.dataflow.sdk.options.BigQueryOptions;

import java.io.IOException;
import java.io.Serializable;

/**
 * An interface for real, mock, or fake implementations of Cloud BigQuery services.
 */
public interface BigQueryServices extends Serializable {

  /**
   * Status of a BigQuery job or request.
   */
  enum Status {
    SUCCEEDED,
    FAILED,
    UNKNOWN,
  }

  /**
   * Returns a real, mock, or fake {@link LoadService}.
   */
  public LoadService getLoadService(BigQueryOptions bqOptions);

  /**
   * An interface for the Cloud BigQuery load service.
   */
  public interface LoadService {
    /**
     * Start a BigQuery load job.
     */
    public void startLoadJob(String jobId, JobConfigurationLoad loadConfig)
        throws InterruptedException, IOException;

    /**
     * Poll the status of a BigQuery load job.
     */
    public Status pollJobStatus(String projectId, String jobId)
        throws InterruptedException, IOException;
  }
}
