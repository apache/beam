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

package com.google.cloud.dataflow.sdk.runners;

/**
 * Signals that a job run by a {@link BlockingDataflowPipelineRunner} was updated during execution.
 */
public class DataflowJobUpdatedException extends DataflowJobException {
  private DataflowPipelineJob replacedByJob;

  /**
   * Create a new {@code DataflowJobUpdatedException} with the specified original {@link
   * DataflowPipelineJob}, message, and replacement {@link DataflowPipelineJob}.
   */
  public DataflowJobUpdatedException(
      DataflowPipelineJob job, String message, DataflowPipelineJob replacedByJob) {
    this(job, message, replacedByJob, null);
  }

  /**
   * Create a new {@code DataflowJobUpdatedException} with the specified original {@link
   * DataflowPipelineJob}, message, replacement {@link DataflowPipelineJob}, and cause.
   */
  public DataflowJobUpdatedException(
      DataflowPipelineJob job, String message, DataflowPipelineJob replacedByJob, Throwable cause) {
    super(job, message, cause);
    this.replacedByJob = replacedByJob;
  }

  /**
   * The new job that replaces the job terminated with this exception.
   */
  public DataflowPipelineJob getReplacedByJob() {
    return replacedByJob;
  }
}

