/*
 * Copyright (C) 2014 Google Inc.
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

package com.google.cloud.dataflow.sdk.testing;

import com.google.cloud.dataflow.sdk.runners.worker.logging.DataflowWorkerLoggingFormatter;

import org.junit.rules.ExternalResource;

/**
 * Saves and restores the current thread-local logging parameters for tests.
 */
public class RestoreDataflowLoggingFormatter extends ExternalResource {
  private String previousJobId;
  private String previousWorkerId;
  private String previousWorkId;

  public RestoreDataflowLoggingFormatter() {
  }

  @Override
  protected void before() throws Throwable {
    previousJobId = DataflowWorkerLoggingFormatter.getJobId();
    previousWorkerId = DataflowWorkerLoggingFormatter.getWorkerId();
    previousWorkId = DataflowWorkerLoggingFormatter.getWorkId();
  }

  @Override
  protected void after() {
    DataflowWorkerLoggingFormatter.setJobId(previousJobId);
    DataflowWorkerLoggingFormatter.setWorkerId(previousWorkerId);
    DataflowWorkerLoggingFormatter.setWorkId(previousWorkId);
  }
}
