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

package com.google.cloud.dataflow.sdk.testing;

import com.google.cloud.dataflow.sdk.runners.worker.logging.DataflowWorkerLoggingMDC;

import org.junit.rules.ExternalResource;

/**
 * Saves and restores the current thread-local logging parameters for tests.
 */
public class RestoreDataflowLoggingMDC extends ExternalResource {
  private String previousJobId;
  private String previousStageName;
  private String previousStepName;
  private String previousWorkerId;
  private String previousWorkId;

  public RestoreDataflowLoggingMDC() {}

  @Override
  protected void before() throws Throwable {
    previousJobId = DataflowWorkerLoggingMDC.getJobId();
    previousStageName = DataflowWorkerLoggingMDC.getStageName();
    previousStepName = DataflowWorkerLoggingMDC.getStepName();
    previousWorkerId = DataflowWorkerLoggingMDC.getWorkerId();
    previousWorkId = DataflowWorkerLoggingMDC.getWorkId();
  }

  @Override
  protected void after() {
    DataflowWorkerLoggingMDC.setJobId(previousJobId);
    DataflowWorkerLoggingMDC.setStageName(previousStageName);
    DataflowWorkerLoggingMDC.setStepName(previousStepName);
    DataflowWorkerLoggingMDC.setWorkerId(previousWorkerId);
    DataflowWorkerLoggingMDC.setWorkId(previousWorkId);
  }
}
