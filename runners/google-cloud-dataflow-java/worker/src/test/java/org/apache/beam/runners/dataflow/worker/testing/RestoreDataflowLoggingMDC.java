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
package org.apache.beam.runners.dataflow.worker.testing;

import org.apache.beam.runners.dataflow.worker.logging.DataflowWorkerLoggingMDC;
import org.junit.rules.ExternalResource;

/** Saves, clears and restores the current thread-local logging parameters for tests. */
public class RestoreDataflowLoggingMDC extends ExternalResource {
  private String previousJobId;
  private String previousStageName;
  private String previousWorkerId;
  private String previousWorkId;

  public RestoreDataflowLoggingMDC() {}

  @Override
  protected void before() throws Throwable {
    previousJobId = DataflowWorkerLoggingMDC.getJobId();
    previousStageName = DataflowWorkerLoggingMDC.getStageName();
    previousWorkerId = DataflowWorkerLoggingMDC.getWorkerId();
    previousWorkId = DataflowWorkerLoggingMDC.getWorkId();
    DataflowWorkerLoggingMDC.setJobId(null);
    DataflowWorkerLoggingMDC.setStageName(null);
    DataflowWorkerLoggingMDC.setWorkerId(null);
    DataflowWorkerLoggingMDC.setWorkId(null);
  }

  @Override
  protected void after() {
    DataflowWorkerLoggingMDC.setJobId(previousJobId);
    DataflowWorkerLoggingMDC.setStageName(previousStageName);
    DataflowWorkerLoggingMDC.setWorkerId(previousWorkerId);
    DataflowWorkerLoggingMDC.setWorkId(previousWorkId);
  }
}
