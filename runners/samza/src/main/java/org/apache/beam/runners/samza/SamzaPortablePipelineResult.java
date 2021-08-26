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
package org.apache.beam.runners.samza;

import org.apache.beam.model.jobmanagement.v1.JobApi;
import org.apache.beam.runners.jobsubmission.PortablePipelineResult;
import org.apache.samza.application.StreamApplication;
import org.apache.samza.config.Config;
import org.apache.samza.runtime.ApplicationRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** The result from executing a Samza Portable Pipeline. */
public class SamzaPortablePipelineResult extends SamzaPipelineResult
    implements PortablePipelineResult {

  private static final Logger LOG = LoggerFactory.getLogger(SamzaPortablePipelineResult.class);

  SamzaPortablePipelineResult(
      StreamApplication app,
      ApplicationRunner runner,
      SamzaExecutionContext executionContext,
      SamzaPipelineLifeCycleListener listener,
      Config config) {
    super(app, runner, executionContext, listener, config);
  }

  @Override
  public JobApi.MetricResults portableMetrics() throws UnsupportedOperationException {
    LOG.warn("Collecting monitoring infos is not implemented yet in Samza portable runner.");
    return JobApi.MetricResults.newBuilder().build();
  }
}
