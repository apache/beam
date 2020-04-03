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
package org.apache.beam.runners.fnexecution.jobsubmission;

import org.apache.beam.model.jobmanagement.v1.JobApi;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.runners.fnexecution.provisioning.JobInfo;
import org.apache.beam.sdk.PipelineResult;

/** Result of a portable {@link PortablePipelineRunner#run(RunnerApi.Pipeline, JobInfo)}. */
public interface PortablePipelineResult extends PipelineResult {

  /**
   * Returns the object to access monitoring infos from the pipeline.
   *
   * @throws UnsupportedOperationException if the runner doesn't support retrieving metrics.
   */
  JobApi.MetricResults portableMetrics() throws UnsupportedOperationException;
}
