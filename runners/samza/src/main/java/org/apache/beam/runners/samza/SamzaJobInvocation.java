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

import static org.apache.beam.model.jobmanagement.v1.JobApi.JobState.Enum.CANCELLED;
import static org.apache.beam.model.jobmanagement.v1.JobApi.JobState.Enum.DONE;
import static org.apache.beam.model.jobmanagement.v1.JobApi.JobState.Enum.FAILED;
import static org.apache.beam.model.jobmanagement.v1.JobApi.JobState.Enum.RUNNING;
import static org.apache.beam.model.jobmanagement.v1.JobApi.JobState.Enum.STARTING;
import static org.apache.beam.model.jobmanagement.v1.JobApi.JobState.Enum.STOPPED;
import static org.apache.beam.model.jobmanagement.v1.JobApi.JobState.Enum.UNRECOGNIZED;
import static org.apache.beam.model.jobmanagement.v1.JobApi.JobState.Enum.UPDATED;

import org.apache.beam.model.jobmanagement.v1.JobApi;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.runners.core.construction.graph.GreedyPipelineFuser;
import org.apache.beam.runners.jobsubmission.JobInvocation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Invocation of a Samza job via {@link SamzaRunner}. */
public class SamzaJobInvocation extends JobInvocation {
  private static final Logger LOG = LoggerFactory.getLogger(SamzaJobInvocation.class);

  private final SamzaPipelineOptions options;
  private final RunnerApi.Pipeline originalPipeline;
  private volatile SamzaPipelineResult pipelineResult;

  public SamzaJobInvocation(RunnerApi.Pipeline pipeline, SamzaPipelineOptions options) {
    super(null, null, pipeline, null);
    this.originalPipeline = pipeline;
    this.options = options;
  }

  private SamzaPipelineResult invokeSamzaJob() {
    // Fused pipeline proto.
    final RunnerApi.Pipeline fusedPipeline =
        GreedyPipelineFuser.fuse(originalPipeline).toPipeline();
    // the pipeline option coming from sdk will set the sdk specific runner which will break
    // serialization
    // so we need to reset the runner here to a valid Java runner
    options.setRunner(SamzaRunner.class);
    try {
      final SamzaRunner runner = SamzaRunner.fromOptions(options);
      return (SamzaPortablePipelineResult) runner.runPortablePipeline(fusedPipeline);
    } catch (Exception e) {
      throw new RuntimeException("Failed to invoke samza job", e);
    }
  }

  @Override
  public synchronized void start() {
    LOG.info("Starting job invocation {}", getId());
    pipelineResult = invokeSamzaJob();
  }

  @Override
  public String getId() {
    return options.getJobName();
  }

  @Override
  public synchronized void cancel() {
    try {
      if (pipelineResult != null) {
        LOG.info("Cancelling pipeline {}", getId());
        pipelineResult.cancel();
      }
    } catch (Exception e) {
      throw new RuntimeException("Failed to cancel job.", e);
    }
  }

  @Override
  public JobApi.JobState.Enum getState() {
    if (pipelineResult == null) {
      return STARTING;
    }
    switch (pipelineResult.getState()) {
      case RUNNING:
        return RUNNING;
      case FAILED:
        return FAILED;
      case DONE:
        return DONE;
      case STOPPED:
        return STOPPED;
      case UPDATED:
        return UPDATED;
      case CANCELLED:
        return CANCELLED;
      default:
        return UNRECOGNIZED;
    }
  }
}
