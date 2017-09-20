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

package org.apache.beam.runners.core.construction;

import static com.google.common.base.Preconditions.checkArgument;

import io.grpc.ManagedChannelBuilder;
import java.io.IOException;
import java.util.Iterator;
import org.apache.beam.runners.core.construction.JobApiRunner.JobApiPipelineResult;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.PipelineRunner;
import org.apache.beam.sdk.common.runner.v1.JobApi.CancelJobRequest;
import org.apache.beam.sdk.common.runner.v1.JobApi.GetJobStateRequest;
import org.apache.beam.sdk.common.runner.v1.JobApi.GetJobStateResponse;
import org.apache.beam.sdk.common.runner.v1.JobApi.JobState.JobStateType;
import org.apache.beam.sdk.common.runner.v1.JobApi.SubmitJobRequest;
import org.apache.beam.sdk.common.runner.v1.JobApi.SubmitJobResponse;
import org.apache.beam.sdk.common.runner.v1.JobServiceGrpc;
import org.apache.beam.sdk.common.runner.v1.JobServiceGrpc.JobServiceBlockingStub;
import org.apache.beam.sdk.metrics.MetricResults;
import org.apache.beam.sdk.options.PipelineOptions;
import org.joda.time.Duration;

/**
 * A {@link PipelineRunner} that executes a {@link Pipeline} by submitting it to a Job API Service
 * endpoint.
 */
public class JobApiRunner extends PipelineRunner<JobApiPipelineResult> {
  /** Create a new {@link JobApiRunner} from the provided {@link PipelineOptions}. */
  public static JobApiRunner fromOptions(PipelineOptions options) {
    return new JobApiRunner(options);
  }

  private final JobServiceBlockingStub service;
  private final PipelineOptions options;

  private JobApiRunner(PipelineOptions options) {
    this.options = options;
    BeamJobApiOptions jobApiOptions = options.as(BeamJobApiOptions.class);
    this.service = createBlockingStub(jobApiOptions);
  }

  private JobServiceBlockingStub createBlockingStub(BeamJobApiOptions jobApiOptions) {
    // TODO: make this easier to manage
    ManagedChannelBuilder<?> channelBuilder;
    if (jobApiOptions.getJobServiceTarget() == null) {
      checkArgument(
          jobApiOptions.getJobServiceHost() != null,
          "JobServiceHost must be provided if JobServiceTarget is not set");
      checkArgument(
          jobApiOptions.getJobServicePort() != null,
          "JobServicePort must be provided if JobServiceTarget is not set");
      channelBuilder =
          ManagedChannelBuilder.forAddress(
              jobApiOptions.getJobServiceHost(), jobApiOptions.getJobServicePort());
    } else {
      channelBuilder = ManagedChannelBuilder.forTarget(jobApiOptions.getJobServiceTarget());
    }
    channelBuilder.usePlaintext(true);
    return JobServiceGrpc.newBlockingStub(channelBuilder.build());
  }

  @Override
  public JobApiPipelineResult run(Pipeline pipeline) {
    SubmitJobResponse submittedJob =
        service.run(
            SubmitJobRequest.newBuilder()
                .setPipeline(PipelineTranslation.toProto(pipeline))
                .setPipelineOptions(PipelineOptionsTranslation.toProto(options))
                .setJobName(options.getJobName())
                .build());
    return new JobApiPipelineResult(service, submittedJob.getJobId());
  }

  /**
   * A {@link PipelineResult} implemented by calling into a {@link JobServiceGrpc Job Service}
   * endpoint.
   */
  public static class JobApiPipelineResult implements PipelineResult {
    private final JobServiceBlockingStub service;
    private final String jobId;

    private JobApiPipelineResult(JobServiceBlockingStub service, String jobId) {
      this.service = service;
      this.jobId = jobId;
    }

    @Override
    public State getState() {
      return stateFromProto(
          service.getState(GetJobStateRequest.newBuilder().setJobId(jobId).build()).getState());
    }

    @Override
    public State cancel() throws IOException {
      return stateFromProto(
          service.cancel(CancelJobRequest.newBuilder().setJobId(jobId).build()).getState());
    }

    @Override
    public State waitUntilFinish(Duration duration) {
      throw new UnsupportedOperationException("WaitUntilFinish does not yet support timeouts");
    }

    @Override
    public State waitUntilFinish() {
      Iterator<GetJobStateResponse> states =
          service.getStateStream(GetJobStateRequest.newBuilder().setJobId(jobId).build());
      State state;
      do {
        state = stateFromProto(states.next().getState());
      } while (!state.isTerminal());
      return state;
    }

    @Override
    public MetricResults metrics() {
      throw new UnsupportedOperationException("metrics are not yet supported over the Job API");
    }

    private State stateFromProto(JobStateType state) {
      switch (state) {
        case STARTING:
        case RUNNING:
          return State.RUNNING;
        case DRAINED:
        case DONE:
          return State.DONE;
        case UPDATED:
          return State.UPDATED;
        case CANCELLED:
          return State.CANCELLED;
        case FAILED:
          return State.FAILED;
        case STOPPED:
          return State.STOPPED;
        case UNKNOWN:
        case UNRECOGNIZED:
        case CANCELLING:
        case DRAINING:
          // States which don't have a known type in the Java JobResult but are non-terminal can
          // be returned as UNKNOWN.
          return State.UNKNOWN;
        default:
          throw new IllegalArgumentException(String.format("Unknown Job State %s", state));
      }
    }
  }
}
