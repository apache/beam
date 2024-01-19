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
package org.apache.beam.testinfra.pipelines.dataflow;

import static org.apache.beam.sdk.util.Preconditions.checkStateNotNull;

import com.google.dataflow.v1beta3.GetJobRequest;
import com.google.dataflow.v1beta3.Job;
import com.google.dataflow.v1beta3.JobsV1Beta3Grpc;
import io.grpc.StatusRuntimeException;
import java.util.Optional;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Throwables;
import org.checkerframework.checker.nullness.qual.MonotonicNonNull;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.joda.time.Duration;
import org.joda.time.Instant;

/**
 * {@link PTransform} for executing {@link GetJobRequest}s using the {@link JobsV1Beta3Grpc}. Emits
 * {@link Job}s or {@link DataflowRequestError}s.
 */
@Internal
public class DataflowGetJobs
    extends PTransform<
        @NonNull PCollection<GetJobRequest>,
        @NonNull DataflowReadResult<Job, DataflowRequestError>> {

  private static final TupleTag<Job> SUCCESS = new TupleTag<Job>() {};

  private static final TupleTag<DataflowRequestError> FAILURE =
      new TupleTag<DataflowRequestError>() {};

  public static DataflowGetJobs create(DataflowClientFactoryConfiguration configuration) {
    return new DataflowGetJobs(configuration);
  }

  private final DataflowClientFactoryConfiguration configuration;

  private DataflowGetJobs(@NonNull DataflowClientFactoryConfiguration configuration) {
    this.configuration = configuration;
  }

  @Override
  public @NonNull DataflowReadResult<Job, DataflowRequestError> expand(
      PCollection<GetJobRequest> input) {

    PCollectionTuple pct =
        input
            .apply(
                Throttle.class.getSimpleName() + " " + DataflowGetJobs.class.getSimpleName(),
                Throttle.of(DataflowGetJobs.class.getName(), Duration.standardSeconds(1L)))
            .apply(
                "GetJobs",
                ParDo.of(new GetJobsFn(this)).withOutputTags(SUCCESS, TupleTagList.of(FAILURE)));

    return DataflowReadResult.of(SUCCESS, FAILURE, pct);
  }

  private static class GetJobsFn extends DoFn<GetJobRequest, Job> {
    final Counter success = Metrics.counter(GetJobRequest.class, "get_jobs_success");
    final Counter failure = Metrics.counter(GetJobRequest.class, "get_jobs_failure");
    private final DataflowGetJobs spec;
    private transient JobsV1Beta3Grpc.@MonotonicNonNull JobsV1Beta3BlockingStub client;

    GetJobsFn(DataflowGetJobs spec) {
      this.spec = spec;
    }

    @Setup
    public void setup() {
      client = DataflowClientFactory.INSTANCE.getOrCreateJobsClient(spec.configuration);
    }

    @ProcessElement
    public void process(@Element GetJobRequest request, MultiOutputReceiver receiver) {
      try {

        Job job = checkStateNotNull(client).getJob(request);
        success.inc();
        receiver.get(SUCCESS).output(job);

      } catch (StatusRuntimeException e) {

        failure.inc();
        receiver
            .get(FAILURE)
            .output(
                DataflowRequestError.fromRequest(request, GetJobRequest.class)
                    .setObservedTime(Instant.now())
                    .setMessage(Optional.ofNullable(e.getMessage()).orElse(""))
                    .setStackTrace(Throwables.getStackTraceAsString(e))
                    .build());
      }
    }
  }
}
