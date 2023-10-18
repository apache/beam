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

import com.google.dataflow.v1beta3.GetStageExecutionDetailsRequest;
import com.google.dataflow.v1beta3.Job;
import com.google.dataflow.v1beta3.MetricsV1Beta3Grpc;
import com.google.dataflow.v1beta3.StageExecutionDetails;
import com.google.dataflow.v1beta3.WorkerDetails;
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
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Strings;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Throwables;
import org.checkerframework.checker.nullness.qual.MonotonicNonNull;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.joda.time.Duration;
import org.joda.time.Instant;

/**
 * {@link PTransform} for executing {@link GetStageExecutionDetailsRequest}s using the {@link
 * MetricsV1Beta3Grpc} client. Emits {@link WorkerDetailsWithAppendedDetails} or {@link
 * DataflowRequestError}s.
 */
@Internal
public class DataflowGetStageExecutionDetails
    extends PTransform<
        @NonNull PCollection<Job>,
        @NonNull DataflowReadResult<WorkerDetailsWithAppendedDetails, DataflowRequestError>> {

  public static DataflowGetStageExecutionDetails create(
      DataflowClientFactoryConfiguration configuration) {
    return new DataflowGetStageExecutionDetails(configuration);
  }

  private static final TupleTag<WorkerDetailsWithAppendedDetails> SUCCESS =
      new TupleTag<WorkerDetailsWithAppendedDetails>() {};

  private static final TupleTag<DataflowRequestError> FAILURE =
      new TupleTag<DataflowRequestError>() {};

  private final DataflowClientFactoryConfiguration configuration;

  private DataflowGetStageExecutionDetails(DataflowClientFactoryConfiguration configuration) {
    this.configuration = configuration;
  }

  @Override
  public @NonNull DataflowReadResult<WorkerDetailsWithAppendedDetails, DataflowRequestError> expand(
      PCollection<Job> input) {

    PCollectionTuple pct =
        input
            .apply(
                Throttle.class.getSimpleName()
                    + " "
                    + DataflowGetStageExecutionDetails.class.getSimpleName(),
                Throttle.of(
                    DataflowGetStageExecutionDetails.class.getName(), Duration.standardSeconds(1L)))
            .apply(
                DataflowGetStageExecutionDetails.class.getSimpleName(),
                ParDo.of(new GetStageExecutionDetailsFn(this))
                    .withOutputTags(SUCCESS, TupleTagList.of(FAILURE)));
    return DataflowReadResult.of(SUCCESS, FAILURE, pct);
  }

  private static class GetStageExecutionDetailsFn
      extends DoFn<Job, WorkerDetailsWithAppendedDetails> {

    final Counter success =
        Metrics.counter(
            GetStageExecutionDetailsRequest.class, "get_stage_execution_details_success");
    final Counter failure =
        Metrics.counter(
            GetStageExecutionDetailsRequest.class, "get_stage_execution_details_failure");

    final Counter items = Metrics.counter(WorkerDetails.class, "stage_execution_details_items");
    private final DataflowGetStageExecutionDetails spec;
    private transient MetricsV1Beta3Grpc.@MonotonicNonNull MetricsV1Beta3BlockingStub client;

    private GetStageExecutionDetailsFn(DataflowGetStageExecutionDetails spec) {
      this.spec = spec;
    }

    @Setup
    public void setup() {
      client = DataflowClientFactory.INSTANCE.getOrCreateMetricsClient(spec.configuration);
    }

    @ProcessElement
    public void process(@Element Job job, MultiOutputReceiver receiver) {
      GetStageExecutionDetailsRequest request =
          GetStageExecutionDetailsRequest.getDefaultInstance()
              .toBuilder()
              .setJobId(job.getId())
              .setProjectId(job.getProjectId())
              .setLocation(job.getLocation())
              .build();
      try {
        StageExecutionDetails response =
            checkStateNotNull(client).getStageExecutionDetails(request);
        success.inc();
        items.inc(response.getWorkersCount());
        emitResponse(job, response, receiver.get(SUCCESS));
        while (!Strings.isNullOrEmpty(response.getNextPageToken())) {
          GetStageExecutionDetailsRequest requestWithPageToken =
              request.toBuilder().setPageToken(response.getNextPageToken()).build();
          response = client.getStageExecutionDetails(requestWithPageToken);
          success.inc();
          emitResponse(job, response, receiver.get(SUCCESS));
        }
      } catch (StatusRuntimeException e) {
        failure.inc();
        receiver
            .get(FAILURE)
            .output(
                DataflowRequestError.fromRequest(request, GetStageExecutionDetailsRequest.class)
                    .setObservedTime(Instant.now())
                    .setMessage(Optional.ofNullable(e.getMessage()).orElse(""))
                    .setStackTrace(Throwables.getStackTraceAsString(e))
                    .build());
      }
    }

    private static void emitResponse(
        @NonNull Job job,
        @NonNull StageExecutionDetails response,
        @NonNull OutputReceiver<WorkerDetailsWithAppendedDetails> receiver) {

      for (WorkerDetails details : response.getWorkersList()) {
        Instant createTime = Instant.ofEpochSecond(job.getCreateTime().getSeconds());
        WorkerDetailsWithAppendedDetails result = new WorkerDetailsWithAppendedDetails();
        result.setJobId(job.getId());
        result.setJobCreateTime(createTime);
        result.setWorkerDetails(details);
        receiver.output(result);
      }
    }
  }
}
