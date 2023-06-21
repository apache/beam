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

import com.google.dataflow.v1beta3.GetJobExecutionDetailsRequest;
import com.google.dataflow.v1beta3.Job;
import com.google.dataflow.v1beta3.JobExecutionDetails;
import com.google.dataflow.v1beta3.MetricsV1Beta3Grpc;
import com.google.dataflow.v1beta3.StageSummary;
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
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Strings;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Throwables;
import org.checkerframework.checker.nullness.qual.MonotonicNonNull;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.joda.time.Duration;
import org.joda.time.Instant;

/**
 * {@link PTransform} for executing {@link GetJobExecutionDetailsRequest}s using the {@link
 * MetricsV1Beta3Grpc} client. Emits {@link StageSummaryWithAppendedDetails} or {@link
 * DataflowRequestError}s.
 */
@Internal
public class DataflowGetJobExecutionDetails
    extends PTransform<
        @NonNull PCollection<Job>,
        @NonNull DataflowReadResult<StageSummaryWithAppendedDetails, DataflowRequestError>> {

  private static final TupleTag<StageSummaryWithAppendedDetails> SUCCESS =
      new TupleTag<StageSummaryWithAppendedDetails>() {};

  private static final TupleTag<DataflowRequestError> FAILURE =
      new TupleTag<DataflowRequestError>() {};

  private final DataflowClientFactoryConfiguration configuration;

  public static DataflowGetJobExecutionDetails create(
      DataflowClientFactoryConfiguration configuration) {
    return new DataflowGetJobExecutionDetails(configuration);
  }

  private DataflowGetJobExecutionDetails(DataflowClientFactoryConfiguration configuration) {
    this.configuration = configuration;
  }

  @Override
  public @NonNull DataflowReadResult<StageSummaryWithAppendedDetails, DataflowRequestError> expand(
      PCollection<Job> input) {

    PCollectionTuple pct =
        input
            .apply(
                Throttle.class.getSimpleName()
                    + " "
                    + DataflowGetJobExecutionDetails.class.getSimpleName(),
                Throttle.of(
                    DataflowGetJobExecutionDetails.class.getName(), Duration.standardSeconds(1L)))
            .apply(
                DataflowGetJobExecutionDetails.class.getSimpleName(),
                ParDo.of(new GetJobExecutionDetailsFn(this))
                    .withOutputTags(SUCCESS, TupleTagList.of(FAILURE)));

    return DataflowReadResult.of(SUCCESS, FAILURE, pct);
  }

  private static class GetJobExecutionDetailsFn extends DoFn<Job, StageSummaryWithAppendedDetails> {
    final Counter success =
        Metrics.counter(GetJobExecutionDetailsRequest.class, "get_job_execution_details_success");
    final Counter failure =
        Metrics.counter(GetJobExecutionDetailsRequest.class, "get_job_execution_details_failure");
    final Counter items = Metrics.counter(StageSummary.class, "job_execution_details_items");
    private final DataflowGetJobExecutionDetails spec;
    private transient MetricsV1Beta3Grpc.@MonotonicNonNull MetricsV1Beta3BlockingStub client;

    private GetJobExecutionDetailsFn(DataflowGetJobExecutionDetails spec) {
      this.spec = spec;
    }

    @Setup
    public void setup() {
      client = DataflowClientFactory.INSTANCE.getOrCreateMetricsClient(spec.configuration);
    }

    @ProcessElement
    public void process(@Element Job job, MultiOutputReceiver receiver) {
      GetJobExecutionDetailsRequest request =
          GetJobExecutionDetailsRequest.newBuilder()
              .setJobId(job.getId())
              .setProjectId(job.getProjectId())
              .setLocation(job.getLocation())
              .build();
      try {
        JobExecutionDetails response = checkStateNotNull(client).getJobExecutionDetails(request);
        success.inc();
        items.inc(response.getStagesCount());
        emitResponse(job, response, receiver.get(SUCCESS));
        while (!Strings.isNullOrEmpty(response.getNextPageToken())) {
          GetJobExecutionDetailsRequest requestWithPageToken =
              request.toBuilder().setPageToken(response.getNextPageToken()).build();
          response = client.getJobExecutionDetails(requestWithPageToken);
          success.inc();
          items.inc(response.getStagesCount());
          emitResponse(job, response, receiver.get(SUCCESS));
        }
      } catch (StatusRuntimeException e) {
        failure.inc();
        receiver
            .get(FAILURE)
            .output(
                DataflowRequestError.fromRequest(request, GetJobExecutionDetailsRequest.class)
                    .setObservedTime(Instant.now())
                    .setMessage(Optional.ofNullable(e.getMessage()).orElse(""))
                    .setStackTrace(Throwables.getStackTraceAsString(e))
                    .build());
      }
    }
  }

  private static void emitResponse(
      @NonNull Job job,
      @NonNull JobExecutionDetails response,
      DoFn.OutputReceiver<StageSummaryWithAppendedDetails> receiver) {
    Instant createTime = Instant.ofEpochSecond(job.getCreateTime().getSeconds());
    for (StageSummary summary : response.getStagesList()) {
      StageSummaryWithAppendedDetails result = new StageSummaryWithAppendedDetails();
      result.setJobId(job.getId());
      result.setJobCreateTime(createTime);
      result.setStageSummary(summary);
      receiver.output(result);
    }
  }
}
