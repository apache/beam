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

import com.google.dataflow.v1beta3.GetJobMetricsRequest;
import com.google.dataflow.v1beta3.Job;
import com.google.dataflow.v1beta3.JobMetrics;
import com.google.dataflow.v1beta3.MetricsV1Beta3Grpc;
import io.grpc.StatusRuntimeException;
import java.util.Optional;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Throwables;
import org.checkerframework.checker.nullness.qual.MonotonicNonNull;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.joda.time.Instant;

public class DataflowGetJobMetrics
    extends PTransform<
        @NonNull PCollection<Job>,
        @NonNull DataflowReadResult<JobMetricsWithAppendedDetails, DataflowRequestError>> {

  public static DataflowGetJobMetrics create(DataflowClientFactoryConfiguration configuration) {
    return new DataflowGetJobMetrics(configuration);
  }

  private static final TupleTag<JobMetricsWithAppendedDetails> SUCCESS =
      new TupleTag<JobMetricsWithAppendedDetails>() {};

  private static final TupleTag<DataflowRequestError> FAILURE =
      new TupleTag<DataflowRequestError>() {};

  private final DataflowClientFactoryConfiguration configuration;

  private DataflowGetJobMetrics(DataflowClientFactoryConfiguration configuration) {
    this.configuration = configuration;
  }

  @Override
  public @NonNull DataflowReadResult<JobMetricsWithAppendedDetails, DataflowRequestError> expand(
      PCollection<Job> input) {

    PCollectionTuple pct =
        input.apply(
            DataflowGetJobMetrics.class.getSimpleName(),
            ParDo.of(new GetJobMetricsFn(this)).withOutputTags(SUCCESS, TupleTagList.of(FAILURE)));

    return DataflowReadResult.of(SUCCESS, FAILURE, pct);
  }

  private static class GetJobMetricsFn extends DoFn<Job, JobMetricsWithAppendedDetails> {

    private final DataflowGetJobMetrics spec;
    private transient MetricsV1Beta3Grpc.@MonotonicNonNull MetricsV1Beta3BlockingStub client;

    private GetJobMetricsFn(DataflowGetJobMetrics spec) {
      this.spec = spec;
    }

    @Setup
    public void setup() {
      client = DataflowClientFactory.createMetricsClient(spec.configuration);
    }

    @ProcessElement
    public void process(@Element Job job, MultiOutputReceiver receiver) {
      GetJobMetricsRequest request =
          GetJobMetricsRequest.newBuilder()
              .setJobId(job.getId())
              .setProjectId(job.getProjectId())
              .setLocation(job.getLocation())
              .build();
      try {
        JobMetrics response = checkStateNotNull(client).getJobMetrics(request);
        com.google.protobuf.Timestamp timestamp = job.getCreateTime();
        JobMetricsWithAppendedDetails result = new JobMetricsWithAppendedDetails();
        result.setJobId(request.getJobId());
        result.setJobCreateTime(Instant.ofEpochSecond(timestamp.getSeconds()));
        result.setJobMetrics(response);

        receiver.get(SUCCESS).output(result);

      } catch (StatusRuntimeException e) {
        receiver
            .get(FAILURE)
            .output(
                DataflowRequestError.fromRequest(request, GetJobMetricsRequest.class)
                    .setObservedTime(Instant.now())
                    .setMessage(Optional.ofNullable(e.getMessage()).orElse(""))
                    .setStackTrace(Throwables.getStackTraceAsString(e))
                    .build());
      }
    }
  }
}
