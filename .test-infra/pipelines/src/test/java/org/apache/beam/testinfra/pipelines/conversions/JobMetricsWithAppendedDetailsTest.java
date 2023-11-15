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
package org.apache.beam.testinfra.pipelines.conversions;

import static org.apache.beam.sdk.util.Preconditions.checkStateNotNull;
import static org.apache.beam.sdk.values.TypeDescriptors.rows;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.google.dataflow.v1beta3.JobMetrics;
import com.google.dataflow.v1beta3.MetricUpdate;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Timestamp;
import com.google.protobuf.Value;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.function.Function;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.testinfra.pipelines.dataflow.JobMetricsWithAppendedDetails;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.joda.time.Instant;
import org.joda.time.ReadableDateTime;
import org.junit.jupiter.api.Test;

/** Tests for {@link WithAppendedDetailsToRow} of {@link JobMetricsWithAppendedDetails}. */
class JobMetricsWithAppendedDetailsTest
    extends WithAppendedDetailsToRowTest<JobMetricsWithAppendedDetails, JobMetrics> {

  @Override
  WithAppendedDetailsToRow<JobMetricsWithAppendedDetails, JobMetrics> transform() {
    return WithAppendedDetailsToRow.jobMetricsWithAppendedDetailsToRow();
  }

  @Override
  Descriptors.Descriptor embeddedTypeDescriptor() {
    return JobMetrics.getDescriptor();
  }

  @Override
  String embeddedTypeFieldName() {
    return "job_metrics";
  }

  @Override
  Function<JobMetricsWithAppendedDetails, String> jobIdGetter() {
    return JobMetricsWithAppendedDetails::getJobId;
  }

  @Override
  Function<JobMetricsWithAppendedDetails, Instant> createTimeGetter() {
    return JobMetricsWithAppendedDetails::getJobCreateTime;
  }

  @Override
  @NonNull
  List<@NonNull JobMetricsWithAppendedDetails> input() {
    JobMetricsWithAppendedDetails details = new JobMetricsWithAppendedDetails();
    details.setJobId("job_id_value");
    details.setJobCreateTime(Instant.ofEpochSecond(1000L));
    details.setJobMetrics(
        JobMetrics.getDefaultInstance()
            .toBuilder()
            .addMetrics(
                MetricUpdate.getDefaultInstance()
                    .toBuilder()
                    .setUpdateTime(Timestamp.newBuilder().setSeconds(10000L).build())
                    .setScalar(Value.newBuilder().setNumberValue(1.23456))
                    .build())
            .build());
    return ImmutableList.of(details);
  }

  @Test
  void jobMetrics() {

    Schema jobMetricsSchema = expectedEmbeddedSchema();

    Pipeline pipeline = Pipeline.create();

    PCollection<JobMetricsWithAppendedDetails> input = pipeline.apply(Create.of(input()));

    RowConversionResult<JobMetricsWithAppendedDetails, ConversionError> result =
        input.apply(transform());

    PAssert.thatSingleton(result.getFailure().apply("count errors", Count.globally()))
        .isEqualTo(0L);

    PCollection<Row> jobMetrics =
        result
            .getSuccess()
            .apply(
                "job_metrics",
                MapElements.into(rows())
                    .via(row -> checkStateNotNull(row.getRow(embeddedTypeFieldName()))))
            .setRowSchema(jobMetricsSchema);

    PAssert.thatSingleton(jobMetrics.apply("count job_metrics", Count.globally())).isEqualTo(1L);

    jobMetrics.apply(
        "metrics",
        ParDo.of(
            new DoFn<Row, Void>() {
              @ProcessElement
              public void process(@Element Row row) {
                Collection<Row> metrics = checkStateNotNull(row.getArray("metrics"));
                assertEquals(1, metrics.size());
                Row metricsUpdate = checkStateNotNull(new ArrayList<>(metrics).get(0));
                ReadableDateTime timestamp =
                    checkStateNotNull(metricsUpdate.getDateTime("update_time"));
                assertEquals(10000000L, timestamp.getMillis());
                String scaler = checkStateNotNull(metricsUpdate.getString("scalar"));
                assertEquals("1.23456", scaler);
              }
            }));

    pipeline.run();
  }
}
