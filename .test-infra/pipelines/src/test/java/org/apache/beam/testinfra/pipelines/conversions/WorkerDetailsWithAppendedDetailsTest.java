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

import com.google.dataflow.v1beta3.MetricUpdate;
import com.google.dataflow.v1beta3.WorkItemDetails;
import com.google.dataflow.v1beta3.WorkerDetails;
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
import org.apache.beam.testinfra.pipelines.dataflow.WorkerDetailsWithAppendedDetails;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.joda.time.Instant;
import org.joda.time.ReadableDateTime;
import org.junit.jupiter.api.Test;

/** Tests for {@link WithAppendedDetailsToRow} of {@link WorkerDetailsWithAppendedDetails}. */
class WorkerDetailsWithAppendedDetailsTest
    extends WithAppendedDetailsToRowTest<WorkerDetailsWithAppendedDetails, WorkerDetails> {

  @Override
  WithAppendedDetailsToRow<WorkerDetailsWithAppendedDetails, WorkerDetails> transform() {
    return WithAppendedDetailsToRow.workerDetailsWithAppendedDetailsToRow();
  }

  @Override
  Descriptors.Descriptor embeddedTypeDescriptor() {
    return WorkerDetails.getDescriptor();
  }

  @Override
  String embeddedTypeFieldName() {
    return "worker_details";
  }

  @Override
  Function<WorkerDetailsWithAppendedDetails, String> jobIdGetter() {
    return WorkerDetailsWithAppendedDetails::getJobId;
  }

  @Override
  Function<WorkerDetailsWithAppendedDetails, Instant> createTimeGetter() {
    return WorkerDetailsWithAppendedDetails::getJobCreateTime;
  }

  @Override
  @NonNull
  List<@NonNull WorkerDetailsWithAppendedDetails> input() {
    WorkerDetailsWithAppendedDetails details = new WorkerDetailsWithAppendedDetails();
    details.setJobId("job_id_value");
    details.setJobCreateTime(Instant.ofEpochSecond(1000L));
    details.setWorkerDetails(
        WorkerDetails.getDefaultInstance()
            .toBuilder()
            .setWorkerName("worker_name_value")
            .addWorkItems(
                WorkItemDetails.getDefaultInstance()
                    .toBuilder()
                    .addMetrics(
                        MetricUpdate.getDefaultInstance()
                            .toBuilder()
                            .setUpdateTime(Timestamp.newBuilder().setSeconds(10000L).build())
                            .setScalar(Value.newBuilder().setNumberValue(1.23456))
                            .build())
                    .build())
            .build());
    return ImmutableList.of(details);
  }

  @Test
  void workerDetails() {

    Schema embeddedSchema = expectedEmbeddedSchema();

    Pipeline pipeline = Pipeline.create();

    PCollection<WorkerDetailsWithAppendedDetails> input = pipeline.apply(Create.of(input()));

    RowConversionResult<WorkerDetailsWithAppendedDetails, ConversionError> result =
        input.apply(transform());

    PAssert.thatSingleton(result.getFailure().apply("count errors", Count.globally()))
        .isEqualTo(0L);

    PCollection<Row> workDetails =
        result
            .getSuccess()
            .apply(
                "work_details",
                MapElements.into(rows())
                    .via(row -> checkStateNotNull(row).getRow(embeddedTypeFieldName())))
            .setRowSchema(embeddedSchema);

    PAssert.thatSingleton(workDetails.apply("count work_details", Count.globally())).isEqualTo(1L);

    workDetails.apply(
        "iterate work_details",
        ParDo.of(
            new DoFn<Row, Void>() {
              @ProcessElement
              public void process(@Element Row row) {
                String workerName = checkStateNotNull(row.getString("worker_name"));
                assertEquals("worker_name_value", workerName);
                Collection<Row> workItems = checkStateNotNull(row.getArray("work_items"));
                assertEquals(1, workItems.size());
                Row workItem = checkStateNotNull(new ArrayList<>(workItems).get(0));
                Collection<Row> metrics = checkStateNotNull(workItem.getArray("metrics"));
                Row metricsUpdate = checkStateNotNull(new ArrayList<>(metrics).get(0));
                ReadableDateTime timestamp =
                    checkStateNotNull(metricsUpdate.getDateTime("update_time"));
                assertEquals(10000L, timestamp.getMillis() / 1000);
                String scalar = checkStateNotNull(metricsUpdate.getString("scalar"));
                assertEquals("1.23456", scalar);
              }
            }));

    pipeline.run();
  }
}
