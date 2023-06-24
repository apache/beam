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
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.google.events.cloud.dataflow.v1beta3.Job;
import com.google.events.cloud.dataflow.v1beta3.JobState;
import com.google.events.cloud.dataflow.v1beta3.JobType;
import java.nio.file.Path;
import java.nio.file.Paths;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.WithFailures;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.testinfra.pipelines.conversions.ConversionError;
import org.apache.beam.testinfra.pipelines.conversions.EventarcConversions;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.junit.jupiter.api.Test;

/** Tests for {@link DataflowFilterEventarcJobs}. */
class DataflowFilterEventarcJobsTest {

  private static final String JSON_RESOURCE_PATH = "eventarc_data/job_state*.json";

  @Test
  void filterBatchJobTypeOnly_excludesStreamingJobs() {
    Pipeline pipeline = Pipeline.create();

    PCollection<String> json = readJsonAndCheckNotEmpty(pipeline);
    WithFailures.Result<@NonNull PCollection<Job>, ConversionError> jobs =
        json.apply(EventarcConversions.fromJson());
    PCollection<Job> result =
        jobs.output()
            .apply(
                DataflowFilterEventarcJobs.builder()
                    .setIncludeJobType(JobType.JOB_TYPE_BATCH)
                    .build());
    PAssert.that(
            result.apply(
                "get job type",
                MapElements.into(TypeDescriptor.of(JobType.class))
                    .via(job -> checkStateNotNull(job).getType())))
        .satisfies(
            itr -> {
              itr.forEach(jobType -> assertEquals(JobType.JOB_TYPE_BATCH, jobType));
              return null;
            });

    pipeline.run();
  }

  @Test
  void filterBatchTerminatedOnly_includesDoneJobs() {
    Pipeline pipeline = Pipeline.create();

    PCollection<String> json = readJsonAndCheckNotEmpty(pipeline);
    WithFailures.Result<@NonNull PCollection<Job>, ConversionError> jobs =
        json.apply(EventarcConversions.fromJson());
    PCollection<Job> result =
        jobs.output()
            .apply(
                DataflowFilterEventarcJobs.builder()
                    .setIncludeJobType(JobType.JOB_TYPE_BATCH)
                    .terminatedOnly()
                    .build());

    PAssert.thatSingleton(
            result.apply(
                "filter batch",
                MapElements.into(TypeDescriptor.of(JobType.class))
                    .via(job -> checkStateNotNull(job).getType())))
        .isEqualTo(JobType.JOB_TYPE_BATCH);

    PAssert.thatSingleton(
            result.apply(
                "filter done",
                MapElements.into(TypeDescriptor.of(JobState.class))
                    .via(job -> checkStateNotNull(job).getCurrentState())))
        .isEqualTo(JobState.JOB_STATE_DONE);

    pipeline.run();
  }

  @Test
  void filterStreamTerminatedOnly_includesCanceledJobs() {
    Pipeline pipeline = Pipeline.create();

    PCollection<String> json = readJsonAndCheckNotEmpty(pipeline);
    WithFailures.Result<@NonNull PCollection<Job>, ConversionError> jobs =
        json.apply(EventarcConversions.fromJson());
    PCollection<Job> result =
        jobs.output()
            .apply(
                DataflowFilterEventarcJobs.builder()
                    .setIncludeJobType(JobType.JOB_TYPE_STREAMING)
                    .terminatedOnly()
                    .build());

    PAssert.thatSingleton(
            result.apply(
                "get type",
                MapElements.into(TypeDescriptor.of(JobType.class))
                    .via(job -> checkStateNotNull(job).getType())))
        .isEqualTo(JobType.JOB_TYPE_STREAMING);

    PAssert.thatSingleton(
            result.apply(
                "get current state",
                MapElements.into(TypeDescriptor.of(JobState.class))
                    .via(job -> checkStateNotNull(job).getCurrentState())))
        .isEqualTo(JobState.JOB_STATE_CANCELLED);

    pipeline.run();
  }

  private static PCollection<String> readJsonAndCheckNotEmpty(Pipeline pipeline) {
    Path resourcePath = Paths.get("build", "resources", "test", JSON_RESOURCE_PATH);
    PCollection<String> json =
        pipeline.apply(TextIO.read().from(resourcePath.toAbsolutePath().toString()));
    PAssert.thatSingleton(json.apply(Count.globally())).notEqualTo(0L);

    return json;
  }
}
