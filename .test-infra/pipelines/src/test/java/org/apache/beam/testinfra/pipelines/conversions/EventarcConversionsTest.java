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
import static org.apache.beam.sdk.values.TypeDescriptors.booleans;
import static org.apache.beam.sdk.values.TypeDescriptors.strings;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import com.google.events.cloud.dataflow.v1beta3.Job;
import com.google.events.cloud.dataflow.v1beta3.JobState;
import com.google.events.cloud.dataflow.v1beta3.JobType;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.WithFailures;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.junit.jupiter.api.Test;

/** Tests for {@link org.apache.beam.testinfra.pipelines.conversions.EventarcConversions}. */
class EventarcConversionsTest {

  private static final EventarcConversions.JsonToJobFn FROM_JSON_FN =
      new EventarcConversions.JsonToJobFn();

  @Test
  void fromJson_emptyStrings_emitsAsConversionErrors() {
    Pipeline pipeline = Pipeline.create();
    WithFailures.Result<@NonNull PCollection<Job>, ConversionError> result =
        pipeline.apply(Create.of("")).apply(EventarcConversions.fromJson());
    PAssert.that(result.output()).empty();
    PAssert.thatSingleton(
            result
                .failures()
                .apply(
                    "get error message",
                    MapElements.into(strings())
                        .via(error -> checkStateNotNull(error).getMessage())))
        .isEqualTo("json input missing path: $.data");

    pipeline.run();
  }

  @Test
  void fromJson_malformedJson_emitsAsConversionErrors() {
    Pipeline pipeline = Pipeline.create();
    WithFailures.Result<@NonNull PCollection<Job>, ConversionError> result =
        pipeline.apply(Create.of("{\"foo\", \"bar\"")).apply(EventarcConversions.fromJson());
    PAssert.that(result.output()).empty();
    PAssert.thatSingleton(
            result
                .failures()
                .apply(
                    "contains message part",
                    MapElements.into(booleans())
                        .via(
                            error ->
                                checkStateNotNull(error)
                                    .getMessage()
                                    .contains("JsonParseException"))))
        .isEqualTo(true);

    pipeline.run();
  }

  @Test
  void fromJson_missingDataKey_emitsAsConversionErrors() {
    Pipeline pipeline = Pipeline.create();
    WithFailures.Result<@NonNull PCollection<Job>, ConversionError> result =
        pipeline.apply(Create.of("{}")).apply(EventarcConversions.fromJson());
    PAssert.that(result.output()).empty();
    PAssert.thatSingleton(
            result
                .failures()
                .apply(
                    "get error message",
                    MapElements.into(strings())
                        .via(error -> checkStateNotNull(error).getMessage())))
        .isEqualTo("json input missing path: $.data");

    pipeline.run();
  }

  @Test
  void fromJson_missingTypeKey_emitsAsConversionErrors() {
    Pipeline pipeline = Pipeline.create();
    WithFailures.Result<@NonNull PCollection<Job>, ConversionError> result =
        pipeline.apply(Create.of("{\"data\":{}}")).apply(EventarcConversions.fromJson());
    PAssert.that(result.output()).empty();
    PAssert.thatSingleton(
            result
                .failures()
                .apply(
                    "get error message",
                    MapElements.into(strings())
                        .via(error -> checkStateNotNull(error).getMessage())))
        .isEqualTo("json input missing path: $.data.@type");

    pipeline.run();
  }

  @Test
  void fromJson_typeMismatch_emitsAsConversionErrors() {
    Pipeline pipeline = Pipeline.create();
    WithFailures.Result<@NonNull PCollection<Job>, ConversionError> result =
        pipeline
            .apply(Create.of("{\"data\":{\"payload\": {},\"@type\": \"bad.type\"}}"))
            .apply(EventarcConversions.fromJson());
    PAssert.that(result.output()).empty();
    PAssert.thatSingleton(
            result
                .failures()
                .apply(
                    "get error message",
                    MapElements.into(strings())
                        .via(error -> checkStateNotNull(error).getMessage())))
        .isEqualTo(
            "expected @type=type.googleapis.com/google.events.cloud.dataflow.v1beta3.JobEventData at json path: $.data.@type, got: bad.type");

    pipeline.run();
  }

  @Test
  void fromJson_missingPayloadKey_emitsAsConversionErrors() {
    Pipeline pipeline = Pipeline.create();
    WithFailures.Result<@NonNull PCollection<Job>, ConversionError> result =
        pipeline
            .apply(
                Create.of(
                    "{\"data\":{\"@type\": \"type.googleapis.com/google.events.cloud.dataflow.v1beta3.JobEventData\"}}"))
            .apply(EventarcConversions.fromJson());
    PAssert.that(result.output()).empty();
    PAssert.thatSingleton(
            result
                .failures()
                .apply(
                    "get error message",
                    MapElements.into(strings())
                        .via(error -> checkStateNotNull(error).getMessage())))
        .isEqualTo("json input missing path: $.data.payload");

    pipeline.run();
  }

  @Test
  void fromJson_hasUnexpectedProperty_emitsAsConversionErrors() throws IOException {
    String resourceName = "eventarc_data/has_extra_data_payload_foo_property.json";
    Pipeline pipeline = Pipeline.create();
    WithFailures.Result<@NonNull PCollection<Job>, ConversionError> result =
        readJsonThenApplyConversion(resourceName, pipeline);
    PAssert.that(result.output()).empty();
    PAssert.thatSingleton(
            result
                .failures()
                .apply(
                    "get error message",
                    MapElements.into(strings())
                        .via(error -> checkStateNotNull(error).getMessage())))
        .isEqualTo(
            "com.google.protobuf.InvalidProtocolBufferException: Cannot find field: foo in message google.events.cloud.dataflow.v1beta3.Job");

    pipeline.run();
  }

  @Test
  void fromJson_JobStateCanceledStreaming_emitsJob() throws IOException {
    String resourceName = "eventarc_data/job_state_canceled_streaming.json";
    Pipeline pipeline = Pipeline.create();
    WithFailures.Result<@NonNull PCollection<Job>, ConversionError> result =
        readJsonThenApplyConversion(resourceName, pipeline);

    PAssert.thatSingleton(
            result
                .output()
                .apply(
                    "get current state",
                    MapElements.into(TypeDescriptor.of(JobState.class))
                        .via(job -> checkStateNotNull(job).getCurrentState())))
        .isEqualTo(JobState.JOB_STATE_CANCELLED);

    PAssert.thatSingleton(
            result
                .output()
                .apply(
                    "get job type",
                    MapElements.into(TypeDescriptor.of(JobType.class))
                        .via(job -> checkStateNotNull(job).getType())))
        .isEqualTo(JobType.JOB_TYPE_STREAMING);

    PAssert.thatSingleton(
            result
                .output()
                .apply(
                    "get job id",
                    MapElements.into(strings()).via(job -> checkStateNotNull(job).getId())))
        .isEqualTo("2023-05-09_13_23_50-11065941757886660214");

    PAssert.that(result.failures()).empty();

    pipeline.run();
  }

  @Test
  void jobStateCanceledStreaming_JsonToDataFn() throws IOException {
    String payload = loadResource("eventarc_data/job_state_canceled_streaming.json");
    Job actual = FROM_JSON_FN.apply(payload);
    assertNotNull(actual);
    assertEquals(JobState.JOB_STATE_CANCELLED, actual.getCurrentState());
    assertEquals(JobType.JOB_TYPE_STREAMING, actual.getType());
    assertEquals("2023-05-09_13_23_50-11065941757886660214", actual.getId());
  }

  @Test
  void jobStateCancelingStreaming_JsonToDataFn() throws IOException {
    String payload = loadResource("eventarc_data/job_state_canceling_streaming.json");
    Job actual = FROM_JSON_FN.apply(payload);
    assertNotNull(actual);
    assertEquals(JobState.JOB_STATE_CANCELLING, actual.getCurrentState());
    assertEquals(JobType.JOB_TYPE_STREAMING, actual.getType());
    assertEquals("2023-05-09_13_23_50-11065941757886660214", actual.getId());
  }

  @Test
  void jobStateDoneBatch_JsonToDataFn() throws IOException {
    String payload = loadResource("eventarc_data/job_state_done_batch.json");
    Job actual = FROM_JSON_FN.apply(payload);
    assertNotNull(actual);
    assertEquals(JobState.JOB_STATE_DONE, actual.getCurrentState());
    assertEquals(JobType.JOB_TYPE_BATCH, actual.getType());
    assertEquals("2023-05-09_13_39_13-18226864771788319755", actual.getId());
  }

  private static WithFailures.Result<@NonNull PCollection<Job>, ConversionError>
      readJsonThenApplyConversion(String resourcePath, Pipeline pipeline) throws IOException {

    String payload = loadResource(resourcePath);
    PCollection<String> json = pipeline.apply(Create.of(payload));

    return json.apply(EventarcConversions.fromJson());
  }

  private static String loadResource(String resourceName) throws IOException {
    Path resourcePath = Paths.get("build", "resources", "test", resourceName);
    byte[] bytes = Files.readAllBytes(resourcePath);
    return new String(bytes, StandardCharsets.UTF_8);
  }
}
