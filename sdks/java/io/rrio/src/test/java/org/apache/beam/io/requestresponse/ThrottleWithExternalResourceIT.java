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
package org.apache.beam.io.requestresponse;

import static org.apache.beam.io.requestresponse.EchoITOptions.GRPC_ENDPOINT_ADDRESS_NAME;
import static org.apache.beam.sdk.io.common.IOITHelper.readIOTestPipelineOptions;
import static org.apache.beam.sdk.util.Preconditions.checkStateNotNull;
import static org.apache.beam.sdk.values.TypeDescriptors.strings;

import com.google.protobuf.ByteString;
import java.net.URI;
import java.util.UUID;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.Coder.NonDeterministicException;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.TestPipelineOptions;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.Distinct;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PeriodicImpulse;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.testinfra.mockapis.echo.v1.Echo.EchoRequest;
import org.apache.beam.testinfra.mockapis.echo.v1.Echo.EchoResponse;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Strings;
import org.checkerframework.checker.nullness.qual.MonotonicNonNull;
import org.joda.time.Duration;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;

/**
 * Integration tests for {@link ThrottleWithExternalResource}. See {@link EchoITOptions} for details
 * on the required parameters and how to provide these for running integration tests.
 */
public class ThrottleWithExternalResourceIT {

  @Rule public TestPipeline pipeline = TestPipeline.create();

  private static final String QUOTA_ID = "echo-ThrottleWithExternalResourceTestIT-10-per-1s-quota";
  private static final Quota QUOTA = new Quota(1L, Duration.standardSeconds(1L));
  private static final ByteString PAYLOAD = ByteString.copyFromUtf8("payload");
  private static @MonotonicNonNull EchoITOptions options;
  private static @MonotonicNonNull EchoGRPCCallerWithSetupTeardown client;
  private static final String CONTAINER_IMAGE_NAME = "redis:5.0.3-alpine";
  private static final Integer PORT = 6379;
  private static final EchoRequestCoder REQUEST_CODER = new EchoRequestCoder();
  private static final Coder<EchoResponse> RESPONSE_CODER =
      SerializableCoder.of(TypeDescriptor.of(EchoResponse.class));

  @Rule
  public GenericContainer<?> redis =
      new GenericContainer<>(DockerImageName.parse(CONTAINER_IMAGE_NAME)).withExposedPorts(PORT);

  @BeforeClass
  public static void setUp() throws UserCodeExecutionException {
    options = readIOTestPipelineOptions(EchoITOptions.class);
    if (Strings.isNullOrEmpty(options.getGrpcEndpointAddress())) {
      throw new RuntimeException(
          "--"
              + GRPC_ENDPOINT_ADDRESS_NAME
              + " is missing. See "
              + EchoITOptions.class
              + "for details.");
    }
    client = EchoGRPCCallerWithSetupTeardown.of(URI.create(options.getGrpcEndpointAddress()));
    checkStateNotNull(client).setup();

    try {
      client.call(createRequest());
    } catch (UserCodeExecutionException e) {
      if (e instanceof UserCodeQuotaException) {
        throw new RuntimeException(
            String.format(
                "The quota: %s is set to refresh on an interval. Unless there are failures in this test, wait for a few seconds before running the test again.",
                QUOTA_ID),
            e);
      }
      throw e;
    }
  }

  @AfterClass
  public static void tearDown() throws UserCodeExecutionException {
    checkStateNotNull(client).teardown();
  }

  @Test
  public void givenThrottleUsingRedis_preventsQuotaErrors() throws NonDeterministicException {
    URI uri =
        URI.create(String.format("redis://%s:%d", redis.getHost(), redis.getFirstMappedPort()));
    pipeline.getOptions().as(TestPipelineOptions.class).setBlockOnRun(false);

    Call.Result<EchoRequest> throttleResult =
        createRequestStream()
            .apply(
                "throttle",
                ThrottleWithExternalResource.usingRedis(
                    uri, QUOTA_ID, UUID.randomUUID().toString(), QUOTA, REQUEST_CODER));

    // Assert that we are not getting any errors and successfully emitting throttled elements.
    PAssert.that(throttleResult.getFailures()).empty();
    PAssert.thatSingleton(
            throttleResult
                .getResponses()
                .apply(
                    "window throttled", Window.into(FixedWindows.of(Duration.standardSeconds(1L))))
                .apply(
                    "count throttled",
                    Combine.globally(Count.<EchoRequest>combineFn()).withoutDefaults()))
        .notEqualTo(0L);

    // Assert that all the throttled data is not corrupted.
    PAssert.that(
            throttleResult
                .getResponses()
                .apply(
                    "window throttled before extraction",
                    Window.into(FixedWindows.of(Duration.standardSeconds(1L))))
                .apply(
                    "extract request id",
                    MapElements.into(strings()).via(input -> checkStateNotNull(input).getId()))
                .apply("distinct", Distinct.create()))
        .containsInAnyOrder(QUOTA_ID);

    // Call the Echo service with throttled requests.
    Call.Result<EchoResponse> echoResult =
        throttleResult
            .getResponses()
            .apply("call", Call.ofCallerAndSetupTeardown(client, RESPONSE_CODER));

    // Assert that there were no errors.
    PAssert.that(echoResult.getFailures()).empty();

    // Assert that the responses match the requests.
    PAssert.that(
            echoResult
                .getResponses()
                .apply(
                    "window responses before extraction",
                    Window.into(FixedWindows.of(Duration.standardSeconds(1L))))
                .apply(
                    "extract response id",
                    MapElements.into(strings()).via(input -> checkStateNotNull(input).getId())))
        .containsInAnyOrder(QUOTA_ID);

    PipelineResult job = pipeline.run();
    job.waitUntilFinish(Duration.standardSeconds(3L));
  }

  private static EchoRequest createRequest() {
    return EchoRequest.newBuilder().setId(QUOTA_ID).setPayload(PAYLOAD).build();
  }

  private PCollection<EchoRequest> createRequestStream() {
    return pipeline
        .apply("impulse", PeriodicImpulse.create().withInterval(Duration.millis(10L)))
        .apply(
            "requests",
            MapElements.into(TypeDescriptor.of(EchoRequest.class)).via(ignored -> createRequest()));
  }
}
