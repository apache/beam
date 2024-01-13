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

import com.google.protobuf.ByteString;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.testinfra.mockapis.echo.v1.Echo.EchoRequest;
import org.apache.beam.testinfra.mockapis.echo.v1.Echo.EchoResponse;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Strings;
import org.checkerframework.checker.nullness.qual.MonotonicNonNull;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;

/**
 * Integration tests for {@link RequestResponseIO}. See {@link EchoITOptions} for details on the
 * required parameters and how to provide these for running integration tests.
 */
public class RequestResponseIOIT {
  @Rule public TestPipeline pipeline = TestPipeline.create();

  private static @MonotonicNonNull EchoITOptions options;
  private static @MonotonicNonNull EchoGRPCCallerWithSetupTeardown client;
  private static final ByteString PAYLOAD = ByteString.copyFromUtf8("payload");

  @BeforeClass
  public static void setUp() throws UserCodeExecutionException {
    options = readIOTestPipelineOptions(EchoITOptions.class);
    if (Strings.isNullOrEmpty(options.getGrpcEndpointAddress())) {
      throw new RuntimeException(
          "--"
              + GRPC_ENDPOINT_ADDRESS_NAME
              + " is missing. See "
              + EchoITOptions.class
              + " for details.");
    }
    client = EchoGRPCCallerWithSetupTeardown.of(URI.create(options.getGrpcEndpointAddress()));
  }

  @Test
  public void givenMinimalConfiguration_executesRequests() {
    Result<EchoResponse> response =
        createShouldNeverExceedQuotaRequestPCollection(10L)
            .apply(
                "echo",
                RequestResponseIO.ofCallerAndSetupTeardown(client, new EchoResponseCoder()));

    PAssert.that(response.getFailures()).empty();
    PAssert.thatSingleton(response.getResponses().apply("count", Count.globally())).isEqualTo(10L);

    pipeline.run();
  }

  private PCollection<EchoRequest> createShouldNeverExceedQuotaRequestPCollection(long size) {
    List<EchoRequest> requests = new ArrayList<>();
    for (long i = 0; i < size; i++) {
      requests.add(createShouldNeverExceedQuotaRequest());
    }
    return pipeline.apply("generate", Create.of(requests));
  }

  private static @NonNull EchoRequest createShouldNeverExceedQuotaRequest() {
    return EchoRequest.newBuilder()
        .setPayload(PAYLOAD)
        .setId(checkStateNotNull(options).getNeverExceedQuotaId())
        .build();
  }
}
