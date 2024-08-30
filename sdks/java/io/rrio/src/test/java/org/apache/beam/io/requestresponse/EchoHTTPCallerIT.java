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

import static org.apache.beam.io.requestresponse.EchoITOptions.HTTP_ENDPOINT_ADDRESS_NAME;
import static org.apache.beam.sdk.io.common.IOITHelper.readIOTestPipelineOptions;
import static org.apache.beam.sdk.util.Preconditions.checkStateNotNull;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import com.google.protobuf.ByteString;
import java.net.URI;
import org.apache.beam.testinfra.mockapis.echo.v1.Echo.EchoRequest;
import org.apache.beam.testinfra.mockapis.echo.v1.Echo.EchoResponse;
import org.apache.beam.testinfra.mockapis.echo.v1.EchoServiceGrpc;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Strings;
import org.checkerframework.checker.nullness.qual.MonotonicNonNull;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Tests for {@link EchoHTTPCallerIT} on a deployed {@link EchoServiceGrpc} instance's HTTP handler.
 * See {@link EchoITOptions} for details on the required parameters and how to provide these for
 * running integration tests.
 */
@RunWith(JUnit4.class)
public class EchoHTTPCallerIT {

  private static @MonotonicNonNull EchoITOptions options;
  private static @MonotonicNonNull EchoHTTPCaller client;
  private static final ByteString PAYLOAD = ByteString.copyFromUtf8("payload");

  @BeforeClass
  public static void setUp() throws UserCodeExecutionException {
    options = readIOTestPipelineOptions(EchoITOptions.class);
    if (Strings.isNullOrEmpty(options.getHttpEndpointAddress())) {
      throw new RuntimeException(
          "--"
              + HTTP_ENDPOINT_ADDRESS_NAME
              + " is missing. See "
              + EchoITOptions.class
              + "for details.");
    }
    client = EchoHTTPCaller.of(URI.create(options.getHttpEndpointAddress()));

    EchoRequest request = createShouldExceedQuotaRequest();

    // The challenge with building and deploying a real quota aware endpoint, the integration with
    // which these tests validate, is that we need a value of at least 1. The allocated quota where
    // we expect to exceed will be shared among many tests and across languages. Code below in this
    // setup ensures that the API is in the state where we can expect a quota exceeded error. There
    // are tests in this file that detect errors in expected responses. We only throw exceptions
    // that are not UserCodeQuotaException.
    try {
      EchoResponse ignored = client.call(request);
      client.call(request);
      client.call(request);
      client.call(request);
    } catch (UserCodeExecutionException e) {
      if (!(e instanceof UserCodeQuotaException)) {
        throw e;
      }
    }
  }

  @Test
  public void givenValidRequest_receivesResponse() throws UserCodeExecutionException {
    EchoRequest request = createShouldNeverExceedQuotaRequest();
    EchoResponse response = client.call(request);
    assertEquals(response.getId(), request.getId());
    assertEquals(response.getPayload(), request.getPayload());
  }

  @Test
  public void givenExceededQuota_shouldThrow() {
    assertThrows(UserCodeQuotaException.class, () -> client.call(createShouldExceedQuotaRequest()));
  }

  @Test
  public void givenNotFound_shouldThrow() {
    UserCodeExecutionException error =
        assertThrows(
            UserCodeExecutionException.class,
            () ->
                client.call(
                    EchoRequest.newBuilder()
                        .setId("i-dont-exist-quota-id")
                        .setPayload(PAYLOAD)
                        .build()));

    assertTrue(error.getMessage().contains("404 Not Found"));
  }

  private static @NonNull EchoRequest createShouldNeverExceedQuotaRequest() {
    return EchoRequest.newBuilder()
        .setPayload(PAYLOAD)
        .setId(checkStateNotNull(options).getNeverExceedQuotaId())
        .build();
  }

  private static @NonNull EchoRequest createShouldExceedQuotaRequest() {
    return EchoRequest.newBuilder()
        .setPayload(PAYLOAD)
        .setId(checkStateNotNull(options).getShouldExceedQuotaId())
        .build();
  }
}
