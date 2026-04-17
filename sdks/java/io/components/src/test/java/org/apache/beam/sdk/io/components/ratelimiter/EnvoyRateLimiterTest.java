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
package org.apache.beam.sdk.io.components.ratelimiter;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.verify;

import io.envoyproxy.envoy.service.ratelimit.v3.RateLimitRequest;
import io.envoyproxy.envoy.service.ratelimit.v3.RateLimitResponse;
import io.envoyproxy.envoy.service.ratelimit.v3.RateLimitServiceGrpc;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.Status;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.StreamObserver;
import java.io.IOException;
import org.apache.beam.sdk.util.Sleeper;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

/** Tests for {@link EnvoyRateLimiterFactory}. */
@RunWith(JUnit4.class)
public class EnvoyRateLimiterTest {
  @Mock private Sleeper sleeper;

  private EnvoyRateLimiterFactory factory;
  private RateLimiterOptions options;
  private EnvoyRateLimiterContext context;

  private Server server;
  private ManagedChannel channel;
  private TestRateLimitService service;

  @Before
  public void setUp() throws Exception {
    MockitoAnnotations.openMocks(this);
    options =
        RateLimiterOptions.builder()
            .setAddress("localhost:8081")
            .setTimeout(java.time.Duration.ofSeconds(1))
            .build();

    String serverName = InProcessServerBuilder.generateName();
    service = new TestRateLimitService();
    server =
        InProcessServerBuilder.forName(serverName)
            .directExecutor()
            .addService(service)
            .build()
            .start();
    channel = InProcessChannelBuilder.forName(serverName).directExecutor().build();

    factory = new EnvoyRateLimiterFactory(options, sleeper);
    factory.setStub(RateLimitServiceGrpc.newBlockingStub(channel));

    context =
        EnvoyRateLimiterContext.builder()
            .setDomain("test-domain")
            .addDescriptor("key", "value")
            .build();
  }

  @After
  public void tearDown() {
    if (channel != null) {
      channel.shutdownNow();
    }
    if (server != null) {
      server.shutdownNow();
    }
  }

  @Test
  public void testAllow_OK() throws Exception {
    service.responseToReturn =
        RateLimitResponse.newBuilder().setOverallCode(RateLimitResponse.Code.OK).build();

    assertTrue(factory.allow(context, 1));
  }

  @Test
  public void testAllow_OverLimit() throws Exception {
    service.responseToReturn =
        RateLimitResponse.newBuilder()
            .setOverallCode(RateLimitResponse.Code.OVER_LIMIT)
            .addStatuses(
                RateLimitResponse.DescriptorStatus.newBuilder()
                    .setCode(RateLimitResponse.Code.OVER_LIMIT)
                    .setDurationUntilReset(
                        com.google.protobuf.Duration.newBuilder().setSeconds(1).build())
                    .build())
            .build();

    factory =
        new EnvoyRateLimiterFactory(
            RateLimiterOptions.builder()
                .setAddress("foo")
                .setTimeout(java.time.Duration.ofSeconds(1))
                .setMaxRetries(1)
                .build(),
            sleeper);
    factory.setStub(RateLimitServiceGrpc.newBlockingStub(channel));

    assertFalse(factory.allow(context, 1));

    // Verify sleep was called.
    verify(sleeper, org.mockito.Mockito.atLeastOnce()).sleep(anyLong());
  }

  @Test
  public void testAllow_RpcError() throws Exception {
    service.errorToThrow = Status.UNAVAILABLE.asRuntimeException();
    assertThrows(IOException.class, () -> factory.allow(context, 1));
  }

  @Test
  public void testInvalidContext() {
    assertThrows(
        IllegalArgumentException.class, () -> factory.allow(new RateLimiterContext() {}, 1));
  }

  static class TestRateLimitService extends RateLimitServiceGrpc.RateLimitServiceImplBase {
    volatile RateLimitResponse responseToReturn;
    volatile RuntimeException errorToThrow;

    @Override
    public void shouldRateLimit(
        RateLimitRequest request, StreamObserver<RateLimitResponse> responseObserver) {
      if (errorToThrow != null) {
        responseObserver.onError(errorToThrow);
        return;
      }
      if (responseToReturn != null) {
        responseObserver.onNext(responseToReturn);
        responseObserver.onCompleted();
      } else {
        // Default OK
        responseObserver.onNext(
            RateLimitResponse.newBuilder().setOverallCode(RateLimitResponse.Code.OK).build());
        responseObserver.onCompleted();
      }
    }
  }
}
