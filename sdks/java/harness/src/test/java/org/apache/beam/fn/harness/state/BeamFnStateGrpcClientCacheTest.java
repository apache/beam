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
package org.apache.beam.fn.harness.state;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.fail;

import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.StateRequest;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.StateResponse;
import org.apache.beam.model.fnexecution.v1.BeamFnStateGrpc;
import org.apache.beam.model.fnexecution.v1.BeamFnStateGrpc.BeamFnStateImplBase;
import org.apache.beam.model.pipeline.v1.Endpoints;
import org.apache.beam.sdk.fn.IdGenerators;
import org.apache.beam.sdk.fn.channel.ManagedChannelFactory;
import org.apache.beam.sdk.fn.stream.OutboundObserverFactory;
import org.apache.beam.sdk.fn.test.TestExecutors;
import org.apache.beam.sdk.fn.test.TestExecutors.TestExecutorService;
import org.apache.beam.sdk.fn.test.TestStreams;
import org.apache.beam.vendor.grpc.v1p60p1.io.grpc.Server;
import org.apache.beam.vendor.grpc.v1p60p1.io.grpc.Status;
import org.apache.beam.vendor.grpc.v1p60p1.io.grpc.StatusRuntimeException;
import org.apache.beam.vendor.grpc.v1p60p1.io.grpc.inprocess.InProcessServerBuilder;
import org.apache.beam.vendor.grpc.v1p60p1.io.grpc.stub.CallStreamObserver;
import org.apache.beam.vendor.grpc.v1p60p1.io.grpc.stub.StreamObserver;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.util.concurrent.Uninterruptibles;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link BeamFnStateGrpcClientCache}. */
@RunWith(JUnit4.class)
public class BeamFnStateGrpcClientCacheTest {
  private static final String SUCCESS = "SUCCESS";
  private static final String FAIL = "FAIL";
  private static final String TEST_ERROR = "TEST ERROR";
  private static final String SERVER_ERROR = "SERVER ERROR";

  @Rule public TestExecutorService executor = TestExecutors.from(Executors::newCachedThreadPool);

  private Endpoints.ApiServiceDescriptor apiServiceDescriptor;
  private Server testServer;
  private BeamFnStateGrpcClientCache clientCache;
  private BlockingQueue<StreamObserver<StateResponse>> outboundServerObservers;
  private BlockingQueue<StateRequest> values;

  @Before
  public void setUp() throws Exception {
    values = new LinkedBlockingQueue<>();
    outboundServerObservers = new LinkedBlockingQueue<>();
    CallStreamObserver<StateRequest> inboundServerObserver =
        TestStreams.withOnNext(values::add).build();

    apiServiceDescriptor =
        Endpoints.ApiServiceDescriptor.newBuilder()
            .setUrl(this.getClass().getName() + "-" + UUID.randomUUID())
            .build();
    testServer =
        InProcessServerBuilder.forName(apiServiceDescriptor.getUrl())
            .addService(
                new BeamFnStateGrpc.BeamFnStateImplBase() {
                  @Override
                  public StreamObserver<StateRequest> state(
                      StreamObserver<StateResponse> outboundObserver) {
                    Uninterruptibles.putUninterruptibly(outboundServerObservers, outboundObserver);
                    return inboundServerObserver;
                  }
                })
            .build();
    testServer.start();

    clientCache =
        new BeamFnStateGrpcClientCache(
            IdGenerators.decrementingLongs(),
            ManagedChannelFactory.createInProcess(),
            OutboundObserverFactory.trivial());
  }

  @After
  public void tearDown() throws Exception {
    testServer.shutdownNow();
  }

  @Test
  public void testCachingOfClient() throws Exception {
    Endpoints.ApiServiceDescriptor otherApiServiceDescriptor =
        Endpoints.ApiServiceDescriptor.newBuilder()
            .setUrl(apiServiceDescriptor.getUrl() + "-other")
            .build();
    Server testServer2 =
        InProcessServerBuilder.forName(otherApiServiceDescriptor.getUrl())
            .addService(
                new BeamFnStateImplBase() {
                  @Override
                  public StreamObserver<StateRequest> state(
                      StreamObserver<StateResponse> outboundObserver) {
                    throw new RuntimeException();
                  }
                })
            .build();
    testServer2.start();

    try {
      assertSame(
          clientCache.forApiServiceDescriptor(apiServiceDescriptor),
          clientCache.forApiServiceDescriptor(apiServiceDescriptor));
      assertNotSame(
          clientCache.forApiServiceDescriptor(apiServiceDescriptor),
          clientCache.forApiServiceDescriptor(otherApiServiceDescriptor));
    } finally {
      testServer2.shutdownNow();
    }
  }

  @Test
  public void testRequestResponses() throws Exception {
    BeamFnStateClient client = clientCache.forApiServiceDescriptor(apiServiceDescriptor);

    CompletableFuture<StateResponse> successfulResponse =
        client.handle(StateRequest.newBuilder().setInstructionId(SUCCESS));
    CompletableFuture<StateResponse> unsuccessfulResponse =
        client.handle(StateRequest.newBuilder().setInstructionId(FAIL));

    // Wait for the client to connect.
    StreamObserver<StateResponse> outboundServerObserver = outboundServerObservers.take();
    // Ensure the client doesn't break when sent garbage.
    outboundServerObserver.onNext(StateResponse.newBuilder().setId("UNKNOWN ID").build());

    // We expect to receive and handle two requests
    handleServerRequest(outboundServerObserver, values.take());
    handleServerRequest(outboundServerObserver, values.take());

    // Ensure that the successful and unsuccessful responses were propagated.
    assertNotNull(successfulResponse.get());
    try {
      unsuccessfulResponse.get();
      fail("Expected unsuccessful response");
    } catch (ExecutionException e) {
      assertThat(e.toString(), containsString(TEST_ERROR));
    }
  }

  @Test
  // The checker erroneously flags that the CompletableFuture is not being resolved since it is the
  // result to Executor#submit.
  @SuppressWarnings("FutureReturnValueIgnored")
  public void testServerErrorCausesPendingAndFutureCallsToFail() throws Exception {
    BeamFnStateClient client = clientCache.forApiServiceDescriptor(apiServiceDescriptor);

    Future<CompletableFuture<StateResponse>> stateResponse =
        executor.submit(() -> client.handle(StateRequest.newBuilder().setInstructionId(SUCCESS)));
    Future<Void> serverResponse =
        executor.submit(
            () -> {
              // Wait for the client to connect.
              StreamObserver<StateResponse> outboundServerObserver = outboundServerObservers.take();
              // Send an error from the server.
              outboundServerObserver.onError(
                  new StatusRuntimeException(Status.INTERNAL.withDescription(SERVER_ERROR)));
              return null;
            });

    CompletableFuture<StateResponse> inflight = stateResponse.get();
    serverResponse.get();
    try {
      inflight.get();
      fail("Expected unsuccessful response due to server error");
    } catch (ExecutionException e) {
      assertThat(e.toString(), containsString(SERVER_ERROR));
    }
  }

  @Test
  // The checker erroneously flags that the CompletableFuture is not being resolved since it is the
  // result to Executor#submit.
  @SuppressWarnings("FutureReturnValueIgnored")
  public void testServerCompletionCausesPendingAndFutureCallsToFail() throws Exception {
    BeamFnStateClient client = clientCache.forApiServiceDescriptor(apiServiceDescriptor);

    Future<CompletableFuture<StateResponse>> stateResponse =
        executor.submit(() -> client.handle(StateRequest.newBuilder().setInstructionId(SUCCESS)));
    Future<Void> serverResponse =
        executor.submit(
            () -> {
              // Wait for the client to connect.
              StreamObserver<StateResponse> outboundServerObserver = outboundServerObservers.take();
              // Send that the server is done.
              outboundServerObserver.onCompleted();
              return null;
            });

    CompletableFuture<StateResponse> inflight = stateResponse.get();
    serverResponse.get();
    try {
      inflight.get();
      fail("Expected unsuccessful response due to server error");
    } catch (ExecutionException e) {
      assertThat(e.toString(), containsString("Server hanged up"));
    }
  }

  private void handleServerRequest(
      StreamObserver<StateResponse> outboundObserver, StateRequest value) {
    switch (value.getInstructionId()) {
      case SUCCESS:
        outboundObserver.onNext(StateResponse.newBuilder().setId(value.getId()).build());
        return;
      case FAIL:
        outboundObserver.onNext(
            StateResponse.newBuilder().setId(value.getId()).setError(TEST_ERROR).build());
        return;
      default:
        outboundObserver.onNext(StateResponse.newBuilder().setId(value.getId()).build());
        return;
    }
  }
}
