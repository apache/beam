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
package org.apache.beam.runners.dataflow.worker.fn;

import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

import java.net.InetAddress;
import java.net.ServerSocket;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.beam.model.fnexecution.v1.BeamFnApi;
import org.apache.beam.model.fnexecution.v1.BeamFnControlGrpc;
import org.apache.beam.model.pipeline.v1.Endpoints;
import org.apache.beam.runners.dataflow.worker.fn.stream.ServerStreamObserverFactory;
import org.apache.beam.runners.fnexecution.GrpcContextHeaderAccessorProvider;
import org.apache.beam.runners.fnexecution.ServerFactory;
import org.apache.beam.runners.fnexecution.control.FnApiControlClient;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.vendor.grpc.v1p26p0.io.grpc.ManagedChannelBuilder;
import org.apache.beam.vendor.grpc.v1p26p0.io.grpc.stub.StreamObserver;
import org.apache.beam.vendor.grpc.v1p26p0.io.grpc.testing.GrpcCleanupRule;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.net.HostAndPort;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

/** Tests for {@link BeamFnControlService}. */
@RunWith(JUnit4.class)
public class BeamFnControlServiceTest {
  @Rule
  public GrpcCleanupRule grpcCleanupRule = new GrpcCleanupRule().setTimeout(10, TimeUnit.SECONDS);

  @Mock private StreamObserver<BeamFnApi.InstructionRequest> requestObserver;
  @Mock private StreamObserver<BeamFnApi.InstructionRequest> anotherRequestObserver;

  @Before
  public void setUp() {
    MockitoAnnotations.initMocks(this);
  }

  private Endpoints.ApiServiceDescriptor findOpenPort() throws Exception {
    InetAddress address = InetAddress.getLoopbackAddress();
    try (ServerSocket socket = new ServerSocket(0, -1, address)) {
      return Endpoints.ApiServiceDescriptor.newBuilder()
          .setUrl(HostAndPort.fromParts(address.getHostAddress(), socket.getLocalPort()).toString())
          .build();
    }
  }

  @Test
  public void testClientConnecting() throws Exception {
    CountDownLatch requestCompleted = new CountDownLatch(1);
    doAnswer(
            invocation -> {
              requestCompleted.countDown();
              return null;
            })
        .when(requestObserver)
        .onCompleted();

    PipelineOptions options = PipelineOptionsFactory.create();
    Endpoints.ApiServiceDescriptor descriptor = findOpenPort();
    BeamFnControlService service =
        new BeamFnControlService(
            descriptor,
            ServerStreamObserverFactory.fromOptions(options)::from,
            GrpcContextHeaderAccessorProvider.getHeaderAccessor());
    grpcCleanupRule.register(
        ServerFactory.createDefault().create(ImmutableList.of(service), descriptor));
    String url = service.getApiServiceDescriptor().getUrl();
    BeamFnControlGrpc.BeamFnControlStub clientStub =
        BeamFnControlGrpc.newStub(ManagedChannelBuilder.forTarget(url).usePlaintext().build());

    // Connect from the client.
    clientStub.control(requestObserver);
    try (FnApiControlClient client = service.get()) {
      assertNotNull(client);
    }

    requestCompleted.await(5, TimeUnit.SECONDS); // Wait until request streams have been closed.

    verify(requestObserver).onCompleted();
    verifyNoMoreInteractions(requestObserver);
  }

  @Test
  public void testMultipleClientsConnecting() throws Exception {
    CountDownLatch requestCompleted = new CountDownLatch(2);
    doAnswer(
            invocation -> {
              requestCompleted.countDown();
              return null;
            })
        .when(requestObserver)
        .onCompleted();
    doAnswer(
            invocation -> {
              requestCompleted.countDown();
              return null;
            })
        .when(anotherRequestObserver)
        .onCompleted();

    PipelineOptions options = PipelineOptionsFactory.create();
    Endpoints.ApiServiceDescriptor descriptor = findOpenPort();
    BeamFnControlService service =
        new BeamFnControlService(
            descriptor,
            ServerStreamObserverFactory.fromOptions(options)::from,
            GrpcContextHeaderAccessorProvider.getHeaderAccessor());
    grpcCleanupRule.register(
        ServerFactory.createDefault().create(ImmutableList.of(service), descriptor));

    String url = service.getApiServiceDescriptor().getUrl();
    BeamFnControlGrpc.BeamFnControlStub clientStub =
        BeamFnControlGrpc.newStub(ManagedChannelBuilder.forTarget(url).usePlaintext().build());
    BeamFnControlGrpc.BeamFnControlStub anotherClientStub =
        BeamFnControlGrpc.newStub(ManagedChannelBuilder.forTarget(url).usePlaintext().build());

    // Connect from the client.
    clientStub.control(requestObserver);

    // Connect again from the client.
    anotherClientStub.control(anotherRequestObserver);

    try (FnApiControlClient client = service.get()) {
      assertNotNull(client);
      try (FnApiControlClient anotherClient = service.get()) {
        assertNotNull(anotherClient);
      }
    }
    requestCompleted.await(5, TimeUnit.SECONDS); // Wait until request streams have been closed.

    verify(requestObserver).onCompleted();
    verifyNoMoreInteractions(requestObserver);
    verify(anotherRequestObserver).onCompleted();
    verifyNoMoreInteractions(anotherRequestObserver);
  }
}
