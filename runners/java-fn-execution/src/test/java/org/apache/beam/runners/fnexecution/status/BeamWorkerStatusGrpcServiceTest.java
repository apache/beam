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
package org.apache.beam.runners.fnexecution.status;

import static org.apache.beam.runners.fnexecution.status.BeamWorkerStatusGrpcService.DEFAULT_ERROR_RESPONSE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;

import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.beam.fn.harness.control.AddHarnessIdInterceptor;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.WorkerStatusRequest;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.WorkerStatusResponse;
import org.apache.beam.model.fnexecution.v1.BeamFnWorkerStatusGrpc;
import org.apache.beam.model.fnexecution.v1.BeamFnWorkerStatusGrpc.BeamFnWorkerStatusStub;
import org.apache.beam.model.pipeline.v1.Endpoints.ApiServiceDescriptor;
import org.apache.beam.runners.fnexecution.GrpcContextHeaderAccessorProvider;
import org.apache.beam.runners.fnexecution.GrpcFnServer;
import org.apache.beam.runners.fnexecution.InProcessServerFactory;
import org.apache.beam.vendor.grpc.v1p21p0.io.grpc.ManagedChannel;
import org.apache.beam.vendor.grpc.v1p21p0.io.grpc.inprocess.InProcessChannelBuilder;
import org.apache.beam.vendor.grpc.v1p21p0.io.grpc.stub.StreamObserver;
import org.apache.beam.vendor.grpc.v1p21p0.io.grpc.testing.GrpcCleanupRule;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Sets;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

@RunWith(JUnit4.class)
public class BeamWorkerStatusGrpcServiceTest {

  @Rule public final GrpcCleanupRule grpcCleanup = new GrpcCleanupRule();
  private BeamWorkerStatusGrpcService service;
  private GrpcFnServer<BeamWorkerStatusGrpcService> server;
  private ManagedChannel channel;
  private BeamFnWorkerStatusStub stub;
  @Mock private StreamObserver<WorkerStatusRequest> mockObserver;

  @Before
  public void setUp() throws Exception {
    MockitoAnnotations.initMocks(this);
    service =
        BeamWorkerStatusGrpcService.create(
            ApiServiceDescriptor.newBuilder().setUrl(UUID.randomUUID().toString()).build(),
            GrpcContextHeaderAccessorProvider.getHeaderAccessor());
    server = GrpcFnServer.allocatePortAndCreateFor(service, InProcessServerFactory.create());
    channel = InProcessChannelBuilder.forName(server.getApiServiceDescriptor().getUrl()).build();
    stub =
        BeamFnWorkerStatusGrpc.newStub(channel)
            .withInterceptors(AddHarnessIdInterceptor.create("id"));
    grpcCleanup.register(server.getServer());
    grpcCleanup.register(channel);
  }

  @After
  public void tearDown() throws Exception {
    if (service != null) {
      service.close();
    }
  }

  @Test
  public void testClientConnected() throws Exception {
    StreamObserver<WorkerStatusResponse> workerStatusResponseStreamObserver =
        stub.workerStatus(mockObserver);
    WorkerStatusClient client = service.getStatusClient("id");
    assertNotNull(client);
  }

  @Test(expected = TimeoutException.class)
  public void testGetWorkerStatusTimeout() throws Exception {
    StreamObserver<WorkerStatusResponse> unused = stub.workerStatus(mockObserver);
    String response = service.getWorkerStatus("id").get(1, TimeUnit.MILLISECONDS);
    assertEquals(DEFAULT_ERROR_RESPONSE, response);
  }

  @Test
  public void testGetWorkerStatusSuccess() throws Exception {
    CountDownLatch requestCompleted = new CountDownLatch(1);
    StreamObserver<WorkerStatusResponse> observer = stub.workerStatus(mockObserver);
    doAnswer(
            (invocation) -> {
              WorkerStatusRequest request = (WorkerStatusRequest) invocation.getArguments()[0];
              observer.onNext(
                  WorkerStatusResponse.newBuilder()
                      .setId(request.getId())
                      .setStatusInfo("status")
                      .build());
              requestCompleted.countDown();
              return null;
            })
        .when(mockObserver)
        .onNext(any());

    CompletableFuture<String> future = service.getWorkerStatus("id");
    // wait for request to be sent.
    requestCompleted.await();
    String response = future.get(5, TimeUnit.SECONDS);
    assertEquals("status", response);
  }

  @Test
  public void testGetWorkerStatusReturnError() throws Exception {
    CountDownLatch requestCompleted = new CountDownLatch(1);
    StreamObserver<WorkerStatusResponse> observer = stub.workerStatus(mockObserver);
    doAnswer(
            (invocation) -> {
              WorkerStatusRequest request = (WorkerStatusRequest) invocation.getArguments()[0];
              observer.onNext(
                  WorkerStatusResponse.newBuilder()
                      .setId(request.getId())
                      .setError("error")
                      .build());
              requestCompleted.countDown();
              return null;
            })
        .when(mockObserver)
        .onNext(any());

    CompletableFuture<String> future = service.getWorkerStatus("id");
    // wait for request to be sent.
    requestCompleted.await();
    String response = future.get(5, TimeUnit.SECONDS);
    assertEquals("error", response);
  }

  @Test
  public void testGetAllWorkerStatuses() throws Exception {
    Set<String> ids = Sets.newHashSet("id0", "id3", "id11", "id12", "id21");
    for (String id : ids) {
      StreamObserver<WorkerStatusRequest> requestObserverMock = mock(StreamObserver.class);
      BeamFnWorkerStatusStub workerStatusStub =
          BeamFnWorkerStatusGrpc.newStub(channel)
              .withInterceptors(AddHarnessIdInterceptor.create(id));
      StreamObserver<WorkerStatusResponse> observer =
          workerStatusStub.workerStatus(requestObserverMock);
      doAnswer(
              (invocation) -> {
                WorkerStatusRequest request = (WorkerStatusRequest) invocation.getArguments()[0];
                observer.onNext(
                    WorkerStatusResponse.newBuilder()
                        .setId(request.getId())
                        .setStatusInfo("status")
                        .build());
                return null;
              })
          .when(requestObserverMock)
          .onNext(any());
    }

    Map<String, String> allWorkerStatuses = service.getAllWorkerStatuses(5, TimeUnit.SECONDS);

    assertEquals(ids, allWorkerStatuses.keySet());

    for (String id : ids) {
      assertEquals("status", allWorkerStatuses.get(id));
      service.getStatusClient(id).close();
    }
  }
}
