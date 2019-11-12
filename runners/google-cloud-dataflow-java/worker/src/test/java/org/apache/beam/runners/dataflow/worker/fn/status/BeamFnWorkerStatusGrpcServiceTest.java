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
package org.apache.beam.runners.dataflow.worker.fn.status;

import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;

import com.google.common.collect.Sets;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.apache.beam.fn.harness.control.AddHarnessIdInterceptor;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.WorkerStatusRequest;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.WorkerStatusResponse;
import org.apache.beam.model.fnexecution.v1.BeamFnWorkerStatusGrpc;
import org.apache.beam.model.fnexecution.v1.BeamFnWorkerStatusGrpc.BeamFnWorkerStatusStub;
import org.apache.beam.model.pipeline.v1.Endpoints.ApiServiceDescriptor;
import org.apache.beam.runners.fnexecution.GrpcContextHeaderAccessorProvider;
import org.apache.beam.runners.fnexecution.GrpcFnServer;
import org.apache.beam.runners.fnexecution.InProcessServerFactory;
import org.apache.beam.runners.fnexecution.status.FnApiWorkerStatusClient;
import org.apache.beam.vendor.grpc.v1p21p0.io.grpc.ManagedChannel;
import org.apache.beam.vendor.grpc.v1p21p0.io.grpc.inprocess.InProcessChannelBuilder;
import org.apache.beam.vendor.grpc.v1p21p0.io.grpc.stub.StreamObserver;
import org.apache.beam.vendor.grpc.v1p21p0.io.grpc.testing.GrpcCleanupRule;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

@RunWith(JUnit4.class)
public class BeamFnWorkerStatusGrpcServiceTest {

  @Rule public final GrpcCleanupRule grpcCleanup = new GrpcCleanupRule();
  private BeamFnWorkerStatusGrpcService service;
  private GrpcFnServer<BeamFnWorkerStatusGrpcService> server;
  private ManagedChannel channel;
  private BeamFnWorkerStatusStub stub;
  @Mock private StreamObserver<WorkerStatusRequest> mockObserver;

  @Before
  public void setUp() throws Exception {
    MockitoAnnotations.initMocks(this);
    service =
        BeamFnWorkerStatusGrpcService.create(
            ApiServiceDescriptor.newBuilder().setUrl("url").build(),
            GrpcContextHeaderAccessorProvider.getHeaderAccessor());
    server = GrpcFnServer.allocatePortAndCreateFor(service, InProcessServerFactory.create());
    channel = InProcessChannelBuilder.forName(server.getApiServiceDescriptor().getUrl()).build();
    stub =
        BeamFnWorkerStatusGrpc.newStub(channel)
            .withInterceptors(AddHarnessIdInterceptor.create("id"));
    grpcCleanup.register(server.getServer());
  }

  @Test
  public void testCreateFromNullApiServiceDescriptor() {
    assertNull(
        BeamFnWorkerStatusGrpcService.create(
            null, GrpcContextHeaderAccessorProvider.getHeaderAccessor()));
    assertNull(
        BeamFnWorkerStatusGrpcService.create(
            ApiServiceDescriptor.newBuilder().build(),
            GrpcContextHeaderAccessorProvider.getHeaderAccessor()));
  }

  @Test
  public void testClientConnected() throws Exception {
    StreamObserver<WorkerStatusResponse> workerStatusResponseStreamObserver =
        stub.workerStatus(mockObserver);
    FnApiWorkerStatusClient client = service.getStatusClient("id", 2000).get();
    assertNotNull(client);
    client.close();
  }

  @Test
  public void testClientWithoutIdConnected() throws Exception {
    BeamFnWorkerStatusStub workerStatusStub = BeamFnWorkerStatusGrpc.newStub(channel);
    StreamObserver<WorkerStatusResponse> workerStatusResponseStreamObserver =
        workerStatusStub.workerStatus(mockObserver);
    FnApiWorkerStatusClient client = service.getStatusClient("unknown_sdk0", 2000).get();
    assertNotNull(client);
    client.close();
  }

  @Test
  public void testGetConnectedSdkIds() throws Exception {
    Set<String> ids = Sets.newHashSet("id0", "id3", "id11", "id12", "id21");
    for (String id : ids) {
      BeamFnWorkerStatusStub workerStatusStub =
          BeamFnWorkerStatusGrpc.newStub(channel)
              .withInterceptors(AddHarnessIdInterceptor.create(id));
      StreamObserver<WorkerStatusResponse> workerStatusResponseStreamObserver =
          workerStatusStub.workerStatus(mockObserver);
    }
    // wait for 2 seconds to avoid race condition.
    Thread.sleep(2000);
    Set<String> connectedSdkIds = service.getConnectedSdkIds();
    assertEquals(ids, connectedSdkIds);
    for (String id : ids) {
      service.getStatusClient(id, 2000).get().close();
    }
  }

  @Test
  public void testSendRequest() throws Exception {
    StreamObserver<WorkerStatusResponse> workerStatusResponseStreamObserver =
        stub.workerStatus(mockObserver);
    doAnswer(
            (answer) -> {
              workerStatusResponseStreamObserver.onNext(
                  WorkerStatusResponse.newBuilder()
                      .setRequestId("fake")
                      .setStatusInfo("status")
                      .build());
              return null;
            })
        .when(mockObserver)
        .onNext(any(WorkerStatusRequest.class));
    FnApiWorkerStatusClient client = service.getStatusClient("id", 2000).get();
    CompletableFuture<WorkerStatusResponse> workerStatus =
        client.getWorkerStatus(WorkerStatusRequest.newBuilder().setRequestId("fake").build());
    assertEquals("status", workerStatus.get(2, TimeUnit.SECONDS).getStatusInfo());
    client.close();
  }
}
