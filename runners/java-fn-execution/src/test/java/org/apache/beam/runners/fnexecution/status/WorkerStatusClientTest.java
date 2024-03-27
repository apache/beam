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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import org.apache.beam.model.fnexecution.v1.BeamFnApi;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.WorkerStatusRequest;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.WorkerStatusResponse;
import org.apache.beam.vendor.grpc.v1p60p1.io.grpc.stub.StreamObserver;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

@RunWith(JUnit4.class)
public class WorkerStatusClientTest {
  @Rule public transient Timeout globalTimeout = Timeout.seconds(600);
  @Mock public StreamObserver<BeamFnApi.WorkerStatusRequest> mockObserver;
  private WorkerStatusClient client;

  @Before
  public void setup() {
    MockitoAnnotations.initMocks(this);
    client = WorkerStatusClient.forRequestObserver("ID", mockObserver);
  }

  @Test
  public void testGetWorkerStatusSuccess() throws Exception {
    CompletableFuture<WorkerStatusResponse> workerStatus =
        client.getWorkerStatus(WorkerStatusRequest.newBuilder().setId("123").build());
    client
        .getResponseObserver()
        .onNext(WorkerStatusResponse.newBuilder().setId("123").setStatusInfo("status").build());
    Assert.assertEquals("status", workerStatus.get().getStatusInfo());
  }

  @Test
  public void testGetWorkerStatusError() throws Exception {
    CompletableFuture<WorkerStatusResponse> workerStatus =
        client.getWorkerStatus(WorkerStatusRequest.newBuilder().setId("123").build());
    client
        .getResponseObserver()
        .onNext(WorkerStatusResponse.newBuilder().setId("123").setError("error").build());
    Assert.assertEquals("error", workerStatus.get().getError());
  }

  @Test
  @SuppressWarnings("FutureReturnValueIgnored")
  public void testGetWorkerStatusRequestSent() {
    client.getWorkerStatus();
    verify(mockObserver).onNext(any(WorkerStatusRequest.class));
  }

  @Test
  public void testUnknownRequestIdResponseIgnored() {
    CompletableFuture<WorkerStatusResponse> workerStatus = client.getWorkerStatus();
    client
        .getResponseObserver()
        .onNext(WorkerStatusResponse.newBuilder().setId("unknown").setStatusInfo("status").build());
    Assert.assertFalse(workerStatus.isDone());
  }

  @Test
  public void testCloseOutstandingRequest() throws IOException {
    CompletableFuture<WorkerStatusResponse> workerStatus = client.getWorkerStatus();
    client.close();
    Assert.assertThrows(ExecutionException.class, workerStatus::get);
  }
}
