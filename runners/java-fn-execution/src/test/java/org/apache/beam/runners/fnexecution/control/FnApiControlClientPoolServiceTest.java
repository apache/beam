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
package org.apache.beam.runners.fnexecution.control;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.argThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.beam.model.fnexecution.v1.BeamFnApi;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.InstructionRequest;
import org.apache.beam.model.fnexecution.v1.BeamFnControlGrpc;
import org.apache.beam.runners.fnexecution.GrpcContextHeaderAccessorProvider;
import org.apache.beam.runners.fnexecution.GrpcFnServer;
import org.apache.beam.runners.fnexecution.InProcessServerFactory;
import org.apache.beam.sdk.util.MoreFutures;
import org.apache.beam.vendor.grpc.v1p26p0.io.grpc.Status;
import org.apache.beam.vendor.grpc.v1p26p0.io.grpc.StatusException;
import org.apache.beam.vendor.grpc.v1p26p0.io.grpc.inprocess.InProcessChannelBuilder;
import org.apache.beam.vendor.grpc.v1p26p0.io.grpc.stub.StreamObserver;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Unit tests for {@link FnApiControlClientPoolService}. */
@RunWith(JUnit4.class)
public class FnApiControlClientPoolServiceTest {

  private final ControlClientPool pool = MapControlClientPool.create();
  private final FnApiControlClientPoolService controlService =
      FnApiControlClientPoolService.offeringClientsToPool(
          pool.getSink(), GrpcContextHeaderAccessorProvider.getHeaderAccessor());
  private GrpcFnServer<FnApiControlClientPoolService> server;
  private BeamFnControlGrpc.BeamFnControlStub stub;

  @Before
  public void setup() throws IOException {
    server = GrpcFnServer.allocatePortAndCreateFor(controlService, InProcessServerFactory.create());
    stub =
        BeamFnControlGrpc.newStub(
            InProcessChannelBuilder.forName(server.getApiServiceDescriptor().getUrl()).build());
  }

  @After
  public void teardown() throws Exception {
    server.close();
  }

  @Test
  public void testIncomingConnection() throws Exception {
    StreamObserver<BeamFnApi.InstructionRequest> requestObserver = mock(StreamObserver.class);
    StreamObserver<BeamFnApi.InstructionResponse> responseObserver =
        controlService.control(requestObserver);

    // TODO: https://issues.apache.org/jira/browse/BEAM-4149 Use proper worker id.
    InstructionRequestHandler client = pool.getSource().take("", Duration.ofSeconds(2));

    // Check that the client is wired up to the request channel
    String id = "fakeInstruction";
    CompletionStage<BeamFnApi.InstructionResponse> responseFuture =
        client.handle(BeamFnApi.InstructionRequest.newBuilder().setInstructionId(id).build());
    verify(requestObserver).onNext(any(BeamFnApi.InstructionRequest.class));
    assertThat(MoreFutures.isDone(responseFuture), is(false));

    // Check that the response channel really came from the client
    responseObserver.onNext(
        BeamFnApi.InstructionResponse.newBuilder().setInstructionId(id).build());
    MoreFutures.get(responseFuture);
  }

  @Test
  public void testCloseCompletesClients() throws Exception {
    CountDownLatch latch = new CountDownLatch(1);
    AtomicBoolean sawComplete = new AtomicBoolean();
    stub.control(
        new StreamObserver<InstructionRequest>() {
          @Override
          public void onNext(InstructionRequest value) {
            Assert.fail("Should never see a request");
          }

          @Override
          public void onError(Throwable t) {
            latch.countDown();
          }

          @Override
          public void onCompleted() {
            sawComplete.set(true);
            latch.countDown();
          }
        });

    // TODO: https://issues.apache.org/jira/browse/BEAM-4149 Use proper worker id.
    pool.getSource().take("", Duration.ofSeconds(2));
    server.close();

    latch.await();
    assertThat(sawComplete.get(), is(true));
  }

  @Test
  public void testUnknownBundle() throws Exception {
    BeamFnApi.GetProcessBundleDescriptorRequest request =
        BeamFnApi.GetProcessBundleDescriptorRequest.newBuilder()
            .setProcessBundleDescriptorId("missing")
            .build();
    StreamObserver<BeamFnApi.ProcessBundleDescriptor> responseObserver = mock(StreamObserver.class);
    controlService.getProcessBundleDescriptor(request, responseObserver);

    verify(responseObserver)
        .onError(
            argThat(
                e ->
                    e instanceof StatusException
                        && ((StatusException) e).getStatus().getCode() == Status.Code.NOT_FOUND));
  }
}
