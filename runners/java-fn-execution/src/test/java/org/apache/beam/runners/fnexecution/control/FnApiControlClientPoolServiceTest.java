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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.stub.StreamObserver;
import java.io.IOException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.beam.model.fnexecution.v1.BeamFnApi;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.InstructionRequest;
import org.apache.beam.model.fnexecution.v1.BeamFnControlGrpc;
import org.apache.beam.runners.fnexecution.GrpcFnServer;
import org.apache.beam.runners.fnexecution.InProcessServerFactory;
import org.apache.beam.sdk.util.MoreFutures;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Unit tests for {@link FnApiControlClientPoolService}. */
@RunWith(JUnit4.class)
public class FnApiControlClientPoolServiceTest {

  // For ease of straight-line testing, we use a LinkedBlockingQueue; in practice a SynchronousQueue
  // for matching incoming connections and server threads is likely.
  private final BlockingQueue<FnApiControlClient> pool = new LinkedBlockingQueue<>();
  private final FnApiControlClientPoolService controlService =
      FnApiControlClientPoolService.offeringClientsToPool(pool);
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

    FnApiControlClient client = pool.take();

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

    pool.take();
    server.close();

    latch.await();
    assertThat(sawComplete.get(), is(true));
  }
}
