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

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.isA;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.verify;

import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.function.Consumer;
import org.apache.beam.model.fnexecution.v1.BeamFnApi;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.InstructionRequest;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.InstructionResponse;
import org.apache.beam.sdk.util.MoreFutures;
import org.apache.beam.vendor.grpc.v1p26p0.io.grpc.stub.StreamObserver;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

/** Unit tests for {@link FnApiControlClient}. */
@RunWith(JUnit4.class)
public class FnApiControlClientTest {

  @Rule public ExpectedException thrown = ExpectedException.none();

  @Mock public StreamObserver<BeamFnApi.InstructionRequest> mockObserver;
  private FnApiControlClient client;
  private ConcurrentMap<String, BeamFnApi.ProcessBundleDescriptor> processBundleDescriptors;

  @Before
  public void setup() {
    MockitoAnnotations.initMocks(this);
    processBundleDescriptors = new ConcurrentHashMap<>();
    client = FnApiControlClient.forRequestObserver("DUMMY", mockObserver, processBundleDescriptors);
  }

  @Test
  public void testRequestSent() {
    String id = "instructionId";
    client.handle(BeamFnApi.InstructionRequest.newBuilder().setInstructionId(id).build());

    verify(mockObserver).onNext(any(BeamFnApi.InstructionRequest.class));
  }

  @Test
  public void testRequestSuccess() throws Exception {
    String id = "successfulInstruction";

    CompletionStage<InstructionResponse> responseFuture =
        client.handle(BeamFnApi.InstructionRequest.newBuilder().setInstructionId(id).build());
    client
        .asResponseObserver()
        .onNext(BeamFnApi.InstructionResponse.newBuilder().setInstructionId(id).build());

    BeamFnApi.InstructionResponse response = MoreFutures.get(responseFuture);

    assertThat(response.getInstructionId(), equalTo(id));
  }

  @Test
  public void testRequestError() throws Exception {
    String id = "instructionId";
    CompletionStage<InstructionResponse> responseFuture =
        client.handle(InstructionRequest.newBuilder().setInstructionId(id).build());
    String error = "Oh no an error!";
    client
        .asResponseObserver()
        .onNext(
            BeamFnApi.InstructionResponse.newBuilder()
                .setInstructionId(id)
                .setError(error)
                .build());

    thrown.expectCause(isA(RuntimeException.class));
    thrown.expectMessage(error);
    MoreFutures.get(responseFuture);
  }

  @Test
  public void testUnknownResponseIgnored() throws Exception {
    String id = "actualInstruction";
    String unknownId = "unknownInstruction";

    CompletionStage<BeamFnApi.InstructionResponse> responseFuture =
        client.handle(BeamFnApi.InstructionRequest.newBuilder().setInstructionId(id).build());

    client
        .asResponseObserver()
        .onNext(BeamFnApi.InstructionResponse.newBuilder().setInstructionId(unknownId).build());

    assertThat(MoreFutures.isDone(responseFuture), is(false));
    assertThat(MoreFutures.isCancelled(responseFuture), is(false));
  }

  @Test
  public void testOnCompletedCancelsOutstanding() throws Exception {
    String id = "clientHangUpInstruction";

    CompletionStage<InstructionResponse> responseFuture =
        client.handle(BeamFnApi.InstructionRequest.newBuilder().setInstructionId(id).build());

    client.asResponseObserver().onCompleted();

    thrown.expect(ExecutionException.class);
    thrown.expectCause(isA(IllegalStateException.class));
    thrown.expectMessage("closed");
    MoreFutures.get(responseFuture);
  }

  @Test
  public void testOnErrorCancelsOutstanding() throws Exception {
    String id = "errorInstruction";

    CompletionStage<InstructionResponse> responseFuture =
        client.handle(BeamFnApi.InstructionRequest.newBuilder().setInstructionId(id).build());

    class FrazzleException extends Exception {}

    client.asResponseObserver().onError(new FrazzleException());

    thrown.expect(ExecutionException.class);
    thrown.expectCause(isA(FrazzleException.class));
    MoreFutures.get(responseFuture);
  }

  @Test
  public void testCloseCancelsOutstanding() throws Exception {
    String id = "serverCloseInstruction";

    CompletionStage<InstructionResponse> responseFuture =
        client.handle(BeamFnApi.InstructionRequest.newBuilder().setInstructionId(id).build());

    client.close();

    thrown.expect(ExecutionException.class);
    thrown.expectCause(isA(IllegalStateException.class));
    thrown.expectMessage("closed");
    MoreFutures.get(responseFuture);
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testOnCloseMultipleListener() throws Exception {
    Consumer<FnApiControlClient> mockConsumer1 = Mockito.mock(Consumer.class);
    Consumer<FnApiControlClient> mockConsumer2 = Mockito.mock(Consumer.class);

    client.onClose(mockConsumer1);
    client.onClose(mockConsumer2);

    client.close();

    verify(mockConsumer1).accept(client);
    verify(mockConsumer2).accept(client);
  }
}
