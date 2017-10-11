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
package org.apache.beam.runners.core.fn;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.isA;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.verify;

import com.google.common.util.concurrent.ListenableFuture;
import io.grpc.stub.StreamObserver;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import org.apache.beam.model.fnexecution.v1.BeamFnApi;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

/** Unit tests for {@link FnApiControlClient}. */
@RunWith(JUnit4.class)
public class FnApiControlClientTest {

  @Rule public ExpectedException thrown = ExpectedException.none();

  @Mock public StreamObserver<BeamFnApi.InstructionRequest> mockObserver;
  private FnApiControlClient client;

  @Before
  public void setup() {
    MockitoAnnotations.initMocks(this);
    client = FnApiControlClient.forRequestObserver(mockObserver);
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

    Future<BeamFnApi.InstructionResponse> responseFuture =
        client.handle(BeamFnApi.InstructionRequest.newBuilder().setInstructionId(id).build());
    client
        .asResponseObserver()
        .onNext(BeamFnApi.InstructionResponse.newBuilder().setInstructionId(id).build());

    BeamFnApi.InstructionResponse response = responseFuture.get();

    assertThat(response.getInstructionId(), equalTo(id));
  }

  @Test
  public void testUnknownResponseIgnored() throws Exception {
    String id = "actualInstruction";
    String unknownId = "unknownInstruction";

    ListenableFuture<BeamFnApi.InstructionResponse> responseFuture =
        client.handle(BeamFnApi.InstructionRequest.newBuilder().setInstructionId(id).build());

    client
        .asResponseObserver()
        .onNext(BeamFnApi.InstructionResponse.newBuilder().setInstructionId(unknownId).build());

    assertThat(responseFuture.isDone(), is(false));
    assertThat(responseFuture.isCancelled(), is(false));
  }

  @Test
  public void testOnCompletedCancelsOutstanding() throws Exception {
    String id = "clientHangUpInstruction";

    Future<BeamFnApi.InstructionResponse> responseFuture =
        client.handle(BeamFnApi.InstructionRequest.newBuilder().setInstructionId(id).build());

    client.asResponseObserver().onCompleted();

    thrown.expect(ExecutionException.class);
    thrown.expectCause(isA(IllegalStateException.class));
    thrown.expectMessage("closed");
    responseFuture.get();
  }

  @Test
  public void testOnErrorCancelsOutstanding() throws Exception {
    String id = "errorInstruction";

    Future<BeamFnApi.InstructionResponse> responseFuture =
        client.handle(BeamFnApi.InstructionRequest.newBuilder().setInstructionId(id).build());

    class FrazzleException extends Exception {}
    client.asResponseObserver().onError(new FrazzleException());

    thrown.expect(ExecutionException.class);
    thrown.expectCause(isA(FrazzleException.class));
    responseFuture.get();
  }

  @Test
  public void testCloseCancelsOutstanding() throws Exception {
    String id = "serverCloseInstruction";

    Future<BeamFnApi.InstructionResponse> responseFuture =
        client.handle(BeamFnApi.InstructionRequest.newBuilder().setInstructionId(id).build());

    client.close();

    thrown.expect(ExecutionException.class);
    thrown.expectCause(isA(IllegalStateException.class));
    thrown.expectMessage("closed");
    responseFuture.get();
  }
}
