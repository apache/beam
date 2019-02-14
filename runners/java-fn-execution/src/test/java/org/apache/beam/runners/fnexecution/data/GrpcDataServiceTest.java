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
package org.apache.beam.runners.fnexecution.data;

import static org.apache.beam.sdk.util.CoderUtils.encodeToByteArray;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.junit.Assert.assertThat;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import org.apache.beam.model.fnexecution.v1.BeamFnApi;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.Elements;
import org.apache.beam.model.fnexecution.v1.BeamFnDataGrpc;
import org.apache.beam.runners.fnexecution.GrpcFnServer;
import org.apache.beam.runners.fnexecution.InProcessServerFactory;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.LengthPrefixCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.fn.data.CloseableFnDataReceiver;
import org.apache.beam.sdk.fn.data.InboundDataClient;
import org.apache.beam.sdk.fn.data.LogicalEndpoint;
import org.apache.beam.sdk.fn.stream.OutboundObserverFactory;
import org.apache.beam.sdk.fn.test.TestStreams;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.vendor.grpc.v1p13p1.com.google.protobuf.ByteString;
import org.apache.beam.vendor.grpc.v1p13p1.io.grpc.ManagedChannel;
import org.apache.beam.vendor.grpc.v1p13p1.io.grpc.inprocess.InProcessChannelBuilder;
import org.apache.beam.vendor.grpc.v1p13p1.io.grpc.stub.StreamObserver;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link GrpcDataService}. */
@RunWith(JUnit4.class)
public class GrpcDataServiceTest {
  private static final BeamFnApi.Target TARGET =
      BeamFnApi.Target.newBuilder().setPrimitiveTransformReference("888").setName("test").build();
  private static final Coder<WindowedValue<String>> CODER =
      LengthPrefixCoder.of(WindowedValue.getValueOnlyCoder(StringUtf8Coder.of()));

  @Test
  public void testMessageReceivedBySingleClientWhenThereAreMultipleClients() throws Exception {
    final LinkedBlockingQueue<Elements> clientInboundElements = new LinkedBlockingQueue<>();
    ExecutorService executorService = Executors.newCachedThreadPool();
    final CountDownLatch waitForInboundElements = new CountDownLatch(1);
    GrpcDataService service =
        GrpcDataService.create(
            Executors.newCachedThreadPool(), OutboundObserverFactory.serverDirect());
    try (GrpcFnServer<GrpcDataService> server =
        GrpcFnServer.allocatePortAndCreateFor(service, InProcessServerFactory.create())) {
      Collection<Future<Void>> clientFutures = new ArrayList<>();
      for (int i = 0; i < 3; ++i) {
        clientFutures.add(
            executorService.submit(
                () -> {
                  ManagedChannel channel =
                      InProcessChannelBuilder.forName(server.getApiServiceDescriptor().getUrl())
                          .build();
                  StreamObserver<Elements> outboundObserver =
                      BeamFnDataGrpc.newStub(channel)
                          .data(TestStreams.withOnNext(clientInboundElements::add).build());
                  waitForInboundElements.await();
                  outboundObserver.onCompleted();
                  return null;
                }));
      }

      for (int i = 0; i < 3; ++i) {
        CloseableFnDataReceiver<WindowedValue<String>> consumer =
            service.send(LogicalEndpoint.of(Integer.toString(i), TARGET), CODER);

        consumer.accept(WindowedValue.valueInGlobalWindow("A" + i));
        consumer.accept(WindowedValue.valueInGlobalWindow("B" + i));
        consumer.accept(WindowedValue.valueInGlobalWindow("C" + i));
        consumer.close();
      }
      waitForInboundElements.countDown();
      for (Future<Void> clientFuture : clientFutures) {
        clientFuture.get();
      }
      assertThat(
          clientInboundElements,
          containsInAnyOrder(elementsWithData("0"), elementsWithData("1"), elementsWithData("2")));
    }
  }

  @Test
  public void testMultipleClientsSendMessagesAreDirectedToProperConsumers() throws Exception {
    final LinkedBlockingQueue<BeamFnApi.Elements> clientInboundElements =
        new LinkedBlockingQueue<>();
    ExecutorService executorService = Executors.newCachedThreadPool();
    final CountDownLatch waitForInboundElements = new CountDownLatch(1);
    GrpcDataService service =
        GrpcDataService.create(
            Executors.newCachedThreadPool(), OutboundObserverFactory.serverDirect());
    try (GrpcFnServer<GrpcDataService> server =
        GrpcFnServer.allocatePortAndCreateFor(service, InProcessServerFactory.create())) {
      Collection<Future<Void>> clientFutures = new ArrayList<>();
      for (int i = 0; i < 3; ++i) {
        final String instructionReference = Integer.toString(i);
        clientFutures.add(
            executorService.submit(
                () -> {
                  ManagedChannel channel =
                      InProcessChannelBuilder.forName(server.getApiServiceDescriptor().getUrl())
                          .build();
                  StreamObserver<Elements> outboundObserver =
                      BeamFnDataGrpc.newStub(channel)
                          .data(TestStreams.withOnNext(clientInboundElements::add).build());
                  outboundObserver.onNext(elementsWithData(instructionReference));
                  waitForInboundElements.await();
                  outboundObserver.onCompleted();
                  return null;
                }));
      }

      List<Collection<WindowedValue<String>>> serverInboundValues = new ArrayList<>();
      Collection<InboundDataClient> readFutures = new ArrayList<>();
      for (int i = 0; i < 3; ++i) {
        final Collection<WindowedValue<String>> serverInboundValue = new ArrayList<>();
        serverInboundValues.add(serverInboundValue);
        readFutures.add(
            service.receive(
                LogicalEndpoint.of(Integer.toString(i), TARGET), CODER, serverInboundValue::add));
      }
      for (InboundDataClient readFuture : readFutures) {
        readFuture.awaitCompletion();
      }
      waitForInboundElements.countDown();
      for (Future<Void> clientFuture : clientFutures) {
        clientFuture.get();
      }
      for (int i = 0; i < 3; ++i) {
        assertThat(
            serverInboundValues.get(i),
            contains(
                WindowedValue.valueInGlobalWindow("A" + i),
                WindowedValue.valueInGlobalWindow("B" + i),
                WindowedValue.valueInGlobalWindow("C" + i)));
      }
      assertThat(clientInboundElements, empty());
    }
  }

  private BeamFnApi.Elements elementsWithData(String id) throws CoderException {
    return BeamFnApi.Elements.newBuilder()
        .addData(
            BeamFnApi.Elements.Data.newBuilder()
                .setInstructionReference(id)
                .setTarget(TARGET)
                .setData(
                    ByteString.copyFrom(
                            encodeToByteArray(CODER, WindowedValue.valueInGlobalWindow("A" + id)))
                        .concat(
                            ByteString.copyFrom(
                                encodeToByteArray(
                                    CODER, WindowedValue.valueInGlobalWindow("B" + id))))
                        .concat(
                            ByteString.copyFrom(
                                encodeToByteArray(
                                    CODER, WindowedValue.valueInGlobalWindow("C" + id))))))
        .addData(BeamFnApi.Elements.Data.newBuilder().setInstructionReference(id).setTarget(TARGET))
        .build();
  }
}
