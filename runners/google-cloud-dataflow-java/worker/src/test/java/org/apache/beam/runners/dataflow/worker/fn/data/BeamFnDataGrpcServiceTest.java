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
package org.apache.beam.runners.dataflow.worker.fn.data;

import static org.apache.beam.sdk.util.CoderUtils.encodeToByteArray;
import static org.apache.beam.sdk.util.WindowedValue.valueInGlobalWindow;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;

import java.net.InetAddress;
import java.net.ServerSocket;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import org.apache.beam.model.fnexecution.v1.BeamFnApi;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.Elements;
import org.apache.beam.model.fnexecution.v1.BeamFnDataGrpc;
import org.apache.beam.model.pipeline.v1.Endpoints;
import org.apache.beam.runners.dataflow.harness.test.TestStreams;
import org.apache.beam.runners.dataflow.worker.fn.stream.ServerStreamObserverFactory;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.LengthPrefixCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.fn.channel.AddHarnessIdInterceptor;
import org.apache.beam.sdk.fn.channel.ManagedChannelFactory;
import org.apache.beam.sdk.fn.data.CloseableFnDataReceiver;
import org.apache.beam.sdk.fn.data.InboundDataClient;
import org.apache.beam.sdk.fn.data.LogicalEndpoint;
import org.apache.beam.sdk.fn.server.GrpcContextHeaderAccessorProvider;
import org.apache.beam.sdk.fn.server.ServerFactory;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.vendor.grpc.v1p48p1.com.google.protobuf.ByteString;
import org.apache.beam.vendor.grpc.v1p48p1.io.grpc.ManagedChannel;
import org.apache.beam.vendor.grpc.v1p48p1.io.grpc.Server;
import org.apache.beam.vendor.grpc.v1p48p1.io.grpc.stub.StreamObserver;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.net.HostAndPort;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link BeamFnDataGrpcService}. */
@RunWith(JUnit4.class)
@SuppressWarnings({
  "FutureReturnValueIgnored",
})
public class BeamFnDataGrpcServiceTest {
  private static final String TRANSFORM_ID = "888";
  private static final Coder<WindowedValue<String>> CODER =
      LengthPrefixCoder.of(WindowedValue.getValueOnlyCoder(StringUtf8Coder.of()));
  private static final String WORKER_ID = "testWorker";

  private Server server;
  private BeamFnDataGrpcService service;

  private Endpoints.ApiServiceDescriptor findOpenPort() throws Exception {
    InetAddress address = InetAddress.getLoopbackAddress();
    try (ServerSocket socket = new ServerSocket(0, -1, address)) {
      return Endpoints.ApiServiceDescriptor.newBuilder()
          .setUrl(HostAndPort.fromParts(address.getHostAddress(), socket.getLocalPort()).toString())
          .build();
    }
  }

  @Before
  public void setUp() throws Exception {
    Endpoints.ApiServiceDescriptor descriptor = findOpenPort();
    PipelineOptions options = PipelineOptionsFactory.create();
    service =
        new BeamFnDataGrpcService(
            options,
            descriptor,
            ServerStreamObserverFactory.fromOptions(options)::from,
            GrpcContextHeaderAccessorProvider.getHeaderAccessor());
    server = ServerFactory.createDefault().create(Arrays.asList(service), descriptor);
  }

  @After
  public void tearDown() {
    server.shutdownNow();
  }

  @Test
  public void testMessageReceivedBySingleClientWhenThereAreMultipleClients() throws Exception {
    BlockingQueue<Elements> clientInboundElements = new LinkedBlockingQueue<>();
    ExecutorService executorService = Executors.newCachedThreadPool();
    CountDownLatch waitForInboundElements = new CountDownLatch(1);
    int numberOfClients = 3;

    for (int client = 0; client < numberOfClients; ++client) {
      executorService.submit(
          () -> {
            ManagedChannel channel =
                ManagedChannelFactory.createDefault()
                    .withInterceptors(Arrays.asList(AddHarnessIdInterceptor.create(WORKER_ID)))
                    .forDescriptor(service.getApiServiceDescriptor());
            StreamObserver<BeamFnApi.Elements> outboundObserver =
                BeamFnDataGrpc.newStub(channel)
                    .data(TestStreams.withOnNext(clientInboundElements::add).build());
            waitForInboundElements.await();
            outboundObserver.onCompleted();
            return null;
          });
    }

    for (int i = 0; i < 3; ++i) {
      CloseableFnDataReceiver<WindowedValue<String>> consumer =
          service
              .getDataService(WORKER_ID)
              .send(LogicalEndpoint.data(Integer.toString(i), TRANSFORM_ID), CODER);

      consumer.accept(valueInGlobalWindow("A" + i));
      consumer.accept(valueInGlobalWindow("B" + i));
      consumer.accept(valueInGlobalWindow("C" + i));
      consumer.close();
    }

    // Specifically copy the elements to a new list so we perform blocking calls on the queue
    // to ensure the elements arrive.
    List<Elements> copy = new ArrayList<>();
    for (int i = 0; i < numberOfClients; ++i) {
      copy.add(clientInboundElements.take());
    }

    assertThat(
        copy,
        containsInAnyOrder(elementsWithData("0"), elementsWithData("1"), elementsWithData("2")));
    waitForInboundElements.countDown();
  }

  @Test
  public void testMessageReceivedByProperClientWhenThereAreMultipleClients() throws Exception {
    ConcurrentHashMap<String, LinkedBlockingQueue<Elements>> clientInboundElements =
        new ConcurrentHashMap<>();
    ExecutorService executorService = Executors.newCachedThreadPool();
    CountDownLatch waitForInboundElements = new CountDownLatch(1);
    int numberOfClients = 3;
    int numberOfMessages = 3;

    for (int client = 0; client < numberOfClients; ++client) {
      String clientId = Integer.toString(client);
      clientInboundElements.put(clientId, new LinkedBlockingQueue<>());
      executorService.submit(
          () -> {
            ManagedChannel channel =
                ManagedChannelFactory.createDefault()
                    .withInterceptors(Arrays.asList(AddHarnessIdInterceptor.create(clientId)))
                    .forDescriptor(service.getApiServiceDescriptor());
            StreamObserver<BeamFnApi.Elements> outboundObserver =
                BeamFnDataGrpc.newStub(channel)
                    .data(TestStreams.withOnNext(clientInboundElements.get(clientId)::add).build());
            waitForInboundElements.await();
            outboundObserver.onCompleted();
            return null;
          });
    }

    for (int client = 0; client < numberOfClients; ++client) {
      for (int i = 0; i < 3; ++i) {
        String instructionId = client + "-" + i;
        CloseableFnDataReceiver<WindowedValue<String>> consumer =
            service
                .getDataService(Integer.toString(client))
                .send(LogicalEndpoint.data(instructionId, TRANSFORM_ID), CODER);

        consumer.accept(valueInGlobalWindow("A" + instructionId));
        consumer.accept(valueInGlobalWindow("B" + instructionId));
        consumer.accept(valueInGlobalWindow("C" + instructionId));
        consumer.close();
      }
    }

    for (int client = 0; client < numberOfClients; ++client) {
      // Specifically copy the elements to a new list so we perform blocking calls on the queue
      // to ensure the elements arrive.
      ArrayList<BeamFnApi.Elements> copy = new ArrayList<>();
      for (int i = 0; i < numberOfMessages; ++i) {
        copy.add(clientInboundElements.get(Integer.toString(client)).take());
      }
      assertThat(
          copy,
          containsInAnyOrder(
              elementsWithData(client + "-" + 0),
              elementsWithData(client + "-" + 1),
              elementsWithData(client + "-" + 2)));
    }
    waitForInboundElements.countDown();
  }

  @Test
  public void testMultipleClientsSendMessagesAreDirectedToProperConsumers() throws Exception {
    LinkedBlockingQueue<BeamFnApi.Elements> clientInboundElements = new LinkedBlockingQueue<>();
    ExecutorService executorService = Executors.newCachedThreadPool();
    CountDownLatch waitForInboundElements = new CountDownLatch(1);

    for (int i = 0; i < 3; ++i) {
      String instructionId = Integer.toString(i);
      executorService.submit(
          () -> {
            ManagedChannel channel =
                ManagedChannelFactory.createDefault()
                    .withInterceptors(Arrays.asList(AddHarnessIdInterceptor.create(WORKER_ID)))
                    .forDescriptor(service.getApiServiceDescriptor());
            StreamObserver<BeamFnApi.Elements> outboundObserver =
                BeamFnDataGrpc.newStub(channel)
                    .data(TestStreams.withOnNext(clientInboundElements::add).build());
            outboundObserver.onNext(elementsWithData(instructionId));
            waitForInboundElements.await();
            outboundObserver.onCompleted();
            return null;
          });
    }

    List<Collection<WindowedValue<String>>> serverInboundValues = new ArrayList<>();
    Collection<InboundDataClient> inboundDataClients = new ArrayList<>();
    for (int i = 0; i < 3; ++i) {
      BlockingQueue<WindowedValue<String>> serverInboundValue = new LinkedBlockingQueue<>();
      serverInboundValues.add(serverInboundValue);
      inboundDataClients.add(
          service
              .getDataService(WORKER_ID)
              .receive(
                  LogicalEndpoint.data(Integer.toString(i), TRANSFORM_ID),
                  CODER,
                  serverInboundValue::add));
    }

    // Waiting for the client provides the necessary synchronization for the elements to arrive.
    for (InboundDataClient inboundDataClient : inboundDataClients) {
      inboundDataClient.awaitCompletion();
    }
    waitForInboundElements.countDown();
    for (int i = 0; i < 3; ++i) {
      assertThat(
          serverInboundValues.get(i),
          contains(
              valueInGlobalWindow("A" + i),
              valueInGlobalWindow("B" + i),
              valueInGlobalWindow("C" + i)));
    }
    assertThat(clientInboundElements, empty());
  }

  private BeamFnApi.Elements elementsWithData(String id) throws CoderException {
    return BeamFnApi.Elements.newBuilder()
        .addData(
            BeamFnApi.Elements.Data.newBuilder()
                .setInstructionId(id)
                .setTransformId(TRANSFORM_ID)
                .setData(
                    ByteString.copyFrom(encodeToByteArray(CODER, valueInGlobalWindow("A" + id)))
                        .concat(
                            ByteString.copyFrom(
                                encodeToByteArray(CODER, valueInGlobalWindow("B" + id))))
                        .concat(
                            ByteString.copyFrom(
                                encodeToByteArray(CODER, valueInGlobalWindow("C" + id))))))
        .addData(
            BeamFnApi.Elements.Data.newBuilder()
                .setInstructionId(id)
                .setTransformId(TRANSFORM_ID)
                .setIsLast(true))
        .build();
  }
}
