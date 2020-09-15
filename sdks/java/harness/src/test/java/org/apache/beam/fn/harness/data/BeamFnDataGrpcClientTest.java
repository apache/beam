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
package org.apache.beam.fn.harness.data;

import static org.apache.beam.sdk.util.CoderUtils.encodeToByteArray;
import static org.apache.beam.sdk.util.WindowedValue.valueInGlobalWindow;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.collection.IsEmptyCollection.empty;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

import java.util.Collection;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.beam.model.fnexecution.v1.BeamFnApi;
import org.apache.beam.model.fnexecution.v1.BeamFnDataGrpc;
import org.apache.beam.model.pipeline.v1.Endpoints;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.LengthPrefixCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.fn.data.CloseableFnDataReceiver;
import org.apache.beam.sdk.fn.data.InboundDataClient;
import org.apache.beam.sdk.fn.data.LogicalEndpoint;
import org.apache.beam.sdk.fn.stream.OutboundObserverFactory;
import org.apache.beam.sdk.fn.test.TestStreams;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.vendor.grpc.v1p26p0.com.google.protobuf.ByteString;
import org.apache.beam.vendor.grpc.v1p26p0.io.grpc.ManagedChannel;
import org.apache.beam.vendor.grpc.v1p26p0.io.grpc.Server;
import org.apache.beam.vendor.grpc.v1p26p0.io.grpc.inprocess.InProcessChannelBuilder;
import org.apache.beam.vendor.grpc.v1p26p0.io.grpc.inprocess.InProcessServerBuilder;
import org.apache.beam.vendor.grpc.v1p26p0.io.grpc.stub.CallStreamObserver;
import org.apache.beam.vendor.grpc.v1p26p0.io.grpc.stub.StreamObserver;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link BeamFnDataGrpcClient}. */
@RunWith(JUnit4.class)
public class BeamFnDataGrpcClientTest {
  private static final Coder<WindowedValue<String>> CODER =
      LengthPrefixCoder.of(
          WindowedValue.getFullCoder(StringUtf8Coder.of(), GlobalWindow.Coder.INSTANCE));
  private static final LogicalEndpoint ENDPOINT_A = LogicalEndpoint.data("12L", "34L");

  private static final LogicalEndpoint ENDPOINT_B = LogicalEndpoint.data("56L", "78L");

  private static final BeamFnApi.Elements ELEMENTS_A_1;
  private static final BeamFnApi.Elements ELEMENTS_A_2;
  private static final BeamFnApi.Elements ELEMENTS_B_1;

  static {
    try {
      ELEMENTS_A_1 =
          BeamFnApi.Elements.newBuilder()
              .addData(
                  BeamFnApi.Elements.Data.newBuilder()
                      .setInstructionId(ENDPOINT_A.getInstructionId())
                      .setTransformId(ENDPOINT_A.getTransformId())
                      .setData(
                          ByteString.copyFrom(encodeToByteArray(CODER, valueInGlobalWindow("ABC")))
                              .concat(
                                  ByteString.copyFrom(
                                      encodeToByteArray(CODER, valueInGlobalWindow("DEF"))))))
              .build();
      ELEMENTS_A_2 =
          BeamFnApi.Elements.newBuilder()
              .addData(
                  BeamFnApi.Elements.Data.newBuilder()
                      .setInstructionId(ENDPOINT_A.getInstructionId())
                      .setTransformId(ENDPOINT_A.getTransformId())
                      .setData(
                          ByteString.copyFrom(
                              encodeToByteArray(CODER, valueInGlobalWindow("GHI")))))
              .addData(
                  BeamFnApi.Elements.Data.newBuilder()
                      .setInstructionId(ENDPOINT_A.getInstructionId())
                      .setTransformId(ENDPOINT_A.getTransformId())
                      .setIsLast(true))
              .build();
      ELEMENTS_B_1 =
          BeamFnApi.Elements.newBuilder()
              .addData(
                  BeamFnApi.Elements.Data.newBuilder()
                      .setInstructionId(ENDPOINT_B.getInstructionId())
                      .setTransformId(ENDPOINT_B.getTransformId())
                      .setData(
                          ByteString.copyFrom(encodeToByteArray(CODER, valueInGlobalWindow("JKL")))
                              .concat(
                                  ByteString.copyFrom(
                                      encodeToByteArray(CODER, valueInGlobalWindow("MNO"))))))
              .addData(
                  BeamFnApi.Elements.Data.newBuilder()
                      .setInstructionId(ENDPOINT_B.getInstructionId())
                      .setTransformId(ENDPOINT_B.getTransformId())
                      .setIsLast(true))
              .build();
    } catch (Exception e) {
      throw new ExceptionInInitializerError(e);
    }
  }

  @Test
  public void testForInboundConsumer() throws Exception {
    CountDownLatch waitForClientToConnect = new CountDownLatch(1);
    Collection<WindowedValue<String>> inboundValuesA = new ConcurrentLinkedQueue<>();
    Collection<WindowedValue<String>> inboundValuesB = new ConcurrentLinkedQueue<>();
    Collection<BeamFnApi.Elements> inboundServerValues = new ConcurrentLinkedQueue<>();
    AtomicReference<StreamObserver<BeamFnApi.Elements>> outboundServerObserver =
        new AtomicReference<>();
    CallStreamObserver<BeamFnApi.Elements> inboundServerObserver =
        TestStreams.withOnNext(inboundServerValues::add).build();

    Endpoints.ApiServiceDescriptor apiServiceDescriptor =
        Endpoints.ApiServiceDescriptor.newBuilder()
            .setUrl(this.getClass().getName() + "-" + UUID.randomUUID().toString())
            .build();
    Server server =
        InProcessServerBuilder.forName(apiServiceDescriptor.getUrl())
            .addService(
                new BeamFnDataGrpc.BeamFnDataImplBase() {
                  @Override
                  public StreamObserver<BeamFnApi.Elements> data(
                      StreamObserver<BeamFnApi.Elements> outboundObserver) {
                    outboundServerObserver.set(outboundObserver);
                    waitForClientToConnect.countDown();
                    return inboundServerObserver;
                  }
                })
            .build();
    server.start();
    try {
      ManagedChannel channel =
          InProcessChannelBuilder.forName(apiServiceDescriptor.getUrl()).build();

      BeamFnDataGrpcClient clientFactory =
          new BeamFnDataGrpcClient(
              PipelineOptionsFactory.create(),
              (Endpoints.ApiServiceDescriptor descriptor) -> channel,
              OutboundObserverFactory.trivial());

      InboundDataClient readFutureA =
          clientFactory.receive(apiServiceDescriptor, ENDPOINT_A, CODER, inboundValuesA::add);

      waitForClientToConnect.await();
      outboundServerObserver.get().onNext(ELEMENTS_A_1);
      // Purposefully transmit some data before the consumer for B is bound showing that
      // data is not lost
      outboundServerObserver.get().onNext(ELEMENTS_B_1);
      Thread.sleep(100);

      InboundDataClient readFutureB =
          clientFactory.receive(apiServiceDescriptor, ENDPOINT_B, CODER, inboundValuesB::add);

      // Show that out of order stream completion can occur.
      readFutureB.awaitCompletion();
      assertThat(inboundValuesB, contains(valueInGlobalWindow("JKL"), valueInGlobalWindow("MNO")));

      outboundServerObserver.get().onNext(ELEMENTS_A_2);
      readFutureA.awaitCompletion();
      assertThat(
          inboundValuesA,
          contains(
              valueInGlobalWindow("ABC"), valueInGlobalWindow("DEF"), valueInGlobalWindow("GHI")));
    } finally {
      server.shutdownNow();
    }
  }

  @Test
  public void testForInboundConsumerThatThrows() throws Exception {
    CountDownLatch waitForClientToConnect = new CountDownLatch(1);
    AtomicInteger consumerInvoked = new AtomicInteger();
    Collection<BeamFnApi.Elements> inboundServerValues = new ConcurrentLinkedQueue<>();
    AtomicReference<StreamObserver<BeamFnApi.Elements>> outboundServerObserver =
        new AtomicReference<>();
    CallStreamObserver<BeamFnApi.Elements> inboundServerObserver =
        TestStreams.withOnNext(inboundServerValues::add).build();

    Endpoints.ApiServiceDescriptor apiServiceDescriptor =
        Endpoints.ApiServiceDescriptor.newBuilder()
            .setUrl(this.getClass().getName() + "-" + UUID.randomUUID().toString())
            .build();
    Server server =
        InProcessServerBuilder.forName(apiServiceDescriptor.getUrl())
            .addService(
                new BeamFnDataGrpc.BeamFnDataImplBase() {
                  @Override
                  public StreamObserver<BeamFnApi.Elements> data(
                      StreamObserver<BeamFnApi.Elements> outboundObserver) {
                    outboundServerObserver.set(outboundObserver);
                    waitForClientToConnect.countDown();
                    return inboundServerObserver;
                  }
                })
            .build();
    server.start();
    RuntimeException exceptionToThrow = new RuntimeException("TestFailure");
    try {
      ManagedChannel channel =
          InProcessChannelBuilder.forName(apiServiceDescriptor.getUrl()).build();

      BeamFnDataGrpcClient clientFactory =
          new BeamFnDataGrpcClient(
              PipelineOptionsFactory.create(),
              (Endpoints.ApiServiceDescriptor descriptor) -> channel,
              OutboundObserverFactory.trivial());

      InboundDataClient readFuture =
          clientFactory.receive(
              apiServiceDescriptor,
              ENDPOINT_A,
              CODER,
              t -> {
                consumerInvoked.incrementAndGet();
                throw exceptionToThrow;
              });

      waitForClientToConnect.await();

      // This first message should cause a failure afterwards all other messages are dropped.
      outboundServerObserver.get().onNext(ELEMENTS_A_1);
      outboundServerObserver.get().onNext(ELEMENTS_A_2);

      try {
        readFuture.awaitCompletion();
        fail("Expected channel to fail");
      } catch (ExecutionException e) {
        assertEquals(exceptionToThrow, e.getCause());
      }
      // The server should not have received any values
      assertThat(inboundServerValues, empty());
      // The consumer should have only been invoked once
      assertEquals(1, consumerInvoked.get());
    } finally {
      server.shutdownNow();
    }
  }

  @Test
  public void testForOutboundConsumer() throws Exception {
    CountDownLatch waitForInboundServerValuesCompletion = new CountDownLatch(2);
    Collection<BeamFnApi.Elements> inboundServerValues = new ConcurrentLinkedQueue<>();
    CallStreamObserver<BeamFnApi.Elements> inboundServerObserver =
        TestStreams.withOnNext(
                (BeamFnApi.Elements t) -> {
                  inboundServerValues.add(t);
                  waitForInboundServerValuesCompletion.countDown();
                })
            .build();

    Endpoints.ApiServiceDescriptor apiServiceDescriptor =
        Endpoints.ApiServiceDescriptor.newBuilder()
            .setUrl(this.getClass().getName() + "-" + UUID.randomUUID().toString())
            .build();
    Server server =
        InProcessServerBuilder.forName(apiServiceDescriptor.getUrl())
            .addService(
                new BeamFnDataGrpc.BeamFnDataImplBase() {
                  @Override
                  public StreamObserver<BeamFnApi.Elements> data(
                      StreamObserver<BeamFnApi.Elements> outboundObserver) {
                    return inboundServerObserver;
                  }
                })
            .build();
    server.start();
    try {
      ManagedChannel channel =
          InProcessChannelBuilder.forName(apiServiceDescriptor.getUrl()).build();

      BeamFnDataGrpcClient clientFactory =
          new BeamFnDataGrpcClient(
              PipelineOptionsFactory.fromArgs(
                      new String[] {"--experiments=data_buffer_size_limit=20"})
                  .create(),
              (Endpoints.ApiServiceDescriptor descriptor) -> channel,
              OutboundObserverFactory.trivial());

      try (CloseableFnDataReceiver<WindowedValue<String>> consumer =
          clientFactory.send(apiServiceDescriptor, ENDPOINT_A, CODER)) {
        consumer.accept(valueInGlobalWindow("ABC"));
        consumer.accept(valueInGlobalWindow("DEF"));
        consumer.accept(valueInGlobalWindow("GHI"));
      }

      waitForInboundServerValuesCompletion.await();

      assertThat(inboundServerValues, contains(ELEMENTS_A_1, ELEMENTS_A_2));
    } finally {
      server.shutdownNow();
    }
  }
}
