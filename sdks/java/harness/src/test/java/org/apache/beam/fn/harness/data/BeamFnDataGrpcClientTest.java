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
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.empty;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.beam.model.fnexecution.v1.BeamFnApi;
import org.apache.beam.model.fnexecution.v1.BeamFnDataGrpc;
import org.apache.beam.model.pipeline.v1.Endpoints;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.LengthPrefixCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.fn.data.BeamFnDataInboundObserver;
import org.apache.beam.sdk.fn.data.BeamFnDataOutboundAggregator;
import org.apache.beam.sdk.fn.data.DataEndpoint;
import org.apache.beam.sdk.fn.data.FnDataReceiver;
import org.apache.beam.sdk.fn.data.LogicalEndpoint;
import org.apache.beam.sdk.fn.stream.OutboundObserverFactory;
import org.apache.beam.sdk.fn.test.TestStreams;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.vendor.grpc.v1p60p1.com.google.protobuf.ByteString;
import org.apache.beam.vendor.grpc.v1p60p1.io.grpc.ManagedChannel;
import org.apache.beam.vendor.grpc.v1p60p1.io.grpc.Server;
import org.apache.beam.vendor.grpc.v1p60p1.io.grpc.inprocess.InProcessChannelBuilder;
import org.apache.beam.vendor.grpc.v1p60p1.io.grpc.inprocess.InProcessServerBuilder;
import org.apache.beam.vendor.grpc.v1p60p1.io.grpc.stub.CallStreamObserver;
import org.apache.beam.vendor.grpc.v1p60p1.io.grpc.stub.StreamObserver;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link BeamFnDataGrpcClient}. */
@RunWith(JUnit4.class)
public class BeamFnDataGrpcClientTest {
  private static final Coder<WindowedValue<String>> CODER =
      LengthPrefixCoder.of(
          WindowedValue.getFullCoder(StringUtf8Coder.of(), GlobalWindow.Coder.INSTANCE));
  private static final String INSTRUCTION_ID_A = "12L";
  private static final String INSTRUCTION_ID_B = "56L";
  private static final String TRANSFORM_ID_A = "34L";
  private static final String TRANSFORM_ID_B = "78L";
  private static final LogicalEndpoint ENDPOINT_A =
      LogicalEndpoint.data(INSTRUCTION_ID_A, TRANSFORM_ID_A);
  private static final LogicalEndpoint ENDPOINT_B =
      LogicalEndpoint.data(INSTRUCTION_ID_B, TRANSFORM_ID_B);

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
            .setUrl(this.getClass().getName() + "-" + UUID.randomUUID())
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

      BeamFnDataInboundObserver observerA =
          BeamFnDataInboundObserver.forConsumers(
              Arrays.asList(DataEndpoint.create(TRANSFORM_ID_A, CODER, inboundValuesA::add)),
              Collections.emptyList());
      BeamFnDataInboundObserver observerB =
          BeamFnDataInboundObserver.forConsumers(
              Arrays.asList(DataEndpoint.create(TRANSFORM_ID_B, CODER, inboundValuesB::add)),
              Collections.emptyList());

      clientFactory.registerReceiver(
          INSTRUCTION_ID_A, Arrays.asList(apiServiceDescriptor), observerA);

      waitForClientToConnect.await();
      outboundServerObserver.get().onNext(ELEMENTS_A_1);
      // Purposefully transmit some data before the consumer for B is bound showing that
      // data is not lost
      outboundServerObserver.get().onNext(ELEMENTS_B_1);
      Thread.sleep(100);

      clientFactory.registerReceiver(
          INSTRUCTION_ID_B, Arrays.asList(apiServiceDescriptor), observerB);

      // Show that out of order stream completion can occur.
      observerB.awaitCompletion();
      assertThat(inboundValuesB, contains(valueInGlobalWindow("JKL"), valueInGlobalWindow("MNO")));

      outboundServerObserver.get().onNext(ELEMENTS_A_2);
      observerA.awaitCompletion();
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
            .setUrl(this.getClass().getName() + "-" + UUID.randomUUID())
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

      BeamFnDataInboundObserver observer =
          BeamFnDataInboundObserver.forConsumers(
              Arrays.asList(
                  DataEndpoint.create(
                      TRANSFORM_ID_A,
                      CODER,
                      t -> {
                        consumerInvoked.incrementAndGet();
                        throw exceptionToThrow;
                      })),
              Collections.emptyList());

      clientFactory.registerReceiver(
          INSTRUCTION_ID_A, Arrays.asList(apiServiceDescriptor), observer);

      waitForClientToConnect.await();

      // This first message should cause a failure afterwards all other messages are dropped.
      outboundServerObserver.get().onNext(ELEMENTS_A_1);
      outboundServerObserver.get().onNext(ELEMENTS_A_2);

      try {
        observer.awaitCompletion();
        fail("Expected channel to fail");
      } catch (Exception e) {
        assertEquals(exceptionToThrow, e);
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
  public void testForInboundConsumerThatIsPoisoned() throws Exception {
    CountDownLatch waitForClientToConnect = new CountDownLatch(1);
    CountDownLatch receivedAElement = new CountDownLatch(1);
    Collection<WindowedValue<String>> inboundValuesA = new ConcurrentLinkedQueue<>();
    Collection<BeamFnApi.Elements> inboundServerValues = new ConcurrentLinkedQueue<>();
    AtomicReference<StreamObserver<BeamFnApi.Elements>> outboundServerObserver =
        new AtomicReference<>();
    CallStreamObserver<BeamFnApi.Elements> inboundServerObserver =
        TestStreams.withOnNext(inboundServerValues::add).build();

    Endpoints.ApiServiceDescriptor apiServiceDescriptor =
        Endpoints.ApiServiceDescriptor.newBuilder()
            .setUrl(this.getClass().getName() + "-" + UUID.randomUUID())
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

      BeamFnDataInboundObserver observerA =
          BeamFnDataInboundObserver.forConsumers(
              Arrays.asList(
                  DataEndpoint.create(
                      TRANSFORM_ID_A,
                      CODER,
                      (WindowedValue<String> elem) -> {
                        receivedAElement.countDown();
                        inboundValuesA.add(elem);
                      })),
              Collections.emptyList());
      CompletableFuture<Void> future =
          CompletableFuture.runAsync(
              () -> {
                try {
                  observerA.awaitCompletion();
                } catch (Exception e) {
                  throw new RuntimeException(e);
                }
              });

      clientFactory.registerReceiver(
          INSTRUCTION_ID_A, Arrays.asList(apiServiceDescriptor), observerA);

      waitForClientToConnect.await();
      outboundServerObserver.get().onNext(ELEMENTS_B_1);
      clientFactory.poisonInstructionId(INSTRUCTION_ID_B);

      outboundServerObserver.get().onNext(ELEMENTS_B_1);
      outboundServerObserver.get().onNext(ELEMENTS_A_1);
      assertTrue(receivedAElement.await(5, TimeUnit.SECONDS));

      clientFactory.poisonInstructionId(INSTRUCTION_ID_A);
      try {
        future.get();
        fail(); // We expect the awaitCompletion to fail due to closing.
      } catch (Exception ignored) {
      }

      outboundServerObserver.get().onNext(ELEMENTS_A_2);

      assertThat(inboundValuesA, contains(valueInGlobalWindow("ABC"), valueInGlobalWindow("DEF")));
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
            .setUrl(this.getClass().getName() + "-" + UUID.randomUUID())
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
      BeamFnDataOutboundAggregator aggregator =
          clientFactory.createOutboundAggregator(
              apiServiceDescriptor, () -> INSTRUCTION_ID_A, false);
      FnDataReceiver<WindowedValue<String>> fnDataReceiver =
          aggregator.registerOutputDataLocation(TRANSFORM_ID_A, CODER);
      fnDataReceiver.accept(valueInGlobalWindow("ABC"));
      fnDataReceiver.accept(valueInGlobalWindow("DEF"));
      fnDataReceiver.accept(valueInGlobalWindow("GHI"));
      aggregator.sendOrCollectBufferedDataAndFinishOutboundStreams();
      waitForInboundServerValuesCompletion.await();

      assertThat(inboundServerValues, contains(ELEMENTS_A_1, ELEMENTS_A_2));
    } finally {
      server.shutdownNow();
    }
  }
}
