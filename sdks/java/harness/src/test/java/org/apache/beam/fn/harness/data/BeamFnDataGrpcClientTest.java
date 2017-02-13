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

import com.google.protobuf.ByteString;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.CallStreamObserver;
import io.grpc.stub.StreamObserver;
import java.util.Collection;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Function;
import org.apache.beam.fn.harness.fn.CloseableThrowingConsumer;
import org.apache.beam.fn.harness.fn.ThrowingConsumer;
import org.apache.beam.fn.harness.test.TestStreams;
import org.apache.beam.fn.v1.BeamFnApi;
import org.apache.beam.fn.v1.BeamFnDataGrpc;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.LengthPrefixCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.KV;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link BeamFnDataGrpcClient}. */
@RunWith(JUnit4.class)
public class BeamFnDataGrpcClientTest {
  private static final Coder<WindowedValue<String>> CODER =
      LengthPrefixCoder.of(
          WindowedValue.getFullCoder(StringUtf8Coder.of(),
              GlobalWindow.Coder.INSTANCE));
  private static final KV<String, BeamFnApi.Target> KEY_A =
      KV.of(
          "12L",
          BeamFnApi.Target.newBuilder()
              .setPrimitiveTransformReference("34L")
              .setName("targetA")
              .build());

  private static final KV<String, BeamFnApi.Target> KEY_B =
      KV.of(
          "56L",
          BeamFnApi.Target.newBuilder()
              .setPrimitiveTransformReference("78L")
              .setName("targetB")
              .build());

  private static final BeamFnApi.Elements ELEMENTS_A_1;
  private static final BeamFnApi.Elements ELEMENTS_A_2;
  private static final BeamFnApi.Elements ELEMENTS_B_1;
  static {
    try {
    ELEMENTS_A_1 = BeamFnApi.Elements.newBuilder()
        .addData(BeamFnApi.Elements.Data.newBuilder()
            .setInstructionReference(KEY_A.getKey())
            .setTarget(KEY_A.getValue())
            .setData(ByteString.copyFrom(encodeToByteArray(CODER, valueInGlobalWindow("ABC")))
                .concat(ByteString.copyFrom(encodeToByteArray(CODER, valueInGlobalWindow("DEF"))))))
        .build();
    ELEMENTS_A_2 = BeamFnApi.Elements.newBuilder()
        .addData(BeamFnApi.Elements.Data.newBuilder()
            .setInstructionReference(KEY_A.getKey())
            .setTarget(KEY_A.getValue())
            .setData(ByteString.copyFrom(encodeToByteArray(CODER, valueInGlobalWindow("GHI")))))
        .addData(BeamFnApi.Elements.Data.newBuilder()
            .setInstructionReference(KEY_A.getKey())
            .setTarget(KEY_A.getValue()))
        .build();
    ELEMENTS_B_1 = BeamFnApi.Elements.newBuilder()
        .addData(BeamFnApi.Elements.Data.newBuilder()
            .setInstructionReference(KEY_B.getKey())
            .setTarget(KEY_B.getValue())
            .setData(ByteString.copyFrom(encodeToByteArray(CODER, valueInGlobalWindow("JKL")))
                .concat(ByteString.copyFrom(encodeToByteArray(CODER, valueInGlobalWindow("MNO"))))))
        .addData(BeamFnApi.Elements.Data.newBuilder()
            .setInstructionReference(KEY_B.getKey())
            .setTarget(KEY_B.getValue()))
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

    BeamFnApi.ApiServiceDescriptor apiServiceDescriptor =
        BeamFnApi.ApiServiceDescriptor.newBuilder()
            .setUrl(this.getClass().getName() + "-" + UUID.randomUUID().toString())
            .build();
    Server server = InProcessServerBuilder.forName(apiServiceDescriptor.getUrl())
            .addService(new BeamFnDataGrpc.BeamFnDataImplBase() {
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

    BeamFnDataGrpcClient clientFactory = new BeamFnDataGrpcClient(
        PipelineOptionsFactory.create(),
        (BeamFnApi.ApiServiceDescriptor descriptor) -> channel,
        this::createStreamForTest);

    CompletableFuture<Void> readFutureA = clientFactory.forInboundConsumer(
        apiServiceDescriptor,
        KEY_A,
        CODER,
        inboundValuesA::add);

      waitForClientToConnect.await();
      outboundServerObserver.get().onNext(ELEMENTS_A_1);
      // Purposefully transmit some data before the consumer for B is bound showing that
      // data is not lost
      outboundServerObserver.get().onNext(ELEMENTS_B_1);
      Thread.sleep(100);

      CompletableFuture<Void> readFutureB = clientFactory.forInboundConsumer(
          apiServiceDescriptor,
          KEY_B,
          CODER,
          inboundValuesB::add);

      // Show that out of order stream completion can occur.
      readFutureB.get();
      assertThat(inboundValuesB, contains(
          valueInGlobalWindow("JKL"), valueInGlobalWindow("MNO")));

      outboundServerObserver.get().onNext(ELEMENTS_A_2);
      readFutureA.get();
      assertThat(inboundValuesA, contains(
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

    BeamFnApi.ApiServiceDescriptor apiServiceDescriptor =
        BeamFnApi.ApiServiceDescriptor.newBuilder()
            .setUrl(this.getClass().getName() + "-" + UUID.randomUUID().toString())
            .build();
    Server server = InProcessServerBuilder.forName(apiServiceDescriptor.getUrl())
            .addService(new BeamFnDataGrpc.BeamFnDataImplBase() {
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

      BeamFnDataGrpcClient clientFactory = new BeamFnDataGrpcClient(
          PipelineOptionsFactory.create(),
          (BeamFnApi.ApiServiceDescriptor descriptor) -> channel,
          this::createStreamForTest);

      CompletableFuture<Void> readFuture = clientFactory.forInboundConsumer(
          apiServiceDescriptor,
          KEY_A,
          CODER,
          new ThrowingConsumer<WindowedValue<String>>() {
            @Override
            public void accept(WindowedValue<String> t) throws Exception {
              consumerInvoked.incrementAndGet();
              throw exceptionToThrow;
            }
          });

      waitForClientToConnect.await();

      // This first message should cause a failure afterwards all other messages are dropped.
      outboundServerObserver.get().onNext(ELEMENTS_A_1);
      outboundServerObserver.get().onNext(ELEMENTS_A_2);

      try {
        readFuture.get();
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
            new Consumer<BeamFnApi.Elements>() {
              @Override
              public void accept(BeamFnApi.Elements t) {
                inboundServerValues.add(t);
                waitForInboundServerValuesCompletion.countDown();
              }
            }
            ).build();

    BeamFnApi.ApiServiceDescriptor apiServiceDescriptor =
        BeamFnApi.ApiServiceDescriptor.newBuilder()
            .setUrl(this.getClass().getName() + "-" + UUID.randomUUID().toString())
            .build();
    Server server = InProcessServerBuilder.forName(apiServiceDescriptor.getUrl())
            .addService(new BeamFnDataGrpc.BeamFnDataImplBase() {
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

      BeamFnDataGrpcClient clientFactory = new BeamFnDataGrpcClient(
          PipelineOptionsFactory.fromArgs(
              new String[]{ "--experiments=beam_fn_api_data_buffer_limit=20" }).create(),
          (BeamFnApi.ApiServiceDescriptor descriptor) -> channel,
          this::createStreamForTest);

      try (CloseableThrowingConsumer<WindowedValue<String>> consumer =
          clientFactory.forOutboundConsumer(apiServiceDescriptor, KEY_A, CODER)) {
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

  private <ReqT, RespT> StreamObserver<RespT> createStreamForTest(
      Function<StreamObserver<ReqT>, StreamObserver<RespT>> clientFactory,
      StreamObserver<ReqT> handler) {
    return clientFactory.apply(handler);
  }
}
