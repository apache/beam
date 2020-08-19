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
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.Collection;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.beam.model.fnexecution.v1.BeamFnApi;
import org.apache.beam.model.fnexecution.v1.BeamFnDataGrpc;
import org.apache.beam.model.pipeline.v1.Endpoints;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.LengthPrefixCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.fn.data.InboundDataClient;
import org.apache.beam.sdk.fn.data.LogicalEndpoint;
import org.apache.beam.sdk.fn.stream.OutboundObserverFactory;
import org.apache.beam.sdk.fn.test.TestExecutors;
import org.apache.beam.sdk.fn.test.TestExecutors.TestExecutorService;
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
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Tests for {@link QueueingBeamFnDataClient}. */
@RunWith(JUnit4.class)
public class QueueingBeamFnDataClientTest {

  private static final Logger LOG = LoggerFactory.getLogger(QueueingBeamFnDataClientTest.class);

  @Rule public TestExecutorService executor = TestExecutors.from(Executors::newCachedThreadPool);

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

  @Test(timeout = 10000)
  public void testBasicInboundConsumerBehaviour() throws Exception {
    CountDownLatch waitForClientToConnect = new CountDownLatch(1);
    CountDownLatch receiveAllValuesA = new CountDownLatch(3);
    CountDownLatch receiveAllValuesB = new CountDownLatch(2);
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
      QueueingBeamFnDataClient queueingClient = new QueueingBeamFnDataClient(clientFactory);

      InboundDataClient readFutureA =
          queueingClient.receive(
              apiServiceDescriptor,
              ENDPOINT_A,
              CODER,
              (WindowedValue<String> wv) -> {
                inboundValuesA.add(wv);
                receiveAllValuesA.countDown();
              });

      waitForClientToConnect.await();

      Future<?> sendElementsFuture =
          executor.submit(
              () -> {
                outboundServerObserver.get().onNext(ELEMENTS_A_1);
                // Purposefully transmit some data before the consumer for B is bound showing that
                // data is not lost
                outboundServerObserver.get().onNext(ELEMENTS_B_1);
              });

      // This can be compeleted before we get values?
      InboundDataClient readFutureB =
          queueingClient.receive(
              apiServiceDescriptor,
              ENDPOINT_B,
              CODER,
              (WindowedValue<String> wv) -> {
                inboundValuesB.add(wv);
                receiveAllValuesB.countDown();
              });

      Future<?> drainElementsFuture =
          executor.submit(
              () -> {
                try {
                  queueingClient.drainAndBlock();
                } catch (Exception e) {
                  LOG.error("Failed ", e);
                  fail();
                }
              });

      receiveAllValuesB.await();
      assertThat(inboundValuesB, contains(valueInGlobalWindow("JKL"), valueInGlobalWindow("MNO")));

      outboundServerObserver.get().onNext(ELEMENTS_A_2);

      receiveAllValuesA.await(); // Wait for A's values to be available
      assertThat(
          inboundValuesA,
          contains(
              valueInGlobalWindow("ABC"), valueInGlobalWindow("DEF"), valueInGlobalWindow("GHI")));

      // Wait for these threads to terminate
      sendElementsFuture.get();
      drainElementsFuture.get();
    } finally {
      server.shutdownNow();
    }
  }

  @Test(timeout = 100000)
  public void testBundleProcessorThrowsExecutionExceptionWhenUserCodeThrows() throws Exception {
    CountDownLatch waitForClientToConnect = new CountDownLatch(1);
    // Collection<WindowedValue<String>> inboundValuesA = new ConcurrentLinkedQueue<>();
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
      QueueingBeamFnDataClient queueingClient = new QueueingBeamFnDataClient(clientFactory);

      InboundDataClient readFutureA =
          queueingClient.receive(
              apiServiceDescriptor,
              ENDPOINT_A,
              CODER,
              (WindowedValue<String> wv) -> {
                throw new RuntimeException("Intentionally fail!"); // Error injected here.
              });

      waitForClientToConnect.await();

      Future<?> sendElementsFuture =
          executor.submit(
              () -> {
                outboundServerObserver.get().onNext(ELEMENTS_A_1);
                // Purposefully transmit some data before the consumer for B is bound showing that
                // data is not lost
                outboundServerObserver.get().onNext(ELEMENTS_B_1);
              });

      InboundDataClient readFutureB =
          queueingClient.receive(
              apiServiceDescriptor,
              ENDPOINT_B,
              CODER,
              (WindowedValue<String> wv) -> {
                inboundValuesB.add(wv);
              });

      Future<?> drainElementsFuture =
          executor.submit(
              () -> {
                boolean intentionallyFailed = false;
                try {
                  queueingClient.drainAndBlock();
                } catch (RuntimeException e) {
                  intentionallyFailed = true;
                } catch (Exception e) {
                  LOG.error("Unintentional failure", e);
                  fail();
                }
                assertTrue(intentionallyFailed);
              });

      // Fail all InboundObservers if any of the downstream consumers fail.
      // This allows the ProcessBundlerHandler to unblock everything and fail properly.

      // Wait for these threads to terminate
      sendElementsFuture.get();
      drainElementsFuture.get();

      boolean intentionallyFailedA = false;
      try {
        readFutureA.awaitCompletion();
      } catch (ExecutionException e) {
        if (e.getCause() instanceof RuntimeException) {
          intentionallyFailedA = true;
        }
      }
      assertTrue(intentionallyFailedA);

      boolean intentionallyFailedB = false;
      try {
        readFutureB.awaitCompletion();
      } catch (ExecutionException e) {
        if (e.getCause() instanceof RuntimeException) {
          intentionallyFailedB = true;
        }
      }
      assertTrue(intentionallyFailedB);

    } finally {
      server.shutdownNow();
    }
  }
}
