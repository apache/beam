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
package org.apache.beam.sdk.fn.data;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.StringContains.containsString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.beam.model.fnexecution.v1.BeamFnApi;
import org.apache.beam.model.pipeline.v1.Endpoints;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.fn.stream.OutboundObserverFactory;
import org.apache.beam.sdk.fn.stream.StalledPollHandler;
import org.apache.beam.sdk.fn.test.TestExecutors;
import org.apache.beam.sdk.fn.test.TestExecutors.TestExecutorService;
import org.apache.beam.sdk.fn.test.TestStreams;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.util.ByteStringOutputStream;
import org.apache.beam.sdk.values.WindowedValue;
import org.apache.beam.sdk.values.WindowedValues;
import org.apache.beam.vendor.grpc.v1p69p0.com.google.protobuf.ByteString;
import org.apache.beam.vendor.grpc.v1p69p0.io.grpc.stub.ClientCallStreamObserver;
import org.apache.beam.vendor.grpc.v1p69p0.io.grpc.stub.ClientResponseObserver;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Iterables;
import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;
import org.junit.Rule;
import org.junit.Test;

/** Tests for {@link BeamFnDataGrpcMultiplexer}. */
public class BeamFnDataGrpcMultiplexerTest {

  private static final Endpoints.ApiServiceDescriptor DESCRIPTOR =
      Endpoints.ApiServiceDescriptor.newBuilder().setUrl("test").build();
  private static final String DATA_INSTRUCTION_ID = "dataInstructionId";
  private static final String TIMER_INSTRUCTION_ID = "timerInstructionId";
  private static final BeamFnApi.Elements ELEMENTS =
      BeamFnApi.Elements.newBuilder()
          .addData(
              BeamFnApi.Elements.Data.newBuilder()
                  .setInstructionId(DATA_INSTRUCTION_ID)
                  .setTransformId("dataTransformId")
                  .setData(ByteString.copyFrom(new byte[1])))
          .addTimers(
              BeamFnApi.Elements.Timers.newBuilder()
                  .setInstructionId(TIMER_INSTRUCTION_ID)
                  .setTransformId("timerTransformId")
                  .setTimerFamilyId("timerFamilyId")
                  .setTimers(ByteString.copyFrom(new byte[2])))
          .build();
  private static final BeamFnApi.Elements TERMINAL_ELEMENTS =
      BeamFnApi.Elements.newBuilder()
          .addData(
              BeamFnApi.Elements.Data.newBuilder()
                  .setInstructionId(DATA_INSTRUCTION_ID)
                  .setTransformId("dataTransformId")
                  .setIsLast(true))
          .addTimers(
              BeamFnApi.Elements.Timers.newBuilder()
                  .setInstructionId(TIMER_INSTRUCTION_ID)
                  .setTransformId("timerTransformId")
                  .setTimerFamilyId("timerFamilyId")
                  .setIsLast(true))
          .build();

  @Rule
  public final TestExecutorService executor = TestExecutors.from(Executors::newCachedThreadPool);

  @Test
  public void testOutboundObserver() {
    Collection<BeamFnApi.Elements> values = new ArrayList<>();
    BeamFnDataGrpcMultiplexer multiplexer =
        new BeamFnDataGrpcMultiplexer(
            DESCRIPTOR,
            OutboundObserverFactory.clientDirect(),
            inboundObserver -> TestStreams.withOnNext(values::add).build());
    multiplexer.getOutboundObserver().onNext(ELEMENTS);
    MatcherAssert.assertThat(values, Matchers.contains(ELEMENTS));
  }

  @Test
  public void testInboundObserverBlocksTillConsumerConnects() throws Exception {
    Collection<BeamFnApi.Elements> outboundValues = new ArrayList<>();
    Collection<BeamFnApi.Elements> dataInboundValues = new ArrayList<>();
    Collection<BeamFnApi.Elements> timerInboundValues = new ArrayList<>();
    BeamFnDataGrpcMultiplexer multiplexer =
        new BeamFnDataGrpcMultiplexer(
            DESCRIPTOR,
            OutboundObserverFactory.clientDirect(),
            inboundObserver -> TestStreams.withOnNext(outboundValues::add).build());
    Future<?> registerFuture =
        executor.submit(
            () -> {
              multiplexer.registerConsumer(
                  DATA_INSTRUCTION_ID,
                  new CloseableFnDataReceiver<BeamFnApi.Elements>() {
                    @Override
                    public void flush() throws Exception {
                      fail("Unexpected call");
                    }

                    @Override
                    public void close() throws Exception {
                      fail("Unexpected call");
                    }

                    @Override
                    public void accept(BeamFnApi.Elements input) throws Exception {
                      dataInboundValues.add(input);
                    }
                  });
              multiplexer.registerConsumer(
                  TIMER_INSTRUCTION_ID,
                  new CloseableFnDataReceiver<BeamFnApi.Elements>() {
                    @Override
                    public void flush() throws Exception {
                      fail("Unexpected call");
                    }

                    @Override
                    public void close() throws Exception {
                      fail("Unexpected call");
                    }

                    @Override
                    public void accept(BeamFnApi.Elements input) throws Exception {
                      timerInboundValues.add(input);
                    }
                  });
            });

    multiplexer.getInboundObserver().onNext(ELEMENTS);
    assertTrue(multiplexer.hasConsumer(DATA_INSTRUCTION_ID));
    assertTrue(multiplexer.hasConsumer(TIMER_INSTRUCTION_ID));

    // Verify that on terminal elements we still wait to be unregistered.
    multiplexer.getInboundObserver().onNext(TERMINAL_ELEMENTS);
    assertTrue(multiplexer.hasConsumer(DATA_INSTRUCTION_ID));
    assertTrue(multiplexer.hasConsumer(TIMER_INSTRUCTION_ID));

    registerFuture.get();
    multiplexer.unregisterConsumer(DATA_INSTRUCTION_ID);
    multiplexer.unregisterConsumer(TIMER_INSTRUCTION_ID);
    assertFalse(multiplexer.hasConsumer(DATA_INSTRUCTION_ID));
    assertFalse(multiplexer.hasConsumer(TIMER_INSTRUCTION_ID));

    // Assert that normal and terminal Elements are passed to the consumer
    MatcherAssert.assertThat(
        dataInboundValues,
        Matchers.contains(
            ELEMENTS.toBuilder().clearTimers().build(),
            TERMINAL_ELEMENTS.toBuilder().clearTimers().build()));
    MatcherAssert.assertThat(
        timerInboundValues,
        Matchers.contains(
            ELEMENTS.toBuilder().clearData().build(),
            TERMINAL_ELEMENTS.toBuilder().clearData().build()));
  }

  @Test
  public void testElementsNeedsPartitioning() throws Exception {
    Collection<BeamFnApi.Elements> outboundValues = new ArrayList<>();
    Collection<BeamFnApi.Elements> dataInboundValues = new ArrayList<>();
    Collection<BeamFnApi.Elements> timerInboundValues = new ArrayList<>();
    BeamFnDataGrpcMultiplexer multiplexer =
        new BeamFnDataGrpcMultiplexer(
            DESCRIPTOR,
            OutboundObserverFactory.clientDirect(),
            inboundObserver -> TestStreams.withOnNext(outboundValues::add).build());
    multiplexer.registerConsumer(
        DATA_INSTRUCTION_ID,
        new CloseableFnDataReceiver<BeamFnApi.Elements>() {
          @Override
          public void flush() throws Exception {
            fail("Unexpected call");
          }

          @Override
          public void close() throws Exception {
            fail("Unexpected call");
          }

          @Override
          public void accept(BeamFnApi.Elements input) throws Exception {
            dataInboundValues.add(input);
          }
        });
    multiplexer.registerConsumer(
        TIMER_INSTRUCTION_ID,
        new CloseableFnDataReceiver<BeamFnApi.Elements>() {
          @Override
          public void flush() throws Exception {
            fail("Unexpected call");
          }

          @Override
          public void close() throws Exception {
            fail("Unexpected call");
          }

          @Override
          public void accept(BeamFnApi.Elements input) throws Exception {
            timerInboundValues.add(input);
          }
        });

    multiplexer.getInboundObserver().onNext(ELEMENTS);
    multiplexer.getInboundObserver().onNext(TERMINAL_ELEMENTS);

    // Assert that elements are partitioned based upon the instruction id.
    MatcherAssert.assertThat(
        dataInboundValues,
        Matchers.contains(
            ELEMENTS.toBuilder().clearTimers().build(),
            TERMINAL_ELEMENTS.toBuilder().clearTimers().build()));
    MatcherAssert.assertThat(
        timerInboundValues,
        Matchers.contains(
            ELEMENTS.toBuilder().clearData().build(),
            TERMINAL_ELEMENTS.toBuilder().clearData().build()));
  }

  @Test
  public void testElementsWithOnlySingleInstructionIdUsingHotPath() throws Exception {
    Collection<BeamFnApi.Elements> outboundValues = new ArrayList<>();
    Collection<BeamFnApi.Elements> dataInboundValues = new ArrayList<>();
    BeamFnDataGrpcMultiplexer multiplexer =
        new BeamFnDataGrpcMultiplexer(
            DESCRIPTOR,
            OutboundObserverFactory.clientDirect(),
            inboundObserver -> TestStreams.withOnNext(outboundValues::add).build());
    multiplexer.registerConsumer(
        DATA_INSTRUCTION_ID,
        new CloseableFnDataReceiver<BeamFnApi.Elements>() {
          @Override
          public void flush() throws Exception {
            fail("Unexpected call");
          }

          @Override
          public void close() throws Exception {
            fail("Unexpected call");
          }

          @Override
          public void accept(BeamFnApi.Elements input) throws Exception {
            dataInboundValues.add(input);
          }
        });

    BeamFnApi.Elements value = ELEMENTS.toBuilder().clearTimers().build();

    multiplexer.getInboundObserver().onNext(value);

    // Assert that we passed the same instance through.
    assertSame(Iterables.getOnlyElement(dataInboundValues), value);
  }

  @Test
  public void testFailedProcessingCausesAdditionalInboundDataToBeIgnored() throws Exception {
    Collection<BeamFnApi.Elements> outboundValues = new ArrayList<>();
    Collection<BeamFnApi.Elements> dataInboundValues = new ArrayList<>();
    BeamFnDataGrpcMultiplexer multiplexer =
        new BeamFnDataGrpcMultiplexer(
            DESCRIPTOR,
            OutboundObserverFactory.clientDirect(),
            inboundObserver -> TestStreams.withOnNext(outboundValues::add).build());
    final AtomicBoolean closed = new AtomicBoolean();
    multiplexer.registerConsumer(
        DATA_INSTRUCTION_ID,
        new CloseableFnDataReceiver<BeamFnApi.Elements>() {
          @Override
          public void flush() throws Exception {
            fail("Unexpected call");
          }

          @Override
          public void close() throws Exception {
            closed.set(true);
          }

          @Override
          public void accept(BeamFnApi.Elements input) throws Exception {
            if (dataInboundValues.size() == 1) {
              throw new Exception("processing failed");
            }
            dataInboundValues.add(input);
          }
        });

    BeamFnApi.Elements.Data.Builder data =
        BeamFnApi.Elements.Data.newBuilder().setInstructionId(DATA_INSTRUCTION_ID);

    multiplexer
        .getInboundObserver()
        .onNext(BeamFnApi.Elements.newBuilder().addData(data.setTransformId("A").build()).build());
    multiplexer
        .getInboundObserver()
        .onNext(BeamFnApi.Elements.newBuilder().addData(data.setTransformId("B").build()).build());
    multiplexer
        .getInboundObserver()
        .onNext(BeamFnApi.Elements.newBuilder().addData(data.setTransformId("C").build()).build());

    // Assert that we ignored the other two elements
    MatcherAssert.assertThat(
        dataInboundValues,
        Matchers.contains(
            BeamFnApi.Elements.newBuilder().addData(data.setTransformId("A").build()).build()));
    assertTrue(closed.get());
  }

  @Test
  public void testClose() throws Exception {
    Collection<BeamFnApi.Elements> outboundValues = new ArrayList<>();
    Collection<Throwable> errorWasReturned = new ArrayList<>();
    AtomicBoolean wasClosed = new AtomicBoolean();
    final BeamFnDataGrpcMultiplexer multiplexer =
        new BeamFnDataGrpcMultiplexer(
            DESCRIPTOR,
            OutboundObserverFactory.clientDirect(),
            inboundObserver ->
                TestStreams.withOnNext(outboundValues::add)
                    .withOnError(errorWasReturned::add)
                    .build());
    multiplexer.registerConsumer(
        DATA_INSTRUCTION_ID,
        new CloseableFnDataReceiver<BeamFnApi.Elements>() {
          @Override
          public void flush() throws Exception {
            fail("Unexpected call");
          }

          @Override
          public void close() throws Exception {
            wasClosed.set(true);
          }

          @Override
          public void accept(BeamFnApi.Elements input) throws Exception {
            fail("Unexpected call");
          }
        });

    multiplexer.close();

    assertTrue(wasClosed.get());
    assertThat(
        Iterables.getOnlyElement(errorWasReturned).getMessage(),
        containsString("Multiplexer hanging up"));
  }

  @Test
  public void testFlowControlManualRequestAndStalledPoll() {
    BeamFnDataGrpcMultiplexer multiplexer =
        new BeamFnDataGrpcMultiplexer(
            DESCRIPTOR,
            OutboundObserverFactory.clientDirect(),
            inboundObserver -> TestStreams.withOnNext((BeamFnApi.Elements val) -> {}).build());

    @SuppressWarnings("unchecked")
    ClientCallStreamObserver<BeamFnApi.Elements> mockCallStream =
        mock(ClientCallStreamObserver.class);

    @SuppressWarnings("unchecked")
    ClientResponseObserver<BeamFnApi.Elements, BeamFnApi.Elements> clientResponseObserver =
        (ClientResponseObserver<BeamFnApi.Elements, BeamFnApi.Elements>)
            multiplexer.getInboundObserver();

    clientResponseObserver.beforeStart(mockCallStream);
    verify(mockCallStream).disableAutoRequestWithInitial(16);

    // Call onStalledPoll directly on multiplexer
    multiplexer.onStalledPoll();
    verify(mockCallStream, times(1)).request(1);

    // Call onStalledPoll on InboundObserver
    StalledPollHandler stalledPollHandler = (StalledPollHandler) multiplexer.getInboundObserver();
    stalledPollHandler.onStalledPoll();
    verify(mockCallStream, times(2)).request(1);

    // inFlightInbound started at 16, incremented twice to 18.
    // requestInbound decrements inFlightInbound.
    // When decremented below 16 (STEADY_STATE_INBOUND_WINDOW), it requests more.
    multiplexer.requestInbound(1); // inFlight becomes 17 (>= 16, no request)
    verify(mockCallStream, times(2)).request(1);

    multiplexer.requestInbound(1); // inFlight becomes 16 (>= 16, no request)
    verify(mockCallStream, times(2)).request(1);

    multiplexer.requestInbound(1); // inFlight becomes 15 (< 16, requests 1 and inFlight becomes 16)
    verify(mockCallStream, times(3)).request(1);
  }

  @Test
  public void testEmptyElementsHandled() {
    BeamFnDataGrpcMultiplexer multiplexer =
        new BeamFnDataGrpcMultiplexer(
            DESCRIPTOR,
            OutboundObserverFactory.clientDirect(),
            inboundObserver -> TestStreams.withOnNext((BeamFnApi.Elements val) -> {}).build());

    // Should not throw and should safely exit
    multiplexer.getInboundObserver().onNext(BeamFnApi.Elements.getDefaultInstance());
  }

  @Test
  public void testReceiverHolderBuffersBeforeConsumerRegisteredAndDrains() throws Exception {
    BeamFnDataGrpcMultiplexer multiplexer =
        new BeamFnDataGrpcMultiplexer(
            DESCRIPTOR,
            OutboundObserverFactory.clientDirect(),
            inboundObserver -> TestStreams.withOnNext((BeamFnApi.Elements val) -> {}).build());

    String instructionId = "unregistered_instruction";
    BeamFnApi.Elements elements =
        BeamFnApi.Elements.newBuilder()
            .addData(
                BeamFnApi.Elements.Data.newBuilder()
                    .setInstructionId(instructionId)
                    .setTransformId("t1")
                    .setData(ByteString.copyFromUtf8("hello")))
            .build();

    // Send elements before consumer is registered - should buffer and not block
    multiplexer.getInboundObserver().onNext(elements);

    // Now register a generic receiver
    List<BeamFnApi.Elements> received = new ArrayList<>();
    multiplexer.registerConsumer(
        instructionId,
        new CloseableFnDataReceiver<BeamFnApi.Elements>() {
          @Override
          public void accept(BeamFnApi.Elements input) {
            received.add(input);
          }

          @Override
          public void flush() {}

          @Override
          public void close() {}
        });

    assertThat(received, Matchers.contains(elements));

    // Registering twice throws IllegalArgumentException.
    assertThrows(
        IllegalArgumentException.class,
        () ->
            multiplexer.registerConsumer(
                instructionId,
                new CloseableFnDataReceiver<BeamFnApi.Elements>() {
                  @Override
                  public void accept(BeamFnApi.Elements input) {}

                  @Override
                  public void flush() {}

                  @Override
                  public void close() {}
                }));
  }

  @Test
  public void testReceiverHolderBuffersAndDrainsWithBeamFnDataInboundObserver() throws Exception {
    BeamFnDataGrpcMultiplexer multiplexer =
        new BeamFnDataGrpcMultiplexer(
            DESCRIPTOR,
            OutboundObserverFactory.clientDirect(),
            inboundObserver -> TestStreams.withOnNext((BeamFnApi.Elements val) -> {}).build());

    String instructionId = "inbound_obs_instruction";
    Coder<WindowedValue<String>> coder =
        WindowedValues.getFullCoder(StringUtf8Coder.of(), GlobalWindow.Coder.INSTANCE);
    ByteStringOutputStream out = new ByteStringOutputStream();
    coder.encode(WindowedValues.valueInGlobalWindow("payload"), out);

    BeamFnApi.Elements elements =
        BeamFnApi.Elements.newBuilder()
            .addData(
                BeamFnApi.Elements.Data.newBuilder()
                    .setInstructionId(instructionId)
                    .setTransformId("t1")
                    .setData(out.toByteString()))
            .build();
    BeamFnApi.Elements terminal =
        BeamFnApi.Elements.newBuilder()
            .addData(
                BeamFnApi.Elements.Data.newBuilder()
                    .setInstructionId(instructionId)
                    .setTransformId("t1")
                    .setIsLast(true))
            .build();

    // Buffer elements before registration
    multiplexer.getInboundObserver().onNext(elements);

    List<WindowedValue<String>> receivedValues = new ArrayList<>();
    BeamFnDataInboundObserver inboundObserver =
        BeamFnDataInboundObserver.forConsumers(
            Collections.singletonList(DataEndpoint.create("t1", coder, receivedValues::add)),
            Collections.emptyList());

    multiplexer.registerConsumer(instructionId, inboundObserver);
    multiplexer.getInboundObserver().onNext(terminal);

    inboundObserver.awaitCompletion();
    assertThat(receivedValues, Matchers.contains(WindowedValues.valueInGlobalWindow("payload")));
  }

  @Test
  public void testUnregisterConsumerNotRegisteredThrows() {
    BeamFnDataGrpcMultiplexer multiplexer =
        new BeamFnDataGrpcMultiplexer(
            DESCRIPTOR,
            OutboundObserverFactory.clientDirect(),
            inboundObserver -> TestStreams.withOnNext((BeamFnApi.Elements val) -> {}).build());

    String instructionId = "unregistered_instruction";
    // Send data to create ReceiverHolder in receivers without registering consumer
    multiplexer
        .getInboundObserver()
        .onNext(
            BeamFnApi.Elements.newBuilder()
                .addData(BeamFnApi.Elements.Data.newBuilder().setInstructionId(instructionId))
                .build());

    assertThrows(
        IllegalArgumentException.class, () -> multiplexer.unregisterConsumer(instructionId));
  }

  @Test
  public void testPoisonInstructionId() throws Exception {
    BeamFnDataGrpcMultiplexer multiplexer =
        new BeamFnDataGrpcMultiplexer(
            DESCRIPTOR,
            OutboundObserverFactory.clientDirect(),
            inboundObserver -> TestStreams.withOnNext((BeamFnApi.Elements val) -> {}).build());

    String instructionId = "poison_instruction";
    AtomicBoolean closed = new AtomicBoolean();

    multiplexer.registerConsumer(
        instructionId,
        new CloseableFnDataReceiver<BeamFnApi.Elements>() {
          @Override
          public void accept(BeamFnApi.Elements input) {}

          @Override
          public void flush() {}

          @Override
          public void close() {
            closed.set(true);
          }
        });

    multiplexer.poisonInstructionId(instructionId);
    assertTrue(closed.get());
    assertFalse(multiplexer.hasConsumer(instructionId));

    // Late elements for the poisoned instruction are dropped and leave no entry behind.
    multiplexer
        .getInboundObserver()
        .onNext(
            BeamFnApi.Elements.newBuilder()
                .addData(BeamFnApi.Elements.Data.newBuilder().setInstructionId(instructionId))
                .build());
    assertFalse(multiplexer.hasConsumer(instructionId));

    // Registering a consumer for the poisoned instruction still fails after the late data.
    assertThrows(
        IllegalArgumentException.class,
        () ->
            multiplexer.registerConsumer(
                instructionId,
                new CloseableFnDataReceiver<BeamFnApi.Elements>() {
                  @Override
                  public void accept(BeamFnApi.Elements input) {}

                  @Override
                  public void flush() {}

                  @Override
                  public void close() {}
                }));
    assertFalse(multiplexer.hasConsumer(instructionId));
  }

  @Test
  public void testPoisonInstructionIdWhenPendingElements() {
    BeamFnDataGrpcMultiplexer multiplexer =
        new BeamFnDataGrpcMultiplexer(
            DESCRIPTOR,
            OutboundObserverFactory.clientDirect(),
            inboundObserver -> TestStreams.withOnNext((BeamFnApi.Elements val) -> {}).build());

    String instructionId = "pending_poison_instruction";
    // Buffer elements
    multiplexer
        .getInboundObserver()
        .onNext(
            BeamFnApi.Elements.newBuilder()
                .addData(BeamFnApi.Elements.Data.newBuilder().setInstructionId(instructionId))
                .build());

    // Poison while pending
    multiplexer.poisonInstructionId(instructionId);

    // Registering now throws
    assertThrows(
        IllegalArgumentException.class,
        () ->
            multiplexer.registerConsumer(
                instructionId,
                new CloseableFnDataReceiver<BeamFnApi.Elements>() {
                  @Override
                  public void accept(BeamFnApi.Elements input) {}

                  @Override
                  public void flush() {}

                  @Override
                  public void close() {}
                }));
  }

  @Test
  public void testToString() {
    BeamFnDataGrpcMultiplexer multiplexer =
        new BeamFnDataGrpcMultiplexer(
            DESCRIPTOR,
            OutboundObserverFactory.clientDirect(),
            inboundObserver -> TestStreams.withOnNext((BeamFnApi.Elements val) -> {}).build());

    String str = multiplexer.toString();
    assertThat(str, containsString("apiServiceDescriptor"));
    assertThat(str, containsString("consumers"));
  }

  @Test
  public void testConsumerAcceptThrowsExceptionPoisonsInstruction() throws Exception {
    BeamFnDataGrpcMultiplexer multiplexer =
        new BeamFnDataGrpcMultiplexer(
            DESCRIPTOR,
            OutboundObserverFactory.clientDirect(),
            inboundObserver -> TestStreams.withOnNext((BeamFnApi.Elements val) -> {}).build());

    String instructionId = "throw_instruction";
    AtomicBoolean closed = new AtomicBoolean();

    multiplexer.registerConsumer(
        instructionId,
        new CloseableFnDataReceiver<BeamFnApi.Elements>() {
          @Override
          public void accept(BeamFnApi.Elements input) throws Exception {
            throw new RuntimeException("Accept failure");
          }

          @Override
          public void flush() {}

          @Override
          public void close() {
            closed.set(true);
          }
        });

    multiplexer
        .getInboundObserver()
        .onNext(
            BeamFnApi.Elements.newBuilder()
                .addData(BeamFnApi.Elements.Data.newBuilder().setInstructionId(instructionId))
                .build());

    assertTrue(closed.get());
  }

  @Test
  public void testBeamFnDataInboundObserverBufferingWhenFull() throws Exception {
    BeamFnDataGrpcMultiplexer multiplexer =
        new BeamFnDataGrpcMultiplexer(
            DESCRIPTOR,
            OutboundObserverFactory.clientDirect(),
            inboundObserver ->
                TestStreams.withOnNext((BeamFnApi.Elements val) -> {})
                    .withOnError(err -> {})
                    .build());

    String instructionId = "full_queue_instruction";
    BeamFnDataInboundObserver mockInbound = mock(BeamFnDataInboundObserver.class);
    when(mockInbound.tryAcceptWithCallback(any(), any())).thenReturn(false).thenReturn(true);

    multiplexer.registerConsumer(instructionId, mockInbound);

    BeamFnApi.Elements elements =
        BeamFnApi.Elements.newBuilder()
            .addData(BeamFnApi.Elements.Data.newBuilder().setInstructionId(instructionId))
            .build();

    // First call returns false -> added to pending
    multiplexer.getInboundObserver().onNext(elements);

    // Closing drains / clears pending
    multiplexer.close();
  }

  @Test
  public void testBeamFnDataInboundObserverDrainPendingExceptionHandled() throws Exception {
    BeamFnDataGrpcMultiplexer multiplexer =
        new BeamFnDataGrpcMultiplexer(
            DESCRIPTOR,
            OutboundObserverFactory.clientDirect(),
            inboundObserver -> TestStreams.withOnNext((BeamFnApi.Elements val) -> {}).build());

    String instructionId = "drain_exception_instruction";
    BeamFnApi.Elements elements =
        BeamFnApi.Elements.newBuilder()
            .addData(BeamFnApi.Elements.Data.newBuilder().setInstructionId(instructionId))
            .build();

    // Buffer elements first
    multiplexer.getInboundObserver().onNext(elements);

    BeamFnDataInboundObserver mockInbound = mock(BeamFnDataInboundObserver.class);
    when(mockInbound.tryAcceptWithCallback(any(), any()))
        .thenThrow(new RuntimeException("queue error"));

    // Registering calls drainPending(), which catches the exception and removes from pending
    multiplexer.registerConsumer(instructionId, mockInbound);
  }

  /**
   * The gRPC thread delivers data for one bundle while the bundle thread drains the pending buffer
   * from its callbacks, and the consumer registers in the middle of the stream. Every element must
   * arrive once and in order.
   */
  @Test(timeout = 60_000)
  public void testConcurrentHandoffKeepsOrder() throws Exception {
    int numElements = 3_000;
    for (int run = 0; run < 3; run++) {
      BeamFnDataGrpcMultiplexer multiplexer =
          new BeamFnDataGrpcMultiplexer(
              DESCRIPTOR,
              OutboundObserverFactory.clientDirect(),
              inboundObserver -> TestStreams.withOnNext((BeamFnApi.Elements val) -> {}).build());
      String instructionId = "concurrent_" + run;
      Coder<WindowedValue<String>> coder =
          WindowedValues.getFullCoder(StringUtf8Coder.of(), GlobalWindow.Coder.INSTANCE);

      List<String> received = new ArrayList<>();
      BeamFnDataInboundObserver observer =
          BeamFnDataInboundObserver.forConsumers(
              Collections.singletonList(
                  DataEndpoint.create(
                      "t1",
                      coder,
                      value -> {
                        received.add(value.getValue());
                        if (received.size() % 50 == 0) {
                          // A slow consumer lets the bundle queue fill, so elements wait in the
                          // pending buffer and the bundle thread drains them.
                          Thread.sleep(1);
                        }
                      })),
              Collections.emptyList());

      CountDownLatch halfway = new CountDownLatch(1);
      Future<?> grpcThread =
          executor.submit(
              () -> {
                for (int i = 0; i < numElements; i++) {
                  ByteStringOutputStream out = new ByteStringOutputStream();
                  coder.encode(WindowedValues.valueInGlobalWindow(Integer.toString(i)), out);
                  multiplexer
                      .getInboundObserver()
                      .onNext(
                          BeamFnApi.Elements.newBuilder()
                              .addData(
                                  BeamFnApi.Elements.Data.newBuilder()
                                      .setInstructionId(instructionId)
                                      .setTransformId("t1")
                                      .setData(out.toByteString()))
                              .build());
                  if (i == 200) {
                    halfway.countDown();
                  }
                }
                multiplexer
                    .getInboundObserver()
                    .onNext(
                        BeamFnApi.Elements.newBuilder()
                            .addData(
                                BeamFnApi.Elements.Data.newBuilder()
                                    .setInstructionId(instructionId)
                                    .setTransformId("t1")
                                    .setIsLast(true))
                            .build());
                return null;
              });

      halfway.await();
      multiplexer.registerConsumer(instructionId, observer);
      observer.awaitCompletion();
      grpcThread.get();

      List<String> expected = new ArrayList<>();
      for (int i = 0; i < numElements; i++) {
        expected.add(Integer.toString(i));
      }
      assertEquals(expected, received);
      multiplexer.unregisterConsumer(instructionId);
    }
  }

  /** A multiplexer whose inbound stream is a mock, so a test can count {@code request(1)} calls. */
  @SuppressWarnings("unchecked")
  private static ClientCallStreamObserver<BeamFnApi.Elements> startWithMockStream(
      BeamFnDataGrpcMultiplexer multiplexer) {
    ClientCallStreamObserver<BeamFnApi.Elements> stream = mock(ClientCallStreamObserver.class);
    ((ClientResponseObserver<BeamFnApi.Elements, BeamFnApi.Elements>)
            multiplexer.getInboundObserver())
        .beforeStart(stream);
    return stream;
  }

  private static BeamFnApi.Elements dataFor(String instructionId, String transformId)
      throws Exception {
    Coder<WindowedValue<String>> coder =
        WindowedValues.getFullCoder(StringUtf8Coder.of(), GlobalWindow.Coder.INSTANCE);
    ByteStringOutputStream out = new ByteStringOutputStream();
    coder.encode(WindowedValues.valueInGlobalWindow("v"), out);
    return BeamFnApi.Elements.newBuilder()
        .addData(
            BeamFnApi.Elements.Data.newBuilder()
                .setInstructionId(instructionId)
                .setTransformId(transformId)
                .setData(out.toByteString()))
        .build();
  }

  private static BeamFnDataGrpcMultiplexer newMultiplexer() {
    return new BeamFnDataGrpcMultiplexer(
        DESCRIPTOR,
        OutboundObserverFactory.clientDirect(),
        inboundObserver -> TestStreams.withOnNext((BeamFnApi.Elements val) -> {}).build());
  }

  /**
   * Each inbound message releases its flow-control permit once, which asks gRPC for one more
   * message. A bundle whose consumer throws must release the permit of the message it was
   * processing as well as of the messages still queued.
   */
  @Test
  public void testPermitsReleasedWhenBundleFails() throws Exception {
    BeamFnDataGrpcMultiplexer multiplexer = newMultiplexer();
    ClientCallStreamObserver<BeamFnApi.Elements> stream = startWithMockStream(multiplexer);
    String instructionId = "failing_bundle";
    Coder<WindowedValue<String>> coder =
        WindowedValues.getFullCoder(StringUtf8Coder.of(), GlobalWindow.Coder.INSTANCE);
    BeamFnDataInboundObserver observer =
        BeamFnDataInboundObserver.forConsumers(
            Collections.singletonList(
                DataEndpoint.create(
                    "t1",
                    coder,
                    value -> {
                      throw new IllegalStateException("consumer failed");
                    })),
            Collections.emptyList());
    multiplexer.registerConsumer(instructionId, observer);

    for (int i = 0; i < 3; i++) {
      multiplexer.getInboundObserver().onNext(dataFor(instructionId, "t1"));
    }
    assertThrows(IllegalStateException.class, observer::awaitCompletion);
    multiplexer.poisonInstructionId(instructionId);

    verify(stream, times(3)).request(1);
    assertFalse(multiplexer.hasConsumer(instructionId));
  }

  @Test
  public void testPermitsReleasedForLateDataOfPoisonedInstruction() throws Exception {
    BeamFnDataGrpcMultiplexer multiplexer = newMultiplexer();
    ClientCallStreamObserver<BeamFnApi.Elements> stream = startWithMockStream(multiplexer);
    String instructionId = "poisoned_late_data";
    multiplexer.poisonInstructionId(instructionId);

    for (int i = 0; i < 3; i++) {
      multiplexer.getInboundObserver().onNext(dataFor(instructionId, "t1"));
    }

    verify(stream, times(3)).request(1);
    assertFalse(multiplexer.hasConsumer(instructionId));
  }

  @Test
  public void testPermitsReleasedWhenReceiverThrowsOnBufferedElements() throws Exception {
    BeamFnDataGrpcMultiplexer multiplexer = newMultiplexer();
    ClientCallStreamObserver<BeamFnApi.Elements> stream = startWithMockStream(multiplexer);
    String instructionId = "throwing_receiver";
    for (int i = 0; i < 3; i++) {
      multiplexer.getInboundObserver().onNext(dataFor(instructionId, "t1"));
    }
    verify(stream, times(0)).request(1);

    // The receiver's own exception reaches the caller, not a wrapped one.
    assertThrows(
        IllegalStateException.class,
        () ->
            multiplexer.registerConsumer(
                instructionId,
                new CloseableFnDataReceiver<BeamFnApi.Elements>() {
                  @Override
                  public void accept(BeamFnApi.Elements input) {
                    throw new IllegalStateException("receiver failed");
                  }

                  @Override
                  public void flush() {}

                  @Override
                  public void close() {}
                }));

    verify(stream, times(3)).request(1);
  }

  @Test
  public void testRequestInboundReleasesCount() {
    BeamFnDataGrpcMultiplexer multiplexer = newMultiplexer();
    ClientCallStreamObserver<BeamFnApi.Elements> stream = startWithMockStream(multiplexer);
    // Three stalled polls open three messages beyond the steady-state window.
    multiplexer.onStalledPoll();
    multiplexer.onStalledPoll();
    multiplexer.onStalledPoll();
    verify(stream, times(3)).request(1);

    // Releasing five brings the window two below steady state, so it asks for two more.
    multiplexer.requestInbound(5);
    verify(stream, times(5)).request(1);
  }
}
