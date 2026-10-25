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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.beam.model.fnexecution.v1.BeamFnApi;
import org.apache.beam.model.pipeline.v1.Endpoints;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.fn.stream.OutboundObserverFactory;
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
  public void testDuplicateRegistrationDoesNotPoison() throws Exception {
    BeamFnDataGrpcMultiplexer multiplexer = newMultiplexer();
    String instructionId = "duplicate";
    AtomicBoolean closed = new AtomicBoolean();
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
          public void close() {
            closed.set(true);
          }
        });
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

    // The first registration keeps working.
    assertTrue(multiplexer.hasConsumer(instructionId));
    assertFalse(closed.get());
    BeamFnApi.Elements elements = dataFor(instructionId, "t1");
    multiplexer.getInboundObserver().onNext(elements);
    assertThat(received, Matchers.contains(elements));
  }
}
