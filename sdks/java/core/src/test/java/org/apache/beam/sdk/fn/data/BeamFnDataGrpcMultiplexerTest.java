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
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.beam.model.fnexecution.v1.BeamFnApi;
import org.apache.beam.model.pipeline.v1.Endpoints;
import org.apache.beam.sdk.fn.stream.OutboundObserverFactory;
import org.apache.beam.sdk.fn.test.TestExecutors;
import org.apache.beam.sdk.fn.test.TestExecutors.TestExecutorService;
import org.apache.beam.sdk.fn.test.TestStreams;
import org.apache.beam.vendor.grpc.v1p60p1.com.google.protobuf.ByteString;
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
}
