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

import static org.hamcrest.Matchers.contains;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.apache.beam.model.fnexecution.v1.BeamFnApi;
import org.apache.beam.model.pipeline.v1.Endpoints;
import org.apache.beam.sdk.fn.stream.OutboundObserverFactory;
import org.apache.beam.sdk.fn.test.TestStreams;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.vendor.grpc.v1p26p0.com.google.protobuf.ByteString;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.util.concurrent.Uninterruptibles;
import org.junit.Test;

/** Tests for {@link BeamFnDataGrpcMultiplexer}. */
public class BeamFnDataGrpcMultiplexerTest {
  private static final Endpoints.ApiServiceDescriptor DESCRIPTOR =
      Endpoints.ApiServiceDescriptor.newBuilder().setUrl("test").build();
  private static final LogicalEndpoint DATA_LOCATION = LogicalEndpoint.data("777L", "888L");
  private static final LogicalEndpoint TIMER_LOCATION =
      LogicalEndpoint.timer("999L", "555L", "333L");
  private static final BeamFnApi.Elements ELEMENTS =
      BeamFnApi.Elements.newBuilder()
          .addData(
              BeamFnApi.Elements.Data.newBuilder()
                  .setInstructionId(DATA_LOCATION.getInstructionId())
                  .setTransformId(DATA_LOCATION.getTransformId())
                  .setData(ByteString.copyFrom(new byte[1])))
          .addTimers(
              BeamFnApi.Elements.Timers.newBuilder()
                  .setInstructionId(TIMER_LOCATION.getInstructionId())
                  .setTransformId(TIMER_LOCATION.getTransformId())
                  .setTimerFamilyId(TIMER_LOCATION.getTimerFamilyId())
                  .setTimers(ByteString.copyFrom(new byte[2])))
          .build();
  private static final BeamFnApi.Elements TERMINAL_ELEMENTS =
      BeamFnApi.Elements.newBuilder()
          .addData(
              BeamFnApi.Elements.Data.newBuilder()
                  .setInstructionId(DATA_LOCATION.getInstructionId())
                  .setTransformId(DATA_LOCATION.getTransformId())
                  .setIsLast(true))
          .addTimers(
              BeamFnApi.Elements.Timers.newBuilder()
                  .setInstructionId(TIMER_LOCATION.getInstructionId())
                  .setTransformId(TIMER_LOCATION.getTransformId())
                  .setTimerFamilyId(TIMER_LOCATION.getTimerFamilyId())
                  .setIsLast(true))
          .build();

  @Test
  public void testOutboundObserver() {
    final Collection<BeamFnApi.Elements> values = new ArrayList<>();
    BeamFnDataGrpcMultiplexer multiplexer =
        new BeamFnDataGrpcMultiplexer(
            DESCRIPTOR,
            OutboundObserverFactory.clientDirect(),
            inboundObserver -> TestStreams.withOnNext(values::add).build());
    multiplexer.getOutboundObserver().onNext(ELEMENTS);
    assertThat(values, contains(ELEMENTS));
  }

  @Test
  public void testInboundObserverBlocksTillConsumerConnects() throws Exception {
    final Collection<BeamFnApi.Elements> outboundValues = new ArrayList<>();
    final Collection<KV<ByteString, Boolean>> dataInboundValues = new ArrayList<>();
    final Collection<KV<ByteString, Boolean>> timerInboundValues = new ArrayList<>();
    final BeamFnDataGrpcMultiplexer multiplexer =
        new BeamFnDataGrpcMultiplexer(
            DESCRIPTOR,
            OutboundObserverFactory.clientDirect(),
            inboundObserver -> TestStreams.withOnNext(outboundValues::add).build());
    ExecutorService executorService = Executors.newCachedThreadPool();
    executorService
        .submit(
            () -> {
              // Purposefully sleep to simulate a delay in a consumer connecting.
              Uninterruptibles.sleepUninterruptibly(100, TimeUnit.MILLISECONDS);
              multiplexer.registerConsumer(
                  DATA_LOCATION,
                  (payload, isLast) -> dataInboundValues.add(KV.of(payload, isLast)));
              multiplexer.registerConsumer(
                  TIMER_LOCATION,
                  (payload, isLast) -> timerInboundValues.add(KV.of(payload, isLast)));
            })
        .get();
    multiplexer.getInboundObserver().onNext(ELEMENTS);
    assertTrue(multiplexer.hasConsumer(DATA_LOCATION));
    assertTrue(multiplexer.hasConsumer(TIMER_LOCATION));
    // Ensure that when we see a terminal Elements object, we remove the consumer
    multiplexer.getInboundObserver().onNext(TERMINAL_ELEMENTS);
    assertFalse(multiplexer.hasConsumer(DATA_LOCATION));
    assertFalse(multiplexer.hasConsumer(TIMER_LOCATION));

    // Assert that normal and terminal Elements are passed to the consumer
    assertThat(
        dataInboundValues,
        contains(KV.of(ELEMENTS.getData(0).getData(), false), KV.of(ByteString.EMPTY, true)));
    assertThat(
        timerInboundValues,
        contains(KV.of(ELEMENTS.getTimers(0).getTimers(), false), KV.of(ByteString.EMPTY, true)));
  }
}
