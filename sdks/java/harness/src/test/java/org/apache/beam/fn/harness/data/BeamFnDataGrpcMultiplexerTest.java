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

import static org.hamcrest.Matchers.contains;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import com.google.common.util.concurrent.Uninterruptibles;
import com.google.protobuf.ByteString;
import io.grpc.stub.StreamObserver;
import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.apache.beam.model.fnexecution.v1.BeamFnApi;
import org.apache.beam.model.pipeline.v1.Endpoints;
import org.apache.beam.sdk.fn.data.LogicalEndpoint;
import org.apache.beam.sdk.fn.test.TestStreams;
import org.junit.Test;

/** Tests for {@link BeamFnDataGrpcMultiplexer}. */
public class BeamFnDataGrpcMultiplexerTest {
  private static final Endpoints.ApiServiceDescriptor DESCRIPTOR =
      Endpoints.ApiServiceDescriptor.newBuilder().setUrl("test").build();
  private static final LogicalEndpoint OUTPUT_LOCATION =
      LogicalEndpoint.of(
          "777L",
          BeamFnApi.Target.newBuilder()
              .setName("name")
              .setPrimitiveTransformReference("888L")
              .build());
  private static final BeamFnApi.Elements ELEMENTS = BeamFnApi.Elements.newBuilder()
      .addData(BeamFnApi.Elements.Data.newBuilder()
          .setInstructionReference(OUTPUT_LOCATION.getInstructionId())
          .setTarget(OUTPUT_LOCATION.getTarget())
          .setData(ByteString.copyFrom(new byte[1])))
      .build();
  private static final BeamFnApi.Elements TERMINAL_ELEMENTS = BeamFnApi.Elements.newBuilder()
      .addData(BeamFnApi.Elements.Data.newBuilder()
          .setInstructionReference(OUTPUT_LOCATION.getInstructionId())
          .setTarget(OUTPUT_LOCATION.getTarget()))
      .build();

  @Test
  public void testOutboundObserver() {
    Collection<BeamFnApi.Elements> values = new ArrayList<>();
    BeamFnDataGrpcMultiplexer multiplexer = new BeamFnDataGrpcMultiplexer(
        DESCRIPTOR,
        (StreamObserver<BeamFnApi.Elements> inboundObserver)
            -> TestStreams.withOnNext(values::add).build());
    multiplexer.getOutboundObserver().onNext(ELEMENTS);
    assertThat(values, contains(ELEMENTS));
  }

  @Test
  public void testInboundObserverBlocksTillConsumerConnects() throws Exception {
    Collection<BeamFnApi.Elements> outboundValues = new ArrayList<>();
    Collection<BeamFnApi.Elements.Data> inboundValues = new ArrayList<>();
    BeamFnDataGrpcMultiplexer multiplexer = new BeamFnDataGrpcMultiplexer(
        DESCRIPTOR,
        (StreamObserver<BeamFnApi.Elements> inboundObserver)
            -> TestStreams.withOnNext(outboundValues::add).build());
    ExecutorService executorService = Executors.newCachedThreadPool();
    executorService.submit(new Runnable() {
      @Override
      public void run() {
        // Purposefully sleep to simulate a delay in a consumer connecting.
        Uninterruptibles.sleepUninterruptibly(100, TimeUnit.MILLISECONDS);
        multiplexer.futureForKey(OUTPUT_LOCATION).complete(inboundValues::add);
      }
    });
    multiplexer.getInboundObserver().onNext(ELEMENTS);
    assertTrue(multiplexer.consumers.containsKey(OUTPUT_LOCATION));
    // Ensure that when we see a terminal Elements object, we remove the consumer
    multiplexer.getInboundObserver().onNext(TERMINAL_ELEMENTS);
    assertFalse(multiplexer.consumers.containsKey(OUTPUT_LOCATION));

    // Assert that normal and terminal Elements are passed to the consumer
    assertThat(inboundValues, contains(ELEMENTS.getData(0), TERMINAL_ELEMENTS.getData(0)));
  }
}
