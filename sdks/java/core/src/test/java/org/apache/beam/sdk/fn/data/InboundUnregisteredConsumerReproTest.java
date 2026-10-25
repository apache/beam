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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.beam.model.fnexecution.v1.BeamFnApi;
import org.apache.beam.model.pipeline.v1.Endpoints;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.fn.stream.OutboundObserverFactory;
import org.apache.beam.sdk.util.ByteStringOutputStream;
import org.apache.beam.vendor.grpc.v1p69p0.io.grpc.stub.ClientCallStreamObserver;
import org.apache.beam.vendor.grpc.v1p69p0.io.grpc.stub.ClientResponseObserver;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.junit.Test;

/**
 * Data for an instruction whose consumer is not registered yet must not block the gRPC inbound
 * thread. gRPC runs inbound onNext and the outbound onReady handler on one serializing executor, so
 * a blocked onNext also stops every bundle that waits on onReady.
 */
public class InboundUnregisteredConsumerReproTest {
  private static final Endpoints.ApiServiceDescriptor DESCRIPTOR =
      Endpoints.ApiServiceDescriptor.newBuilder().setUrl("test").build();

  @Test
  public void testUnregisteredConsumerDoesNotBlockOnReady() throws Exception {
    AtomicReference<@Nullable Runnable> onReadyHandler = new AtomicReference<>();
    AtomicBoolean ready = new AtomicBoolean(false);
    BeamFnDataGrpcMultiplexer multiplexer =
        new BeamFnDataGrpcMultiplexer(
            DESCRIPTOR,
            OutboundObserverFactory.clientDirect(),
            inbound -> {
              ClientCallStreamObserver<BeamFnApi.Elements> call =
                  new ClientCallStreamObserver<BeamFnApi.Elements>() {
                    @Override
                    public boolean isReady() {
                      return ready.get();
                    }

                    @Override
                    public void setOnReadyHandler(Runnable handler) {
                      onReadyHandler.set(handler);
                    }

                    @Override
                    public void disableAutoInboundFlowControl() {}

                    @Override
                    public void disableAutoRequestWithInitial(int request) {}

                    @Override
                    public void request(int count) {}

                    @Override
                    public void setMessageCompression(boolean enable) {}

                    @Override
                    public void cancel(@Nullable String message, @Nullable Throwable cause) {}

                    @Override
                    public void onNext(BeamFnApi.Elements value) {}

                    @Override
                    public void onError(Throwable t) {}

                    @Override
                    public void onCompleted() {}
                  };
              if (inbound instanceof ClientResponseObserver) {
                @SuppressWarnings("unchecked")
                ClientResponseObserver<BeamFnApi.Elements, BeamFnApi.Elements> r =
                    (ClientResponseObserver<BeamFnApi.Elements, BeamFnApi.Elements>) inbound;
                r.beforeStart(call);
              }
              return call;
            });

    // Stands in for gRPC's per-call SerializingExecutor.
    ExecutorService grpcExecutor = Executors.newSingleThreadExecutor();
    try {
      String instructionId = "not-yet-registered";
      ByteStringOutputStream encoded = new ByteStringOutputStream();
      StringUtf8Coder.of().encode("elem", encoded);
      BeamFnApi.Elements data =
          BeamFnApi.Elements.newBuilder()
              .addData(
                  BeamFnApi.Elements.Data.newBuilder()
                      .setInstructionId(instructionId)
                      .setTransformId("t")
                      .setData(encoded.toByteString()))
              .addData(
                  BeamFnApi.Elements.Data.newBuilder()
                      .setInstructionId(instructionId)
                      .setTransformId("t")
                      .setIsLast(true))
              .build();

      // 1. The runner sends data before the bundle processor registers its consumer.
      Future<?> onNextFuture =
          grpcExecutor.submit(() -> multiplexer.getInboundObserver().onNext(data));

      // 2. The outbound stream becomes ready; gRPC queues onReady on the same executor.
      ready.set(true);
      CountDownLatch onReadyRan = new CountDownLatch(1);
      Future<?> onReadyFuture =
          grpcExecutor.submit(
              () -> {
                Runnable handler = onReadyHandler.get();
                if (handler != null) {
                  handler.run();
                }
                onReadyRan.countDown();
              });

      boolean onReadyRanInTime = onReadyRan.await(2, TimeUnit.SECONDS);

      // 3. The consumer registers; the data must still arrive.
      List<String> received = Collections.synchronizedList(new ArrayList<>());
      BeamFnDataInboundObserver consumer =
          BeamFnDataInboundObserver.forConsumers(
              Collections.singletonList(
                  DataEndpoint.create("t", StringUtf8Coder.of(), received::add)),
              Collections.emptyList());
      multiplexer.registerConsumer(instructionId, consumer);
      ExecutorService bundle = Executors.newSingleThreadExecutor();
      try {
        Future<?> done =
            bundle.submit(
                () -> {
                  consumer.awaitCompletion();
                  return null;
                });
        done.get(5, TimeUnit.SECONDS);
      } finally {
        bundle.shutdownNow();
      }
      onNextFuture.get(5, TimeUnit.SECONDS);
      onReadyFuture.get(5, TimeUnit.SECONDS);
      assertEquals(Collections.singletonList("elem"), received);

      assertTrue(
          "onReady was stuck behind an inbound onNext waiting for an unregistered consumer",
          onReadyRanInTime);
    } finally {
      grpcExecutor.shutdownNow();
    }
  }
}
