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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.MoreObjects;
import io.grpc.stub.StreamObserver;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.function.Consumer;
import java.util.function.Function;
import org.apache.beam.model.fnexecution.v1.BeamFnApi;
import org.apache.beam.model.pipeline.v1.Endpoints;
import org.apache.beam.sdk.fn.data.LogicalEndpoint;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A gRPC multiplexer for a specific {@link
 * Endpoints.ApiServiceDescriptor}.
 *
 * <p>Multiplexes data for inbound consumers based upon their individual {@link
 * org.apache.beam.model.fnexecution.v1.BeamFnApi.Target}s.
 *
 * <p>Multiplexing inbound and outbound streams is as thread safe as the consumers of those streams.
 * For inbound streams, this is as thread safe as the inbound observers. For outbound streams, this
 * is as thread safe as the underlying stream observer.
 *
 * <p>TODO: Add support for multiplexing over multiple outbound observers by stickying the output
 * location with a specific outbound observer.
 */
public class BeamFnDataGrpcMultiplexer {
  private static final Logger LOG = LoggerFactory.getLogger(BeamFnDataGrpcMultiplexer.class);
  private final Endpoints.ApiServiceDescriptor apiServiceDescriptor;
  private final StreamObserver<BeamFnApi.Elements> inboundObserver;
  private final StreamObserver<BeamFnApi.Elements> outboundObserver;
  @VisibleForTesting
  final ConcurrentMap<
          LogicalEndpoint, CompletableFuture<Consumer<BeamFnApi.Elements.Data>>>
      consumers;

  public BeamFnDataGrpcMultiplexer(
      Endpoints.ApiServiceDescriptor apiServiceDescriptor,
      Function<StreamObserver<BeamFnApi.Elements>,
               StreamObserver<BeamFnApi.Elements>> outboundObserverFactory) {
    this.apiServiceDescriptor = apiServiceDescriptor;
    this.consumers = new ConcurrentHashMap<>();
    this.inboundObserver = new InboundObserver();
    this.outboundObserver = outboundObserverFactory.apply(inboundObserver);
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("apiServiceDescriptor", apiServiceDescriptor)
        .add("consumers", consumers)
        .toString();
  }

  public StreamObserver<BeamFnApi.Elements> getInboundObserver() {
    return inboundObserver;
  }

  public StreamObserver<BeamFnApi.Elements> getOutboundObserver() {
    return outboundObserver;
  }

  public CompletableFuture<Consumer<BeamFnApi.Elements.Data>> futureForKey(
      LogicalEndpoint key) {
    return consumers.computeIfAbsent(key, (LogicalEndpoint unused) -> new CompletableFuture<>());
  }

  /**
   * A multiplexing {@link StreamObserver} that selects the inbound {@link Consumer} to pass
   * the elements to.
   *
   * <p>The inbound observer blocks until the {@link Consumer} is bound allowing for the
   * sending harness to initiate transmitting data without needing for the receiving harness to
   * signal that it is ready to consume that data.
   */
  private final class InboundObserver implements StreamObserver<BeamFnApi.Elements> {
    @Override
    public void onNext(BeamFnApi.Elements value) {
      for (BeamFnApi.Elements.Data data : value.getDataList()) {
        try {
          LogicalEndpoint key =
              LogicalEndpoint.of(data.getInstructionReference(), data.getTarget());
          CompletableFuture<Consumer<BeamFnApi.Elements.Data>> consumer = futureForKey(key);
          if (!consumer.isDone()) {
            LOG.debug("Received data for key {} without consumer ready. "
                + "Waiting for consumer to be registered.", key);
          }
          consumer.get().accept(data);
          if (data.getData().isEmpty()) {
            consumers.remove(key);
          }
        /*
         * TODO: On failure we should fail any bundles that were impacted eagerly
         * instead of relying on the Runner harness to do all the failure handling.
         */
        } catch (ExecutionException | InterruptedException e) {
          LOG.error(
              "Client interrupted during handling of data for instruction {} and target {}",
              data.getInstructionReference(),
              data.getTarget(),
              e);
          outboundObserver.onError(e);
        } catch (RuntimeException e) {
          LOG.error(
              "Client failed to handle data for instruction {} and target {}",
              data.getInstructionReference(),
              data.getTarget(),
              e);
          outboundObserver.onError(e);
        }
      }
    }

    @Override
    public void onError(Throwable t) {
      LOG.error("Failed to handle for {}", apiServiceDescriptor, t);
    }

    @Override
    public void onCompleted() {
      LOG.warn("Hanged up for {}.", apiServiceDescriptor);
    }
  }
}
