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

import io.grpc.ManagedChannel;
import io.grpc.stub.StreamObserver;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.BiFunction;
import java.util.function.Function;
import org.apache.beam.model.fnexecution.v1.BeamFnApi;
import org.apache.beam.model.fnexecution.v1.BeamFnDataGrpc;
import org.apache.beam.model.pipeline.v1.Endpoints;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.fn.data.CloseableFnDataReceiver;
import org.apache.beam.sdk.fn.data.FnDataReceiver;
import org.apache.beam.sdk.fn.data.LogicalEndpoint;
import org.apache.beam.sdk.fn.stream.StreamObserverFactory.StreamObserverClientFactory;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.util.WindowedValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link BeamFnDataClient} that uses gRPC for sending and receiving data.
 *
 * <p>TODO: Handle closing clients that are currently not a consumer nor are being consumed.
 */
public class BeamFnDataGrpcClient implements BeamFnDataClient {
  private static final Logger LOG = LoggerFactory.getLogger(BeamFnDataGrpcClient.class);

  private final ConcurrentMap<Endpoints.ApiServiceDescriptor, BeamFnDataGrpcMultiplexer> cache;
  private final Function<Endpoints.ApiServiceDescriptor, ManagedChannel> channelFactory;
  private final BiFunction<
          StreamObserverClientFactory<BeamFnApi.Elements, BeamFnApi.Elements>,
          StreamObserver<BeamFnApi.Elements>, StreamObserver<BeamFnApi.Elements>>
      streamObserverFactory;
  private final PipelineOptions options;

  public BeamFnDataGrpcClient(
      PipelineOptions options,
      Function<Endpoints.ApiServiceDescriptor, ManagedChannel> channelFactory,
      BiFunction<
              StreamObserverClientFactory<BeamFnApi.Elements, BeamFnApi.Elements>,
              StreamObserver<BeamFnApi.Elements>, StreamObserver<BeamFnApi.Elements>>
          streamObserverFactory) {
    this.options = options;
    this.channelFactory = channelFactory;
    this.streamObserverFactory = streamObserverFactory;
    this.cache = new ConcurrentHashMap<>();
  }

  /**
   * Registers the following inbound stream consumer for the provided instruction id and target.
   *
   * <p>The provided coder is used to decode elements on the inbound stream. The decoded elements
   * are passed to the provided consumer. Any failure during decoding or processing of the element
   * will complete the returned future exceptionally. On successful termination of the stream
   * (signaled by an empty data block), the returned future is completed successfully.
   */
  @Override
  public <T> CompletableFuture<Void> receive(
      Endpoints.ApiServiceDescriptor apiServiceDescriptor,
      LogicalEndpoint inputLocation,
      Coder<WindowedValue<T>> coder,
      FnDataReceiver<WindowedValue<T>> consumer) {
    LOG.debug("Registering consumer for instruction {} and target {}",
        inputLocation.getInstructionId(),
        inputLocation.getTarget());

    CompletableFuture<Void> readFuture = new CompletableFuture<>();
    BeamFnDataGrpcMultiplexer client = getClientFor(apiServiceDescriptor);
    client.futureForKey(inputLocation).complete(
        new BeamFnDataInboundObserver<>(coder, consumer, readFuture));
    return readFuture;
  }

  /**
   * Creates a closeable consumer using the provided instruction id and target.
   *
   * <p>The provided coder is used to encode elements on the outbound stream.
   *
   * <p>On closing the returned consumer, an empty data block is sent as a signal of the
   * logical data stream finishing.
   *
   * <p>The returned closeable consumer is not thread safe.
   */
  @Override
  public <T> CloseableFnDataReceiver<WindowedValue<T>> send(
      Endpoints.ApiServiceDescriptor apiServiceDescriptor,
      LogicalEndpoint outputLocation,
      Coder<WindowedValue<T>> coder) {
    BeamFnDataGrpcMultiplexer client = getClientFor(apiServiceDescriptor);

    LOG.debug("Creating output consumer for instruction {} and target {}",
        outputLocation.getInstructionId(),
        outputLocation.getTarget());
    return new BeamFnDataBufferingOutboundObserver<>(
        options, outputLocation, coder, client.getOutboundObserver());
  }

  private BeamFnDataGrpcMultiplexer getClientFor(
      Endpoints.ApiServiceDescriptor apiServiceDescriptor) {
    return cache.computeIfAbsent(apiServiceDescriptor,
        (Endpoints.ApiServiceDescriptor descriptor) -> new BeamFnDataGrpcMultiplexer(
            descriptor,
            (StreamObserver<BeamFnApi.Elements> inboundObserver) -> streamObserverFactory.apply(
                BeamFnDataGrpc.newStub(channelFactory.apply(apiServiceDescriptor))::data,
                inboundObserver)));
  }
}
