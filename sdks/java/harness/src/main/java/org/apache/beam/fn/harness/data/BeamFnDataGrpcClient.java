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

import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Function;
import org.apache.beam.model.fnexecution.v1.BeamFnDataGrpc;
import org.apache.beam.model.pipeline.v1.Endpoints;
import org.apache.beam.model.pipeline.v1.Endpoints.ApiServiceDescriptor;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.fn.data.BeamFnDataBufferingOutboundObserver;
import org.apache.beam.sdk.fn.data.BeamFnDataGrpcMultiplexer;
import org.apache.beam.sdk.fn.data.BeamFnDataInboundObserver;
import org.apache.beam.sdk.fn.data.CloseableFnDataReceiver;
import org.apache.beam.sdk.fn.data.FnDataReceiver;
import org.apache.beam.sdk.fn.data.InboundDataClient;
import org.apache.beam.sdk.fn.data.LogicalEndpoint;
import org.apache.beam.sdk.fn.stream.OutboundObserverFactory;
import org.apache.beam.sdk.options.ExperimentalOptions;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.vendor.grpc.v1p13p1.io.grpc.ManagedChannel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link BeamFnDataClient} that uses gRPC for sending and receiving data.
 *
 * <p>TODO: Handle closing clients that are currently not a consumer nor are being consumed.
 */
public class BeamFnDataGrpcClient implements BeamFnDataClient {
  private static final Logger LOG = LoggerFactory.getLogger(BeamFnDataGrpcClient.class);
  private static final String BEAM_FN_API_DATA_BUFFER_LIMIT = "beam_fn_api_data_buffer_limit=";

  private final ConcurrentMap<Endpoints.ApiServiceDescriptor, BeamFnDataGrpcMultiplexer> cache;
  private final Function<Endpoints.ApiServiceDescriptor, ManagedChannel> channelFactory;
  private final OutboundObserverFactory outboundObserverFactory;
  private final PipelineOptions options;

  public BeamFnDataGrpcClient(
      PipelineOptions options,
      Function<Endpoints.ApiServiceDescriptor, ManagedChannel> channelFactory,
      OutboundObserverFactory outboundObserverFactory) {
    this.options = options;
    this.channelFactory = channelFactory;
    this.outboundObserverFactory = outboundObserverFactory;
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
  public <T> InboundDataClient receive(
      ApiServiceDescriptor apiServiceDescriptor,
      LogicalEndpoint inputLocation,
      Coder<WindowedValue<T>> coder,
      FnDataReceiver<WindowedValue<T>> consumer) {
    LOG.debug(
        "Registering consumer for instruction {} and target {}",
        inputLocation.getInstructionId(),
        inputLocation.getTarget());

    BeamFnDataGrpcMultiplexer client = getClientFor(apiServiceDescriptor);
    BeamFnDataInboundObserver<T> inboundObserver =
        BeamFnDataInboundObserver.forConsumer(coder, consumer);
    client.registerConsumer(inputLocation, inboundObserver);
    return inboundObserver;
  }

  /**
   * Creates a closeable consumer using the provided instruction id and target.
   *
   * <p>The provided coder is used to encode elements on the outbound stream.
   *
   * <p>On closing the returned consumer, an empty data block is sent as a signal of the logical
   * data stream finishing.
   *
   * <p>The returned closeable consumer is not thread safe.
   */
  @Override
  public <T> CloseableFnDataReceiver<WindowedValue<T>> send(
      Endpoints.ApiServiceDescriptor apiServiceDescriptor,
      LogicalEndpoint outputLocation,
      Coder<WindowedValue<T>> coder) {
    BeamFnDataGrpcMultiplexer client = getClientFor(apiServiceDescriptor);

    LOG.debug(
        "Creating output consumer for instruction {} and target {}",
        outputLocation.getInstructionId(),
        outputLocation.getTarget());
    Optional<Integer> bufferLimit = getBufferLimit(options);
    if (bufferLimit.isPresent()) {
      return BeamFnDataBufferingOutboundObserver.forLocationWithBufferLimit(
          bufferLimit.get(), outputLocation, coder, client.getOutboundObserver());
    } else {
      return BeamFnDataBufferingOutboundObserver.forLocation(
          outputLocation, coder, client.getOutboundObserver());
    }
  }

  /** Returns the {@code beam_fn_api_data_buffer_limit=<int>} experiment value if set. */
  private static Optional<Integer> getBufferLimit(PipelineOptions options) {
    List<String> experiments = options.as(ExperimentalOptions.class).getExperiments();
    for (String experiment : experiments == null ? Collections.<String>emptyList() : experiments) {
      if (experiment.startsWith(BEAM_FN_API_DATA_BUFFER_LIMIT)) {
        return Optional.of(
            Integer.parseInt(experiment.substring(BEAM_FN_API_DATA_BUFFER_LIMIT.length())));
      }
    }
    return Optional.empty();
  }

  private BeamFnDataGrpcMultiplexer getClientFor(
      Endpoints.ApiServiceDescriptor apiServiceDescriptor) {
    return cache.computeIfAbsent(
        apiServiceDescriptor,
        (Endpoints.ApiServiceDescriptor descriptor) ->
            new BeamFnDataGrpcMultiplexer(
                descriptor,
                outboundObserverFactory,
                BeamFnDataGrpc.newStub(channelFactory.apply(apiServiceDescriptor))::data));
  }
}
