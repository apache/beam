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

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Function;
import java.util.function.Supplier;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.Elements;
import org.apache.beam.model.fnexecution.v1.BeamFnDataGrpc;
import org.apache.beam.model.pipeline.v1.Endpoints;
import org.apache.beam.model.pipeline.v1.Endpoints.ApiServiceDescriptor;
import org.apache.beam.sdk.fn.data.BeamFnDataGrpcMultiplexer;
import org.apache.beam.sdk.fn.data.BeamFnDataOutboundAggregator;
import org.apache.beam.sdk.fn.data.CloseableFnDataReceiver;
import org.apache.beam.sdk.fn.stream.OutboundObserverFactory;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.vendor.grpc.v1p60p1.io.grpc.ManagedChannel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link BeamFnDataClient} that uses gRPC for sending and receiving data.
 *
 * <p>TODO: Handle closing clients that are currently not a consumer nor are being consumed.
 */
public class BeamFnDataGrpcClient implements BeamFnDataClient {

  private static final Logger LOG = LoggerFactory.getLogger(BeamFnDataGrpcClient.class);

  private final ConcurrentMap<Endpoints.ApiServiceDescriptor, BeamFnDataGrpcMultiplexer>
      multiplexerCache;
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
    this.multiplexerCache = new ConcurrentHashMap<>();
  }

  @Override
  public void registerReceiver(
      String instructionId,
      List<ApiServiceDescriptor> apiServiceDescriptors,
      CloseableFnDataReceiver<Elements> receiver) {
    LOG.debug("Registering consumer for {}", instructionId);
    for (int i = 0, size = apiServiceDescriptors.size(); i < size; i++) {
      BeamFnDataGrpcMultiplexer client = getClientFor(apiServiceDescriptors.get(i));
      client.registerConsumer(instructionId, receiver);
    }
  }

  @Override
  public void unregisterReceiver(
      String instructionId, List<ApiServiceDescriptor> apiServiceDescriptors) {
    LOG.debug("Unregistering consumer for {}", instructionId);
    for (int i = 0, size = apiServiceDescriptors.size(); i < size; i++) {
      BeamFnDataGrpcMultiplexer client = getClientFor(apiServiceDescriptors.get(i));
      client.unregisterConsumer(instructionId);
    }
  }

  @Override
  public void poisonInstructionId(String instructionId) {
    LOG.debug("Poisoning instruction {}", instructionId);
    for (BeamFnDataGrpcMultiplexer client : multiplexerCache.values()) {
      client.poisonInstructionId(instructionId);
    }
  }

  @Override
  public BeamFnDataOutboundAggregator createOutboundAggregator(
      ApiServiceDescriptor apiServiceDescriptor,
      Supplier<String> processBundleRequestIdSupplier,
      boolean collectElementsIfNoFlushes) {
    return new BeamFnDataOutboundAggregator(
        options,
        processBundleRequestIdSupplier,
        getClientFor(apiServiceDescriptor).getOutboundObserver(),
        collectElementsIfNoFlushes);
  }

  private BeamFnDataGrpcMultiplexer getClientFor(
      Endpoints.ApiServiceDescriptor apiServiceDescriptor) {
    return multiplexerCache.computeIfAbsent(
        apiServiceDescriptor,
        (Endpoints.ApiServiceDescriptor descriptor) ->
            new BeamFnDataGrpcMultiplexer(
                descriptor,
                outboundObserverFactory,
                BeamFnDataGrpc.newStub(channelFactory.apply(apiServiceDescriptor))::data));
  }
}
