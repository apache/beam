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
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Function;
import javax.annotation.Nullable;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.Elements;
import org.apache.beam.model.fnexecution.v1.BeamFnDataGrpc;
import org.apache.beam.model.pipeline.v1.Endpoints;
import org.apache.beam.model.pipeline.v1.Endpoints.ApiServiceDescriptor;
import org.apache.beam.sdk.fn.data.BeamFnDataGrpcMultiplexer;
import org.apache.beam.sdk.fn.data.CloseableFnDataReceiver;
import org.apache.beam.sdk.fn.stream.OutboundObserverFactory;
import org.apache.beam.vendor.grpc.v1p69p0.io.grpc.ManagedChannel;
import org.apache.beam.vendor.grpc.v1p69p0.io.grpc.Metadata;
import org.apache.beam.vendor.grpc.v1p69p0.io.grpc.stub.MetadataUtils;
import org.apache.beam.vendor.grpc.v1p69p0.io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link BeamFnDataClient} that uses gRPC for sending and receiving data.
 *
 * <p>TODO: Handle closing clients that are currently not a consumer nor are being consumed.
 */
public class BeamFnDataGrpcClient implements BeamFnDataClient {

  private static final Logger LOG = LoggerFactory.getLogger(BeamFnDataGrpcClient.class);

  private static class MultiplexerKey {
    private final Endpoints.ApiServiceDescriptor apiServiceDescriptor;
    private final String dataStreamId;

    private MultiplexerKey(
        Endpoints.ApiServiceDescriptor apiServiceDescriptor, String dataStreamId) {
      this.apiServiceDescriptor = apiServiceDescriptor;
      this.dataStreamId = dataStreamId;
    }

    @Override
    public boolean equals(@Nullable Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof MultiplexerKey)) {
        return false;
      }
      MultiplexerKey that = (MultiplexerKey) o;
      return Objects.equals(dataStreamId, that.dataStreamId)
          && Objects.equals(apiServiceDescriptor, that.apiServiceDescriptor);
    }

    @Override
    public int hashCode() {
      return Objects.hash(apiServiceDescriptor, dataStreamId);
    }
  }

  private final ConcurrentMap<MultiplexerKey, BeamFnDataGrpcMultiplexer> multiplexerCache;
  private final Function<Endpoints.ApiServiceDescriptor, ManagedChannel> channelFactory;
  private final OutboundObserverFactory outboundObserverFactory;

  public BeamFnDataGrpcClient(
      Function<Endpoints.ApiServiceDescriptor, ManagedChannel> channelFactory,
      OutboundObserverFactory outboundObserverFactory) {
    this.channelFactory = channelFactory;
    this.outboundObserverFactory = outboundObserverFactory;
    this.multiplexerCache = new ConcurrentHashMap<>();
  }

  @Override
  public void registerReceiver(
      String instructionId,
      String dataStreamId,
      List<ApiServiceDescriptor> apiServiceDescriptors,
      CloseableFnDataReceiver<Elements> receiver) {
    LOG.debug("Registering consumer for {}", instructionId);
    for (int i = 0, size = apiServiceDescriptors.size(); i < size; i++) {
      BeamFnDataGrpcMultiplexer client = getMultiplexer(apiServiceDescriptors.get(i), dataStreamId);
      client.registerConsumer(instructionId, receiver);
    }
  }

  @Override
  public void unregisterReceiver(
      String instructionId, String dataStreamId, List<ApiServiceDescriptor> apiServiceDescriptors) {
    LOG.debug("Unregistering consumer for {}", instructionId);
    for (int i = 0, size = apiServiceDescriptors.size(); i < size; i++) {
      BeamFnDataGrpcMultiplexer client = getMultiplexer(apiServiceDescriptors.get(i), dataStreamId);
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
  public StreamObserver<Elements> getOutboundObserver(
      ApiServiceDescriptor apiServiceDescriptor, String dataStreamId) {
    return getMultiplexer(apiServiceDescriptor, dataStreamId).getOutboundObserver();
  }

  private BeamFnDataGrpcMultiplexer getMultiplexer(
      Endpoints.ApiServiceDescriptor apiServiceDescriptor, String dataStreamId) {
    MultiplexerKey key = new MultiplexerKey(apiServiceDescriptor, dataStreamId);
    return multiplexerCache.computeIfAbsent(
        key,
        k -> {
          OutboundObserverFactory.BasicFactory<Elements, Elements> baseOutboundObserverFactory =
              inboundObserver -> {
                BeamFnDataGrpc.BeamFnDataStub stub =
                    BeamFnDataGrpc.newStub(channelFactory.apply(apiServiceDescriptor));
                if (dataStreamId != null && !dataStreamId.isEmpty()) {
                  Metadata headers = new Metadata();
                  headers.put(
                      Metadata.Key.of("data_stream_id", Metadata.ASCII_STRING_MARSHALLER),
                      dataStreamId);
                  stub = stub.withInterceptors(MetadataUtils.newAttachHeadersInterceptor(headers));
                }
                return stub.data(inboundObserver);
              };
          return new BeamFnDataGrpcMultiplexer(
              apiServiceDescriptor, outboundObserverFactory, baseOutboundObserverFactory);
        });
  }
}
