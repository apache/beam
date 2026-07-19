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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
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

  /**
   * Guards creation and removal of entries in {@link #multiplexerCache} as well as all accesses to
   * {@link #dataStreamRefCounts} so that a named data stream is not concurrently created and
   * closed.
   */
  private final Object dataStreamLifecycleLock = new Object();

  /**
   * The number of instructions currently retaining each named data stream. Guarded by {@link
   * #dataStreamLifecycleLock}. The default (empty) data stream is not tracked as it is kept open
   * for the lifetime of the client.
   */
  private final Map<String, Integer> dataStreamRefCounts;

  public BeamFnDataGrpcClient(
      Function<Endpoints.ApiServiceDescriptor, ManagedChannel> channelFactory,
      OutboundObserverFactory outboundObserverFactory) {
    this.channelFactory = channelFactory;
    this.outboundObserverFactory = outboundObserverFactory;
    this.multiplexerCache = new ConcurrentHashMap<>();
    this.dataStreamRefCounts = new HashMap<>();
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

  @Override
  public void retainDataStream(String dataStreamId) {
    if (dataStreamId == null || dataStreamId.isEmpty()) {
      // The default data stream is kept open for the lifetime of the client.
      return;
    }
    synchronized (dataStreamLifecycleLock) {
      dataStreamRefCounts.merge(dataStreamId, 1, Integer::sum);
    }
  }

  @Override
  public void releaseDataStream(String dataStreamId) {
    if (dataStreamId == null || dataStreamId.isEmpty()) {
      // The default data stream is kept open for the lifetime of the client.
      return;
    }
    List<BeamFnDataGrpcMultiplexer> multiplexersToClose = new ArrayList<>();
    synchronized (dataStreamLifecycleLock) {
      Integer refCount = dataStreamRefCounts.get(dataStreamId);
      if (refCount == null) {
        LOG.warn("Released data stream {} which was not retained.", dataStreamId);
        return;
      }
      if (refCount > 1) {
        dataStreamRefCounts.put(dataStreamId, refCount - 1);
        return;
      }
      dataStreamRefCounts.remove(dataStreamId);
      Iterator<Map.Entry<MultiplexerKey, BeamFnDataGrpcMultiplexer>> iterator =
          multiplexerCache.entrySet().iterator();
      while (iterator.hasNext()) {
        Map.Entry<MultiplexerKey, BeamFnDataGrpcMultiplexer> entry = iterator.next();
        if (dataStreamId.equals(entry.getKey().dataStreamId)) {
          multiplexersToClose.add(entry.getValue());
          iterator.remove();
        }
      }
    }
    // Close outside of the lock as closing terminates the underlying gRPC stream and may block.
    for (BeamFnDataGrpcMultiplexer multiplexer : multiplexersToClose) {
      LOG.debug("Closing multiplexer for released data stream {}", dataStreamId);
      try {
        multiplexer.close();
      } catch (Exception e) {
        LOG.warn("Failed to close multiplexer for data stream {}", dataStreamId, e);
      }
    }
  }

  private BeamFnDataGrpcMultiplexer getMultiplexer(
      Endpoints.ApiServiceDescriptor apiServiceDescriptor, String dataStreamId) {
    MultiplexerKey key = new MultiplexerKey(apiServiceDescriptor, dataStreamId);
    BeamFnDataGrpcMultiplexer existingMultiplexer = multiplexerCache.get(key);
    if (existingMultiplexer != null) {
      return existingMultiplexer;
    }
    // Create under the lifecycle lock so that a named data stream being concurrently closed by
    // releaseDataStream is not observed in a partially removed state. Callers are expected to
    // retain named data streams for the duration of their usage which prevents the returned
    // multiplexer from being closed while in use.
    synchronized (dataStreamLifecycleLock) {
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
                    stub =
                        stub.withInterceptors(MetadataUtils.newAttachHeadersInterceptor(headers));
                  }
                  return stub.data(inboundObserver);
                };
            return new BeamFnDataGrpcMultiplexer(
                apiServiceDescriptor, outboundObserverFactory, baseOutboundObserverFactory);
          });
    }
  }
}
