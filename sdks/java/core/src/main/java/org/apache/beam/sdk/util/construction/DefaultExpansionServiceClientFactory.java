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
package org.apache.beam.sdk.util.construction;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import org.apache.beam.model.expansion.v1.ExpansionApi;
import org.apache.beam.model.expansion.v1.ExpansionServiceGrpc;
import org.apache.beam.model.pipeline.v1.Endpoints;
import org.apache.beam.vendor.grpc.v1p69p0.io.grpc.ManagedChannel;
import org.apache.beam.vendor.grpc.v1p69p0.io.grpc.Status;
import org.apache.beam.vendor.grpc.v1p69p0.io.grpc.StatusRuntimeException;

/** Default factory for ExpansionServiceClient used by External transform. */
public class DefaultExpansionServiceClientFactory implements ExpansionServiceClientFactory {
  private Map<Endpoints.ApiServiceDescriptor, ExpansionServiceClient> expansionServiceMap;
  private Function<Endpoints.ApiServiceDescriptor, ManagedChannel> channelFactory;

  private DefaultExpansionServiceClientFactory(
      Function<Endpoints.ApiServiceDescriptor, ManagedChannel> channelFactory) {
    this.expansionServiceMap = new ConcurrentHashMap<>();
    this.channelFactory = channelFactory;
  }

  public static DefaultExpansionServiceClientFactory create(
      Function<Endpoints.ApiServiceDescriptor, ManagedChannel> channelFactory) {
    return new DefaultExpansionServiceClientFactory(channelFactory);
  }

  @Override
  public void close() throws Exception {
    for (ExpansionServiceClient client : expansionServiceMap.values()) {
      try (AutoCloseable closer = client) {}
    }
  }

  @Override
  public ExpansionServiceClient getExpansionServiceClient(Endpoints.ApiServiceDescriptor endpoint) {
    return expansionServiceMap.computeIfAbsent(
        endpoint,
        e ->
            new ExpansionServiceClient() {
              private final ManagedChannel channel = channelFactory.apply(endpoint);
              private final ExpansionServiceGrpc.ExpansionServiceBlockingStub service =
                  ExpansionServiceGrpc.newBlockingStub(channel);

              private <T> T callWithRetry(java.util.concurrent.Callable<T> rpcCall) {
                int maxAttempts = 4;
                long backoffMs = 1000L;
                for (int attempt = 1; ; attempt++) {
                  try {
                    return rpcCall.call();
                  } catch (StatusRuntimeException ex) {
                    Status.Code code = ex.getStatus().getCode();
                    boolean retryable =
                        code == Status.Code.UNAVAILABLE
                            || code == Status.Code.DEADLINE_EXCEEDED
                            || code == Status.Code.INTERNAL
                            || code == Status.Code.UNKNOWN;
                    if (!retryable || attempt >= maxAttempts) {
                      throw ex;
                    }
                    channel.resetConnectBackoff();
                    try {
                      Thread.sleep(backoffMs);
                    } catch (InterruptedException ie) {
                      Thread.currentThread().interrupt();
                      throw ex;
                    }
                    backoffMs *= 2;
                  } catch (RuntimeException ex) {
                    throw ex;
                  } catch (Exception ex) {
                    throw new RuntimeException(ex);
                  }
                }
              }

              @Override
              public ExpansionApi.ExpansionResponse expand(ExpansionApi.ExpansionRequest request) {
                return callWithRetry(() -> service.expand(request));
              }

              @Override
              public ExpansionApi.DiscoverSchemaTransformResponse discover(
                  ExpansionApi.DiscoverSchemaTransformRequest request) {
                return callWithRetry(() -> service.discoverSchemaTransform(request));
              }

              @Override
              public void close() throws Exception {
                channel.shutdown();
              }
            });
  }
}
