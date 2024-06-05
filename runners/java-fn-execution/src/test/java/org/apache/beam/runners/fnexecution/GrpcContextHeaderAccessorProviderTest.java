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
package org.apache.beam.runners.fnexecution;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import org.apache.beam.model.fnexecution.v1.BeamFnApi;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.Elements;
import org.apache.beam.model.fnexecution.v1.BeamFnDataGrpc;
import org.apache.beam.model.pipeline.v1.Endpoints.ApiServiceDescriptor;
import org.apache.beam.sdk.fn.server.GrpcContextHeaderAccessorProvider;
import org.apache.beam.sdk.fn.server.InProcessServerFactory;
import org.apache.beam.vendor.grpc.v1p60p1.io.grpc.CallOptions;
import org.apache.beam.vendor.grpc.v1p60p1.io.grpc.Channel;
import org.apache.beam.vendor.grpc.v1p60p1.io.grpc.ClientCall;
import org.apache.beam.vendor.grpc.v1p60p1.io.grpc.ClientInterceptor;
import org.apache.beam.vendor.grpc.v1p60p1.io.grpc.ForwardingClientCall.SimpleForwardingClientCall;
import org.apache.beam.vendor.grpc.v1p60p1.io.grpc.Metadata;
import org.apache.beam.vendor.grpc.v1p60p1.io.grpc.MethodDescriptor;
import org.apache.beam.vendor.grpc.v1p60p1.io.grpc.inprocess.InProcessChannelBuilder;
import org.apache.beam.vendor.grpc.v1p60p1.io.grpc.stub.StreamObserver;
import org.apache.beam.vendor.grpc.v1p60p1.io.grpc.testing.GrpcCleanupRule;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mockito;

/** Tests for {@link GrpcContextHeaderAccessorProvider}. */
@RunWith(JUnit4.class)
public class GrpcContextHeaderAccessorProviderTest {
  @Rule public GrpcCleanupRule cleanupRule = new GrpcCleanupRule().setTimeout(10, TimeUnit.SECONDS);
  @Rule public transient Timeout globalTimeout = Timeout.seconds(600);

  @SuppressWarnings("unchecked")
  @Test
  public void testWorkerIdOnConnect() throws Exception {
    final String worker1 = "worker1";
    CompletableFuture<String> workerId = new CompletableFuture<>();
    Consumer<StreamObserver<Elements>> consumer =
        elementsStreamObserver -> {
          workerId.complete(GrpcContextHeaderAccessorProvider.getHeaderAccessor().getSdkWorkerId());
          elementsStreamObserver.onCompleted();
        };
    TestDataService testService = new TestDataService(Mockito.mock(StreamObserver.class), consumer);
    ApiServiceDescriptor serviceDescriptor =
        ApiServiceDescriptor.newBuilder().setUrl("testServer").build();
    cleanupRule.register(
        InProcessServerFactory.create().create(ImmutableList.of(testService), serviceDescriptor));
    final Metadata.Key<String> workerIdKey =
        Metadata.Key.of("worker_id", Metadata.ASCII_STRING_MARSHALLER);
    Channel channel =
        cleanupRule.register(
            InProcessChannelBuilder.forName(serviceDescriptor.getUrl())
                .intercept(
                    new ClientInterceptor() {
                      @Override
                      public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(
                          MethodDescriptor<ReqT, RespT> method,
                          CallOptions callOptions,
                          Channel next) {
                        ClientCall<ReqT, RespT> call = next.newCall(method, callOptions);
                        return new SimpleForwardingClientCall<ReqT, RespT>(call) {
                          @Override
                          public void start(
                              ClientCall.Listener<RespT> responseListener, Metadata headers) {
                            headers.put(workerIdKey, worker1);
                            super.start(responseListener, headers);
                          }
                        };
                      }
                    })
                .build());
    BeamFnDataGrpc.BeamFnDataStub stub = BeamFnDataGrpc.newStub(channel);
    stub.data(Mockito.mock(StreamObserver.class)).onCompleted();

    Assert.assertEquals(worker1, workerId.get());
  }

  /** A test gRPC service that uses the provided inbound observer for all clients. */
  private static class TestDataService extends BeamFnDataGrpc.BeamFnDataImplBase {
    private final StreamObserver<BeamFnApi.Elements> inboundObserver;
    private final Consumer<StreamObserver<Elements>> consumer;

    private TestDataService(
        StreamObserver<BeamFnApi.Elements> inboundObserver,
        Consumer<StreamObserver<BeamFnApi.Elements>> consumer) {
      this.inboundObserver = inboundObserver;
      this.consumer = consumer;
    }

    @Override
    public StreamObserver<BeamFnApi.Elements> data(
        StreamObserver<BeamFnApi.Elements> outboundObserver) {
      consumer.accept(outboundObserver);
      return inboundObserver;
    }
  }
}
