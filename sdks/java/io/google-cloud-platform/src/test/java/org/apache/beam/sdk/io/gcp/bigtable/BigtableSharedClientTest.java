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
package org.apache.beam.sdk.io.gcp.bigtable;

import static org.hamcrest.MatcherAssert.assertThat;

import com.google.api.gax.grpc.ChannelPoolSettings;
import com.google.api.gax.grpc.InstantiatingGrpcChannelProvider;
import com.google.bigtable.v2.BigtableGrpc;
import com.google.bigtable.v2.MutateRowsRequest;
import com.google.bigtable.v2.MutateRowsResponse;
import com.google.bigtable.v2.MutateRowsResponse.Entry;
import com.google.bigtable.v2.Mutation;
import com.google.bigtable.v2.Mutation.SetCell;
import com.google.bigtable.v2.PingAndWarmRequest;
import com.google.bigtable.v2.PingAndWarmResponse;
import com.google.bigtable.v2.ReadRowsRequest;
import com.google.bigtable.v2.ReadRowsResponse;
import com.google.cloud.bigtable.data.v2.BigtableDataSettings.Builder;
import com.google.cloud.bigtable.data.v2.stub.metrics.NoopMetricsProvider;
import com.google.protobuf.ByteString;
import com.google.rpc.Code;
import com.google.rpc.Status;
import io.grpc.BindableService;
import io.grpc.ForwardingServerCallListener.SimpleForwardingServerCallListener;
import io.grpc.Grpc;
import io.grpc.Metadata;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.ServerCall;
import io.grpc.ServerCall.Listener;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.stub.StreamObserver;
import java.io.IOException;
import java.net.ServerSocket;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;
import org.apache.beam.runners.direct.DirectRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult.State;
import org.apache.beam.sdk.io.GenerateSequence;
import org.apache.beam.sdk.options.ExperimentalOptions;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.hamcrest.Matchers;
import org.joda.time.Duration;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Ensure that BigtableIO.write() reuses the same instance of the underlying bigtable client. This
 * test will create a toy pipeline using DirectRunner and have it write to a local emulator. The
 * emulator will record all of the client connections. Then the test will check that only a single
 * connection was used.
 */
@RunWith(JUnit4.class)
public class BigtableSharedClientTest {
  private FakeBigtable fakeService;
  private ServerClientConnectionCounterInterceptor clientConnectionInterceptor;
  private Server fakeServer;

  @Before
  public void setUp() throws Exception {
    clientConnectionInterceptor = new ServerClientConnectionCounterInterceptor();
    this.fakeService = new FakeBigtable();

    IOException lastError = null;

    for (int i = 0; i < 10; i++) {
      try {
        this.fakeServer = createServer(fakeService, clientConnectionInterceptor);
        lastError = null;
        break;
      } catch (IOException e) {
        lastError = e;
      }
    }
    if (lastError != null) {
      throw lastError;
    }
  }

  @After
  public void tearDown() throws Exception {
    if (fakeServer != null) {
      fakeServer.shutdownNow();
    }
  }

  private static Server createServer(BindableService service, ServerInterceptor... interceptors)
      throws IOException {
    int port;
    try (ServerSocket ss = new ServerSocket(0)) {
      port = ss.getLocalPort();
    }

    ServerBuilder<?> serverBuilder = ServerBuilder.forPort(port).addService(service);

    for (ServerInterceptor interceptor : interceptors) {
      serverBuilder.intercept(interceptor);
    }
    return serverBuilder.build().start();
  }

  @Test
  public void testClientReusedAcrossBundles() {
    PipelineOptions opts = PipelineOptionsFactory.create();
    opts.setRunner(DirectRunner.class);
    ExperimentalOptions.addExperiment(
        opts.as(ExperimentalOptions.class),
        String.format(
            "%s=%s",
            BigtableConfigTranslator.BIGTABLE_SETTINGS_OVERRIDE,
            ClientSettingsOverride.class.getName()));

    Pipeline pipeline = Pipeline.create(opts);

    pipeline
        .apply(
            // Create an unbounded source with a rate limit to ensure creation of multiple bundles
            GenerateSequence.from(0)
                .withRate(10, Duration.millis(100))
                .withMaxReadTime(Duration.standardSeconds(2)))
        .apply(ParDo.of(new MutationsDoFn())) // create Mutations & count bundles
        .apply(
            BigtableIO.write()
                .withProjectId("fake-project")
                .withInstanceId("fake-instance")
                .withTableId("fake-table")
                .withEmulator("localhost:" + fakeServer.getPort()));

    assertThat(pipeline.run().waitUntilFinish(), Matchers.equalTo(State.DONE));
    // Make sure that the test is valid by making sure that multiple bundles were processed
    assertThat(MutationsDoFn.bundleCount.get(), Matchers.greaterThan(1));
    // Make sure that a single client was shared across all the bundles
    assertThat(clientConnectionInterceptor.getClientConnections(), Matchers.hasSize(1));

    Assert.assertTrue("BigtableServiceFactory should be empty", BigtableServiceFactory.isEmpty());
  }

  /** Minimal implementation of a Bigtable emulator for BigtableIO.write(). */
  static class FakeBigtable extends BigtableGrpc.BigtableImplBase {

    @Override
    public void readRows(
        ReadRowsRequest request, StreamObserver<ReadRowsResponse> responseObserver) {
      responseObserver.onCompleted();
    }

    @Override
    public void mutateRows(
        MutateRowsRequest request, StreamObserver<MutateRowsResponse> responseObserver) {
      MutateRowsResponse.Builder builder = MutateRowsResponse.newBuilder();

      for (int i = 0; i < request.getEntriesCount(); i++) {
        builder.addEntries(
            Entry.newBuilder()
                .setIndex(i)
                .setStatus(Status.newBuilder().setCode(Code.OK_VALUE))
                .build());
      }
      responseObserver.onNext(builder.build());
      responseObserver.onCompleted();
    }

    @Override
    public void pingAndWarm(
        PingAndWarmRequest request, StreamObserver<PingAndWarmResponse> responseObserver) {
      responseObserver.onCompleted();
    }
  }

  static class MutationsDoFn extends DoFn<Long, KV<ByteString, Iterable<Mutation>>> {
    private static final AtomicInteger bundleCount = new AtomicInteger();

    @StartBundle
    public void startBundle(StartBundleContext ctx) {
      bundleCount.incrementAndGet();
    }

    @ProcessElement
    public void processElement(
        @Element Long input, OutputReceiver<KV<ByteString, Iterable<Mutation>>> output) {
      output.output(
          KV.of(
              ByteString.copyFromUtf8(input.toString()),
              ImmutableList.of(
                  Mutation.newBuilder()
                      .setSetCell(
                          SetCell.newBuilder()
                              .setFamilyName("fake-family")
                              .setColumnQualifier(ByteString.copyFromUtf8("fake-qualifier"))
                              .setTimestampMicros(System.currentTimeMillis() * 1000)
                              .setValue(ByteString.copyFromUtf8("fake-value")))
                      .build())));
    }
  }

  /** Overrides the default settings to ensure 1 channel per client. */
  public static class ClientSettingsOverride
      implements BiFunction<Builder, PipelineOptions, Builder> {

    @Override
    public Builder apply(Builder builder, PipelineOptions pipelineOptions) {
      InstantiatingGrpcChannelProvider oldTransport =
          (InstantiatingGrpcChannelProvider) builder.stubSettings().getTransportChannelProvider();

      builder
          .stubSettings()
          .setTransportChannelProvider(
              oldTransport
                  .toBuilder()
                  .setChannelPoolSettings(ChannelPoolSettings.staticallySized(1))
                  .build());
      // Make sure to disable builtin metrics
      builder.stubSettings().setMetricsProvider(NoopMetricsProvider.INSTANCE);
      return builder;
    }
  }

  static class ServerClientConnectionCounterInterceptor implements ServerInterceptor {
    private Set<String> clientConnections = Collections.synchronizedSet(new HashSet<>());

    @Override
    public <ReqT, RespT> Listener<ReqT> interceptCall(
        ServerCall<ReqT, RespT> call, Metadata headers, ServerCallHandler<ReqT, RespT> next) {

      return new SimpleForwardingServerCallListener<ReqT>(next.startCall(call, headers)) {
        @Override
        public void onComplete() {
          if (call.getMethodDescriptor().equals(BigtableGrpc.getMutateRowsMethod())) {
            clientConnections.add(
                call.getAttributes().get(Grpc.TRANSPORT_ATTR_REMOTE_ADDR).toString());
          }
          super.onComplete();
        }
      };
    }

    public Set<String> getClientConnections() {
      return clientConnections;
    }
  }
}
