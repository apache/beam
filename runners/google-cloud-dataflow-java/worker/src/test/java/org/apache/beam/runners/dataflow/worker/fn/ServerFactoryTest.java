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
package org.apache.beam.runners.dataflow.worker.fn;

import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assume.assumeTrue;

import com.google.common.collect.ImmutableList;
import com.google.common.net.HostAndPort;
import com.google.common.util.concurrent.Uninterruptibles;
import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import org.apache.beam.model.fnexecution.v1.BeamFnApi;
import org.apache.beam.model.fnexecution.v1.BeamFnDataGrpc;
import org.apache.beam.model.pipeline.v1.Endpoints;
import org.apache.beam.model.pipeline.v1.Endpoints.ApiServiceDescriptor;
import org.apache.beam.runners.dataflow.harness.test.TestStreams;
import org.apache.beam.runners.dataflow.options.DataflowPipelineDebugOptions;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.vendor.grpc.v1_13_1.io.grpc.ManagedChannel;
import org.apache.beam.vendor.grpc.v1_13_1.io.grpc.ManagedChannelBuilder;
import org.apache.beam.vendor.grpc.v1_13_1.io.grpc.Server;
import org.apache.beam.vendor.grpc.v1_13_1.io.grpc.netty.NettyChannelBuilder;
import org.apache.beam.vendor.grpc.v1_13_1.io.grpc.stub.CallStreamObserver;
import org.apache.beam.vendor.grpc.v1_13_1.io.grpc.stub.StreamObserver;
import org.apache.beam.vendor.grpc.v1_13_1.io.netty.channel.epoll.Epoll;
import org.apache.beam.vendor.grpc.v1_13_1.io.netty.channel.epoll.EpollDomainSocketChannel;
import org.apache.beam.vendor.grpc.v1_13_1.io.netty.channel.epoll.EpollEventLoopGroup;
import org.apache.beam.vendor.grpc.v1_13_1.io.netty.channel.epoll.EpollSocketChannel;
import org.apache.beam.vendor.grpc.v1_13_1.io.netty.channel.unix.DomainSocketAddress;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link ServerFactory}. */
@RunWith(JUnit4.class)
public class ServerFactoryTest {
  private static final BeamFnApi.Elements CLIENT_DATA =
      BeamFnApi.Elements.newBuilder()
          .addData(BeamFnApi.Elements.Data.newBuilder().setInstructionReference("1"))
          .build();
  private static final BeamFnApi.Elements SERVER_DATA =
      BeamFnApi.Elements.newBuilder()
          .addData(BeamFnApi.Elements.Data.newBuilder().setInstructionReference("1"))
          .build();

  @Test
  public void testCreatingDefaultServer() throws Exception {
    Endpoints.ApiServiceDescriptor apiServiceDescriptor =
        runTestUsing(PipelineOptionsFactory.create());
    HostAndPort hostAndPort = HostAndPort.fromString(apiServiceDescriptor.getUrl());
    assertThat(
        hostAndPort.getHost(),
        anyOf(
            equalTo(InetAddress.getLoopbackAddress().getHostName()),
            equalTo(InetAddress.getLoopbackAddress().getHostAddress())));
    assertThat(hostAndPort.getPort(), allOf(greaterThan(0), lessThan(65536)));
  }

  @Test
  public void testCreatingEpollServer() throws Exception {
    assumeTrue(Epoll.isAvailable());
    // tcnative only supports the ipv4 address family
    assumeTrue(InetAddress.getLoopbackAddress() instanceof Inet4Address);
    Endpoints.ApiServiceDescriptor apiServiceDescriptor =
        runTestUsing(
            PipelineOptionsFactory.fromArgs(new String[] {"--experiments=beam_fn_api_epoll"})
                .create());
    HostAndPort hostAndPort = HostAndPort.fromString(apiServiceDescriptor.getUrl());
    assertThat(
        hostAndPort.getHost(),
        anyOf(
            equalTo(InetAddress.getLoopbackAddress().getHostName()),
            equalTo(InetAddress.getLoopbackAddress().getHostAddress())));
    assertThat(hostAndPort.getPort(), allOf(greaterThan(0), lessThan(65536)));
  }

  @Test
  public void testCreatingUnixDomainSocketServer() throws Exception {
    assumeTrue(Epoll.isAvailable());
    Endpoints.ApiServiceDescriptor apiServiceDescriptor =
        runTestUsing(
            PipelineOptionsFactory.fromArgs(
                    new String[] {
                      "--experiments=beam_fn_api_epoll,beam_fn_api_epoll_domain_socket"
                    })
                .create());
    assertThat(
        apiServiceDescriptor.getUrl(),
        startsWith("unix://" + System.getProperty("java.io.tmpdir")));
  }

  private Endpoints.ApiServiceDescriptor runTestUsing(PipelineOptions options) throws Exception {
    ManagedChannelFactory channelFactory = ManagedChannelFactory.from(options);
    Endpoints.ApiServiceDescriptor.Builder apiServiceDescriptorBuilder =
        Endpoints.ApiServiceDescriptor.newBuilder();

    Collection<BeamFnApi.Elements> serverElements = new ArrayList<>();
    CountDownLatch clientHangedUp = new CountDownLatch(1);
    CallStreamObserver<BeamFnApi.Elements> serverInboundObserver =
        TestStreams.withOnNext(serverElements::add)
            .withOnCompleted(clientHangedUp::countDown)
            .build();
    TestDataService service = new TestDataService(serverInboundObserver);

    ServerFactory serverFactory = ServerFactory.fromOptions(options);
    Server server =
        serverFactory.allocatePortAndCreate(apiServiceDescriptorBuilder, ImmutableList.of(service));
    assertFalse(server.isShutdown());
    ManagedChannel channel = channelFactory.forDescriptor(apiServiceDescriptorBuilder.build());
    BeamFnDataGrpc.BeamFnDataStub stub = BeamFnDataGrpc.newStub(channel);
    Collection<BeamFnApi.Elements> clientElements = new ArrayList<>();
    CountDownLatch serverHangedUp = new CountDownLatch(1);
    CallStreamObserver<BeamFnApi.Elements> clientInboundObserver =
        TestStreams.withOnNext(clientElements::add)
            .withOnCompleted(serverHangedUp::countDown)
            .build();

    StreamObserver<BeamFnApi.Elements> clientOutboundObserver = stub.data(clientInboundObserver);
    StreamObserver<BeamFnApi.Elements> serverOutboundObserver = service.outboundObservers.take();

    clientOutboundObserver.onNext(CLIENT_DATA);
    serverOutboundObserver.onNext(SERVER_DATA);
    clientOutboundObserver.onCompleted();
    clientHangedUp.await();
    serverOutboundObserver.onCompleted();
    serverHangedUp.await();

    assertThat(clientElements, contains(SERVER_DATA));
    assertThat(serverElements, contains(CLIENT_DATA));
    server.shutdown();
    server.awaitTermination(1, TimeUnit.SECONDS);
    server.shutdownNow();

    return apiServiceDescriptorBuilder.build();
  }

  /** A test gRPC service that uses the provided inbound observer for all clients. */
  private static class TestDataService extends BeamFnDataGrpc.BeamFnDataImplBase {
    private final LinkedBlockingQueue<StreamObserver<BeamFnApi.Elements>> outboundObservers;
    private final StreamObserver<BeamFnApi.Elements> inboundObserver;

    private TestDataService(StreamObserver<BeamFnApi.Elements> inboundObserver) {
      this.inboundObserver = inboundObserver;
      this.outboundObservers = new LinkedBlockingQueue<>();
    }

    @Override
    public StreamObserver<BeamFnApi.Elements> data(
        StreamObserver<BeamFnApi.Elements> outboundObserver) {
      Uninterruptibles.putUninterruptibly(outboundObservers, outboundObserver);
      return inboundObserver;
    }
  }

  /**
   * Uses {@link PipelineOptions} to configure which underlying {@link ManagedChannel}
   * implementation to use.
   *
   * <p>TODO: Remove this fork once available from a common shared library.
   */
  public abstract static class ManagedChannelFactory {
    public static ManagedChannelFactory from(PipelineOptions options) {
      List<String> experiments = options.as(DataflowPipelineDebugOptions.class).getExperiments();
      if (experiments != null && experiments.contains("beam_fn_api_epoll")) {
        org.apache.beam.vendor.grpc.v1_13_1.io.netty.channel.epoll.Epoll.ensureAvailability();
        return new Epoll();
      }
      return new Default();
    }

    public abstract ManagedChannel forDescriptor(ApiServiceDescriptor apiServiceDescriptor);

    /**
     * Creates a {@link ManagedChannel} backed by an {@link EpollDomainSocketChannel} if the address
     * is a {@link DomainSocketAddress}. Otherwise creates a {@link ManagedChannel} backed by an
     * {@link EpollSocketChannel}.
     */
    private static class Epoll extends ManagedChannelFactory {
      @Override
      public ManagedChannel forDescriptor(ApiServiceDescriptor apiServiceDescriptor) {
        SocketAddress address = SocketAddressFactory.createFrom(apiServiceDescriptor.getUrl());
        return NettyChannelBuilder.forAddress(address)
            .channelType(
                address instanceof DomainSocketAddress
                    ? EpollDomainSocketChannel.class
                    : EpollSocketChannel.class)
            .eventLoopGroup(new EpollEventLoopGroup())
            .usePlaintext(true)
            // Set the message size to max value here. The actual size is governed by the
            // buffer size in the layers above.
            .maxInboundMessageSize(Integer.MAX_VALUE)
            .build();
      }
    }

    /**
     * Creates a {@link ManagedChannel} relying on the {@link ManagedChannelBuilder} to create
     * instances.
     */
    private static class Default extends ManagedChannelFactory {
      @Override
      public ManagedChannel forDescriptor(ApiServiceDescriptor apiServiceDescriptor) {
        return ManagedChannelBuilder.forTarget(apiServiceDescriptor.getUrl())
            .usePlaintext(true)
            // Set the message size to max value here. The actual size is governed by the
            // buffer size in the layers above.
            .maxInboundMessageSize(Integer.MAX_VALUE)
            .build();
      }
    }
  }
}
