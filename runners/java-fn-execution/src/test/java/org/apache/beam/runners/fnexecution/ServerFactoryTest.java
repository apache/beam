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

import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assume.assumeTrue;

import java.net.Inet4Address;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import org.apache.beam.model.fnexecution.v1.BeamFnApi;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.Elements;
import org.apache.beam.model.fnexecution.v1.BeamFnDataGrpc;
import org.apache.beam.model.pipeline.v1.Endpoints;
import org.apache.beam.model.pipeline.v1.Endpoints.ApiServiceDescriptor;
import org.apache.beam.sdk.fn.channel.ManagedChannelFactory;
import org.apache.beam.sdk.fn.test.TestStreams;
import org.apache.beam.vendor.grpc.v1p26p0.io.grpc.ManagedChannel;
import org.apache.beam.vendor.grpc.v1p26p0.io.grpc.Server;
import org.apache.beam.vendor.grpc.v1p26p0.io.grpc.stub.CallStreamObserver;
import org.apache.beam.vendor.grpc.v1p26p0.io.grpc.stub.StreamObserver;
import org.apache.beam.vendor.grpc.v1p26p0.io.grpc.testing.GrpcCleanupRule;
import org.apache.beam.vendor.grpc.v1p26p0.io.netty.channel.epoll.Epoll;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.net.HostAndPort;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.util.concurrent.Uninterruptibles;
import org.junit.Rule;
import org.junit.Test;

/** Tests for {@link ServerFactory}. */
public class ServerFactoryTest {

  private static final BeamFnApi.Elements CLIENT_DATA =
      BeamFnApi.Elements.newBuilder()
          .addData(BeamFnApi.Elements.Data.newBuilder().setInstructionId("1"))
          .build();
  private static final BeamFnApi.Elements SERVER_DATA =
      BeamFnApi.Elements.newBuilder()
          .addData(BeamFnApi.Elements.Data.newBuilder().setInstructionId("1"))
          .build();

  @Rule
  public GrpcCleanupRule grpcCleanupRule = new GrpcCleanupRule().setTimeout(10, TimeUnit.SECONDS);

  @Test
  public void defaultServerWorks() throws Exception {
    Endpoints.ApiServiceDescriptor apiServiceDescriptor =
        runTestUsing(ServerFactory.createDefault(), ManagedChannelFactory.createDefault());
    HostAndPort hostAndPort = HostAndPort.fromString(apiServiceDescriptor.getUrl());
    assertThat(
        hostAndPort.getHost(),
        anyOf(
            equalTo(InetAddress.getLoopbackAddress().getHostName()),
            equalTo(InetAddress.getLoopbackAddress().getHostAddress())));
    assertThat(hostAndPort.getPort(), allOf(greaterThan(0), lessThan(65536)));
  }

  @Test
  public void usesUrlFactory() throws Exception {
    ServerFactory serverFactory = ServerFactory.createWithUrlFactory((host, port) -> "foo");
    CallStreamObserver<Elements> observer =
        TestStreams.withOnNext((Elements unused) -> {}).withOnCompleted(() -> {}).build();
    TestDataService service = new TestDataService(observer);
    ApiServiceDescriptor.Builder descriptorBuilder = ApiServiceDescriptor.newBuilder();
    grpcCleanupRule.register(
        serverFactory.allocateAddressAndCreate(ImmutableList.of(service), descriptorBuilder));
    assertThat(descriptorBuilder.getUrl(), is("foo"));
  }

  @Test
  public void defaultServerWithPortSupplier() throws Exception {
    Endpoints.ApiServiceDescriptor apiServiceDescriptor =
        runTestUsing(
            ServerFactory.createWithPortSupplier(() -> 65535),
            ManagedChannelFactory.createDefault());
    HostAndPort hostAndPort = HostAndPort.fromString(apiServiceDescriptor.getUrl());
    assertThat(
        hostAndPort.getHost(),
        anyOf(
            equalTo(InetAddress.getLoopbackAddress().getHostName()),
            equalTo(InetAddress.getLoopbackAddress().getHostAddress())));
    assertThat(hostAndPort.getPort(), is(65535));
  }

  @Test
  public void urlFactoryWithPortSupplier() throws Exception {
    ServerFactory serverFactory =
        ServerFactory.createWithUrlFactoryAndPortSupplier(
            (host, port) -> "foo" + ":" + port, () -> 65535);
    CallStreamObserver<Elements> observer =
        TestStreams.withOnNext((Elements unused) -> {}).withOnCompleted(() -> {}).build();
    TestDataService service = new TestDataService(observer);
    ApiServiceDescriptor.Builder descriptorBuilder = ApiServiceDescriptor.newBuilder();
    grpcCleanupRule.register(
        serverFactory.allocateAddressAndCreate(ImmutableList.of(service), descriptorBuilder));
    assertThat(descriptorBuilder.getUrl(), is("foo:65535"));
  }

  @Test
  public void testCreatingEpollServer() throws Exception {
    assumeTrue(Epoll.isAvailable());
    // tcnative only supports the ipv4 address family
    assumeTrue(InetAddress.getLoopbackAddress() instanceof Inet4Address);
    Endpoints.ApiServiceDescriptor apiServiceDescriptor =
        runTestUsing(ServerFactory.createEpollSocket(), ManagedChannelFactory.createEpoll());
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
        runTestUsing(ServerFactory.createEpollDomainSocket(), ManagedChannelFactory.createEpoll());
    assertThat(
        apiServiceDescriptor.getUrl(),
        startsWith("unix://" + System.getProperty("java.io.tmpdir")));
  }

  private Endpoints.ApiServiceDescriptor runTestUsing(
      ServerFactory serverFactory, ManagedChannelFactory channelFactory) throws Exception {
    Endpoints.ApiServiceDescriptor.Builder apiServiceDescriptorBuilder =
        Endpoints.ApiServiceDescriptor.newBuilder();

    final Collection<Elements> serverElements = new ArrayList<>();
    final CountDownLatch clientHangedUp = new CountDownLatch(1);
    CallStreamObserver<Elements> serverInboundObserver =
        TestStreams.withOnNext(serverElements::add)
            .withOnCompleted(clientHangedUp::countDown)
            .build();
    TestDataService service = new TestDataService(serverInboundObserver);
    Server server =
        serverFactory.allocateAddressAndCreate(
            ImmutableList.of(service), apiServiceDescriptorBuilder);
    assertFalse(server.isShutdown());

    ManagedChannel channel = channelFactory.forDescriptor(apiServiceDescriptorBuilder.build());
    BeamFnDataGrpc.BeamFnDataStub stub = BeamFnDataGrpc.newStub(channel);
    final Collection<BeamFnApi.Elements> clientElements = new ArrayList<>();
    final CountDownLatch serverHangedUp = new CountDownLatch(1);
    CallStreamObserver<BeamFnApi.Elements> clientInboundObserver =
        TestStreams.withOnNext(clientElements::add)
            .withOnCompleted(serverHangedUp::countDown)
            .build();

    StreamObserver<Elements> clientOutboundObserver = stub.data(clientInboundObserver);
    StreamObserver<BeamFnApi.Elements> serverOutboundObserver = service.outboundObservers.take();

    clientOutboundObserver.onNext(CLIENT_DATA);
    serverOutboundObserver.onNext(SERVER_DATA);
    clientOutboundObserver.onCompleted();
    clientHangedUp.await();
    serverOutboundObserver.onCompleted();
    serverHangedUp.await();

    assertThat(clientElements, contains(SERVER_DATA));
    assertThat(serverElements, contains(CLIENT_DATA));

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
}
