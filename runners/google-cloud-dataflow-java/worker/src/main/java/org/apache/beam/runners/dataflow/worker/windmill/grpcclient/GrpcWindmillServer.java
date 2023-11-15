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
package org.apache.beam.runners.dataflow.worker.windmill.grpcclient;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;
import org.apache.beam.runners.dataflow.worker.options.StreamingDataflowWorkerOptions;
import org.apache.beam.runners.dataflow.worker.windmill.AbstractWindmillStream;
import org.apache.beam.runners.dataflow.worker.windmill.CloudWindmillServiceV1Alpha1Grpc;
import org.apache.beam.runners.dataflow.worker.windmill.StreamObserverFactory;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.CommitWorkRequest;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.CommitWorkResponse;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.GetConfigRequest;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.GetConfigResponse;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.GetDataRequest;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.GetDataResponse;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.GetWorkRequest;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.GetWorkResponse;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.JobHeader;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.ReportStatsRequest;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.ReportStatsResponse;
import org.apache.beam.runners.dataflow.worker.windmill.WindmillApplianceGrpc;
import org.apache.beam.runners.dataflow.worker.windmill.WindmillServerStub;
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.util.BackOff;
import org.apache.beam.sdk.util.BackOffUtils;
import org.apache.beam.sdk.util.FluentBackoff;
import org.apache.beam.sdk.util.Sleeper;
import org.apache.beam.vendor.grpc.v1p54p0.io.grpc.CallCredentials;
import org.apache.beam.vendor.grpc.v1p54p0.io.grpc.Channel;
import org.apache.beam.vendor.grpc.v1p54p0.io.grpc.StatusRuntimeException;
import org.apache.beam.vendor.grpc.v1p54p0.io.grpc.auth.MoreCallCredentials;
import org.apache.beam.vendor.grpc.v1p54p0.io.grpc.inprocess.InProcessChannelBuilder;
import org.apache.beam.vendor.grpc.v1p54p0.io.grpc.netty.GrpcSslContexts;
import org.apache.beam.vendor.grpc.v1p54p0.io.grpc.netty.NegotiationType;
import org.apache.beam.vendor.grpc.v1p54p0.io.grpc.netty.NettyChannelBuilder;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Splitter;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableSet;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.net.HostAndPort;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** gRPC client for communicating with Windmill Service. */
// Very likely real potential for bugs - https://github.com/apache/beam/issues/19273
// Very likely real potential for bugs - https://github.com/apache/beam/issues/19271
@SuppressFBWarnings({"JLM_JSR166_UTILCONCURRENT_MONITORENTER", "IS2_INCONSISTENT_SYNC"})
@SuppressWarnings({
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
public final class GrpcWindmillServer extends WindmillServerStub {
  private static final Logger LOG = LoggerFactory.getLogger(GrpcWindmillServer.class);

  // If a connection cannot be established, gRPC will fail fast so this deadline can be relatively
  // high.
  private static final long DEFAULT_UNARY_RPC_DEADLINE_SECONDS = 300;
  private static final long DEFAULT_STREAM_RPC_DEADLINE_SECONDS = 300;
  private static final int DEFAULT_LOG_EVERY_N_FAILURES = 20;
  private static final String LOCALHOST = "localhost";
  private static final Duration MIN_BACKOFF = Duration.millis(1);
  private static final Duration MAX_BACKOFF = Duration.standardSeconds(30);
  private static final AtomicLong nextId = new AtomicLong(0);
  private static final int NO_HEALTH_CHECK = -1;

  private final StreamingDataflowWorkerOptions options;
  private final int streamingRpcBatchLimit;
  private final List<CloudWindmillServiceV1Alpha1Grpc.CloudWindmillServiceV1Alpha1Stub> stubList;
  private final List<CloudWindmillServiceV1Alpha1Grpc.CloudWindmillServiceV1Alpha1BlockingStub>
      syncStubList;
  private final ThrottleTimer getWorkThrottleTimer;
  private final ThrottleTimer getDataThrottleTimer;
  private final ThrottleTimer commitWorkThrottleTimer;
  private final Random rand;
  private final Set<AbstractWindmillStream<?, ?>> streamRegistry;

  private long unaryDeadlineSeconds;
  private ImmutableSet<HostAndPort> endpoints;
  private int logEveryNStreamFailures;
  private Duration maxBackoff = MAX_BACKOFF;
  private WindmillApplianceGrpc.WindmillApplianceBlockingStub syncApplianceStub = null;

  private GrpcWindmillServer(StreamingDataflowWorkerOptions options) {
    this.options = options;
    this.streamingRpcBatchLimit = options.getWindmillServiceStreamingRpcBatchLimit();
    this.stubList = new ArrayList<>();
    this.syncStubList = new ArrayList<>();
    this.logEveryNStreamFailures = options.getWindmillServiceStreamingLogEveryNStreamFailures();
    this.endpoints = ImmutableSet.of();
    this.getWorkThrottleTimer = new ThrottleTimer();
    this.getDataThrottleTimer = new ThrottleTimer();
    this.commitWorkThrottleTimer = new ThrottleTimer();
    this.rand = new Random();
    this.streamRegistry = Collections.newSetFromMap(new ConcurrentHashMap<>());
    this.unaryDeadlineSeconds = DEFAULT_UNARY_RPC_DEADLINE_SECONDS;
  }

  private static StreamingDataflowWorkerOptions testOptions() {
    StreamingDataflowWorkerOptions options =
        PipelineOptionsFactory.create().as(StreamingDataflowWorkerOptions.class);
    options.setProject("project");
    options.setJobId("job");
    options.setWorkerId("worker");
    List<String> experiments =
        options.getExperiments() == null ? new ArrayList<>() : options.getExperiments();
    experiments.add(GcpOptions.STREAMING_ENGINE_EXPERIMENT);
    options.setExperiments(experiments);

    options.setWindmillServiceStreamingRpcBatchLimit(Integer.MAX_VALUE);
    options.setWindmillServiceStreamingRpcHealthCheckPeriodMs(NO_HEALTH_CHECK);
    options.setWindmillServiceStreamingLogEveryNStreamFailures(DEFAULT_LOG_EVERY_N_FAILURES);

    return options;
  }

  /** Create new instance of {@link GrpcWindmillServer}. */
  public static GrpcWindmillServer create(StreamingDataflowWorkerOptions workerOptions)
      throws IOException {
    GrpcWindmillServer grpcWindmillServer = new GrpcWindmillServer(workerOptions);
    if (workerOptions.getWindmillServiceEndpoint() != null) {
      grpcWindmillServer.configureWindmillServiceEndpoints();
    } else if (!workerOptions.isEnableStreamingEngine()
        && workerOptions.getLocalWindmillHostport() != null) {
      grpcWindmillServer.configureLocalHost();
    }

    if (workerOptions.getWindmillServiceStreamingRpcHealthCheckPeriodMs() > 0) {
      grpcWindmillServer.scheduleHealthCheckTimer(
          workerOptions, () -> grpcWindmillServer.streamRegistry);
    }

    return grpcWindmillServer;
  }

  @VisibleForTesting
  static GrpcWindmillServer newTestInstance(String name) {
    GrpcWindmillServer testServer = new GrpcWindmillServer(testOptions());
    testServer.stubList.add(CloudWindmillServiceV1Alpha1Grpc.newStub(inProcessChannel(name)));
    return testServer;
  }

  private static Channel inProcessChannel(String name) {
    return InProcessChannelBuilder.forName(name).directExecutor().build();
  }

  private static Channel localhostChannel(int port) {
    return NettyChannelBuilder.forAddress(LOCALHOST, port)
        .maxInboundMessageSize(Integer.MAX_VALUE)
        .negotiationType(NegotiationType.PLAINTEXT)
        .build();
  }

  private void scheduleHealthCheckTimer(
      StreamingDataflowWorkerOptions options, Supplier<Set<AbstractWindmillStream<?, ?>>> streams) {
    new Timer("WindmillHealthCheckTimer")
        .schedule(
            new HealthCheckTimerTask(options, streams),
            0,
            options.getWindmillServiceStreamingRpcHealthCheckPeriodMs());
  }

  private void configureWindmillServiceEndpoints() throws IOException {
    Set<HostAndPort> endpoints = new HashSet<>();
    for (String endpoint : Splitter.on(',').split(options.getWindmillServiceEndpoint())) {
      endpoints.add(
          HostAndPort.fromString(endpoint).withDefaultPort(options.getWindmillServicePort()));
    }
    initializeWindmillService(endpoints);
  }

  private void configureLocalHost() {
    int portStart = options.getLocalWindmillHostport().lastIndexOf(':');
    String endpoint = options.getLocalWindmillHostport().substring(0, portStart);
    assert ("grpc:localhost".equals(endpoint));
    int port = Integer.parseInt(options.getLocalWindmillHostport().substring(portStart + 1));
    this.endpoints = ImmutableSet.of(HostAndPort.fromParts(LOCALHOST, port));
    initializeLocalHost(port);
  }

  @Override
  public synchronized void setWindmillServiceEndpoints(Set<HostAndPort> endpoints)
      throws IOException {
    Preconditions.checkNotNull(endpoints);
    if (endpoints.equals(this.endpoints)) {
      // The endpoints are equal don't recreate the stubs.
      return;
    }
    LOG.info("Creating a new windmill stub, endpoints: {}", endpoints);
    if (this.endpoints != null) {
      LOG.info("Previous windmill stub endpoints: {}", this.endpoints);
    }
    initializeWindmillService(endpoints);
  }

  @Override
  public synchronized boolean isReady() {
    return !stubList.isEmpty();
  }

  private synchronized void initializeLocalHost(int port) {
    this.logEveryNStreamFailures = 1;
    this.maxBackoff = Duration.millis(500);
    this.unaryDeadlineSeconds = 10; // For local testing use short deadlines.
    Channel channel = localhostChannel(port);
    if (options.isEnableStreamingEngine()) {
      this.stubList.add(CloudWindmillServiceV1Alpha1Grpc.newStub(channel));
      this.syncStubList.add(CloudWindmillServiceV1Alpha1Grpc.newBlockingStub(channel));
    } else {
      this.syncApplianceStub = WindmillApplianceGrpc.newBlockingStub(channel);
    }
  }

  private synchronized void initializeWindmillService(Set<HostAndPort> endpoints)
      throws IOException {
    LOG.info("Initializing Streaming Engine GRPC client for endpoints: {}", endpoints);
    this.stubList.clear();
    this.syncStubList.clear();
    this.endpoints = ImmutableSet.copyOf(endpoints);
    for (HostAndPort endpoint : this.endpoints) {
      if (LOCALHOST.equals(endpoint.getHost())) {
        initializeLocalHost(endpoint.getPort());
      } else {
        CallCredentials creds =
            MoreCallCredentials.from(new VendoredCredentialsAdapter(options.getGcpCredential()));
        this.stubList.add(
            CloudWindmillServiceV1Alpha1Grpc.newStub(remoteChannel(endpoint))
                .withCallCredentials(creds));
        this.syncStubList.add(
            CloudWindmillServiceV1Alpha1Grpc.newBlockingStub(remoteChannel(endpoint))
                .withCallCredentials(creds));
      }
    }
  }

  private Channel remoteChannel(HostAndPort endpoint) throws IOException {
    NettyChannelBuilder builder =
        NettyChannelBuilder.forAddress(endpoint.getHost(), endpoint.getPort());
    int timeoutSec = options.getWindmillServiceRpcChannelAliveTimeoutSec();
    if (timeoutSec > 0) {
      builder
          .keepAliveTime(timeoutSec, TimeUnit.SECONDS)
          .keepAliveTimeout(timeoutSec, TimeUnit.SECONDS)
          .keepAliveWithoutCalls(true);
    }
    return builder
        .flowControlWindow(10 * 1024 * 1024)
        .maxInboundMessageSize(Integer.MAX_VALUE)
        .maxInboundMetadataSize(1024 * 1024)
        .negotiationType(NegotiationType.TLS)
        // Set ciphers(null) to not use GCM, which is disabled for Dataflow
        // due to it being horribly slow.
        .sslContext(GrpcSslContexts.forClient().ciphers(null).build())
        .build();
  }

  private synchronized CloudWindmillServiceV1Alpha1Grpc.CloudWindmillServiceV1Alpha1Stub stub() {
    if (stubList.isEmpty()) {
      throw new RuntimeException("windmillServiceEndpoint has not been set");
    }
    if (stubList.size() == 1) {
      return stubList.get(0);
    }
    return stubList.get(rand.nextInt(stubList.size()));
  }

  private synchronized CloudWindmillServiceV1Alpha1Grpc.CloudWindmillServiceV1Alpha1BlockingStub
      syncStub() {
    if (syncStubList.isEmpty()) {
      throw new RuntimeException("windmillServiceEndpoint has not been set");
    }
    if (syncStubList.size() == 1) {
      return syncStubList.get(0);
    }
    return syncStubList.get(rand.nextInt(syncStubList.size()));
  }

  @Override
  public void appendSummaryHtml(PrintWriter writer) {
    writer.write("Active Streams:<br>");
    for (AbstractWindmillStream<?, ?> stream : streamRegistry) {
      stream.appendSummaryHtml(writer);
      writer.write("<br>");
    }
  }

  // Configure backoff to retry calls forever, with a maximum sane retry interval.
  private BackOff grpcBackoff() {
    return FluentBackoff.DEFAULT
        .withInitialBackoff(MIN_BACKOFF)
        .withMaxBackoff(maxBackoff)
        .backoff();
  }

  private <ResponseT> ResponseT callWithBackoff(Supplier<ResponseT> function) {
    BackOff backoff = grpcBackoff();
    int rpcErrors = 0;
    while (true) {
      try {
        return function.get();
      } catch (StatusRuntimeException e) {
        try {
          if (++rpcErrors % 20 == 0) {
            LOG.warn(
                "Many exceptions calling gRPC. Last exception: {} with status {}",
                e,
                e.getStatus());
          }
          if (!BackOffUtils.next(Sleeper.DEFAULT, backoff)) {
            throw new RpcException(e);
          }
        } catch (IOException | InterruptedException i) {
          if (i instanceof InterruptedException) {
            Thread.currentThread().interrupt();
          }
          RpcException rpcException = new RpcException(e);
          rpcException.addSuppressed(i);
          throw rpcException;
        }
      }
    }
  }

  @Override
  public GetWorkResponse getWork(GetWorkRequest request) {
    if (syncApplianceStub == null) {
      return callWithBackoff(
          () ->
              syncStub()
                  .withDeadlineAfter(unaryDeadlineSeconds, TimeUnit.SECONDS)
                  .getWork(
                      request
                          .toBuilder()
                          .setJobId(options.getJobId())
                          .setProjectId(options.getProject())
                          .setWorkerId(options.getWorkerId())
                          .build()));
    } else {
      return callWithBackoff(
          () ->
              syncApplianceStub
                  .withDeadlineAfter(unaryDeadlineSeconds, TimeUnit.SECONDS)
                  .getWork(request));
    }
  }

  @Override
  public GetDataResponse getData(GetDataRequest request) {
    if (syncApplianceStub == null) {
      return callWithBackoff(
          () ->
              syncStub()
                  .withDeadlineAfter(unaryDeadlineSeconds, TimeUnit.SECONDS)
                  .getData(
                      request
                          .toBuilder()
                          .setJobId(options.getJobId())
                          .setProjectId(options.getProject())
                          .build()));
    } else {
      return callWithBackoff(
          () ->
              syncApplianceStub
                  .withDeadlineAfter(unaryDeadlineSeconds, TimeUnit.SECONDS)
                  .getData(request));
    }
  }

  @Override
  public CommitWorkResponse commitWork(CommitWorkRequest request) {
    if (syncApplianceStub == null) {
      return callWithBackoff(
          () ->
              syncStub()
                  .withDeadlineAfter(unaryDeadlineSeconds, TimeUnit.SECONDS)
                  .commitWork(
                      request
                          .toBuilder()
                          .setJobId(options.getJobId())
                          .setProjectId(options.getProject())
                          .build()));
    } else {
      return callWithBackoff(
          () ->
              syncApplianceStub
                  .withDeadlineAfter(unaryDeadlineSeconds, TimeUnit.SECONDS)
                  .commitWork(request));
    }
  }

  private StreamObserverFactory newStreamObserverFactory() {
    return StreamObserverFactory.direct(
        DEFAULT_STREAM_RPC_DEADLINE_SECONDS * 2, options.getWindmillMessagesBetweenIsReadyChecks());
  }

  @Override
  public GetWorkStream getWorkStream(GetWorkRequest request, WorkItemReceiver receiver) {
    GetWorkRequest getWorkRequest =
        GetWorkRequest.newBuilder(request)
            .setJobId(options.getJobId())
            .setProjectId(options.getProject())
            .setWorkerId(options.getWorkerId())
            .build();

    return GrpcGetWorkStream.create(
        stub(),
        getWorkRequest,
        grpcBackoff(),
        newStreamObserverFactory(),
        streamRegistry,
        logEveryNStreamFailures,
        getWorkThrottleTimer,
        receiver);
  }

  @Override
  public GetDataStream getDataStream() {
    return GrpcGetDataStream.create(
        stub(),
        grpcBackoff(),
        newStreamObserverFactory(),
        streamRegistry,
        logEveryNStreamFailures,
        getDataThrottleTimer,
        makeHeader(),
        nextId,
        streamingRpcBatchLimit);
  }

  @Override
  public CommitWorkStream commitWorkStream() {
    return GrpcCommitWorkStream.create(
        stub(),
        grpcBackoff(),
        newStreamObserverFactory(),
        streamRegistry,
        logEveryNStreamFailures,
        commitWorkThrottleTimer,
        makeHeader(),
        nextId,
        streamingRpcBatchLimit);
  }

  @Override
  public GetConfigResponse getConfig(GetConfigRequest request) {
    if (syncApplianceStub == null) {
      throw new RpcException(
          new UnsupportedOperationException("GetConfig not supported with windmill service."));
    } else {
      return callWithBackoff(
          () ->
              syncApplianceStub
                  .withDeadlineAfter(unaryDeadlineSeconds, TimeUnit.SECONDS)
                  .getConfig(request));
    }
  }

  @Override
  public ReportStatsResponse reportStats(ReportStatsRequest request) {
    if (syncApplianceStub == null) {
      throw new RpcException(
          new UnsupportedOperationException("ReportStats not supported with windmill service."));
    } else {
      return callWithBackoff(
          () ->
              syncApplianceStub
                  .withDeadlineAfter(unaryDeadlineSeconds, TimeUnit.SECONDS)
                  .reportStats(request));
    }
  }

  @Override
  public long getAndResetThrottleTime() {
    return getWorkThrottleTimer.getAndResetThrottleTime()
        + getDataThrottleTimer.getAndResetThrottleTime()
        + commitWorkThrottleTimer.getAndResetThrottleTime();
  }

  private JobHeader makeHeader() {
    return JobHeader.newBuilder()
        .setJobId(options.getJobId())
        .setProjectId(options.getProject())
        .setWorkerId(options.getWorkerId())
        .build();
  }

  /**
   * Create a wrapper around credentials callback that delegates to the underlying vendored {@link
   * com.google.auth.RequestMetadataCallback}. Note that this class should override every method
   * that is not final and not static and call the delegate directly.
   *
   * <p>TODO: Replace this with an auto generated proxy which calls the underlying implementation
   * delegate to reduce maintenance burden.
   */
  private static class VendoredRequestMetadataCallbackAdapter
      implements com.google.auth.RequestMetadataCallback {

    private final org.apache.beam.vendor.grpc.v1p54p0.com.google.auth.RequestMetadataCallback
        callback;

    private VendoredRequestMetadataCallbackAdapter(
        org.apache.beam.vendor.grpc.v1p54p0.com.google.auth.RequestMetadataCallback callback) {
      this.callback = callback;
    }

    @Override
    public void onSuccess(Map<String, List<String>> metadata) {
      callback.onSuccess(metadata);
    }

    @Override
    public void onFailure(Throwable exception) {
      callback.onFailure(exception);
    }
  }

  /**
   * Create a wrapper around credentials that delegates to the underlying {@link
   * com.google.auth.Credentials}. Note that this class should override every method that is not
   * final and not static and call the delegate directly.
   *
   * <p>TODO: Replace this with an auto generated proxy which calls the underlying implementation
   * delegate to reduce maintenance burden.
   */
  private static class VendoredCredentialsAdapter
      extends org.apache.beam.vendor.grpc.v1p54p0.com.google.auth.Credentials {

    private final com.google.auth.Credentials credentials;

    private VendoredCredentialsAdapter(com.google.auth.Credentials credentials) {
      this.credentials = credentials;
    }

    @Override
    public String getAuthenticationType() {
      return credentials.getAuthenticationType();
    }

    @Override
    public Map<String, List<String>> getRequestMetadata() throws IOException {
      return credentials.getRequestMetadata();
    }

    @Override
    public void getRequestMetadata(
        final URI uri,
        Executor executor,
        final org.apache.beam.vendor.grpc.v1p54p0.com.google.auth.RequestMetadataCallback
            callback) {
      credentials.getRequestMetadata(
          uri, executor, new VendoredRequestMetadataCallbackAdapter(callback));
    }

    @Override
    public Map<String, List<String>> getRequestMetadata(URI uri) throws IOException {
      return credentials.getRequestMetadata(uri);
    }

    @Override
    public boolean hasRequestMetadata() {
      return credentials.hasRequestMetadata();
    }

    @Override
    public boolean hasRequestMetadataOnly() {
      return credentials.hasRequestMetadataOnly();
    }

    @Override
    public void refresh() throws IOException {
      credentials.refresh();
    }
  }

  private static class HealthCheckTimerTask extends TimerTask {
    private final StreamingDataflowWorkerOptions options;
    private final Supplier<Set<AbstractWindmillStream<?, ?>>> streams;

    public HealthCheckTimerTask(
        StreamingDataflowWorkerOptions options,
        Supplier<Set<AbstractWindmillStream<?, ?>>> streams) {
      this.options = options;
      this.streams = streams;
    }

    @Override
    public void run() {
      Instant reportThreshold =
          Instant.now()
              .minus(Duration.millis(options.getWindmillServiceStreamingRpcHealthCheckPeriodMs()));
      for (AbstractWindmillStream<?, ?> stream : streams.get()) {
        stream.maybeSendHealthCheck(reportThreshold);
      }
    }
  }
}
