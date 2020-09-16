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
package org.apache.beam.runners.dataflow.worker.windmill;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.io.SequenceInputStream;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Random;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import org.apache.beam.runners.dataflow.worker.WindmillTimeUtils;
import org.apache.beam.runners.dataflow.worker.options.StreamingDataflowWorkerOptions;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.CommitStatus;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.CommitWorkRequest;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.CommitWorkResponse;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.ComputationGetDataRequest;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.GetConfigRequest;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.GetConfigResponse;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.GetDataRequest;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.GetDataResponse;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.GetWorkRequest;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.GetWorkResponse;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.GlobalData;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.GlobalDataRequest;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.JobHeader;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.KeyedGetDataRequest;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.KeyedGetDataResponse;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.ReportStatsRequest;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.ReportStatsResponse;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.StreamingCommitRequestChunk;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.StreamingCommitResponse;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.StreamingCommitWorkRequest;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.StreamingGetDataRequest;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.StreamingGetDataResponse;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.StreamingGetWorkRequest;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.StreamingGetWorkRequestExtension;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.StreamingGetWorkResponseChunk;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.WorkItemCommitRequest;
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.util.BackOff;
import org.apache.beam.sdk.util.BackOffUtils;
import org.apache.beam.sdk.util.FluentBackoff;
import org.apache.beam.sdk.util.Sleeper;
import org.apache.beam.vendor.grpc.v1p26p0.com.google.protobuf.ByteString;
import org.apache.beam.vendor.grpc.v1p26p0.io.grpc.CallCredentials;
import org.apache.beam.vendor.grpc.v1p26p0.io.grpc.Channel;
import org.apache.beam.vendor.grpc.v1p26p0.io.grpc.Status;
import org.apache.beam.vendor.grpc.v1p26p0.io.grpc.StatusRuntimeException;
import org.apache.beam.vendor.grpc.v1p26p0.io.grpc.auth.MoreCallCredentials;
import org.apache.beam.vendor.grpc.v1p26p0.io.grpc.inprocess.InProcessChannelBuilder;
import org.apache.beam.vendor.grpc.v1p26p0.io.grpc.netty.GrpcSslContexts;
import org.apache.beam.vendor.grpc.v1p26p0.io.grpc.netty.NegotiationType;
import org.apache.beam.vendor.grpc.v1p26p0.io.grpc.netty.NettyChannelBuilder;
import org.apache.beam.vendor.grpc.v1p26p0.io.grpc.stub.StreamObserver;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Splitter;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Verify;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableSet;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.net.HostAndPort;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** gRPC client for communicating with Windmill Service. */
// Very likely real potential for bugs - https://issues.apache.org/jira/browse/BEAM-6562
// Very likely real potential for bugs - https://issues.apache.org/jira/browse/BEAM-6564
@SuppressFBWarnings({"JLM_JSR166_UTILCONCURRENT_MONITORENTER", "IS2_INCONSISTENT_SYNC"})
public class GrpcWindmillServer extends WindmillServerStub {
  private static final Logger LOG = LoggerFactory.getLogger(GrpcWindmillServer.class);

  // If a connection cannot be established, gRPC will fail fast so this deadline can be relatively
  // high.
  private static final long DEFAULT_UNARY_RPC_DEADLINE_SECONDS = 300;
  private static final long DEFAULT_STREAM_RPC_DEADLINE_SECONDS = 300;

  private static final Duration MIN_BACKOFF = Duration.millis(1);
  private static final Duration MAX_BACKOFF = Duration.standardSeconds(30);
  // Default gRPC streams to 2MB chunks, which has shown to be a large enough chunk size to reduce
  // per-chunk overhead, and small enough that we can still granularly flow-control.
  private static final int COMMIT_STREAM_CHUNK_SIZE = 2 << 20;
  private static final int GET_DATA_STREAM_CHUNK_SIZE = 2 << 20;

  private static final long HEARTBEAT_REQUEST_ID = Long.MAX_VALUE;
  private static final AtomicLong nextId = new AtomicLong(0);

  private final StreamingDataflowWorkerOptions options;
  private final int streamingRpcBatchLimit;
  private final List<CloudWindmillServiceV1Alpha1Grpc.CloudWindmillServiceV1Alpha1Stub> stubList =
      new ArrayList<>();
  private final List<CloudWindmillServiceV1Alpha1Grpc.CloudWindmillServiceV1Alpha1BlockingStub>
      syncStubList = new ArrayList<>();
  private WindmillApplianceGrpc.WindmillApplianceBlockingStub syncApplianceStub = null;
  private long unaryDeadlineSeconds = DEFAULT_UNARY_RPC_DEADLINE_SECONDS;
  private long streamDeadlineSeconds = DEFAULT_STREAM_RPC_DEADLINE_SECONDS;
  private ImmutableSet<HostAndPort> endpoints;
  private int logEveryNStreamFailures = 20;
  private Duration maxBackoff = MAX_BACKOFF;
  private final ThrottleTimer getWorkThrottleTimer = new ThrottleTimer();
  private final ThrottleTimer getDataThrottleTimer = new ThrottleTimer();
  private final ThrottleTimer commitWorkThrottleTimer = new ThrottleTimer();
  private final Random rand = new Random();

  private final Set<AbstractWindmillStream<?, ?>> streamRegistry =
      Collections.newSetFromMap(new ConcurrentHashMap<AbstractWindmillStream<?, ?>, Boolean>());

  private final Timer healthCheckTimer;

  public GrpcWindmillServer(StreamingDataflowWorkerOptions options) throws IOException {
    this.options = options;
    this.streamingRpcBatchLimit = options.getWindmillServiceStreamingRpcBatchLimit();
    this.logEveryNStreamFailures = options.getWindmillServiceStreamingLogEveryNStreamFailures();
    this.endpoints = ImmutableSet.of();
    if (options.getWindmillServiceEndpoint() != null) {
      Set<HostAndPort> endpoints = new HashSet<>();
      for (String endpoint : Splitter.on(',').split(options.getWindmillServiceEndpoint())) {
        endpoints.add(
            HostAndPort.fromString(endpoint).withDefaultPort(options.getWindmillServicePort()));
      }
      initializeWindmillService(endpoints);
    } else if (!streamingEngineEnabled() && options.getLocalWindmillHostport() != null) {
      int portStart = options.getLocalWindmillHostport().lastIndexOf(':');
      String endpoint = options.getLocalWindmillHostport().substring(0, portStart);
      assert ("grpc:localhost".equals(endpoint));
      int port = Integer.parseInt(options.getLocalWindmillHostport().substring(portStart + 1));
      this.endpoints = ImmutableSet.<HostAndPort>of(HostAndPort.fromParts("localhost", port));
      initializeLocalHost(port);
    }
    if (options.getWindmillServiceStreamingRpcHealthCheckPeriodMs() > 0) {
      this.healthCheckTimer = new Timer("WindmillHealthCheckTimer");
      this.healthCheckTimer.schedule(
          new TimerTask() {
            @Override
            public void run() {
              Instant reportThreshold =
                  Instant.now()
                      .minus(
                          Duration.millis(
                              options.getWindmillServiceStreamingRpcHealthCheckPeriodMs()));
              for (AbstractWindmillStream<?, ?> stream : streamRegistry) {
                stream.maybeSendHealthCheck(reportThreshold);
              }
            }
          },
          0,
          options.getWindmillServiceStreamingRpcHealthCheckPeriodMs());
    } else {
      this.healthCheckTimer = null;
    }
  }

  private GrpcWindmillServer(String name, boolean enableStreamingEngine) {
    this.options = PipelineOptionsFactory.create().as(StreamingDataflowWorkerOptions.class);
    this.streamingRpcBatchLimit = Integer.MAX_VALUE;
    options.setProject("project");
    options.setJobId("job");
    options.setWorkerId("worker");
    if (enableStreamingEngine) {
      List<String> experiments = this.options.getExperiments();
      if (experiments == null) {
        experiments = new ArrayList<>();
      }
      experiments.add(GcpOptions.STREAMING_ENGINE_EXPERIMENT);
      options.setExperiments(experiments);
    }
    this.stubList.add(CloudWindmillServiceV1Alpha1Grpc.newStub(inProcessChannel(name)));
    this.healthCheckTimer = null;
  }

  private boolean streamingEngineEnabled() {
    return options.isEnableStreamingEngine();
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

  private synchronized void initializeLocalHost(int port) throws IOException {
    this.logEveryNStreamFailures = 1;
    this.maxBackoff = Duration.millis(500);
    this.unaryDeadlineSeconds = 10; // For local testing use short deadlines.
    Channel channel = localhostChannel(port);
    if (streamingEngineEnabled()) {
      this.stubList.add(CloudWindmillServiceV1Alpha1Grpc.newStub(channel));
      this.syncStubList.add(CloudWindmillServiceV1Alpha1Grpc.newBlockingStub(channel));
    } else {
      this.syncApplianceStub = WindmillApplianceGrpc.newBlockingStub(channel);
    }
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
    private final org.apache.beam.vendor.grpc.v1p26p0.com.google.auth.RequestMetadataCallback
        callback;

    private VendoredRequestMetadataCallbackAdapter(
        org.apache.beam.vendor.grpc.v1p26p0.com.google.auth.RequestMetadataCallback callback) {
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
      extends org.apache.beam.vendor.grpc.v1p26p0.com.google.auth.Credentials {
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
        final org.apache.beam.vendor.grpc.v1p26p0.com.google.auth.RequestMetadataCallback
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

  private synchronized void initializeWindmillService(Set<HostAndPort> endpoints)
      throws IOException {
    LOG.info("Initializing Streaming Engine GRPC client for endpoints: {}", endpoints);
    this.stubList.clear();
    this.syncStubList.clear();
    this.endpoints = ImmutableSet.<HostAndPort>copyOf(endpoints);
    for (HostAndPort endpoint : this.endpoints) {
      if ("localhost".equals(endpoint.getHost())) {
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

  @VisibleForTesting
  static GrpcWindmillServer newTestInstance(String name, boolean enableStreamingEngine) {
    return new GrpcWindmillServer(name, enableStreamingEngine);
  }

  private Channel inProcessChannel(String name) {
    return InProcessChannelBuilder.forName(name).directExecutor().build();
  }

  private Channel localhostChannel(int port) {
    return NettyChannelBuilder.forAddress("localhost", port)
        .maxInboundMessageSize(java.lang.Integer.MAX_VALUE)
        .negotiationType(NegotiationType.PLAINTEXT)
        .build();
  }

  private Channel remoteChannel(HostAndPort endpoint) throws IOException {
    NettyChannelBuilder builder =
        NettyChannelBuilder.forAddress(endpoint.getHost(), endpoint.getPort());
    int timeoutSec = options.getWindmillServiceRpcChannelAliveTimeoutSec();
    if (timeoutSec > 0) {
      builder =
          builder
              .keepAliveTime(timeoutSec, TimeUnit.SECONDS)
              .keepAliveTimeout(timeoutSec, TimeUnit.SECONDS)
              .keepAliveWithoutCalls(true);
    }
    return builder
        .flowControlWindow(10 * 1024 * 1024)
        .maxInboundMessageSize(java.lang.Integer.MAX_VALUE)
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
            throw new WindmillServerStub.RpcException(e);
          }
        } catch (IOException | InterruptedException i) {
          if (i instanceof InterruptedException) {
            Thread.currentThread().interrupt();
          }
          WindmillServerStub.RpcException rpcException = new WindmillServerStub.RpcException(e);
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

  @Override
  public GetWorkStream getWorkStream(GetWorkRequest request, WorkItemReceiver receiver) {
    return new GrpcGetWorkStream(
        GetWorkRequest.newBuilder(request)
            .setJobId(options.getJobId())
            .setProjectId(options.getProject())
            .setWorkerId(options.getWorkerId())
            .build(),
        receiver);
  }

  @Override
  public GetDataStream getDataStream() {
    return new GrpcGetDataStream();
  }

  @Override
  public CommitWorkStream commitWorkStream() {
    return new GrpcCommitWorkStream();
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

  /** Returns a long that is unique to this process. */
  private static long uniqueId() {
    return nextId.incrementAndGet();
  }

  /**
   * Base class for persistent streams connecting to Windmill.
   *
   * <p>This class handles the underlying gRPC StreamObservers, and automatically reconnects the
   * stream if it is broken. Subclasses are responsible for retrying requests that have been lost on
   * a broken stream.
   *
   * <p>Subclasses should override onResponse to handle responses from the server, and onNewStream
   * to perform any work that must be done when a new stream is created, such as sending headers or
   * retrying requests.
   *
   * <p>send and startStream should not be called from onResponse; use executor() instead.
   *
   * <p>Synchronization on this is used to synchronize the gRpc stream state and internal data
   * structures. Since grpc channel operations may block, synchronization on this stream may also
   * block. This is generally not a problem since streams are used in a single-threaded manner.
   * However some accessors used for status page and other debugging need to take care not to
   * require synchronizing on this.
   */
  private abstract class AbstractWindmillStream<RequestT, ResponseT> implements WindmillStream {
    private final StreamObserverFactory streamObserverFactory = StreamObserverFactory.direct();
    private final Function<StreamObserver<ResponseT>, StreamObserver<RequestT>> clientFactory;
    private final Executor executor = Executors.newSingleThreadExecutor();

    // The following should be protected by synchronizing on this, except for
    // the atomics which may be read atomically for status pages.
    private StreamObserver<RequestT> requestObserver;
    private final AtomicLong startTimeMs = new AtomicLong();
    private final AtomicLong lastSendTimeMs = new AtomicLong();
    private final AtomicLong lastResponseTimeMs = new AtomicLong();
    private final AtomicInteger errorCount = new AtomicInteger();
    private final BackOff backoff = grpcBackoff();
    private final AtomicLong sleepUntil = new AtomicLong();
    protected final AtomicBoolean clientClosed = new AtomicBoolean();
    private final CountDownLatch finishLatch = new CountDownLatch(1);

    protected AbstractWindmillStream(
        Function<StreamObserver<ResponseT>, StreamObserver<RequestT>> clientFactory) {
      this.clientFactory = clientFactory;
    }

    /** Called on each response from the server. */
    protected abstract void onResponse(ResponseT response);
    /** Called when a new underlying stream to the server has been opened. */
    protected abstract void onNewStream();
    /** Returns whether there are any pending requests that should be retried on a stream break. */
    protected abstract boolean hasPendingRequests();
    /**
     * Called when the stream is throttled due to resource exhausted errors. Will be called for each
     * resource exhausted error not just the first. onResponse() must stop throttling on receipt of
     * the first good message.
     */
    protected abstract void startThrottleTimer();
    /** Send a request to the server. */
    protected final void send(RequestT request) {
      lastSendTimeMs.set(Instant.now().getMillis());
      synchronized (this) {
        requestObserver.onNext(request);
      }
    }

    /** Starts the underlying stream. */
    protected final void startStream() {
      // Add the stream to the registry after it has been fully constructed.
      streamRegistry.add(this);
      BackOff backoff = grpcBackoff();
      while (true) {
        try {
          synchronized (this) {
            startTimeMs.set(Instant.now().getMillis());
            lastResponseTimeMs.set(0);
            requestObserver = streamObserverFactory.from(clientFactory, new ResponseObserver());
            onNewStream();
            if (clientClosed.get()) {
              close();
            }
            return;
          }
        } catch (Exception e) {
          LOG.error("Failed to create new stream, retrying: ", e);
          try {
            long sleep = backoff.nextBackOffMillis();
            sleepUntil.set(Instant.now().getMillis() + sleep);
            Thread.sleep(sleep);
          } catch (InterruptedException i) {
            // Keep trying to create the stream.
          } catch (IOException i) {
            // Ignore.
          }
        }
      }
    }

    protected final Executor executor() {
      return executor;
    }

    public final synchronized void maybeSendHealthCheck(Instant lastSendThreshold) {
      if (lastSendTimeMs.get() < lastSendThreshold.getMillis() && !clientClosed.get()) {
        try {
          sendHealthCheck();
        } catch (RuntimeException e) {
          LOG.debug("Received exception sending health check {}", e);
        }
      }
    }

    protected abstract void sendHealthCheck();

    protected final long debugDuration(long nowMs, long startMs) {
      if (startMs <= 0) {
        return -1;
      }
      return Math.max(0, nowMs - startMs);
    }

    // Care is taken that synchronization on this is unnecessary for all status page information.
    // Blocking sends are made beneath this stream object's lock which could block status page
    // rendering.
    public final void appendSummaryHtml(PrintWriter writer) {
      appendSpecificHtml(writer);
      if (errorCount.get() > 0) {
        writer.format(", %d errors", errorCount.get());
      }
      if (clientClosed.get()) {
        writer.write(", client closed");
      }
      long nowMs = Instant.now().getMillis();
      long sleepLeft = sleepUntil.get() - nowMs;
      if (sleepLeft > 0) {
        writer.format(", %dms backoff remaining", sleepLeft);
      }
      writer.format(
          ", current stream is %dms old, last send %dms, last response %dms",
          debugDuration(nowMs, startTimeMs.get()),
          debugDuration(nowMs, lastSendTimeMs.get()),
          debugDuration(nowMs, lastResponseTimeMs.get()));
    }

    // Don't require synchronization on stream, see the appendSummaryHtml comment.
    protected abstract void appendSpecificHtml(PrintWriter writer);

    private class ResponseObserver implements StreamObserver<ResponseT> {
      @Override
      public void onNext(ResponseT response) {
        try {
          backoff.reset();
        } catch (IOException e) {
          // Ignore.
        }
        lastResponseTimeMs.set(Instant.now().getMillis());
        onResponse(response);
      }

      @Override
      public void onError(Throwable t) {
        onStreamFinished(t);
      }

      @Override
      public void onCompleted() {
        onStreamFinished(null);
      }

      private void onStreamFinished(@Nullable Throwable t) {
        synchronized (this) {
          if (clientClosed.get() && !hasPendingRequests()) {
            streamRegistry.remove(AbstractWindmillStream.this);
            finishLatch.countDown();
            return;
          }
        }
        if (t != null) {
          Status status = null;
          if (t instanceof StatusRuntimeException) {
            status = ((StatusRuntimeException) t).getStatus();
          }
          if (errorCount.getAndIncrement() % logEveryNStreamFailures == 0) {
            long nowMillis = Instant.now().getMillis();
            String responseDebug;
            if (lastResponseTimeMs.get() == 0) {
              responseDebug = "never received response";
            } else {
              responseDebug =
                  "received response " + (nowMillis - lastResponseTimeMs.get()) + "ms ago";
            }
            LOG.debug(
                "{} streaming Windmill RPC errors for {}, last was: {} with status {}."
                    + " created {}ms ago, {}. This is normal with autoscaling.",
                AbstractWindmillStream.this.getClass(),
                errorCount.get(),
                t.toString(),
                status,
                nowMillis - startTimeMs.get(),
                responseDebug);
          }
          // If the stream was stopped due to a resource exhausted error then we are throttled.
          if (status != null && status.getCode() == Status.Code.RESOURCE_EXHAUSTED) {
            startThrottleTimer();
          }

          try {
            long sleep = backoff.nextBackOffMillis();
            sleepUntil.set(Instant.now().getMillis() + sleep);
            Thread.sleep(sleep);
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
          } catch (IOException e) {
            // Ignore.
          }
        } else {
          errorCount.incrementAndGet();
          LOG.warn(
              "Stream completed successfully but did not complete requested operations, "
                  + "recreating");
        }
        executor.execute(AbstractWindmillStream.this::startStream);
      }
    }

    @Override
    public final synchronized void close() {
      // Synchronization of close and onCompleted necessary for correct retry logic in onNewStream.
      clientClosed.set(true);
      requestObserver.onCompleted();
    }

    @Override
    public final boolean awaitTermination(int time, TimeUnit unit) throws InterruptedException {
      return finishLatch.await(time, unit);
    }

    @Override
    public final Instant startTime() {
      return new Instant(startTimeMs.get());
    }
  }

  private class GrpcGetWorkStream
      extends AbstractWindmillStream<StreamingGetWorkRequest, StreamingGetWorkResponseChunk>
      implements GetWorkStream {
    private final GetWorkRequest request;
    private final WorkItemReceiver receiver;
    private final Map<Long, WorkItemBuffer> buffers = new ConcurrentHashMap<>();
    private final AtomicLong inflightMessages = new AtomicLong();
    private final AtomicLong inflightBytes = new AtomicLong();

    private GrpcGetWorkStream(GetWorkRequest request, WorkItemReceiver receiver) {
      super(
          responseObserver ->
              stub()
                  .withDeadlineAfter(streamDeadlineSeconds, TimeUnit.SECONDS)
                  .getWorkStream(responseObserver));
      this.request = request;
      this.receiver = receiver;
      startStream();
    }

    @Override
    protected synchronized void onNewStream() {
      buffers.clear();
      inflightMessages.set(request.getMaxItems());
      inflightBytes.set(request.getMaxBytes());
      send(StreamingGetWorkRequest.newBuilder().setRequest(request).build());
    }

    @Override
    protected boolean hasPendingRequests() {
      return false;
    }

    @Override
    public void appendSpecificHtml(PrintWriter writer) {
      // Number of buffers is same as distict workers that sent work on this stream.
      writer.format(
          "GetWorkStream: %d buffers, %d inflight messages allowed, %d inflight bytes allowed",
          buffers.size(), inflightMessages.intValue(), inflightBytes.intValue());
    }

    @Override
    public void sendHealthCheck() {
      send(
          StreamingGetWorkRequest.newBuilder()
              .setRequestExtension(
                  StreamingGetWorkRequestExtension.newBuilder()
                      .setMaxItems(0)
                      .setMaxBytes(0)
                      .build())
              .build());
    }

    @Override
    protected void onResponse(StreamingGetWorkResponseChunk chunk) {
      getWorkThrottleTimer.stop();
      long id = chunk.getStreamId();

      WorkItemBuffer buffer = buffers.computeIfAbsent(id, (Long l) -> new WorkItemBuffer());
      buffer.append(chunk);

      if (chunk.getRemainingBytesForWorkItem() == 0) {
        long size = buffer.bufferedSize();
        buffer.runAndReset();

        // Record the fact that there are now fewer outstanding messages and bytes on the stream.
        long numInflight = inflightMessages.decrementAndGet();
        long bytesInflight = inflightBytes.addAndGet(-size);

        // If the outstanding items or bytes limit has gotten too low, top both off with a
        // GetWorkExtension.  The goal is to keep the limits relatively close to their maximum
        // values without sending too many extension requests.
        if (numInflight < request.getMaxItems() / 2 || bytesInflight < request.getMaxBytes() / 2) {
          long moreItems = request.getMaxItems() - numInflight;
          long moreBytes = request.getMaxBytes() - bytesInflight;
          inflightMessages.getAndAdd(moreItems);
          inflightBytes.getAndAdd(moreBytes);
          final StreamingGetWorkRequest extension =
              StreamingGetWorkRequest.newBuilder()
                  .setRequestExtension(
                      StreamingGetWorkRequestExtension.newBuilder()
                          .setMaxItems(moreItems)
                          .setMaxBytes(moreBytes))
                  .build();
          executor()
              .execute(
                  () -> {
                    try {
                      send(extension);
                    } catch (IllegalStateException e) {
                      // Stream was closed.
                    }
                  });
        }
      }
    }

    @Override
    protected void startThrottleTimer() {
      getWorkThrottleTimer.start();
    }

    private class WorkItemBuffer {
      private String computation;
      private Instant inputDataWatermark;
      private Instant synchronizedProcessingTime;
      private ByteString data = ByteString.EMPTY;
      private long bufferedSize = 0;

      private void setMetadata(Windmill.ComputationWorkItemMetadata metadata) {
        this.computation = metadata.getComputationId();
        this.inputDataWatermark =
            WindmillTimeUtils.windmillToHarnessWatermark(metadata.getInputDataWatermark());
        this.synchronizedProcessingTime =
            WindmillTimeUtils.windmillToHarnessWatermark(
                metadata.getDependentRealtimeInputWatermark());
      }

      public void append(StreamingGetWorkResponseChunk chunk) {
        if (chunk.hasComputationMetadata()) {
          setMetadata(chunk.getComputationMetadata());
        }

        this.data = data.concat(chunk.getSerializedWorkItem());
        this.bufferedSize += chunk.getSerializedWorkItem().size();
      }

      public long bufferedSize() {
        return bufferedSize;
      }

      public void runAndReset() {
        try {
          receiver.receiveWork(
              computation,
              inputDataWatermark,
              synchronizedProcessingTime,
              Windmill.WorkItem.parseFrom(data.newInput()));
        } catch (IOException e) {
          LOG.error("Failed to parse work item from stream: ", e);
        }
        data = ByteString.EMPTY;
        bufferedSize = 0;
      }
    }
  }

  private class GrpcGetDataStream
      extends AbstractWindmillStream<StreamingGetDataRequest, StreamingGetDataResponse>
      implements GetDataStream {
    private class QueuedRequest {
      public QueuedRequest(String computation, KeyedGetDataRequest request) {
        this.id = uniqueId();
        this.globalDataRequest = null;
        this.dataRequest =
            ComputationGetDataRequest.newBuilder()
                .setComputationId(computation)
                .addRequests(request)
                .build();
        this.byteSize = this.dataRequest.getSerializedSize();
      }

      public QueuedRequest(GlobalDataRequest request) {
        this.id = uniqueId();
        this.globalDataRequest = request;
        this.dataRequest = null;
        this.byteSize = this.globalDataRequest.getSerializedSize();
      }

      final long id;
      final long byteSize;
      final GlobalDataRequest globalDataRequest;
      final ComputationGetDataRequest dataRequest;
      AppendableInputStream responseStream = null;
    }

    private class QueuedBatch {
      public QueuedBatch() {}

      final List<QueuedRequest> requests = new ArrayList<>();
      long byteSize = 0;
      boolean finalized = false;
      final CountDownLatch sent = new CountDownLatch(1);
    };

    private final Deque<QueuedBatch> batches = new ConcurrentLinkedDeque<>();
    private final Map<Long, AppendableInputStream> pending = new ConcurrentHashMap<>();

    @Override
    public void appendSpecificHtml(PrintWriter writer) {
      writer.format(
          "GetDataStream: %d queued batches, %d pending requests [",
          batches.size(), pending.size());
      for (Map.Entry<Long, AppendableInputStream> entry : pending.entrySet()) {
        writer.format("Stream %d ", entry.getKey());
        if (entry.getValue().cancelled.get()) {
          writer.append("cancelled ");
        }
        if (entry.getValue().complete.get()) {
          writer.append("complete ");
        }
        int queueSize = entry.getValue().queue.size();
        if (queueSize > 0) {
          writer.format("%d queued responses ", queueSize);
        }
        long blockedMs = entry.getValue().blockedStartMs.get();
        if (blockedMs > 0) {
          writer.format("blocked for %dms", Instant.now().getMillis() - blockedMs);
        }
      }
      writer.append("]");
    }

    GrpcGetDataStream() {
      super(
          responseObserver ->
              stub()
                  .withDeadlineAfter(streamDeadlineSeconds, TimeUnit.SECONDS)
                  .getDataStream(responseObserver));
      startStream();
    }

    @Override
    protected synchronized void onNewStream() {
      send(StreamingGetDataRequest.newBuilder().setHeader(makeHeader()).build());

      if (clientClosed.get()) {
        // We rely on close only occurring after all methods on the stream have returned.
        // Since the requestKeyedData and requestGlobalData methods are blocking this
        // means there should be no pending requests.
        Verify.verify(!hasPendingRequests());
      } else {
        for (AppendableInputStream responseStream : pending.values()) {
          responseStream.cancel();
        }
      }
    }

    @Override
    protected boolean hasPendingRequests() {
      return !pending.isEmpty() || !batches.isEmpty();
    }

    @Override
    protected void onResponse(StreamingGetDataResponse chunk) {
      Preconditions.checkArgument(chunk.getRequestIdCount() == chunk.getSerializedResponseCount());
      Preconditions.checkArgument(
          chunk.getRemainingBytesForResponse() == 0 || chunk.getRequestIdCount() == 1);
      getDataThrottleTimer.stop();

      for (int i = 0; i < chunk.getRequestIdCount(); ++i) {
        AppendableInputStream responseStream = pending.get(chunk.getRequestId(i));
        Verify.verify(responseStream != null, "No pending response stream");
        responseStream.append(chunk.getSerializedResponse(i).newInput());
        if (chunk.getRemainingBytesForResponse() == 0) {
          responseStream.complete();
        }
      }
    }

    @Override
    protected void startThrottleTimer() {
      getDataThrottleTimer.start();
    }

    @Override
    public KeyedGetDataResponse requestKeyedData(String computation, KeyedGetDataRequest request) {
      return issueRequest(new QueuedRequest(computation, request), KeyedGetDataResponse::parseFrom);
    }

    @Override
    public GlobalData requestGlobalData(GlobalDataRequest request) {
      return issueRequest(new QueuedRequest(request), GlobalData::parseFrom);
    }

    @Override
    public void refreshActiveWork(Map<String, List<KeyedGetDataRequest>> active) {
      long builderBytes = 0;
      StreamingGetDataRequest.Builder builder = StreamingGetDataRequest.newBuilder();
      for (Map.Entry<String, List<KeyedGetDataRequest>> entry : active.entrySet()) {
        for (KeyedGetDataRequest request : entry.getValue()) {
          // Calculate the bytes with some overhead for proto encoding.
          long bytes = (long) entry.getKey().length() + request.getSerializedSize() + 10;
          if (builderBytes > 0
              && (builderBytes + bytes > GET_DATA_STREAM_CHUNK_SIZE
                  || builder.getRequestIdCount() >= streamingRpcBatchLimit)) {
            send(builder.build());
            builderBytes = 0;
            builder.clear();
          }
          builderBytes += bytes;
          builder.addStateRequest(
              ComputationGetDataRequest.newBuilder()
                  .setComputationId(entry.getKey())
                  .addRequests(request));
        }
      }
      if (builderBytes > 0) {
        send(builder.build());
      }
    }

    @Override
    public void sendHealthCheck() {
      if (hasPendingRequests()) {
        send(StreamingGetDataRequest.newBuilder().build());
      }
    }

    private <ResponseT> ResponseT issueRequest(QueuedRequest request, ParseFn<ResponseT> parseFn) {
      while (true) {
        request.responseStream = new AppendableInputStream();
        try {
          queueRequestAndWait(request);
          return parseFn.parse(request.responseStream);
        } catch (CancellationException e) {
          // Retry issuing the request since the response stream was cancelled.
          continue;
        } catch (IOException e) {
          LOG.error("Parsing GetData response failed: ", e);
          continue;
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          throw new RuntimeException(e);
        } finally {
          pending.remove(request.id);
        }
      }
    }

    private void queueRequestAndWait(QueuedRequest request) throws InterruptedException {
      QueuedBatch batch;
      boolean responsibleForSend = false;
      CountDownLatch waitForSendLatch = null;
      synchronized (batches) {
        batch = batches.isEmpty() ? null : batches.getLast();
        if (batch == null
            || batch.finalized
            || batch.requests.size() >= streamingRpcBatchLimit
            || batch.byteSize + request.byteSize > GET_DATA_STREAM_CHUNK_SIZE) {
          if (batch != null) {
            waitForSendLatch = batch.sent;
          }
          batch = new QueuedBatch();
          batches.addLast(batch);
          responsibleForSend = true;
        }
        batch.requests.add(request);
        batch.byteSize += request.byteSize;
      }
      if (responsibleForSend) {
        if (waitForSendLatch == null) {
          // If there was not a previous batch wait a little while to improve
          // batching.
          Thread.sleep(1);
        } else {
          waitForSendLatch.await();
        }
        // Finalize the batch so that no additional requests will be added.  Leave the batch in the
        // queue so that a subsequent batch will wait for it's completion.
        synchronized (batches) {
          Verify.verify(batch == batches.peekFirst());
          batch.finalized = true;
        }
        sendBatch(batch.requests);
        synchronized (batches) {
          Verify.verify(batch == batches.pollFirst());
        }
        // Notify all waiters with requests in this batch as well as the sender
        // of the next batch (if one exists).
        batch.sent.countDown();
      } else {
        // Wait for this batch to be sent before parsing the response.
        batch.sent.await();
      }
    }

    private void sendBatch(List<QueuedRequest> requests) {
      StreamingGetDataRequest batchedRequest = flushToBatch(requests);
      synchronized (this) {
        // Synchronization of pending inserts is necessary with send to ensure duplicates are not
        // sent on stream reconnect.
        for (QueuedRequest request : requests) {
          Verify.verify(pending.put(request.id, request.responseStream) == null);
        }
        try {
          send(batchedRequest);
        } catch (IllegalStateException e) {
          // The stream broke before this call went through; onNewStream will retry the fetch.
          LOG.warn("GetData stream broke before call started {}", e);
        }
      }
    }

    private StreamingGetDataRequest flushToBatch(List<QueuedRequest> requests) {
      // Put all global data requests first because there is only a single repeated field for
      // request ids and the initial ids correspond to global data requests if they are present.
      requests.sort(
          (QueuedRequest r1, QueuedRequest r2) -> {
            boolean r1gd = r1.globalDataRequest != null;
            boolean r2gd = r2.globalDataRequest != null;
            return r1gd == r2gd ? 0 : (r1gd ? -1 : 1);
          });
      StreamingGetDataRequest.Builder builder = StreamingGetDataRequest.newBuilder();
      for (QueuedRequest request : requests) {
        builder.addRequestId(request.id);
        if (request.globalDataRequest == null) {
          builder.addStateRequest(request.dataRequest);
        } else {
          builder.addGlobalDataRequest(request.globalDataRequest);
        }
      }
      return builder.build();
    }
  }

  private class GrpcCommitWorkStream
      extends AbstractWindmillStream<StreamingCommitWorkRequest, StreamingCommitResponse>
      implements CommitWorkStream {
    private class PendingRequest {
      private final String computation;
      private final WorkItemCommitRequest request;
      private final Consumer<CommitStatus> onDone;

      PendingRequest(
          String computation, WorkItemCommitRequest request, Consumer<CommitStatus> onDone) {
        this.computation = computation;
        this.request = request;
        this.onDone = onDone;
      }

      long getBytes() {
        return (long) request.getSerializedSize() + computation.length();
      }
    }

    private final Map<Long, PendingRequest> pending = new ConcurrentHashMap<>();

    private class Batcher {
      long queuedBytes = 0;
      final Map<Long, PendingRequest> queue = new HashMap<>();

      boolean canAccept(PendingRequest request) {
        return queue.isEmpty()
            || (queue.size() < streamingRpcBatchLimit
                && (request.getBytes() + queuedBytes) < COMMIT_STREAM_CHUNK_SIZE);
      }

      void add(long id, PendingRequest request) {
        assert (canAccept(request));
        queuedBytes += request.getBytes();
        queue.put(id, request);
      }

      void flush() {
        flushInternal(queue);
        queuedBytes = 0;
        queue.clear();
      }
    }

    private final Batcher batcher = new Batcher();

    GrpcCommitWorkStream() {
      super(
          responseObserver ->
              stub()
                  .withDeadlineAfter(streamDeadlineSeconds, TimeUnit.SECONDS)
                  .commitWorkStream(responseObserver));
      startStream();
    }

    @Override
    public void appendSpecificHtml(PrintWriter writer) {
      writer.format("CommitWorkStream: %d pending", pending.size());
    }

    @Override
    protected synchronized void onNewStream() {
      send(StreamingCommitWorkRequest.newBuilder().setHeader(makeHeader()).build());
      Batcher resendBatcher = new Batcher();
      for (Map.Entry<Long, PendingRequest> entry : pending.entrySet()) {
        if (!resendBatcher.canAccept(entry.getValue())) {
          resendBatcher.flush();
        }
        resendBatcher.add(entry.getKey(), entry.getValue());
      }
      resendBatcher.flush();
    }

    @Override
    protected boolean hasPendingRequests() {
      return !pending.isEmpty();
    }

    @Override
    public void sendHealthCheck() {
      if (hasPendingRequests()) {
        StreamingCommitWorkRequest.Builder builder = StreamingCommitWorkRequest.newBuilder();
        builder.addCommitChunkBuilder().setRequestId(HEARTBEAT_REQUEST_ID);
        send(builder.build());
      }
    }

    @Override
    protected void onResponse(StreamingCommitResponse response) {
      commitWorkThrottleTimer.stop();

      RuntimeException finalException = null;
      for (int i = 0; i < response.getRequestIdCount(); ++i) {
        long requestId = response.getRequestId(i);
        if (requestId == HEARTBEAT_REQUEST_ID) {
          continue;
        }
        PendingRequest done = pending.remove(requestId);
        if (done == null) {
          LOG.error("Got unknown commit request ID: {}", requestId);
        } else {
          try {
            done.onDone.accept(
                (i < response.getStatusCount()) ? response.getStatus(i) : CommitStatus.OK);
          } catch (RuntimeException e) {
            // Catch possible exceptions to ensure that an exception for one commit does not prevent
            // other commits from being processed.
            LOG.warn("Exception while processing commit response {} ", e);
            finalException = e;
          }
        }
      }
      if (finalException != null) {
        throw finalException;
      }
    }

    @Override
    protected void startThrottleTimer() {
      commitWorkThrottleTimer.start();
    }

    @Override
    public boolean commitWorkItem(
        String computation, WorkItemCommitRequest commitRequest, Consumer<CommitStatus> onDone) {
      PendingRequest request = new PendingRequest(computation, commitRequest, onDone);
      if (!batcher.canAccept(request)) {
        return false;
      }
      batcher.add(uniqueId(), request);
      return true;
    }

    @Override
    public void flush() {
      batcher.flush();
    }

    private void flushInternal(Map<Long, PendingRequest> requests) {
      if (requests.isEmpty()) {
        return;
      }
      if (requests.size() == 1) {
        Map.Entry<Long, PendingRequest> elem = requests.entrySet().iterator().next();
        if (elem.getValue().request.getSerializedSize() > COMMIT_STREAM_CHUNK_SIZE) {
          issueMultiChunkRequest(elem.getKey(), elem.getValue());
        } else {
          issueSingleRequest(elem.getKey(), elem.getValue());
        }
      } else {
        issueBatchedRequest(requests);
      }
    }

    private void issueSingleRequest(final long id, PendingRequest pendingRequest) {
      StreamingCommitWorkRequest.Builder requestBuilder = StreamingCommitWorkRequest.newBuilder();
      requestBuilder
          .addCommitChunkBuilder()
          .setComputationId(pendingRequest.computation)
          .setRequestId(id)
          .setShardingKey(pendingRequest.request.getShardingKey())
          .setSerializedWorkItemCommit(pendingRequest.request.toByteString());
      StreamingCommitWorkRequest chunk = requestBuilder.build();
      synchronized (this) {
        pending.put(id, pendingRequest);
        try {
          send(chunk);
        } catch (IllegalStateException e) {
          // Stream was broken, request will be retried when stream is reopened.
        }
      }
    }

    private void issueBatchedRequest(Map<Long, PendingRequest> requests) {
      StreamingCommitWorkRequest.Builder requestBuilder = StreamingCommitWorkRequest.newBuilder();
      String lastComputation = null;
      for (Map.Entry<Long, PendingRequest> entry : requests.entrySet()) {
        PendingRequest request = entry.getValue();
        StreamingCommitRequestChunk.Builder chunkBuilder = requestBuilder.addCommitChunkBuilder();
        if (lastComputation == null || !lastComputation.equals(request.computation)) {
          chunkBuilder.setComputationId(request.computation);
          lastComputation = request.computation;
        }
        chunkBuilder.setRequestId(entry.getKey());
        chunkBuilder.setShardingKey(request.request.getShardingKey());
        chunkBuilder.setSerializedWorkItemCommit(request.request.toByteString());
      }
      StreamingCommitWorkRequest request = requestBuilder.build();
      synchronized (this) {
        pending.putAll(requests);
        try {
          send(request);
        } catch (IllegalStateException e) {
          // Stream was broken, request will be retried when stream is reopened.
        }
      }
    }

    private void issueMultiChunkRequest(final long id, PendingRequest pendingRequest) {
      Preconditions.checkNotNull(pendingRequest.computation);
      final ByteString serializedCommit = pendingRequest.request.toByteString();

      synchronized (this) {
        pending.put(id, pendingRequest);
        for (int i = 0; i < serializedCommit.size(); i += COMMIT_STREAM_CHUNK_SIZE) {
          int end = i + COMMIT_STREAM_CHUNK_SIZE;
          ByteString chunk = serializedCommit.substring(i, Math.min(end, serializedCommit.size()));

          StreamingCommitRequestChunk.Builder chunkBuilder =
              StreamingCommitRequestChunk.newBuilder()
                  .setRequestId(id)
                  .setSerializedWorkItemCommit(chunk)
                  .setComputationId(pendingRequest.computation)
                  .setShardingKey(pendingRequest.request.getShardingKey());
          int remaining = serializedCommit.size() - end;
          if (remaining > 0) {
            chunkBuilder.setRemainingBytesForWorkItem(remaining);
          }

          StreamingCommitWorkRequest requestChunk =
              StreamingCommitWorkRequest.newBuilder().addCommitChunk(chunkBuilder).build();
          try {
            send(requestChunk);
          } catch (IllegalStateException e) {
            // Stream was broken, request will be retried when stream is reopened.
            break;
          }
        }
      }
    }
  }

  @FunctionalInterface
  private interface ParseFn<ResponseT> {
    ResponseT parse(InputStream input) throws IOException;
  }

  /** An InputStream that can be dynamically extended with additional InputStreams. */
  @SuppressWarnings("JdkObsolete")
  private static class AppendableInputStream extends InputStream {
    private static final InputStream POISON_PILL = ByteString.EMPTY.newInput();
    private final AtomicBoolean cancelled = new AtomicBoolean(false);
    private final AtomicBoolean complete = new AtomicBoolean(false);
    private final AtomicLong blockedStartMs = new AtomicLong();
    private final BlockingDeque<InputStream> queue = new LinkedBlockingDeque<>(10);
    private final InputStream stream =
        new SequenceInputStream(
            new Enumeration<InputStream>() {
              // The first stream is eagerly read on SequenceInputStream creation. For this reason
              // we use an empty element as the first input to avoid blocking from the queue when
              // creating the AppendableInputStream.
              InputStream current = ByteString.EMPTY.newInput();

              @Override
              public boolean hasMoreElements() {
                if (current != null) {
                  return true;
                }

                try {
                  blockedStartMs.set(Instant.now().getMillis());
                  current = queue.take();
                  if (current != POISON_PILL) {
                    return true;
                  }
                  if (cancelled.get()) {
                    throw new CancellationException();
                  }
                  if (complete.get()) {
                    return false;
                  }
                  throw new IllegalStateException("Got poison pill but stream is not done.");
                } catch (InterruptedException e) {
                  Thread.currentThread().interrupt();
                  throw new CancellationException();
                }
              }

              @Override
              public InputStream nextElement() {
                if (!hasMoreElements()) {
                  throw new NoSuchElementException();
                }
                blockedStartMs.set(0);
                InputStream next = current;
                current = null;
                return next;
              }
            });

    /** Appends a new InputStream to the tail of this stream. */
    public synchronized void append(InputStream chunk) {
      try {
        queue.put(chunk);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        LOG.debug("interrupted append");
      }
    }

    /** Cancels the stream. Future calls to InputStream methods will throw CancellationException. */
    public synchronized void cancel() {
      cancelled.set(true);
      try {
        // Put the poison pill at the head of the queue to cancel as quickly as possible.
        queue.clear();
        queue.putFirst(POISON_PILL);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        LOG.debug("interrupted cancel");
      }
    }

    /** Signals that no new InputStreams will be added to this stream. */
    public synchronized void complete() {
      complete.set(true);
      try {
        queue.put(POISON_PILL);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        LOG.debug("interrupted complete");
      }
    }

    @Override
    public int read() throws IOException {
      if (cancelled.get()) {
        throw new CancellationException();
      }
      return stream.read();
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
      if (cancelled.get()) {
        throw new CancellationException();
      }
      return stream.read(b, off, len);
    }

    @Override
    public int available() throws IOException {
      if (cancelled.get()) {
        throw new CancellationException();
      }
      return stream.available();
    }

    @Override
    public void close() throws IOException {
      stream.close();
    }
  }

  /**
   * A stopwatch used to track the amount of time spent throttled due to Resource Exhausted errors.
   * Throttle time is cumulative for all three rpcs types but not for all streams. So if GetWork and
   * CommitWork are both blocked for x, totalTime will be 2x. However, if 2 GetWork streams are both
   * blocked for x totalTime will be x. All methods are thread safe.
   */
  private static class ThrottleTimer {

    // This is -1 if not currently being throttled or the time in
    // milliseconds when throttling for this type started.
    private long startTime = -1;
    // This is the collected total throttle times since the last poll.  Throttle times are
    // reported as a delta so this is cleared whenever it gets reported.
    private long totalTime = 0;

    /**
     * Starts the timer if it has not been started and does nothing if it has already been started.
     */
    public synchronized void start() {
      if (!throttled()) { // This timer is not started yet so start it now.
        startTime = Instant.now().getMillis();
      }
    }

    /** Stops the timer if it has been started and does nothing if it has not been started. */
    public synchronized void stop() {
      if (throttled()) { // This timer has been started already so stop it now.
        totalTime += Instant.now().getMillis() - startTime;
        startTime = -1;
      }
    }

    /** Returns if the specified type is currently being throttled. */
    public synchronized boolean throttled() {
      return startTime != -1;
    }

    /** Returns the combined total of all throttle times and resets those times to 0. */
    public synchronized long getAndResetThrottleTime() {
      if (throttled()) {
        stop();
        start();
      }
      long toReturn = totalTime;
      totalTime = 0;
      return toReturn;
    }
  }
}
