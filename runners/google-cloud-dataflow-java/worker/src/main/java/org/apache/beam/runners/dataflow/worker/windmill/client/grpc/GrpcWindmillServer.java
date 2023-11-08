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
package org.apache.beam.runners.dataflow.worker.windmill.client.grpc;

import static org.apache.beam.runners.dataflow.worker.windmill.client.grpc.stubs.WindmillChannelFactory.LOCALHOST;
import static org.apache.beam.runners.dataflow.worker.windmill.client.grpc.stubs.WindmillChannelFactory.inProcessChannel;
import static org.apache.beam.runners.dataflow.worker.windmill.client.grpc.stubs.WindmillChannelFactory.localhostChannel;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Supplier;
import javax.annotation.Nullable;
import org.apache.beam.runners.dataflow.worker.options.StreamingDataflowWorkerOptions;
import org.apache.beam.runners.dataflow.worker.windmill.CloudWindmillServiceV1Alpha1Grpc;
import org.apache.beam.runners.dataflow.worker.windmill.CloudWindmillServiceV1Alpha1Grpc.CloudWindmillServiceV1Alpha1Stub;
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
import org.apache.beam.runners.dataflow.worker.windmill.client.WindmillStream.CommitWorkStream;
import org.apache.beam.runners.dataflow.worker.windmill.client.WindmillStream.GetDataStream;
import org.apache.beam.runners.dataflow.worker.windmill.client.WindmillStream.GetWorkStream;
import org.apache.beam.runners.dataflow.worker.windmill.client.grpc.stubs.WindmillStubFactory;
import org.apache.beam.runners.dataflow.worker.windmill.client.throttling.StreamingEngineThrottleTimers;
import org.apache.beam.runners.dataflow.worker.windmill.work.WorkItemReceiver;
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.util.BackOff;
import org.apache.beam.sdk.util.BackOffUtils;
import org.apache.beam.sdk.util.FluentBackoff;
import org.apache.beam.sdk.util.Sleeper;
import org.apache.beam.vendor.grpc.v1p54p0.io.grpc.Channel;
import org.apache.beam.vendor.grpc.v1p54p0.io.grpc.ManagedChannel;
import org.apache.beam.vendor.grpc.v1p54p0.io.grpc.StatusRuntimeException;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Splitter;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableSet;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Lists;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Sets;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.net.HostAndPort;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** gRPC client for communicating with Streaming Engine. */
@SuppressFBWarnings({
  // Very likely real potentials for bugs.
  "JLM_JSR166_UTILCONCURRENT_MONITORENTER", // https://github.com/apache/beam/issues/19273
  "IS2_INCONSISTENT_SYNC" // https://github.com/apache/beam/issues/19271
})
@SuppressWarnings("nullness") // TODO(https://github.com/apache/beam/issues/20497
public final class GrpcWindmillServer extends WindmillServerStub {
  private static final Logger LOG = LoggerFactory.getLogger(GrpcWindmillServer.class);
  private static final int DEFAULT_LOG_EVERY_N_FAILURES = 20;
  private static final Duration MIN_BACKOFF = Duration.millis(1);
  private static final Duration MAX_BACKOFF = Duration.standardSeconds(30);
  private static final int NO_HEALTH_CHECK = -1;
  private static final String GRPC_LOCALHOST = "grpc:localhost";

  private final GrpcWindmillStreamFactory windmillStreamFactory;
  private final GrpcDispatcherClient dispatcherClient;
  private final StreamingDataflowWorkerOptions options;
  private final StreamingEngineThrottleTimers throttleTimers;
  private Duration maxBackoff;
  private @Nullable WindmillApplianceGrpc.WindmillApplianceBlockingStub syncApplianceStub;

  private GrpcWindmillServer(
      StreamingDataflowWorkerOptions options, GrpcDispatcherClient grpcDispatcherClient) {
    this.options = options;
    this.throttleTimers = StreamingEngineThrottleTimers.create();
    this.maxBackoff = MAX_BACKOFF;
    this.windmillStreamFactory =
        GrpcWindmillStreamFactory.of(
                JobHeader.newBuilder()
                    .setJobId(options.getJobId())
                    .setProjectId(options.getProject())
                    .setWorkerId(options.getWorkerId())
                    .build())
            .setWindmillMessagesBetweenIsReadyChecks(
                options.getWindmillMessagesBetweenIsReadyChecks())
            .setMaxBackOffSupplier(() -> maxBackoff)
            .setLogEveryNStreamFailures(
                options.getWindmillServiceStreamingLogEveryNStreamFailures())
            .setStreamingRpcBatchLimit(options.getWindmillServiceStreamingRpcBatchLimit())
            .build();
    windmillStreamFactory.scheduleHealthChecks(
        options.getWindmillServiceStreamingRpcHealthCheckPeriodMs());

    this.dispatcherClient = grpcDispatcherClient;
    this.syncApplianceStub = null;
  }

  private static StreamingDataflowWorkerOptions testOptions(boolean enableStreamingEngine) {
    StreamingDataflowWorkerOptions options =
        PipelineOptionsFactory.create().as(StreamingDataflowWorkerOptions.class);
    options.setProject("project");
    options.setJobId("job");
    options.setWorkerId("worker");
    List<String> experiments =
        options.getExperiments() == null ? new ArrayList<>() : options.getExperiments();
    if (enableStreamingEngine) {
      experiments.add(GcpOptions.STREAMING_ENGINE_EXPERIMENT);
    }
    options.setExperiments(experiments);

    options.setWindmillServiceStreamingRpcBatchLimit(Integer.MAX_VALUE);
    options.setWindmillServiceStreamingRpcHealthCheckPeriodMs(NO_HEALTH_CHECK);
    options.setWindmillServiceStreamingLogEveryNStreamFailures(DEFAULT_LOG_EVERY_N_FAILURES);

    return options;
  }

  /** Create new instance of {@link GrpcWindmillServer}. */
  public static GrpcWindmillServer create(StreamingDataflowWorkerOptions workerOptions)
      throws IOException {

    GrpcWindmillServer grpcWindmillServer =
        new GrpcWindmillServer(
            workerOptions,
            GrpcDispatcherClient.create(
                WindmillStubFactory.remoteStubFactory(
                    workerOptions.getWindmillServiceRpcChannelAliveTimeoutSec(),
                    workerOptions.getGcpCredential())));
    if (workerOptions.getWindmillServiceEndpoint() != null) {
      grpcWindmillServer.configureWindmillServiceEndpoints();
    } else if (!workerOptions.isEnableStreamingEngine()
        && workerOptions.getLocalWindmillHostport() != null) {
      grpcWindmillServer.configureLocalHost();
    }

    return grpcWindmillServer;
  }

  @VisibleForTesting
  static GrpcWindmillServer newTestInstance(String name) {
    ManagedChannel inProcessChannel = inProcessChannel(name);
    CloudWindmillServiceV1Alpha1Stub stub =
        CloudWindmillServiceV1Alpha1Grpc.newStub(inProcessChannel);
    List<CloudWindmillServiceV1Alpha1Stub> dispatcherStubs = Lists.newArrayList(stub);
    Set<HostAndPort> dispatcherEndpoints = Sets.newHashSet(HostAndPort.fromHost(name));
    GrpcDispatcherClient dispatcherClient =
        GrpcDispatcherClient.forTesting(
            WindmillStubFactory.inProcessStubFactory(name, unused -> inProcessChannel),
            dispatcherStubs,
            dispatcherEndpoints);
    return new GrpcWindmillServer(testOptions(/* enableStreamingEngine= */ true), dispatcherClient);
  }

  @VisibleForTesting
  static GrpcWindmillServer newApplianceTestInstance(Channel channel) {
    GrpcWindmillServer testServer =
        new GrpcWindmillServer(
            testOptions(/* enableStreamingEngine= */ false),
            // No-op, Appliance does not use Dispatcher to call Streaming Engine.
            GrpcDispatcherClient.create(WindmillStubFactory.inProcessStubFactory("test")));
    testServer.syncApplianceStub = createWindmillApplianceStubWithDeadlineInterceptor(channel);
    return testServer;
  }

  private static WindmillApplianceGrpc.WindmillApplianceBlockingStub
      createWindmillApplianceStubWithDeadlineInterceptor(Channel channel) {
    return WindmillApplianceGrpc.newBlockingStub(channel)
        .withInterceptors(GrpcDeadlineClientInterceptor.withDefaultUnaryRpcDeadline());
  }

  private static UnsupportedOperationException unsupportedUnaryRequestInStreamingEngineException(
      String rpcName) {
    return new UnsupportedOperationException(
        String.format("Unary %s calls are not supported in Streaming Engine.", rpcName));
  }

  private void configureWindmillServiceEndpoints() {
    Set<HostAndPort> endpoints = new HashSet<>();
    for (String endpoint : Splitter.on(',').split(options.getWindmillServiceEndpoint())) {
      endpoints.add(
          HostAndPort.fromString(endpoint).withDefaultPort(options.getWindmillServicePort()));
    }

    dispatcherClient.consumeWindmillDispatcherEndpoints(ImmutableSet.copyOf(endpoints));
  }

  private void configureLocalHost() {
    int portStart = options.getLocalWindmillHostport().lastIndexOf(':');
    String endpoint = options.getLocalWindmillHostport().substring(0, portStart);
    Preconditions.checkState(GRPC_LOCALHOST.equals(endpoint));
    int port = Integer.parseInt(options.getLocalWindmillHostport().substring(portStart + 1));
    dispatcherClient.consumeWindmillDispatcherEndpoints(
        ImmutableSet.of(HostAndPort.fromParts(LOCALHOST, port)));
    initializeLocalHost(port);
  }

  @Override
  public void setWindmillServiceEndpoints(Set<HostAndPort> endpoints) {
    dispatcherClient.consumeWindmillDispatcherEndpoints(ImmutableSet.copyOf(endpoints));
  }

  @Override
  public boolean isReady() {
    return dispatcherClient.isReady();
  }

  private synchronized void initializeLocalHost(int port) {
    this.maxBackoff = Duration.millis(500);
    if (options.isEnableStreamingEngine()) {
      dispatcherClient.consumeWindmillDispatcherEndpoints(
          ImmutableSet.of(HostAndPort.fromParts(LOCALHOST, port)));
    } else {
      this.syncApplianceStub =
          createWindmillApplianceStubWithDeadlineInterceptor(localhostChannel(port));
    }
  }

  @Override
  public void appendSummaryHtml(PrintWriter writer) {
    windmillStreamFactory.appendSummaryHtml(writer);
  }

  private <ResponseT> ResponseT callWithBackoff(Supplier<ResponseT> function) {
    // Configure backoff to retry calls forever, with a maximum sane retry interval.
    BackOff backoff =
        FluentBackoff.DEFAULT.withInitialBackoff(MIN_BACKOFF).withMaxBackoff(maxBackoff).backoff();

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
    if (syncApplianceStub != null) {
      return callWithBackoff(() -> syncApplianceStub.getWork(request));
    }

    throw new RpcException(unsupportedUnaryRequestInStreamingEngineException("GetWork"));
  }

  @Override
  public GetDataResponse getData(GetDataRequest request) {
    if (syncApplianceStub != null) {
      return callWithBackoff(() -> syncApplianceStub.getData(request));
    }

    throw new RpcException(unsupportedUnaryRequestInStreamingEngineException("GetData"));
  }

  @Override
  public CommitWorkResponse commitWork(CommitWorkRequest request) {
    if (syncApplianceStub != null) {
      return callWithBackoff(() -> syncApplianceStub.commitWork(request));
    }
    throw new RpcException(unsupportedUnaryRequestInStreamingEngineException("CommitWork"));
  }

  @Override
  public GetWorkStream getWorkStream(GetWorkRequest request, WorkItemReceiver receiver) {
    return windmillStreamFactory.createGetWorkStream(
        dispatcherClient.getDispatcherStub(),
        GetWorkRequest.newBuilder(request)
            .setJobId(options.getJobId())
            .setProjectId(options.getProject())
            .setWorkerId(options.getWorkerId())
            .build(),
        throttleTimers.getWorkThrottleTimer(),
        receiver);
  }

  @Override
  public GetDataStream getDataStream() {
    return windmillStreamFactory.createGetDataStream(
        dispatcherClient.getDispatcherStub(), throttleTimers.getDataThrottleTimer());
  }

  @Override
  public CommitWorkStream commitWorkStream() {
    return windmillStreamFactory.createCommitWorkStream(
        dispatcherClient.getDispatcherStub(), throttleTimers.commitWorkThrottleTimer());
  }

  @Override
  public GetConfigResponse getConfig(GetConfigRequest request) {
    if (syncApplianceStub != null) {
      return callWithBackoff(() -> syncApplianceStub.getConfig(request));
    }

    throw new RpcException(
        new UnsupportedOperationException("GetConfig not supported in Streaming Engine."));
  }

  @Override
  public ReportStatsResponse reportStats(ReportStatsRequest request) {
    if (syncApplianceStub != null) {
      return callWithBackoff(() -> syncApplianceStub.reportStats(request));
    }

    throw new RpcException(
        new UnsupportedOperationException("ReportStats not supported in Streaming Engine."));
  }

  @Override
  public long getAndResetThrottleTime() {
    return throttleTimers.getAndResetThrottleTime();
  }
}
