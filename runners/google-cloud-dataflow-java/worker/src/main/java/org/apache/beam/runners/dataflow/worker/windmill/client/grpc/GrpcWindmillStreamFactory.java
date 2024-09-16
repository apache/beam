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

import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableListMultimap.toImmutableListMultimap;

import com.google.auto.value.AutoBuilder;
import java.io.PrintWriter;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import javax.annotation.concurrent.ThreadSafe;
import org.apache.beam.runners.dataflow.worker.status.StatusDataProvider;
import org.apache.beam.runners.dataflow.worker.windmill.CloudWindmillMetadataServiceV1Alpha1Grpc.CloudWindmillMetadataServiceV1Alpha1Stub;
import org.apache.beam.runners.dataflow.worker.windmill.CloudWindmillServiceV1Alpha1Grpc.CloudWindmillServiceV1Alpha1Stub;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.ComputationHeartbeatResponse;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.GetWorkRequest;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.JobHeader;
import org.apache.beam.runners.dataflow.worker.windmill.WindmillConnection;
import org.apache.beam.runners.dataflow.worker.windmill.WindmillEndpoints;
import org.apache.beam.runners.dataflow.worker.windmill.client.AbstractWindmillStream;
import org.apache.beam.runners.dataflow.worker.windmill.client.WindmillStream.CommitWorkStream;
import org.apache.beam.runners.dataflow.worker.windmill.client.WindmillStream.GetDataStream;
import org.apache.beam.runners.dataflow.worker.windmill.client.WindmillStream.GetWorkStream;
import org.apache.beam.runners.dataflow.worker.windmill.client.WindmillStream.GetWorkerMetadataStream;
import org.apache.beam.runners.dataflow.worker.windmill.client.commits.WorkCommitter;
import org.apache.beam.runners.dataflow.worker.windmill.client.getdata.GetDataClient;
import org.apache.beam.runners.dataflow.worker.windmill.client.grpc.observers.StreamObserverFactory;
import org.apache.beam.runners.dataflow.worker.windmill.client.throttling.ThrottleTimer;
import org.apache.beam.runners.dataflow.worker.windmill.work.WorkItemReceiver;
import org.apache.beam.runners.dataflow.worker.windmill.work.WorkItemScheduler;
import org.apache.beam.runners.dataflow.worker.windmill.work.refresh.HeartbeatSender;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.sdk.util.BackOff;
import org.apache.beam.sdk.util.FluentBackoff;
import org.apache.beam.vendor.grpc.v1p60p1.io.grpc.stub.AbstractStub;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Suppliers;
import org.joda.time.Duration;
import org.joda.time.Instant;

/**
 * Creates gRPC streaming connections to Windmill Service. Maintains a set of all currently opened
 * RPC streams for health check/heartbeat requests to keep the streams alive.
 */
@ThreadSafe
@Internal
public class GrpcWindmillStreamFactory implements StatusDataProvider {
  private static final Duration MIN_BACKOFF = Duration.millis(1);
  private static final Duration DEFAULT_MAX_BACKOFF = Duration.standardSeconds(30);
  private static final int DEFAULT_LOG_EVERY_N_STREAM_FAILURES = 1;
  private static final int DEFAULT_STREAMING_RPC_BATCH_LIMIT = Integer.MAX_VALUE;
  private static final int DEFAULT_WINDMILL_MESSAGES_BETWEEN_IS_READY_CHECKS = 1;
  private static final int NO_HEALTH_CHECKS = -1;
  private static final String NO_BACKEND_WORKER_TOKEN = "";
  private static final long NO_DEADLINE = -1;
  private static final long DEFAULT_DEADLINE_SECONDS =
      AbstractWindmillStream.DEFAULT_STREAM_RPC_DEADLINE_SECONDS * 2;
  private static final String DISPATCHER_DEBUG_NAME = "Dispatcher";

  private final JobHeader jobHeader;
  private final int logEveryNStreamFailures;
  private final int streamingRpcBatchLimit;
  private final int windmillMessagesBetweenIsReadyChecks;
  private final Supplier<BackOff> grpcBackOff;
  private final Set<AbstractWindmillStream<?, ?>> streamRegistry;
  private final AtomicLong streamIdGenerator;
  // If true, then active work refreshes will be sent as KeyedGetDataRequests. Otherwise, use the
  // newer ComputationHeartbeatRequests.
  private final boolean sendKeyedGetDataRequests;
  private final Consumer<List<ComputationHeartbeatResponse>> processHeartbeatResponses;

  private GrpcWindmillStreamFactory(
      JobHeader jobHeader,
      int logEveryNStreamFailures,
      int streamingRpcBatchLimit,
      int windmillMessagesBetweenIsReadyChecks,
      boolean sendKeyedGetDataRequests,
      Consumer<List<ComputationHeartbeatResponse>> processHeartbeatResponses,
      Supplier<Duration> maxBackOffSupplier) {
    this.jobHeader = jobHeader;
    this.logEveryNStreamFailures = logEveryNStreamFailures;
    this.streamingRpcBatchLimit = streamingRpcBatchLimit;
    this.windmillMessagesBetweenIsReadyChecks = windmillMessagesBetweenIsReadyChecks;
    // Configure backoff to retry calls forever, with a maximum sane retry interval.
    this.grpcBackOff =
        Suppliers.memoize(
            () ->
                FluentBackoff.DEFAULT
                    .withInitialBackoff(MIN_BACKOFF)
                    .withMaxBackoff(maxBackOffSupplier.get())
                    .backoff());
    this.streamRegistry = ConcurrentHashMap.newKeySet();
    this.sendKeyedGetDataRequests = sendKeyedGetDataRequests;
    this.processHeartbeatResponses = processHeartbeatResponses;
    this.streamIdGenerator = new AtomicLong();
  }

  /** @implNote Used for {@link AutoBuilder} {@link Builder} class, do not call directly. */
  static GrpcWindmillStreamFactory create(
      JobHeader jobHeader,
      int logEveryNStreamFailures,
      int streamingRpcBatchLimit,
      int windmillMessagesBetweenIsReadyChecks,
      boolean sendKeyedGetDataRequests,
      Consumer<List<ComputationHeartbeatResponse>> processHeartbeatResponses,
      Supplier<Duration> maxBackOffSupplier,
      int healthCheckIntervalMillis) {
    GrpcWindmillStreamFactory streamFactory =
        new GrpcWindmillStreamFactory(
            jobHeader,
            logEveryNStreamFailures,
            streamingRpcBatchLimit,
            windmillMessagesBetweenIsReadyChecks,
            sendKeyedGetDataRequests,
            processHeartbeatResponses,
            maxBackOffSupplier);

    if (healthCheckIntervalMillis >= 0) {
      // Health checks are run on background daemon thread, which will only be cleaned up on JVM
      // shutdown.
      new Timer("WindmillHealthCheckTimer")
          .schedule(
              new TimerTask() {
                @Override
                public void run() {
                  Instant reportThreshold =
                      Instant.now().minus(Duration.millis(healthCheckIntervalMillis));
                  for (AbstractWindmillStream<?, ?> stream : streamFactory.streamRegistry) {
                    stream.maybeSendHealthCheck(reportThreshold);
                  }
                }
              },
              0,
              healthCheckIntervalMillis);
    }

    return streamFactory;
  }

  /**
   * Returns a new {@link Builder} for {@link GrpcWindmillStreamFactory} with default values set for
   * the given {@link JobHeader}.
   */
  public static GrpcWindmillStreamFactory.Builder of(JobHeader jobHeader) {
    return new AutoBuilder_GrpcWindmillStreamFactory_Builder()
        .setJobHeader(jobHeader)
        .setWindmillMessagesBetweenIsReadyChecks(DEFAULT_WINDMILL_MESSAGES_BETWEEN_IS_READY_CHECKS)
        .setMaxBackOffSupplier(() -> DEFAULT_MAX_BACKOFF)
        .setLogEveryNStreamFailures(DEFAULT_LOG_EVERY_N_STREAM_FAILURES)
        .setStreamingRpcBatchLimit(DEFAULT_STREAMING_RPC_BATCH_LIMIT)
        .setHealthCheckIntervalMillis(NO_HEALTH_CHECKS)
        .setSendKeyedGetDataRequests(true)
        .setProcessHeartbeatResponses(ignored -> {});
  }

  private static <T extends AbstractStub<T>> T withDefaultDeadline(T stub) {
    // Deadlines are absolute points in time, so generate a new one everytime this function is
    // called.
    return stub.withDeadlineAfter(
        AbstractWindmillStream.DEFAULT_STREAM_RPC_DEADLINE_SECONDS, TimeUnit.SECONDS);
  }

  private static void printSummaryHtmlForWorker(
      String workerToken, Collection<AbstractWindmillStream<?, ?>> streams, PrintWriter writer) {
    writer.write(
        "<strong>" + (workerToken.isEmpty() ? DISPATCHER_DEBUG_NAME : workerToken) + "</strong>");
    writer.write("<br>");
    streams.forEach(
        stream -> {
          stream.appendSummaryHtml(writer);
          writer.write("<br>");
        });
    writer.write("<br>");
  }

  public GetWorkStream createGetWorkStream(
      CloudWindmillServiceV1Alpha1Stub stub,
      GetWorkRequest request,
      ThrottleTimer getWorkThrottleTimer,
      WorkItemReceiver processWorkItem) {
    return GrpcGetWorkStream.create(
        NO_BACKEND_WORKER_TOKEN,
        responseObserver -> withDefaultDeadline(stub).getWorkStream(responseObserver),
        request,
        grpcBackOff.get(),
        newStreamObserverFactory(DEFAULT_DEADLINE_SECONDS),
        streamRegistry,
        logEveryNStreamFailures,
        getWorkThrottleTimer,
        processWorkItem);
  }

  public GetWorkStream createDirectGetWorkStream(
      WindmillConnection connection,
      GetWorkRequest request,
      ThrottleTimer getWorkThrottleTimer,
      Supplier<HeartbeatSender> heartbeatSender,
      Supplier<GetDataClient> getDataClient,
      Supplier<WorkCommitter> workCommitter,
      WorkItemScheduler workItemScheduler) {
    return GrpcDirectGetWorkStream.create(
        connection.backendWorkerToken(),
        responseObserver -> connection.stub().getWorkStream(responseObserver),
        request,
        grpcBackOff.get(),
        newStreamObserverFactory(NO_DEADLINE),
        streamRegistry,
        logEveryNStreamFailures,
        getWorkThrottleTimer,
        heartbeatSender,
        getDataClient,
        workCommitter,
        workItemScheduler);
  }

  public GetDataStream createGetDataStream(
      CloudWindmillServiceV1Alpha1Stub stub, ThrottleTimer getDataThrottleTimer) {
    return GrpcGetDataStream.create(
        NO_BACKEND_WORKER_TOKEN,
        responseObserver -> withDefaultDeadline(stub).getDataStream(responseObserver),
        grpcBackOff.get(),
        newStreamObserverFactory(DEFAULT_DEADLINE_SECONDS),
        streamRegistry,
        logEveryNStreamFailures,
        getDataThrottleTimer,
        jobHeader,
        streamIdGenerator,
        streamingRpcBatchLimit,
        sendKeyedGetDataRequests,
        processHeartbeatResponses);
  }

  public GetDataStream createDirectGetDataStream(
      WindmillConnection connection, ThrottleTimer getDataThrottleTimer) {
    return GrpcGetDataStream.create(
        connection.backendWorkerToken(),
        responseObserver -> connection.stub().getDataStream(responseObserver),
        grpcBackOff.get(),
        newStreamObserverFactory(NO_DEADLINE),
        streamRegistry,
        logEveryNStreamFailures,
        getDataThrottleTimer,
        jobHeader,
        streamIdGenerator,
        streamingRpcBatchLimit,
        sendKeyedGetDataRequests,
        processHeartbeatResponses);
  }

  public CommitWorkStream createCommitWorkStream(
      CloudWindmillServiceV1Alpha1Stub stub, ThrottleTimer commitWorkThrottleTimer) {
    return GrpcCommitWorkStream.create(
        NO_BACKEND_WORKER_TOKEN,
        responseObserver -> withDefaultDeadline(stub).commitWorkStream(responseObserver),
        grpcBackOff.get(),
        newStreamObserverFactory(DEFAULT_DEADLINE_SECONDS),
        streamRegistry,
        logEveryNStreamFailures,
        commitWorkThrottleTimer,
        jobHeader,
        streamIdGenerator,
        streamingRpcBatchLimit);
  }

  public CommitWorkStream createDirectCommitWorkStream(
      WindmillConnection connection, ThrottleTimer commitWorkThrottleTimer) {
    return GrpcCommitWorkStream.create(
        connection.backendWorkerToken(),
        responseObserver -> connection.stub().commitWorkStream(responseObserver),
        grpcBackOff.get(),
        newStreamObserverFactory(NO_DEADLINE),
        streamRegistry,
        logEveryNStreamFailures,
        commitWorkThrottleTimer,
        jobHeader,
        streamIdGenerator,
        streamingRpcBatchLimit);
  }

  public GetWorkerMetadataStream createGetWorkerMetadataStream(
      CloudWindmillMetadataServiceV1Alpha1Stub stub,
      ThrottleTimer getWorkerMetadataThrottleTimer,
      Consumer<WindmillEndpoints> onNewWindmillEndpoints) {
    return GrpcGetWorkerMetadataStream.create(
        responseObserver -> withDefaultDeadline(stub).getWorkerMetadata(responseObserver),
        grpcBackOff.get(),
        newStreamObserverFactory(DEFAULT_DEADLINE_SECONDS),
        streamRegistry,
        logEveryNStreamFailures,
        jobHeader,
        0,
        getWorkerMetadataThrottleTimer,
        onNewWindmillEndpoints);
  }

  private StreamObserverFactory newStreamObserverFactory(long deadline) {
    return StreamObserverFactory.direct(deadline, windmillMessagesBetweenIsReadyChecks);
  }

  @Override
  public void appendSummaryHtml(PrintWriter writer) {
    writer.write("Active Streams:<br>");
    streamRegistry.stream()
        .collect(
            toImmutableListMultimap(
                AbstractWindmillStream::backendWorkerToken, Function.identity()))
        .asMap()
        .forEach((workerToken, streams) -> printSummaryHtmlForWorker(workerToken, streams, writer));
  }

  @Internal
  @AutoBuilder(callMethod = "create")
  public interface Builder {
    Builder setJobHeader(JobHeader jobHeader);

    Builder setLogEveryNStreamFailures(int logEveryNStreamFailures);

    Builder setStreamingRpcBatchLimit(int streamingRpcBatchLimit);

    Builder setWindmillMessagesBetweenIsReadyChecks(int windmillMessagesBetweenIsReadyChecks);

    Builder setMaxBackOffSupplier(Supplier<Duration> maxBackOff);

    Builder setSendKeyedGetDataRequests(boolean sendKeyedGetDataRequests);

    Builder setProcessHeartbeatResponses(
        Consumer<List<ComputationHeartbeatResponse>> processHeartbeatResponses);

    Builder setHealthCheckIntervalMillis(int healthCheckIntervalMillis);

    GrpcWindmillStreamFactory build();
  }
}
