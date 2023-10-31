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

import static org.apache.beam.runners.dataflow.worker.windmill.client.AbstractWindmillStream.DEFAULT_STREAM_RPC_DEADLINE_SECONDS;

import com.google.auto.value.AutoBuilder;
import java.io.PrintWriter;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.Supplier;
import javax.annotation.concurrent.ThreadSafe;
import org.apache.beam.runners.dataflow.worker.status.StatusDataProvider;
import org.apache.beam.runners.dataflow.worker.windmill.CloudWindmillServiceV1Alpha1Grpc.CloudWindmillServiceV1Alpha1Stub;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.GetWorkRequest;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.JobHeader;
import org.apache.beam.runners.dataflow.worker.windmill.WindmillEndpoints;
import org.apache.beam.runners.dataflow.worker.windmill.client.AbstractWindmillStream;
import org.apache.beam.runners.dataflow.worker.windmill.client.WindmillStream.CommitWorkStream;
import org.apache.beam.runners.dataflow.worker.windmill.client.WindmillStream.GetDataStream;
import org.apache.beam.runners.dataflow.worker.windmill.client.WindmillStream.GetWorkStream;
import org.apache.beam.runners.dataflow.worker.windmill.client.WindmillStream.GetWorkerMetadataStream;
import org.apache.beam.runners.dataflow.worker.windmill.client.grpc.observers.StreamObserverFactory;
import org.apache.beam.runners.dataflow.worker.windmill.client.throttling.ThrottleTimer;
import org.apache.beam.runners.dataflow.worker.windmill.work.WorkItemReceiver;
import org.apache.beam.sdk.util.BackOff;
import org.apache.beam.sdk.util.FluentBackoff;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Suppliers;
import org.joda.time.Duration;
import org.joda.time.Instant;

/**
 * Creates gRPC streaming connections to Windmill Service. Maintains a set of all currently opened
 * RPC streams for health check/heartbeat requests to keep the streams alive.
 */
@ThreadSafe
public final class GrpcWindmillStreamFactory implements StatusDataProvider {
  private static final Duration MIN_BACKOFF = Duration.millis(1);
  private static final Duration DEFAULT_MAX_BACKOFF = Duration.standardSeconds(30);
  private static final int DEFAULT_LOG_EVERY_N_STREAM_FAILURES = 1;
  private static final int DEFAULT_STREAMING_RPC_BATCH_LIMIT = Integer.MAX_VALUE;
  private static final int DEFAULT_WINDMILL_MESSAGES_BETWEEN_IS_READY_CHECKS = 1;

  private final JobHeader jobHeader;
  private final int logEveryNStreamFailures;
  private final int streamingRpcBatchLimit;
  private final int windmillMessagesBetweenIsReadyChecks;
  private final Supplier<BackOff> grpcBackOff;
  private final Set<AbstractWindmillStream<?, ?>> streamRegistry;
  private final AtomicLong streamIdGenerator;

  GrpcWindmillStreamFactory(
      JobHeader jobHeader,
      int logEveryNStreamFailures,
      int streamingRpcBatchLimit,
      int windmillMessagesBetweenIsReadyChecks,
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
    this.streamIdGenerator = new AtomicLong();
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
        .setStreamingRpcBatchLimit(DEFAULT_STREAMING_RPC_BATCH_LIMIT);
  }

  private static CloudWindmillServiceV1Alpha1Stub withDeadline(
      CloudWindmillServiceV1Alpha1Stub stub) {
    // Deadlines are absolute points in time, so generate a new one everytime this function is
    // called.
    return stub.withDeadlineAfter(
        AbstractWindmillStream.DEFAULT_STREAM_RPC_DEADLINE_SECONDS, TimeUnit.SECONDS);
  }

  public GetWorkStream createGetWorkStream(
      CloudWindmillServiceV1Alpha1Stub stub,
      GetWorkRequest request,
      ThrottleTimer getWorkThrottleTimer,
      WorkItemReceiver processWorkItem) {
    return GrpcGetWorkStream.create(
        responseObserver -> withDeadline(stub).getWorkStream(responseObserver),
        request,
        grpcBackOff.get(),
        newStreamObserverFactory(),
        streamRegistry,
        logEveryNStreamFailures,
        getWorkThrottleTimer,
        processWorkItem);
  }

  public GetDataStream createGetDataStream(
      CloudWindmillServiceV1Alpha1Stub stub, ThrottleTimer getDataThrottleTimer) {
    return GrpcGetDataStream.create(
        responseObserver -> withDeadline(stub).getDataStream(responseObserver),
        grpcBackOff.get(),
        newStreamObserverFactory(),
        streamRegistry,
        logEveryNStreamFailures,
        getDataThrottleTimer,
        jobHeader,
        streamIdGenerator,
        streamingRpcBatchLimit);
  }

  public CommitWorkStream createCommitWorkStream(
      CloudWindmillServiceV1Alpha1Stub stub, ThrottleTimer commitWorkThrottleTimer) {
    return GrpcCommitWorkStream.create(
        responseObserver -> withDeadline(stub).commitWorkStream(responseObserver),
        grpcBackOff.get(),
        newStreamObserverFactory(),
        streamRegistry,
        logEveryNStreamFailures,
        commitWorkThrottleTimer,
        jobHeader,
        streamIdGenerator,
        streamingRpcBatchLimit);
  }

  public GetWorkerMetadataStream createGetWorkerMetadataStream(
      CloudWindmillServiceV1Alpha1Stub stub,
      ThrottleTimer getWorkerMetadataThrottleTimer,
      Consumer<WindmillEndpoints> onNewWindmillEndpoints) {
    return GrpcGetWorkerMetadataStream.create(
        responseObserver -> withDeadline(stub).getWorkerMetadataStream(responseObserver),
        grpcBackOff.get(),
        newStreamObserverFactory(),
        streamRegistry,
        logEveryNStreamFailures,
        jobHeader,
        0,
        getWorkerMetadataThrottleTimer,
        onNewWindmillEndpoints);
  }

  private StreamObserverFactory newStreamObserverFactory() {
    return StreamObserverFactory.direct(
        DEFAULT_STREAM_RPC_DEADLINE_SECONDS * 2, windmillMessagesBetweenIsReadyChecks);
  }

  /**
   * Schedules streaming RPC health checks to run on a background daemon thread, which will be
   * cleaned up when the JVM shutdown.
   */
  public void scheduleHealthChecks(int healthCheckInterval) {
    if (healthCheckInterval < 0) {
      return;
    }

    new Timer("WindmillHealthCheckTimer")
        .schedule(
            new TimerTask() {
              @Override
              public void run() {
                Instant reportThreshold = Instant.now().minus(Duration.millis(healthCheckInterval));
                for (AbstractWindmillStream<?, ?> stream : streamRegistry) {
                  stream.maybeSendHealthCheck(reportThreshold);
                }
              }
            },
            0,
            healthCheckInterval);
  }

  @Override
  public void appendSummaryHtml(PrintWriter writer) {
    writer.write("Active Streams:<br>");
    for (AbstractWindmillStream<?, ?> stream : streamRegistry) {
      stream.appendSummaryHtml(writer);
      writer.write("<br>");
    }
  }

  @AutoBuilder(ofClass = GrpcWindmillStreamFactory.class)
  interface Builder {
    Builder setJobHeader(JobHeader jobHeader);

    Builder setLogEveryNStreamFailures(int logEveryNStreamFailures);

    Builder setStreamingRpcBatchLimit(int streamingRpcBatchLimit);

    Builder setWindmillMessagesBetweenIsReadyChecks(int windmillMessagesBetweenIsReadyChecks);

    Builder setMaxBackOffSupplier(Supplier<Duration> maxBackOff);

    GrpcWindmillStreamFactory build();
  }
}
