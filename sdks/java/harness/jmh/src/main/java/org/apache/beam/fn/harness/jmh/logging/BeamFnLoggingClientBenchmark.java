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
package org.apache.beam.fn.harness.jmh.logging;

import java.io.Closeable;
import java.util.HashMap;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.beam.fn.harness.logging.BeamFnLoggingClient;
import org.apache.beam.fn.harness.logging.BeamFnLoggingMDC;
import org.apache.beam.fn.harness.logging.LoggingClient;
import org.apache.beam.fn.harness.logging.LoggingClientFactory;
import org.apache.beam.fn.harness.logging.QuotaEvent;
import org.apache.beam.model.fnexecution.v1.BeamFnApi;
import org.apache.beam.model.fnexecution.v1.BeamFnLoggingGrpc;
import org.apache.beam.model.pipeline.v1.Endpoints.ApiServiceDescriptor;
import org.apache.beam.runners.core.metrics.ExecutionStateTracker;
import org.apache.beam.runners.core.metrics.MonitoringInfoConstants;
import org.apache.beam.runners.core.metrics.SimpleExecutionState;
import org.apache.beam.sdk.fn.channel.ManagedChannelFactory;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.vendor.grpc.v1p60p1.io.grpc.Server;
import org.apache.beam.vendor.grpc.v1p60p1.io.grpc.inprocess.InProcessServerBuilder;
import org.apache.beam.vendor.grpc.v1p60p1.io.grpc.stub.StreamObserver;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Threads;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

/** Benchmarks for {@link BeamFnLoggingClient}. */
public class BeamFnLoggingClientBenchmark {
  private static final Logger LOG = LoggerFactory.getLogger(BeamFnLoggingClientBenchmark.class);

  /** A logging service which counts the number of calls it received. */
  public static class CallCountLoggingService extends BeamFnLoggingGrpc.BeamFnLoggingImplBase {
    private AtomicInteger callCount = new AtomicInteger();

    @Override
    public StreamObserver<BeamFnApi.LogEntry.List> logging(
        StreamObserver<BeamFnApi.LogControl> outboundObserver) {
      return new StreamObserver<BeamFnApi.LogEntry.List>() {

        @Override
        public void onNext(BeamFnApi.LogEntry.List list) {
          callCount.incrementAndGet();
        }

        @Override
        public void onError(Throwable throwable) {
          outboundObserver.onError(throwable);
        }

        @Override
        public void onCompleted() {
          outboundObserver.onCompleted();
        }
      };
    }
  }

  /** Setup a simple logging service and configure the {@link BeamFnLoggingClient}. */
  @State(Scope.Benchmark)
  public static class ManageLoggingClientAndService {
    public final BeamFnLoggingClient loggingClient;
    public final CallCountLoggingService loggingService;
    public final Server server;

    public ManageLoggingClientAndService() {
      try {
        ApiServiceDescriptor apiServiceDescriptor =
            ApiServiceDescriptor.newBuilder()
                .setUrl(BeamFnLoggingClientBenchmark.class.getName() + "#" + UUID.randomUUID())
                .build();
        ManagedChannelFactory managedChannelFactory = ManagedChannelFactory.createInProcess();
        loggingService = new CallCountLoggingService();
        server =
            InProcessServerBuilder.forName(apiServiceDescriptor.getUrl())
                .addService(loggingService)
                .build();
        server.start();
        loggingClient =
            LoggingClientFactory.createAndStart(
                PipelineOptionsFactory.create(),
                apiServiceDescriptor,
                managedChannelFactory::forDescriptor);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }

    @TearDown(Level.Trial)
    public void tearDown() throws Exception {
      loggingClient.close();
      server.shutdown();
      if (server.awaitTermination(30, TimeUnit.SECONDS)) {
        server.shutdownNow();
      }
    }
  }

  /**
   * A {@link ManageLoggingClientAndService} which validates that more than zero calls made it to
   * the service.
   */
  @State(Scope.Benchmark)
  public static class ManyExpectedCallsLoggingClientAndService
      extends ManageLoggingClientAndService {
    @Override
    @TearDown
    public void tearDown() throws Exception {
      super.tearDown();
      if (loggingService.callCount.get() <= 0) {
        throw new IllegalStateException(
            "Server expected greater then zero calls. Benchmark misconfigured?");
      }
    }
  }

  /**
   * A {@link ManageLoggingClientAndService} which validates that exactly zero calls made it to the
   * service.
   */
  @State(Scope.Benchmark)
  public static class ZeroExpectedCallsLoggingClientAndService
      extends ManageLoggingClientAndService {
    @Override
    @TearDown
    public void tearDown() throws Exception {
      super.tearDown();
      if (loggingService.callCount.get() != 0) {
        throw new IllegalStateException("Server expected zero calls. Benchmark misconfigured?");
      }
    }
  }

  /** Sets up the {@link ExecutionStateTracker} and an execution state. */
  @State(Scope.Benchmark)
  public static class ManageExecutionState {
    private final ExecutionStateTracker executionStateTracker;
    private final SimpleExecutionState simpleExecutionState;

    public ManageExecutionState() {
      executionStateTracker = ExecutionStateTracker.newForTest();
      HashMap<String, String> labelsMetadata = new HashMap<>();
      labelsMetadata.put(MonitoringInfoConstants.Labels.PTRANSFORM, "ptransformId");
      simpleExecutionState =
          new SimpleExecutionState(
              ExecutionStateTracker.PROCESS_STATE_NAME,
              MonitoringInfoConstants.Urns.PROCESS_BUNDLE_MSECS,
              labelsMetadata);
    }

    @TearDown
    public void tearDown() throws Exception {
      executionStateTracker.reset();
    }
  }

  @Benchmark
  @Threads(16) // Use several threads since we expect contention during logging
  public void testLogging(ManyExpectedCallsLoggingClientAndService client) {
    LOG.warn("log me");
  }

  @Benchmark
  @Threads(16) // Use several threads since we expect contention during logging
  public void testLoggingWithAllOptionalParameters(
      ManyExpectedCallsLoggingClientAndService client, ManageExecutionState executionState)
      throws Exception {
    BeamFnLoggingMDC.setInstructionId("instruction id");
    try (Closeable state =
        executionState.executionStateTracker.enterState(executionState.simpleExecutionState)) {
      LOG.warn("log me");
    }
    BeamFnLoggingMDC.setInstructionId(null);
  }

  @Benchmark
  @Threads(16) // Use several threads since we expect contention during logging
  public void testLoggingWithCustomData(
      ManyExpectedCallsLoggingClientAndService client, ManageExecutionState executionState)
      throws Exception {
    try (Closeable state =
        executionState.executionStateTracker.enterState(executionState.simpleExecutionState)) {
      try (Closeable mdc = MDC.putCloseable("key", "value")) {
        LOG.warn("log me");
      }
    }
  }

  @Benchmark
  @Threads(16) // Use several threads since we expect contention during logging
  public void testLoggingWithQuotaEvent(
      ManyExpectedCallsLoggingClientAndService client, ManageExecutionState executionState)
      throws Exception {
    try (Closeable state =
        executionState.executionStateTracker.enterState(executionState.simpleExecutionState)) {
      try (AutoCloseable ac =
          new QuotaEvent.Builder()
              .withOperation("test")
              .withFullResourceName("//test.googleapis.com/abc/123")
              .create()) {
        LOG.warn("log me");
      }
    }
  }

  @Benchmark
  @Threads(16) // Use several threads since we expect contention during logging
  public void testSkippedLogging(ZeroExpectedCallsLoggingClientAndService client) {
    LOG.trace("no log");
  }
}
