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
package org.apache.beam.fn.harness.logging;

import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Throwables.getStackTraceAsString;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.Collection;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogManager;
import java.util.logging.LogRecord;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;
import org.apache.beam.fn.harness.control.ExecutionStateSampler;
import org.apache.beam.model.fnexecution.v1.BeamFnApi;
import org.apache.beam.model.fnexecution.v1.BeamFnLoggingGrpc;
import org.apache.beam.model.pipeline.v1.Endpoints;
import org.apache.beam.sdk.fn.test.TestStreams;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.vendor.grpc.v1p60p1.com.google.protobuf.Struct;
import org.apache.beam.vendor.grpc.v1p60p1.com.google.protobuf.Timestamp;
import org.apache.beam.vendor.grpc.v1p60p1.com.google.protobuf.Value;
import org.apache.beam.vendor.grpc.v1p60p1.io.grpc.CallOptions;
import org.apache.beam.vendor.grpc.v1p60p1.io.grpc.Channel;
import org.apache.beam.vendor.grpc.v1p60p1.io.grpc.ClientCall;
import org.apache.beam.vendor.grpc.v1p60p1.io.grpc.ClientInterceptor;
import org.apache.beam.vendor.grpc.v1p60p1.io.grpc.ForwardingClientCall;
import org.apache.beam.vendor.grpc.v1p60p1.io.grpc.ManagedChannel;
import org.apache.beam.vendor.grpc.v1p60p1.io.grpc.MethodDescriptor;
import org.apache.beam.vendor.grpc.v1p60p1.io.grpc.Server;
import org.apache.beam.vendor.grpc.v1p60p1.io.grpc.Status;
import org.apache.beam.vendor.grpc.v1p60p1.io.grpc.inprocess.InProcessChannelBuilder;
import org.apache.beam.vendor.grpc.v1p60p1.io.grpc.inprocess.InProcessServerBuilder;
import org.apache.beam.vendor.grpc.v1p60p1.io.grpc.stub.CallStreamObserver;
import org.apache.beam.vendor.grpc.v1p60p1.io.grpc.stub.StreamObserver;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TestRule;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.MDC;

/** Tests for {@link BeamFnLoggingClient}. */
@RunWith(JUnit4.class)
public class BeamFnLoggingClientTest {
  @Rule public TestRule restoreLogging = new RestoreBeamFnLoggingMDC();
  private static final LogRecord FILTERED_RECORD;
  private static final LogRecord TEST_RECORD;
  private static final LogRecord TEST_RECORD_WITH_EXCEPTION;

  static {
    FILTERED_RECORD = new LogRecord(Level.SEVERE, "FilteredMessage");

    TEST_RECORD = new LogRecord(Level.FINE, "Message");
    TEST_RECORD.setLoggerName("LoggerName");
    TEST_RECORD.setMillis(1234567890L);
    TEST_RECORD.setThreadID(12345);

    TEST_RECORD_WITH_EXCEPTION = new LogRecord(Level.WARNING, "MessageWithException");
    TEST_RECORD_WITH_EXCEPTION.setLoggerName("LoggerName");
    TEST_RECORD_WITH_EXCEPTION.setMillis(1234567890L);
    TEST_RECORD_WITH_EXCEPTION.setThreadID(12345);
    TEST_RECORD_WITH_EXCEPTION.setThrown(new RuntimeException("ExceptionMessage"));
  }

  private static final BeamFnApi.LogEntry TEST_ENTRY =
      BeamFnApi.LogEntry.newBuilder()
          .setInstructionId("instruction-1")
          .setSeverity(BeamFnApi.LogEntry.Severity.Enum.DEBUG)
          .setMessage("Message")
          .setTransformId("ptransformId")
          .setThread("12345")
          .setTimestamp(Timestamp.newBuilder().setSeconds(1234567).setNanos(890000000).build())
          .setLogLocation("LoggerName")
          .build();
  private static final BeamFnApi.LogEntry TEST_ENTRY_WITH_CUSTOM_FORMATTER =
      BeamFnApi.LogEntry.newBuilder()
          .setInstructionId("instruction-1")
          .setSeverity(BeamFnApi.LogEntry.Severity.Enum.DEBUG)
          .setMessage("testMdcValue:Message")
          .setTransformId("ptransformId")
          .setCustomData(
              Struct.newBuilder()
                  .putFields(
                      "testMdcKey", Value.newBuilder().setStringValue("testMdcValue").build()))
          .setThread("12345")
          .setTimestamp(Timestamp.newBuilder().setSeconds(1234567).setNanos(890000000).build())
          .setLogLocation("LoggerName")
          .build();
  private static final BeamFnApi.LogEntry TEST_ENTRY_WITH_EXCEPTION =
      BeamFnApi.LogEntry.newBuilder()
          .setInstructionId("instruction-1")
          .setSeverity(BeamFnApi.LogEntry.Severity.Enum.WARN)
          .setMessage("MessageWithException")
          .setTransformId("errorPtransformId")
          .setTrace(getStackTraceAsString(TEST_RECORD_WITH_EXCEPTION.getThrown()))
          .setThread("12345")
          .setTimestamp(Timestamp.newBuilder().setSeconds(1234567).setNanos(890000000).build())
          .setLogLocation("LoggerName")
          .build();
  @Rule public ExpectedException thrown = ExpectedException.none();

  @Test
  public void testLogging() throws Exception {
    ExecutionStateSampler sampler =
        new ExecutionStateSampler(PipelineOptionsFactory.create(), null);
    ExecutionStateSampler.ExecutionStateTracker stateTracker = sampler.create();
    ExecutionStateSampler.ExecutionState state =
        stateTracker.create("shortId", "ptransformId", "ptransformIdName", "process");
    state.activate();

    BeamFnLoggingMDC.setInstructionId("instruction-1");
    BeamFnLoggingMDC.setStateTracker(stateTracker);

    AtomicBoolean clientClosedStream = new AtomicBoolean();
    Collection<BeamFnApi.LogEntry> values = new ConcurrentLinkedQueue<>();
    AtomicReference<StreamObserver<BeamFnApi.LogControl>> outboundServerObserver =
        new AtomicReference<>();
    CallStreamObserver<BeamFnApi.LogEntry.List> inboundServerObserver =
        TestStreams.withOnNext(
                (BeamFnApi.LogEntry.List logEntries) ->
                    values.addAll(logEntries.getLogEntriesList()))
            .withOnCompleted(
                () -> {
                  // Remember that the client told us that this stream completed
                  clientClosedStream.set(true);
                  outboundServerObserver.get().onCompleted();
                })
            .build();

    Endpoints.ApiServiceDescriptor apiServiceDescriptor =
        Endpoints.ApiServiceDescriptor.newBuilder()
            .setUrl(this.getClass().getName() + "-" + UUID.randomUUID().toString())
            .build();
    Server server =
        InProcessServerBuilder.forName(apiServiceDescriptor.getUrl())
            .addService(
                new BeamFnLoggingGrpc.BeamFnLoggingImplBase() {
                  @Override
                  public StreamObserver<BeamFnApi.LogEntry.List> logging(
                      StreamObserver<BeamFnApi.LogControl> outboundObserver) {
                    outboundServerObserver.set(outboundObserver);
                    return inboundServerObserver;
                  }
                })
            .build();
    server.start();

    ManagedChannel channel = InProcessChannelBuilder.forName(apiServiceDescriptor.getUrl()).build();
    try {

      BeamFnLoggingClient client =
          BeamFnLoggingClient.createAndStart(
              PipelineOptionsFactory.fromArgs(
                      new String[] {
                        "--defaultSdkHarnessLogLevel=OFF",
                        "--sdkHarnessLogLevelOverrides={\"ConfiguredLogger\": \"DEBUG\"}"
                      })
                  .create(),
              apiServiceDescriptor,
              (Endpoints.ApiServiceDescriptor descriptor) -> channel);

      // Keep a strong reference to the loggers in this block. Otherwise the call to client.close()
      // removes the only reference and the logger may get GC'd before the assertions (BEAM-4136).
      Logger rootLogger = LogManager.getLogManager().getLogger("");
      Logger configuredLogger = LogManager.getLogManager().getLogger("ConfiguredLogger");

      // Ensure that log levels were correctly set.
      assertEquals(Level.OFF, rootLogger.getLevel());
      assertEquals(Level.FINE, configuredLogger.getLevel());

      // Should be filtered because the default log level override is OFF
      rootLogger.log(FILTERED_RECORD);
      // Should not be filtered because the default log level override for ConfiguredLogger is DEBUG
      configuredLogger.log(TEST_RECORD);

      // Simulate an exception. This sets an internal error state where the PTransform should come
      // from.
      ExecutionStateSampler.ExecutionState errorState =
          stateTracker.create("shortId", "errorPtransformId", "errorPtransformIdName", "process");
      errorState.activate();
      configuredLogger.log(TEST_RECORD_WITH_EXCEPTION);
      errorState.deactivate();

      // Ensure that configuring a custom formatter on the logging handler will be honored.
      for (Handler handler : rootLogger.getHandlers()) {
        handler.setFormatter(
            new SimpleFormatter() {
              @Override
              public synchronized String formatMessage(LogRecord record) {
                return MDC.get("testMdcKey") + ":" + super.formatMessage(record);
              }
            });
      }
      MDC.put("testMdcKey", "testMdcValue");
      configuredLogger.log(TEST_RECORD);

      client.close();

      // Verify that after close, log levels are reset.
      assertEquals(Level.INFO, rootLogger.getLevel());
      assertNull(configuredLogger.getLevel());

      assertTrue(clientClosedStream.get());
      assertTrue(channel.isShutdown());
      assertThat(
          values,
          contains(TEST_ENTRY, TEST_ENTRY_WITH_EXCEPTION, TEST_ENTRY_WITH_CUSTOM_FORMATTER));
    } finally {
      server.shutdownNow();
    }
  }

  @Test
  public void testWhenServerFailsThatClientIsAbleToCleanup() throws Exception {
    BeamFnLoggingMDC.setInstructionId("instruction-1");
    Collection<BeamFnApi.LogEntry> values = new ConcurrentLinkedQueue<>();
    AtomicReference<StreamObserver<BeamFnApi.LogControl>> outboundServerObserver =
        new AtomicReference<>();
    CallStreamObserver<BeamFnApi.LogEntry.List> inboundServerObserver =
        TestStreams.withOnNext(
                (BeamFnApi.LogEntry.List logEntries) ->
                    values.addAll(logEntries.getLogEntriesList()))
            .build();

    // Keep a strong reference to the loggers. Otherwise the call to client.close()
    // removes the only reference and the logger may get GC'd before the assertions (BEAM-4136).
    Logger rootLogger = null;
    Logger configuredLogger = null;
    CompletableFuture<Object> streamBlocker = new CompletableFuture<Object>();

    Endpoints.ApiServiceDescriptor apiServiceDescriptor =
        Endpoints.ApiServiceDescriptor.newBuilder()
            .setUrl(this.getClass().getName() + "-" + UUID.randomUUID().toString())
            .build();
    Server server =
        InProcessServerBuilder.forName(apiServiceDescriptor.getUrl())
            .addService(
                new BeamFnLoggingGrpc.BeamFnLoggingImplBase() {
                  @Override
                  public StreamObserver<BeamFnApi.LogEntry.List> logging(
                      StreamObserver<BeamFnApi.LogControl> outboundObserver) {
                    // Block before returning an error on the stream so that we can observe the
                    // loggers before they are reset.
                    streamBlocker.join();
                    outboundServerObserver.set(outboundObserver);
                    outboundObserver.onError(
                        Status.INTERNAL.withDescription("TEST ERROR").asException());
                    return inboundServerObserver;
                  }
                })
            .build();
    server.start();

    ManagedChannel channel = InProcessChannelBuilder.forName(apiServiceDescriptor.getUrl()).build();
    try {
      BeamFnLoggingClient client =
          BeamFnLoggingClient.createAndStart(
              PipelineOptionsFactory.fromArgs(
                      new String[] {
                        "--defaultSdkHarnessLogLevel=OFF",
                        "--sdkHarnessLogLevelOverrides={\"ConfiguredLogger\": \"DEBUG\"}"
                      })
                  .create(),
              apiServiceDescriptor,
              (Endpoints.ApiServiceDescriptor descriptor) -> channel);
      // The loggers should be installed before createAndStart returns.
      rootLogger = LogManager.getLogManager().getLogger("");
      configuredLogger = LogManager.getLogManager().getLogger("ConfiguredLogger");
      // Allow the stream to return with an error.
      streamBlocker.complete(new Object());
      thrown.expectMessage("TEST ERROR");
      client.close();
    } finally {
      assertNotNull("rootLogger should be initialized before exception", rootLogger);
      assertNotNull("configuredLogger should be initialized before exception", configuredLogger);

      // Verify that after close, log levels are reset.
      assertEquals(Level.INFO, rootLogger.getLevel());
      assertNull(configuredLogger.getLevel());

      assertTrue(channel.isShutdown());

      server.shutdownNow();
    }
  }

  @Test
  public void testWhenServerHangsUpEarlyThatClientIsAbleCleanup() throws Exception {
    BeamFnLoggingMDC.setInstructionId("instruction-1");
    Collection<BeamFnApi.LogEntry> values = new ConcurrentLinkedQueue<>();
    AtomicReference<StreamObserver<BeamFnApi.LogControl>> outboundServerObserver =
        new AtomicReference<>();
    CallStreamObserver<BeamFnApi.LogEntry.List> inboundServerObserver =
        TestStreams.withOnNext(
                (BeamFnApi.LogEntry.List logEntries) ->
                    values.addAll(logEntries.getLogEntriesList()))
            .build();

    Endpoints.ApiServiceDescriptor apiServiceDescriptor =
        Endpoints.ApiServiceDescriptor.newBuilder()
            .setUrl(this.getClass().getName() + "-" + UUID.randomUUID().toString())
            .build();
    Server server =
        InProcessServerBuilder.forName(apiServiceDescriptor.getUrl())
            .addService(
                new BeamFnLoggingGrpc.BeamFnLoggingImplBase() {
                  @Override
                  public StreamObserver<BeamFnApi.LogEntry.List> logging(
                      StreamObserver<BeamFnApi.LogControl> outboundObserver) {
                    outboundServerObserver.set(outboundObserver);
                    outboundObserver.onCompleted();
                    return inboundServerObserver;
                  }
                })
            .build();
    server.start();
    thrown.expectMessage("Logging stream terminated unexpectedly");

    ManagedChannel channel = InProcessChannelBuilder.forName(apiServiceDescriptor.getUrl()).build();
    try {
      BeamFnLoggingClient client =
          BeamFnLoggingClient.createAndStart(
              PipelineOptionsFactory.fromArgs(
                      new String[] {
                        "--defaultSdkHarnessLogLevel=OFF",
                        "--sdkHarnessLogLevelOverrides={\"ConfiguredLogger\": \"DEBUG\"}"
                      })
                  .create(),
              apiServiceDescriptor,
              (Endpoints.ApiServiceDescriptor descriptor) -> channel);

      // Keep a strong reference to the loggers in this block. Otherwise the call to client.close()
      // removes the only reference and the logger may get GC'd before the assertions (BEAM-4136).
      Logger rootLogger = LogManager.getLogManager().getLogger("");
      Logger configuredLogger = LogManager.getLogManager().getLogger("ConfiguredLogger");
      client.close();

      // Verify that after close, log levels are reset.
      assertEquals(Level.INFO, rootLogger.getLevel());
      assertNull(configuredLogger.getLevel());
    } finally {
      assertTrue(channel.isShutdown());

      server.shutdownNow();
    }
  }

  @Test
  public void testClosableWhenBlockingForOnReady() throws Exception {
    BeamFnLoggingMDC.setInstructionId("instruction-1");
    AtomicInteger testEntriesObserved = new AtomicInteger();
    AtomicBoolean onReadyBlocking = new AtomicBoolean();
    AtomicReference<StreamObserver<BeamFnApi.LogControl>> outboundServerObserver =
        new AtomicReference<>();

    final AtomicBoolean elementsAllowed = new AtomicBoolean(true);
    CallStreamObserver<BeamFnApi.LogEntry.List> inboundServerObserver =
        TestStreams.withOnNext(
                (BeamFnApi.LogEntry.List logEntries) -> {
                  for (BeamFnApi.LogEntry entry : logEntries.getLogEntriesList()) {
                    if (entry.toBuilder().clearCustomData().build().equals(TEST_ENTRY)) {
                      testEntriesObserved.addAndGet(1);
                    }
                  }
                })
            .withOnCompleted(() -> outboundServerObserver.get().onCompleted())
            .build();

    Endpoints.ApiServiceDescriptor apiServiceDescriptor =
        Endpoints.ApiServiceDescriptor.newBuilder()
            .setUrl(this.getClass().getName() + "-" + UUID.randomUUID().toString())
            .build();
    Server server =
        InProcessServerBuilder.forName(apiServiceDescriptor.getUrl())
            .addService(
                new BeamFnLoggingGrpc.BeamFnLoggingImplBase() {
                  @Override
                  public StreamObserver<BeamFnApi.LogEntry.List> logging(
                      StreamObserver<BeamFnApi.LogControl> outboundObserver) {
                    outboundServerObserver.set(outboundObserver);
                    return inboundServerObserver;
                  }
                })
            .build();
    server.start();

    ManagedChannel channel =
        InProcessChannelBuilder.forName(apiServiceDescriptor.getUrl())
            .intercept(
                new ClientInterceptor() {
                  @Override
                  public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(
                      MethodDescriptor<ReqT, RespT> method, CallOptions callOptions, Channel next) {
                    ClientCall<ReqT, RespT> delegate = next.newCall(method, callOptions);
                    return new ForwardingClientCall.SimpleForwardingClientCall<ReqT, RespT>(
                        delegate) {
                      @Override
                      public boolean isReady() {
                        if (elementsAllowed.get()) {
                          return true;
                        }
                        onReadyBlocking.set(true);
                        return elementsAllowed.get();
                      }
                    };
                  }
                })
            .build();

    // Keep a strong reference to the loggers. Otherwise the call to client.close()
    // removes the only reference and the logger may get GC'd before the assertions (BEAM-4136).
    Logger rootLogger = null;
    Logger configuredLogger = null;

    try {
      BeamFnLoggingClient client =
          BeamFnLoggingClient.createAndStart(
              PipelineOptionsFactory.fromArgs(
                      new String[] {
                        "--defaultSdkHarnessLogLevel=OFF",
                        "--sdkHarnessLogLevelOverrides={\"ConfiguredLogger\": \"DEBUG\"}"
                      })
                  .create(),
              apiServiceDescriptor,
              (Endpoints.ApiServiceDescriptor descriptor) -> channel);

      rootLogger = LogManager.getLogManager().getLogger("");
      configuredLogger = LogManager.getLogManager().getLogger("ConfiguredLogger");

      long numEntries = 2000;
      for (int i = 0; i < numEntries; ++i) {
        configuredLogger.log(TEST_RECORD);
      }
      // Measure how long it takes all the logs to appear.
      int sleepTime = 0;
      while (testEntriesObserved.get() < numEntries) {
        ++sleepTime;
        Thread.sleep(1);
      }
      // Attempt to enter the blocking state by pushing back on the stream, publishing records and
      // then giving them time for it to block.
      elementsAllowed.set(false);
      int postAllowedLogs = 0;
      while (!onReadyBlocking.get()) {
        ++postAllowedLogs;
        configuredLogger.log(TEST_RECORD);
        Thread.sleep(1);
      }

      // Even with sleeping to give some additional time for the logs that were sent by the client
      // to be observed by the server we should not observe all the client logs, indicating we're
      // blocking as intended.
      Thread.sleep(sleepTime * 3);
      assertTrue(testEntriesObserved.get() < numEntries + postAllowedLogs);

      // Allow entries to drain to speed up close.
      elementsAllowed.set(true);

      client.close();

      assertNotNull("rootLogger should be initialized before exception", rootLogger);
      assertNotNull("configuredLogger should be initialized before exception", rootLogger);

      // Verify that after stream terminates, log levels are reset.
      assertEquals(Level.INFO, rootLogger.getLevel());
      assertNull(configuredLogger.getLevel());

      assertTrue(channel.isShutdown());
    } finally {
      server.shutdownNow();
    }
  }

  @Test
  public void testServerCloseNotifiesTermination() throws Exception {
    BeamFnLoggingMDC.setInstructionId("instruction-1");
    AtomicReference<StreamObserver<BeamFnApi.LogControl>> outboundServerObserver =
        new AtomicReference<>();
    CallStreamObserver<BeamFnApi.LogEntry.List> inboundServerObserver =
        TestStreams.withOnNext((BeamFnApi.LogEntry.List logEntries) -> {})
            .withOnCompleted(() -> outboundServerObserver.get().onCompleted())
            .build();

    Endpoints.ApiServiceDescriptor apiServiceDescriptor =
        Endpoints.ApiServiceDescriptor.newBuilder()
            .setUrl(this.getClass().getName() + "-" + UUID.randomUUID().toString())
            .build();
    Server server =
        InProcessServerBuilder.forName(apiServiceDescriptor.getUrl())
            .addService(
                new BeamFnLoggingGrpc.BeamFnLoggingImplBase() {
                  @Override
                  public StreamObserver<BeamFnApi.LogEntry.List> logging(
                      StreamObserver<BeamFnApi.LogControl> outboundObserver) {
                    outboundServerObserver.set(outboundObserver);
                    outboundObserver.onCompleted();
                    return inboundServerObserver;
                  }
                })
            .build();
    server.start();

    ManagedChannel channel = InProcessChannelBuilder.forName(apiServiceDescriptor.getUrl()).build();
    try {
      BeamFnLoggingClient client =
          BeamFnLoggingClient.createAndStart(
              PipelineOptionsFactory.fromArgs(
                      new String[] {
                        "--defaultSdkHarnessLogLevel=OFF",
                        "--sdkHarnessLogLevelOverrides={\"ConfiguredLogger\": \"DEBUG\"}"
                      })
                  .create(),
              apiServiceDescriptor,
              (Endpoints.ApiServiceDescriptor descriptor) -> channel);

      thrown.expectMessage("Logging stream terminated unexpectedly");
      client.terminationFuture().get();
    } finally {
      // Verify that after termination, log levels are reset.
      assertEquals(Level.INFO, LogManager.getLogManager().getLogger("").getLevel());
      assertNull(LogManager.getLogManager().getLogger("ConfiguredLogger").getLevel());

      assertTrue(channel.isShutdown());
      server.shutdownNow();
    }
  }
}
