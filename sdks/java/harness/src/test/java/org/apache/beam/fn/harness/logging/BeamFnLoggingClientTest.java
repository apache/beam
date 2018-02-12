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

import static com.google.common.base.Throwables.getStackTraceAsString;
import static org.hamcrest.Matchers.contains;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import com.google.protobuf.Timestamp;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.Status;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.CallStreamObserver;
import io.grpc.stub.StreamObserver;
import java.util.Collection;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Level;
import java.util.logging.LogManager;
import java.util.logging.LogRecord;
import org.apache.beam.model.fnexecution.v1.BeamFnApi;
import org.apache.beam.model.fnexecution.v1.BeamFnLoggingGrpc;
import org.apache.beam.model.pipeline.v1.Endpoints;
import org.apache.beam.sdk.fn.test.TestStreams;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link BeamFnLoggingClient}. */
@RunWith(JUnit4.class)
public class BeamFnLoggingClientTest {

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
          .setSeverity(BeamFnApi.LogEntry.Severity.Enum.DEBUG)
          .setMessage("Message")
          .setThread("12345")
          .setTimestamp(Timestamp.newBuilder().setSeconds(1234567).setNanos(890000000).build())
          .setLogLocation("LoggerName")
          .build();
  private static final BeamFnApi.LogEntry TEST_ENTRY_WITH_EXCEPTION =
      BeamFnApi.LogEntry.newBuilder()
          .setSeverity(BeamFnApi.LogEntry.Severity.Enum.WARN)
          .setMessage("MessageWithException")
          .setTrace(getStackTraceAsString(TEST_RECORD_WITH_EXCEPTION.getThrown()))
          .setThread("12345")
          .setTimestamp(Timestamp.newBuilder().setSeconds(1234567).setNanos(890000000).build())
          .setLogLocation("LoggerName")
          .build();
  @Rule public ExpectedException thrown = ExpectedException.none();

  @Test
  public void testLogging() throws Exception {
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
    Server server = InProcessServerBuilder.forName(apiServiceDescriptor.getUrl())
            .addService(new BeamFnLoggingGrpc.BeamFnLoggingImplBase() {
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
        InProcessChannelBuilder.forName(apiServiceDescriptor.getUrl()).build();
    try {

      BeamFnLoggingClient client = new BeamFnLoggingClient(
          PipelineOptionsFactory.fromArgs(new String[] {
              "--defaultSdkHarnessLogLevel=OFF",
              "--sdkHarnessLogLevelOverrides={\"ConfiguredLogger\": \"DEBUG\"}"
          }).create(),
          apiServiceDescriptor,
          (Endpoints.ApiServiceDescriptor descriptor) -> channel);

      // Ensure that log levels were correctly set.
      assertEquals(Level.OFF,
          LogManager.getLogManager().getLogger("").getLevel());
      assertEquals(Level.FINE,
          LogManager.getLogManager().getLogger("ConfiguredLogger").getLevel());

      // Should be filtered because the default log level override is OFF
      LogManager.getLogManager().getLogger("").log(FILTERED_RECORD);
      // Should not be filtered because the default log level override for ConfiguredLogger is DEBUG
      LogManager.getLogManager().getLogger("ConfiguredLogger").log(TEST_RECORD);
      LogManager.getLogManager().getLogger("ConfiguredLogger").log(TEST_RECORD_WITH_EXCEPTION);
      client.close();

      // Verify that after close, log levels are reset.
      assertEquals(Level.INFO, LogManager.getLogManager().getLogger("").getLevel());
      assertNull(LogManager.getLogManager().getLogger("ConfiguredLogger").getLevel());

      assertTrue(clientClosedStream.get());
      assertTrue(channel.isShutdown());
      assertThat(values, contains(TEST_ENTRY, TEST_ENTRY_WITH_EXCEPTION));
    } finally {
      server.shutdownNow();
    }
  }

  @Test
  public void testWhenServerFailsThatClientIsAbleToCleanup() throws Exception {
    Collection<BeamFnApi.LogEntry> values = new ConcurrentLinkedQueue<>();
    AtomicReference<StreamObserver<BeamFnApi.LogControl>> outboundServerObserver =
        new AtomicReference<>();
    CallStreamObserver<BeamFnApi.LogEntry.List> inboundServerObserver = TestStreams.withOnNext(
        (BeamFnApi.LogEntry.List logEntries) -> values.addAll(logEntries.getLogEntriesList()))
        .build();

    Endpoints.ApiServiceDescriptor apiServiceDescriptor =
        Endpoints.ApiServiceDescriptor.newBuilder()
            .setUrl(this.getClass().getName() + "-" + UUID.randomUUID().toString())
            .build();
    Server server = InProcessServerBuilder.forName(apiServiceDescriptor.getUrl())
        .addService(new BeamFnLoggingGrpc.BeamFnLoggingImplBase() {
          @Override
          public StreamObserver<BeamFnApi.LogEntry.List> logging(
              StreamObserver<BeamFnApi.LogControl> outboundObserver) {
            outboundServerObserver.set(outboundObserver);
            outboundObserver.onError(Status.INTERNAL.withDescription("TEST ERROR").asException());
            return inboundServerObserver;
          }
        })
        .build();
    server.start();

    ManagedChannel channel =
        InProcessChannelBuilder.forName(apiServiceDescriptor.getUrl()).build();
    try {
      BeamFnLoggingClient client = new BeamFnLoggingClient(
          PipelineOptionsFactory.fromArgs(new String[] {
              "--defaultSdkHarnessLogLevel=OFF",
              "--sdkHarnessLogLevelOverrides={\"ConfiguredLogger\": \"DEBUG\"}"
          }).create(),
          apiServiceDescriptor,
          (Endpoints.ApiServiceDescriptor descriptor) -> channel);

      thrown.expectMessage("TEST ERROR");
      client.close();
    } finally {
      // Verify that after close, log levels are reset.
      assertEquals(Level.INFO, LogManager.getLogManager().getLogger("").getLevel());
      assertNull(LogManager.getLogManager().getLogger("ConfiguredLogger").getLevel());

      assertTrue(channel.isShutdown());

      server.shutdownNow();
    }
  }

  @Test
  public void testWhenServerHangsUpEarlyThatClientIsAbleCleanup() throws Exception {
    Collection<BeamFnApi.LogEntry> values = new ConcurrentLinkedQueue<>();
    AtomicReference<StreamObserver<BeamFnApi.LogControl>> outboundServerObserver =
        new AtomicReference<>();
    CallStreamObserver<BeamFnApi.LogEntry.List> inboundServerObserver =
        TestStreams.withOnNext(
            (BeamFnApi.LogEntry.List logEntries) -> values.addAll(logEntries.getLogEntriesList()))
            .build();

    Endpoints.ApiServiceDescriptor apiServiceDescriptor =
        Endpoints.ApiServiceDescriptor.newBuilder()
            .setUrl(this.getClass().getName() + "-" + UUID.randomUUID().toString())
            .build();
    Server server = InProcessServerBuilder.forName(apiServiceDescriptor.getUrl())
        .addService(new BeamFnLoggingGrpc.BeamFnLoggingImplBase() {
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

    ManagedChannel channel =
        InProcessChannelBuilder.forName(apiServiceDescriptor.getUrl()).build();
    try {
      BeamFnLoggingClient client = new BeamFnLoggingClient(
          PipelineOptionsFactory.fromArgs(new String[] {
              "--defaultSdkHarnessLogLevel=OFF",
              "--sdkHarnessLogLevelOverrides={\"ConfiguredLogger\": \"DEBUG\"}"
          }).create(),
          apiServiceDescriptor,
          (Endpoints.ApiServiceDescriptor descriptor) -> channel);

      client.close();
    } finally {
      // Verify that after close, log levels are reset.
      assertEquals(Level.INFO, LogManager.getLogManager().getLogger("").getLevel());
      assertNull(LogManager.getLogManager().getLogger("ConfiguredLogger").getLevel());

      assertTrue(channel.isShutdown());

      server.shutdownNow();
    }
  }
}
