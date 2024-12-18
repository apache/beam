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

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkNotNull;
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Throwables.getStackTraceAsString;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;
import java.util.logging.Formatter;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogManager;
import java.util.logging.LogRecord;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;
import org.apache.beam.fn.harness.control.ExecutionStateSampler;
import org.apache.beam.model.fnexecution.v1.BeamFnApi;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.LogEntry;
import org.apache.beam.model.fnexecution.v1.BeamFnLoggingGrpc;
import org.apache.beam.model.pipeline.v1.Endpoints;
import org.apache.beam.sdk.fn.stream.AdvancingPhaser;
import org.apache.beam.sdk.fn.stream.DirectStreamObserver;
import org.apache.beam.sdk.options.ExecutorOptions;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.SdkHarnessOptions;
import org.apache.beam.vendor.grpc.v1p60p1.com.google.protobuf.Struct;
import org.apache.beam.vendor.grpc.v1p60p1.com.google.protobuf.Timestamp;
import org.apache.beam.vendor.grpc.v1p60p1.com.google.protobuf.Value;
import org.apache.beam.vendor.grpc.v1p60p1.io.grpc.ManagedChannel;
import org.apache.beam.vendor.grpc.v1p60p1.io.grpc.stub.CallStreamObserver;
import org.apache.beam.vendor.grpc.v1p60p1.io.grpc.stub.ClientCallStreamObserver;
import org.apache.beam.vendor.grpc.v1p60p1.io.grpc.stub.ClientResponseObserver;
import org.apache.beam.vendor.grpc.v1p60p1.io.grpc.stub.StreamObserver;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.MoreObjects;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.checkerframework.checker.initialization.qual.UnderInitialization;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.checkerframework.checker.nullness.qual.RequiresNonNull;
import org.slf4j.MDC;

/**
 * Configures {@link java.util.logging} to send all {@link LogRecord}s via the Beam Fn Logging API.
 */
public class BeamFnLoggingClient implements LoggingClient {
  private static final String ROOT_LOGGER_NAME = "";
  private static final ImmutableMap<Level, BeamFnApi.LogEntry.Severity.Enum> LOG_LEVEL_MAP =
      ImmutableMap.<Level, BeamFnApi.LogEntry.Severity.Enum>builder()
          .put(Level.SEVERE, BeamFnApi.LogEntry.Severity.Enum.ERROR)
          .put(Level.WARNING, BeamFnApi.LogEntry.Severity.Enum.WARN)
          .put(Level.INFO, BeamFnApi.LogEntry.Severity.Enum.INFO)
          .put(Level.FINE, BeamFnApi.LogEntry.Severity.Enum.DEBUG)
          .put(Level.FINEST, BeamFnApi.LogEntry.Severity.Enum.TRACE)
          .build();
  private static final ImmutableMap<BeamFnApi.LogEntry.Severity.Enum, Level> REVERSE_LOG_LEVEL_MAP =
      ImmutableMap.<BeamFnApi.LogEntry.Severity.Enum, Level>builder()
          .putAll(LOG_LEVEL_MAP.asMultimap().inverse().entries())
          .build();

  private static final Formatter DEFAULT_FORMATTER = new SimpleFormatter();

  /**
   * The number of log messages that will be buffered. Assuming log messages are at most 1 KiB, this
   * represents a buffer of about 10 MiBs.
   */
  private static final int MAX_BUFFERED_LOG_ENTRY_COUNT = 10_000;

  private static final Object COMPLETED = new Object();

  private final Endpoints.ApiServiceDescriptor apiServiceDescriptor;

  private final StreamWriter streamWriter;

  private final LogRecordHandler logRecordHandler;

  /* We need to store a reference to the configured loggers so that they are not
   * garbage collected. java.util.logging only has weak references to the loggers
   * so if they are garbage collected, our hierarchical configuration will be lost. */
  private final Collection<Logger> configuredLoggers = new ArrayList<>();

  private final BlockingQueue<LogEntry> bufferedLogEntries =
      new ArrayBlockingQueue<>(MAX_BUFFERED_LOG_ENTRY_COUNT);

  /**
   * Future that completes with the background thread consuming logs from bufferedLogEntries.
   * Completes with COMPLETED or with exception.
   */
  private final CompletableFuture<?> bufferedLogConsumer;

  /**
   * Safe object publishing is not required since we only care if the thread that set this field is
   * equal to the thread also attempting to add a log entry.
   */
  private @Nullable Thread logEntryHandlerThread = null;

  static BeamFnLoggingClient createAndStart(
      PipelineOptions options,
      Endpoints.ApiServiceDescriptor apiServiceDescriptor,
      Function<Endpoints.ApiServiceDescriptor, ManagedChannel> channelFactory) {
    BeamFnLoggingClient client =
        new BeamFnLoggingClient(
            apiServiceDescriptor,
            new StreamWriter(channelFactory.apply(apiServiceDescriptor)),
            options.as(SdkHarnessOptions.class).getLogMdc(),
            options.as(ExecutorOptions.class).getScheduledExecutorService(),
            options.as(SdkHarnessOptions.class));
    return client;
  }

  private BeamFnLoggingClient(
      Endpoints.ApiServiceDescriptor apiServiceDescriptor,
      StreamWriter streamWriter,
      boolean logMdc,
      ScheduledExecutorService executorService,
      SdkHarnessOptions options) {
    this.apiServiceDescriptor = apiServiceDescriptor;
    this.streamWriter = streamWriter;
    this.logRecordHandler = new LogRecordHandler(logMdc);
    logRecordHandler.setLevel(Level.ALL);
    logRecordHandler.setFormatter(DEFAULT_FORMATTER);

    CompletableFuture<Object> started = new CompletableFuture<>();
    this.bufferedLogConsumer =
        CompletableFuture.supplyAsync(
            () -> {
              try {
                logEntryHandlerThread = Thread.currentThread();
                installLogging(options);
                started.complete(COMPLETED);

                // Logging which occurs in this thread will attempt to publish log entries into the
                // above handler which should never block if the queue is full otherwise
                // this thread will get stuck.
                streamWriter.drainQueueToStream(bufferedLogEntries);
              } finally {
                restoreLoggers();
                // Now that loggers are restored, do a final flush of any buffered logs
                // in case they help with understanding above failures.
                flushFinalLogs();
              }
              return COMPLETED;
            },
            executorService);
    try {
      // Wait for the thread to be running and log handlers installed or an error with the thread
      // that is supposed to be consuming logs.
      CompletableFuture.anyOf(this.bufferedLogConsumer, started).get();
    } catch (ExecutionException e) {
      throw new RuntimeException("Error starting background log thread " + e.getCause());
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException(e);
    }
  }

  @RequiresNonNull("logRecordHandler")
  @RequiresNonNull("configuredLoggers")
  private void installLogging(
      @UnderInitialization BeamFnLoggingClient this, SdkHarnessOptions options) {
    // Reset the global log manager, get the root logger and remove the default log handlers.
    LogManager logManager = LogManager.getLogManager();
    logManager.reset();
    Logger rootLogger = logManager.getLogger(ROOT_LOGGER_NAME);
    for (Handler handler : rootLogger.getHandlers()) {
      rootLogger.removeHandler(handler);
    }
    // configure loggers from default sdk harness log level and log level overrides
    this.configuredLoggers.addAll(SdkHarnessOptions.getConfiguredLoggerFromOptions(options));

    // Install a handler that queues to the buffer read by the background thread.
    rootLogger.addHandler(logRecordHandler);
  }

  private static class StreamWriter {
    private final ManagedChannel channel;
    private final StreamObserver<BeamFnApi.LogEntry.List> outboundObserver;
    private final LogControlObserver inboundObserver;

    private final CompletableFuture<Object> inboundObserverCompletion;
    private final AdvancingPhaser streamPhaser;

    // Used to note we are attempting to close the logging client and to gracefully drain the
    // current logs to the stream.
    private final CompletableFuture<Object> softClosing = new CompletableFuture<>();

    public StreamWriter(ManagedChannel channel) {
      this.inboundObserverCompletion = new CompletableFuture<>();
      this.streamPhaser = new AdvancingPhaser(1);
      this.channel = channel;

      BeamFnLoggingGrpc.BeamFnLoggingStub stub = BeamFnLoggingGrpc.newStub(channel);
      this.inboundObserver = new LogControlObserver();
      this.outboundObserver =
          new DirectStreamObserver<BeamFnApi.LogEntry.List>(
              this.streamPhaser,
              (CallStreamObserver<BeamFnApi.LogEntry.List>) stub.logging(inboundObserver));
    }

    public void drainQueueToStream(BlockingQueue<BeamFnApi.LogEntry> bufferedLogEntries) {
      Throwable thrown = null;
      try {
        List<BeamFnApi.LogEntry> additionalLogEntries =
            new ArrayList<>(MAX_BUFFERED_LOG_ENTRY_COUNT);
        // As long as we haven't yet terminated the stream, then attempt to send on it.
        while (!streamPhaser.isTerminated()) {
          // We wait for a limited period so that we can evaluate if the stream closed or if
          // we are gracefully closing the client.
          BeamFnApi.LogEntry logEntry = bufferedLogEntries.poll(1, SECONDS);
          if (logEntry == null) {
            if (softClosing.isDone()) {
              break;
            }
            continue;
          }

          // Batch together as many log messages as possible that are held within the buffer
          BeamFnApi.LogEntry.List.Builder builder =
              BeamFnApi.LogEntry.List.newBuilder().addLogEntries(logEntry);
          bufferedLogEntries.drainTo(additionalLogEntries);
          builder.addAllLogEntries(additionalLogEntries);
          outboundObserver.onNext(builder.build());
          additionalLogEntries.clear();
        }
        if (inboundObserverCompletion.isDone()) {
          try {
            // If the inbound observer failed with an exception, get() will throw an
            // ExecutionException.
            inboundObserverCompletion.get();
            // Otherwise it is an error for the server to close the stream before we closed our end.
            throw new IllegalStateException(
                "Logging stream terminated unexpectedly with success before it was closed by the client.");
          } catch (ExecutionException e) {
            throw new IllegalStateException(
                "Logging stream terminated unexpectedly before it was closed by the client with error: "
                    + e.getCause());
          } catch (InterruptedException e) {
            // Should never happen because of the isDone check.
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
          }
        }
      } catch (Throwable t) {
        thrown = t;
        throw new RuntimeException(t);
      } finally {
        if (thrown == null) {
          outboundObserver.onCompleted();
        } else {
          outboundObserver.onError(thrown);
        }
        channel.shutdown();
        boolean shutdownFinished = false;
        try {
          shutdownFinished = channel.awaitTermination(10, SECONDS);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        } finally {
          if (!shutdownFinished) {
            channel.shutdownNow();
          }
        }
      }
    }

    public void softClose() {
      softClosing.complete(COMPLETED);
    }

    public void hardClose() {
      streamPhaser.forceTermination();
    }

    private class LogControlObserver
        implements ClientResponseObserver<BeamFnApi.LogEntry, BeamFnApi.LogControl> {

      @Override
      public void beforeStart(ClientCallStreamObserver<BeamFnApi.LogEntry> requestStream) {
        requestStream.setOnReadyHandler(streamPhaser::arrive);
      }

      @Override
      public void onNext(BeamFnApi.LogControl value) {}

      @Override
      public void onError(Throwable t) {
        inboundObserverCompletion.completeExceptionally(t);
        hardClose();
      }

      @Override
      public void onCompleted() {
        inboundObserverCompletion.complete(COMPLETED);
        hardClose();
      }
    }
  }

  @Override
  public void close() throws Exception {
    checkNotNull(bufferedLogConsumer, "BeamFnLoggingClient not fully started");
    try {
      try {
        // Wait for buffered log messages to drain for a short period.
        streamWriter.softClose();
        bufferedLogConsumer.get(10, SECONDS);
      } catch (TimeoutException e) {
        // Terminate the phaser that we block on when attempting to honor flow control on the
        // outbound observer.
        streamWriter.hardClose();
        // Wait for the sending thread to exit.
        bufferedLogConsumer.get();
      }
    } catch (ExecutionException e) {
      if (e.getCause() instanceof Exception) {
        throw (Exception) e.getCause();
      }
      throw e;
    }
  }

  // Reset the logging configuration to what it is at startup.
  @RequiresNonNull("configuredLoggers")
  @RequiresNonNull("logRecordHandler")
  private void restoreLoggers(@UnderInitialization BeamFnLoggingClient this) {
    for (Logger logger : configuredLoggers) {
      logger.setLevel(null);
      // Explicitly remove the installed handler in case reading the configuration fails.
      logger.removeHandler(logRecordHandler);
    }
    configuredLoggers.clear();
    LogManager.getLogManager().getLogger(ROOT_LOGGER_NAME).removeHandler(logRecordHandler);
    try {
      LogManager.getLogManager().readConfiguration();
    } catch (IOException e) {
      System.out.print("Unable to restore log managers from configuration: " + e.toString());
    }
  }

  @RequiresNonNull("bufferedLogEntries")
  void flushFinalLogs(@UnderInitialization BeamFnLoggingClient this) {
    List<BeamFnApi.LogEntry> finalLogEntries = new ArrayList<>(MAX_BUFFERED_LOG_ENTRY_COUNT);
    bufferedLogEntries.drainTo(finalLogEntries);
    for (BeamFnApi.LogEntry logEntry : finalLogEntries) {
      LogRecord logRecord =
          new LogRecord(
              checkNotNull(REVERSE_LOG_LEVEL_MAP.get(logEntry.getSeverity())),
              logEntry.getMessage());
      logRecord.setLoggerName(logEntry.getLogLocation());
      logRecord.setMillis(
          logEntry.getTimestamp().getSeconds() * 1000
              + logEntry.getTimestamp().getNanos() / 1_000_000);
      logRecord.setThreadID(Integer.parseInt(logEntry.getThread()));
      if (!logEntry.getTrace().isEmpty()) {
        logRecord.setThrown(new Throwable(logEntry.getTrace()));
      }
      LogManager.getLogManager().getLogger(ROOT_LOGGER_NAME).log(logRecord);
    }
  }

  @Override
  public CompletableFuture<?> terminationFuture() {
    checkNotNull(bufferedLogConsumer, "BeamFnLoggingClient not fully started");
    return bufferedLogConsumer;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(BeamFnLoggingClient.class)
        .add("apiServiceDescriptor", apiServiceDescriptor)
        .toString();
  }

  private class LogRecordHandler extends Handler {
    private final boolean logMdc;

    LogRecordHandler(boolean logMdc) {
      this.logMdc = logMdc;
    }

    @Override
    public void publish(LogRecord record) {
      BeamFnApi.LogEntry.Severity.Enum severity = LOG_LEVEL_MAP.get(record.getLevel());
      if (severity == null) {
        return;
      }
      if (record == null) {
        return;
      }
      String messageString = getFormatter().formatMessage(record);

      BeamFnApi.LogEntry.Builder builder =
          BeamFnApi.LogEntry.newBuilder()
              .setSeverity(severity)
              .setMessage(messageString == null ? "null" : messageString)
              .setThread(Integer.toString(record.getThreadID()))
              .setTimestamp(
                  Timestamp.newBuilder()
                      .setSeconds(record.getMillis() / 1000)
                      .setNanos((int) (record.getMillis() % 1000) * 1_000_000));

      String instructionId = BeamFnLoggingMDC.getInstructionId();
      if (instructionId != null) {
        builder.setInstructionId(instructionId);
      }

      Throwable thrown = record.getThrown();
      if (thrown != null) {
        builder.setTrace(getStackTraceAsString(thrown));
      }

      String loggerName = record.getLoggerName();
      if (loggerName != null) {
        builder.setLogLocation(loggerName);
      }

      ExecutionStateSampler.ExecutionStateTracker stateTracker = BeamFnLoggingMDC.getStateTracker();
      if (stateTracker != null) {
        String transformId = stateTracker.getCurrentThreadsPTransformId();
        if (transformId != null) {
          builder.setTransformId(transformId);
        }
      }

      if (logMdc) {
        Map<String, String> mdc = MDC.getCopyOfContextMap();
        if (mdc != null) {
          Struct.Builder customDataBuilder = builder.getCustomDataBuilder();
          mdc.forEach(
              (k, v) -> {
                customDataBuilder.putFields(k, Value.newBuilder().setStringValue(v).build());
              });
        }
      }

      // The thread that sends log records should never perform a blocking publish and
      if (Thread.currentThread() != logEntryHandlerThread) {
        // Blocks caller till enough space exists to publish this log entry.
        try {
          bufferedLogEntries.put(builder.build());
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          throw new RuntimeException(e);
        }
      } else {
        // Never blocks caller, will drop log message if buffer is full.
        dropIfBufferFull(builder.build());
      }
    }

    private boolean dropIfBufferFull(BeamFnApi.LogEntry logEntry) {
      return bufferedLogEntries.offer(logEntry);
    }

    @Override
    public void flush() {}

    @Override
    public synchronized void close() {}
  }
}
