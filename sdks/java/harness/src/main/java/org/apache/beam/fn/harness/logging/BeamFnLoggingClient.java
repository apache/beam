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

import com.google.common.base.MoreObjects;
import com.google.common.collect.ImmutableMap;
import com.google.protobuf.Timestamp;
import io.grpc.ManagedChannel;
import io.grpc.Status;
import io.grpc.stub.CallStreamObserver;
import io.grpc.stub.ClientCallStreamObserver;
import io.grpc.stub.ClientResponseObserver;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.Phaser;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.logging.Formatter;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogManager;
import java.util.logging.LogRecord;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;
import org.apache.beam.model.fnexecution.v1.BeamFnApi;
import org.apache.beam.model.fnexecution.v1.BeamFnLoggingGrpc;
import org.apache.beam.model.pipeline.v1.Endpoints;
import org.apache.beam.sdk.extensions.gcp.options.GcsOptions;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.SdkHarnessOptions;

/**
 * Configures {@link java.util.logging} to send all {@link LogRecord}s via the Beam Fn Logging API.
 */
public class BeamFnLoggingClient implements AutoCloseable {
  private static final String ROOT_LOGGER_NAME = "";
  private static final ImmutableMap<Level, BeamFnApi.LogEntry.Severity.Enum> LOG_LEVEL_MAP =
      ImmutableMap.<Level, BeamFnApi.LogEntry.Severity.Enum>builder()
      .put(Level.SEVERE, BeamFnApi.LogEntry.Severity.Enum.ERROR)
      .put(Level.WARNING, BeamFnApi.LogEntry.Severity.Enum.WARN)
      .put(Level.INFO, BeamFnApi.LogEntry.Severity.Enum.INFO)
      .put(Level.FINE, BeamFnApi.LogEntry.Severity.Enum.DEBUG)
      .put(Level.FINEST, BeamFnApi.LogEntry.Severity.Enum.TRACE)
      .build();

  private static final ImmutableMap<SdkHarnessOptions.LogLevel, Level> LEVEL_CONFIGURATION =
      ImmutableMap.<SdkHarnessOptions.LogLevel, Level>builder()
          .put(SdkHarnessOptions.LogLevel.OFF, Level.OFF)
          .put(SdkHarnessOptions.LogLevel.ERROR, Level.SEVERE)
          .put(SdkHarnessOptions.LogLevel.WARN, Level.WARNING)
          .put(SdkHarnessOptions.LogLevel.INFO, Level.INFO)
          .put(SdkHarnessOptions.LogLevel.DEBUG, Level.FINE)
          .put(SdkHarnessOptions.LogLevel.TRACE, Level.FINEST)
          .build();

  private static final Formatter FORMATTER = new SimpleFormatter();

  /**
   * The number of log messages that will be buffered. Assuming log messages are at most 1 KiB,
   * this represents a buffer of about 10 MiBs.
   */
  private static final int MAX_BUFFERED_LOG_ENTRY_COUNT = 10_000;

  private static final Object COMPLETED = new Object();

  /* We need to store a reference to the configured loggers so that they are not
   * garbage collected. java.util.logging only has weak references to the loggers
   * so if they are garbage collected, our hierarchical configuration will be lost. */
  private final Collection<Logger> configuredLoggers;
  private final Endpoints.ApiServiceDescriptor apiServiceDescriptor;
  private final ManagedChannel channel;
  private final CallStreamObserver<BeamFnApi.LogEntry.List> outboundObserver;
  private final LogControlObserver inboundObserver;
  private final LogRecordHandler logRecordHandler;
  private final CompletableFuture<Object> inboundObserverCompletion;
  private final Phaser phaser;

  public BeamFnLoggingClient(
      PipelineOptions options,
      Endpoints.ApiServiceDescriptor apiServiceDescriptor,
      Function<Endpoints.ApiServiceDescriptor, ManagedChannel> channelFactory) {
    this.apiServiceDescriptor = apiServiceDescriptor;
    this.inboundObserverCompletion = new CompletableFuture<>();
    this.configuredLoggers = new ArrayList<>();
    this.phaser = new Phaser(1);
    this.channel = channelFactory.apply(apiServiceDescriptor);

    // Reset the global log manager, get the root logger and remove the default log handlers.
    LogManager logManager = LogManager.getLogManager();
    logManager.reset();
    Logger rootLogger = logManager.getLogger(ROOT_LOGGER_NAME);
    for (Handler handler : rootLogger.getHandlers()) {
      //rootLogger.removeHandler(handler);
    }

    // Use the passed in logging options to configure the various logger levels.
    SdkHarnessOptions loggingOptions = options.as(SdkHarnessOptions.class);
    if (loggingOptions.getDefaultSdkHarnessLogLevel() != null) {
      rootLogger.setLevel(LEVEL_CONFIGURATION.get(loggingOptions.getDefaultSdkHarnessLogLevel()));
    }

    if (loggingOptions.getSdkHarnessLogLevelOverrides() != null) {
      for (Map.Entry<String, SdkHarnessOptions.LogLevel> loggerOverride :
        loggingOptions.getSdkHarnessLogLevelOverrides().entrySet()) {
        Logger logger = Logger.getLogger(loggerOverride.getKey());
        logger.setLevel(LEVEL_CONFIGURATION.get(loggerOverride.getValue()));
        configuredLoggers.add(logger);
      }
    }

    BeamFnLoggingGrpc.BeamFnLoggingStub stub = BeamFnLoggingGrpc.newStub(channel);
    inboundObserver = new LogControlObserver();
    logRecordHandler = new LogRecordHandler(options.as(GcsOptions.class).getExecutorService());
    logRecordHandler.setLevel(Level.ALL);
    outboundObserver =
        (CallStreamObserver<BeamFnApi.LogEntry.List>) stub.logging(inboundObserver);
    rootLogger.addHandler(logRecordHandler);
  }

  @Override
  public void close() throws Exception {
    try {
      // Reset the logging configuration to what it is at startup
      for (Logger logger : configuredLoggers) {
        logger.setLevel(null);
      }
      configuredLoggers.clear();
      LogManager.getLogManager().readConfiguration();

      // Hang up with the server
      logRecordHandler.close();

      // Wait for the server to hang up
      inboundObserverCompletion.get();
    } finally {
      // Shut the channel down
      channel.shutdown();
      if (!channel.awaitTermination(10, TimeUnit.SECONDS)) {
        channel.shutdownNow();
      }
    }
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(BeamFnLoggingClient.class)
        .add("apiServiceDescriptor", apiServiceDescriptor)
        .toString();
  }

  private class LogRecordHandler extends Handler implements Runnable {
    private final BlockingDeque<BeamFnApi.LogEntry> bufferedLogEntries =
        new LinkedBlockingDeque<>(MAX_BUFFERED_LOG_ENTRY_COUNT);
    private final Future<?> bufferedLogWriter;
    /**
     * Safe object publishing is not required since we only care if the thread that set
     * this field is equal to the thread also attempting to add a log entry.
     */
    private Thread logEntryHandlerThread;

    private LogRecordHandler(ExecutorService executorService) {
      bufferedLogWriter = executorService.submit(this);
    }

    @Override
    public void publish(LogRecord record) {
      BeamFnApi.LogEntry.Severity.Enum severity = LOG_LEVEL_MAP.get(record.getLevel());
      if (severity == null) {
        return;
      }
      BeamFnApi.LogEntry.Builder builder = BeamFnApi.LogEntry.newBuilder()
          .setSeverity(severity)
          .setLogLocation(record.getLoggerName())
          .setMessage(FORMATTER.formatMessage(record))
          .setThread(Integer.toString(record.getThreadID()))
          .setTimestamp(Timestamp.newBuilder()
              .setSeconds(record.getMillis() / 1000)
              .setNanos((int) (record.getMillis() % 1000) * 1_000_000));
      if (record.getThrown() != null) {
        builder.setTrace(getStackTraceAsString(record.getThrown()));
      }
      // The thread that sends log records should never perform a blocking publish and
      // only insert log records best effort.
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
        bufferedLogEntries.offer(builder.build());
      }
    }

    @Override
    public void run() {
      // Logging which occurs in this thread will attempt to publish log entries into the
      // above handler which should never block if the queue is full otherwise
      // this thread will get stuck.
      logEntryHandlerThread = Thread.currentThread();

      List<BeamFnApi.LogEntry> additionalLogEntries =
          new ArrayList<>(MAX_BUFFERED_LOG_ENTRY_COUNT);
      Throwable thrown = null;
      try {
        // As long as we haven't yet terminated, then attempt
        while (!phaser.isTerminated()) {
          // Try to wait for a message to show up.
          BeamFnApi.LogEntry logEntry = bufferedLogEntries.poll(1, TimeUnit.SECONDS);
          // If we don't have a message then we need to try this loop again.
          if (logEntry == null) {
            continue;
          }

          // Attempt to honor flow control. Phaser termination causes await advance to return
          // immediately.
          int phase = phaser.getPhase();
          if (!outboundObserver.isReady()) {
            phaser.awaitAdvance(phase);
          }

          // Batch together as many log messages as possible that are held within the buffer
          BeamFnApi.LogEntry.List.Builder builder =
              BeamFnApi.LogEntry.List.newBuilder().addLogEntries(logEntry);
          bufferedLogEntries.drainTo(additionalLogEntries);
          builder.addAllLogEntries(additionalLogEntries);
          outboundObserver.onNext(builder.build());
          additionalLogEntries.clear();
        }

        // Perform one more final check to see if there are any log entries to guarantee that
        // if a log entry was added on the thread performing termination that we will send it.
        bufferedLogEntries.drainTo(additionalLogEntries);
        if (!additionalLogEntries.isEmpty()) {
          outboundObserver.onNext(
              BeamFnApi.LogEntry.List.newBuilder().addAllLogEntries(additionalLogEntries).build());
        }
      } catch (Throwable t) {
        thrown = t;
      }
      if (thrown != null) {
        outboundObserver.onError(
            Status.INTERNAL.withDescription(getStackTraceAsString(thrown)).asException());
        throw new IllegalStateException(thrown);
      } else {
        outboundObserver.onCompleted();
      }
    }

    @Override
    public void flush() {
    }

    @Override
    public synchronized void close() {
      // If we are done, then a previous caller has already shutdown the queue processing thread
      // hence we don't need to do it again.
      if (phaser.isTerminated()) {
        return;
      }

      // Terminate the phaser that we block on when attempting to honor flow control on the
      // outbound observer.
      phaser.forceTermination();

      try {
        bufferedLogWriter.get();
      } catch (CancellationException e) {
        // Ignore cancellations
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new RuntimeException(e);
      } catch (ExecutionException e) {
        throw new RuntimeException(e);
      }
    }
  }

  private class LogControlObserver
      implements ClientResponseObserver<BeamFnApi.LogEntry, BeamFnApi.LogControl> {

    @Override
    public void beforeStart(ClientCallStreamObserver requestStream) {
      requestStream.setOnReadyHandler(phaser::arrive);
    }

    @Override
    public void onNext(BeamFnApi.LogControl value) {
    }

    @Override
    public void onError(Throwable t) {
      inboundObserverCompletion.completeExceptionally(t);
    }

    @Override
    public void onCompleted() {
      inboundObserverCompletion.complete(COMPLETED);
    }

  }
}
