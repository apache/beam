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

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Throwables.getStackTraceAsString;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
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
import org.apache.beam.fn.harness.control.ProcessBundleHandler;
import org.apache.beam.fn.harness.control.ProcessBundleHandler.BundleProcessor;
import org.apache.beam.model.fnexecution.v1.BeamFnApi;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.LogEntry;
import org.apache.beam.model.fnexecution.v1.BeamFnLoggingGrpc;
import org.apache.beam.model.pipeline.v1.Endpoints;
import org.apache.beam.sdk.options.ExecutorOptions;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.SdkHarnessOptions;
import org.apache.beam.vendor.grpc.v1p54p0.com.google.protobuf.Struct;
import org.apache.beam.vendor.grpc.v1p54p0.com.google.protobuf.Timestamp;
import org.apache.beam.vendor.grpc.v1p54p0.com.google.protobuf.Value;
import org.apache.beam.vendor.grpc.v1p54p0.io.grpc.ManagedChannel;
import org.apache.beam.vendor.grpc.v1p54p0.io.grpc.Status;
import org.apache.beam.vendor.grpc.v1p54p0.io.grpc.stub.CallStreamObserver;
import org.apache.beam.vendor.grpc.v1p54p0.io.grpc.stub.ClientCallStreamObserver;
import org.apache.beam.vendor.grpc.v1p54p0.io.grpc.stub.ClientResponseObserver;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.MoreObjects;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.MDC;

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

  private static final Formatter DEFAULT_FORMATTER = new SimpleFormatter();

  /**
   * The number of log messages that will be buffered. Assuming log messages are at most 1 KiB, this
   * represents a buffer of about 10 MiBs.
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
  private @Nullable ProcessBundleHandler processBundleHandler;

  public BeamFnLoggingClient(
      PipelineOptions options,
      Endpoints.ApiServiceDescriptor apiServiceDescriptor,
      Function<Endpoints.ApiServiceDescriptor, ManagedChannel> channelFactory) {
    this.apiServiceDescriptor = apiServiceDescriptor;
    this.inboundObserverCompletion = new CompletableFuture<>();
    this.phaser = new Phaser(1);
    this.channel = channelFactory.apply(apiServiceDescriptor);

    // Reset the global log manager, get the root logger and remove the default log handlers.
    LogManager logManager = LogManager.getLogManager();
    logManager.reset();
    Logger rootLogger = logManager.getLogger(ROOT_LOGGER_NAME);
    for (Handler handler : rootLogger.getHandlers()) {
      rootLogger.removeHandler(handler);
    }
    // configure loggers from default sdk harness log level and log level overrides
    this.configuredLoggers =
        SdkHarnessOptions.getConfiguredLoggerFromOptions(options.as(SdkHarnessOptions.class));

    BeamFnLoggingGrpc.BeamFnLoggingStub stub = BeamFnLoggingGrpc.newStub(channel);
    inboundObserver = new LogControlObserver();
    logRecordHandler = new LogRecordHandler();
    logRecordHandler.setLevel(Level.ALL);
    logRecordHandler.setFormatter(DEFAULT_FORMATTER);
    logRecordHandler.executeOn(options.as(ExecutorOptions.class).getScheduledExecutorService());
    logRecordHandler.setLogMdc(options.as(SdkHarnessOptions.class).getLogMdc());
    outboundObserver = (CallStreamObserver<BeamFnApi.LogEntry.List>) stub.logging(inboundObserver);
    rootLogger.addHandler(logRecordHandler);
  }

  public void setProcessBundleHandler(ProcessBundleHandler processBundleHandler) {
    this.processBundleHandler = processBundleHandler;
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
    private final BlockingQueue<LogEntry> bufferedLogEntries =
        new ArrayBlockingQueue<>(MAX_BUFFERED_LOG_ENTRY_COUNT);
    private @Nullable Future<?> bufferedLogWriter = null;
    /**
     * Safe object publishing is not required since we only care if the thread that set this field
     * is equal to the thread also attempting to add a log entry.
     */
    private @Nullable Thread logEntryHandlerThread = null;

    private boolean logMdc = true;

    private void setLogMdc(boolean value) {
      logMdc = value;
    }

    private void executeOn(ExecutorService executorService) {
      bufferedLogWriter = executorService.submit(this);
    }

    @Override
    public void publish(LogRecord record) {
      BeamFnApi.LogEntry.Severity.Enum severity = LOG_LEVEL_MAP.get(record.getLevel());
      if (severity == null) {
        return;
      }

      BeamFnApi.LogEntry.Builder builder =
          BeamFnApi.LogEntry.newBuilder()
              .setSeverity(severity)
              .setMessage(getFormatter().formatMessage(record))
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
      if (instructionId != null && processBundleHandler != null) {
        BundleProcessor bundleProcessor =
            processBundleHandler.getBundleProcessorCache().find(instructionId);
        if (bundleProcessor != null) {
          String transformId = bundleProcessor.getStateTracker().getCurrentThreadsPTransformId();
          if (transformId != null) {
            builder.setTransformId(transformId);
          }
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
        dropIfBufferFull(builder.build());
      }
    }

    @SuppressFBWarnings("RV_RETURN_VALUE_IGNORED_BAD_PRACTICE")
    private void dropIfBufferFull(BeamFnApi.LogEntry logEntry) {
      bufferedLogEntries.offer(logEntry);
    }

    @Override
    public void run() {
      // Logging which occurs in this thread will attempt to publish log entries into the
      // above handler which should never block if the queue is full otherwise
      // this thread will get stuck.
      logEntryHandlerThread = Thread.currentThread();

      List<BeamFnApi.LogEntry> additionalLogEntries = new ArrayList<>(MAX_BUFFERED_LOG_ENTRY_COUNT);
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
    public void flush() {}

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

      if (bufferedLogWriter != null) {
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
  }

  private class LogControlObserver
      implements ClientResponseObserver<BeamFnApi.LogEntry, BeamFnApi.LogControl> {

    @Override
    public void beforeStart(ClientCallStreamObserver<BeamFnApi.LogEntry> requestStream) {
      requestStream.setOnReadyHandler(phaser::arrive);
    }

    @Override
    public void onNext(BeamFnApi.LogControl value) {}

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
