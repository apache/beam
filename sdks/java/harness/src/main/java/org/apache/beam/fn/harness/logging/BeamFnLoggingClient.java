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
import io.grpc.stub.StreamObserver;
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
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.logging.Formatter;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogManager;
import java.util.logging.LogRecord;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;
import org.apache.beam.fn.v1.BeamFnApi;
import org.apache.beam.fn.v1.BeamFnLoggingGrpc;
import org.apache.beam.runners.dataflow.options.DataflowWorkerLoggingOptions;
import org.apache.beam.sdk.options.GcsOptions;
import org.apache.beam.sdk.options.PipelineOptions;

/**
 * Configures {@link java.util.logging} to send all {@link LogRecord}s via the Beam Fn Logging API.
 */
public class BeamFnLoggingClient implements AutoCloseable {
  private static final String ROOT_LOGGER_NAME = "";
  private static final ImmutableMap<Level, BeamFnApi.LogEntry.Severity> LOG_LEVEL_MAP =
      ImmutableMap.<Level, BeamFnApi.LogEntry.Severity>builder()
      .put(Level.SEVERE, BeamFnApi.LogEntry.Severity.ERROR)
      .put(Level.WARNING, BeamFnApi.LogEntry.Severity.WARN)
      .put(Level.INFO, BeamFnApi.LogEntry.Severity.INFO)
      .put(Level.FINE, BeamFnApi.LogEntry.Severity.DEBUG)
      .put(Level.FINEST, BeamFnApi.LogEntry.Severity.TRACE)
      .build();

  private static final ImmutableMap<DataflowWorkerLoggingOptions.Level, Level> LEVEL_CONFIGURATION =
      ImmutableMap.<DataflowWorkerLoggingOptions.Level, Level>builder()
          .put(DataflowWorkerLoggingOptions.Level.OFF, Level.OFF)
          .put(DataflowWorkerLoggingOptions.Level.ERROR, Level.SEVERE)
          .put(DataflowWorkerLoggingOptions.Level.WARN, Level.WARNING)
          .put(DataflowWorkerLoggingOptions.Level.INFO, Level.INFO)
          .put(DataflowWorkerLoggingOptions.Level.DEBUG, Level.FINE)
          .put(DataflowWorkerLoggingOptions.Level.TRACE, Level.FINEST)
          .build();

  private static final Formatter FORMATTER = new SimpleFormatter();

  private static final String FAKE_INSTRUCTION_ID = "FAKE_INSTRUCTION_ID";

  /* Used to signal to a thread processing a queue to finish its work gracefully. */
  private static final BeamFnApi.LogEntry POISON_PILL =
      BeamFnApi.LogEntry.newBuilder().setInstructionReference(FAKE_INSTRUCTION_ID).build();

  /**
   * The number of log messages that will be buffered. Assuming log messages are at most 1 KiB,
   * this represents a buffer of about 10 MiBs.
   */
  private static final int MAX_BUFFERED_LOG_ENTRY_COUNT = 10_000;

  /* We need to store a reference to the configured loggers so that they are not
   * garbage collected. java.util.logging only has weak references to the loggers
   * so if they are garbage collected, our hierarchical configuration will be lost. */
  private final Collection<Logger> configuredLoggers;
  private final BeamFnApi.ApiServiceDescriptor apiServiceDescriptor;
  private final ManagedChannel channel;
  private final StreamObserver<BeamFnApi.LogEntry.List> outboundObserver;
  private final LogControlObserver inboundObserver;
  private final LogRecordHandler logRecordHandler;
  private final CompletableFuture<Object> inboundObserverCompletion;

  public BeamFnLoggingClient(
      PipelineOptions options,
      BeamFnApi.ApiServiceDescriptor apiServiceDescriptor,
      Function<BeamFnApi.ApiServiceDescriptor, ManagedChannel> channelFactory,
      BiFunction<Function<StreamObserver<BeamFnApi.LogControl>,
                          StreamObserver<BeamFnApi.LogEntry.List>>,
                 StreamObserver<BeamFnApi.LogControl>,
                 StreamObserver<BeamFnApi.LogEntry.List>> streamObserverFactory) {
    this.apiServiceDescriptor = apiServiceDescriptor;
    this.inboundObserverCompletion = new CompletableFuture<>();
    this.configuredLoggers = new ArrayList<>();
    this.channel = channelFactory.apply(apiServiceDescriptor);

    // Reset the global log manager, get the root logger and remove the default log handlers.
    LogManager logManager = LogManager.getLogManager();
    logManager.reset();
    Logger rootLogger = logManager.getLogger(ROOT_LOGGER_NAME);
    for (Handler handler : rootLogger.getHandlers()) {
      rootLogger.removeHandler(handler);
    }

    // Use the passed in logging options to configure the various logger levels.
    DataflowWorkerLoggingOptions loggingOptions = options.as(DataflowWorkerLoggingOptions.class);
    if (loggingOptions.getDefaultWorkerLogLevel() != null) {
      rootLogger.setLevel(LEVEL_CONFIGURATION.get(loggingOptions.getDefaultWorkerLogLevel()));
    }

    if (loggingOptions.getWorkerLogLevelOverrides() != null) {
      for (Map.Entry<String, DataflowWorkerLoggingOptions.Level> loggerOverride :
        loggingOptions.getWorkerLogLevelOverrides().entrySet()) {
        Logger logger = Logger.getLogger(loggerOverride.getKey());
        logger.setLevel(LEVEL_CONFIGURATION.get(loggerOverride.getValue()));
        configuredLoggers.add(logger);
      }
    }

    BeamFnLoggingGrpc.BeamFnLoggingStub stub = BeamFnLoggingGrpc.newStub(channel);
    inboundObserver = new LogControlObserver();
    logRecordHandler = new LogRecordHandler(options.as(GcsOptions.class).getExecutorService());
    logRecordHandler.setLevel(Level.ALL);
    outboundObserver = streamObserverFactory.apply(stub::logging, inboundObserver);
    rootLogger.addHandler(logRecordHandler);
  }

  @Override
  public void close() throws Exception {
    // Hang up with the server
    logRecordHandler.close();

    // Wait for the server to hang up
    inboundObserverCompletion.get();

    // Reset the logging configuration to what it is at startup
    for (Logger logger : configuredLoggers) {
      logger.setLevel(null);
    }
    configuredLoggers.clear();
    LogManager.getLogManager().readConfiguration();

    // Shut the channel down
    channel.shutdown();
    if (!channel.awaitTermination(10, TimeUnit.SECONDS)) {
      channel.shutdownNow();
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
    private final ThreadLocal<Consumer<BeamFnApi.LogEntry>> logEntryHandler;

    private LogRecordHandler(ExecutorService executorService) {
      bufferedLogWriter = executorService.submit(this);
      logEntryHandler = new ThreadLocal<>();
    }

    @Override
    public void publish(LogRecord record) {
      BeamFnApi.LogEntry.Severity severity = LOG_LEVEL_MAP.get(record.getLevel());
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
      // only insert log records best effort. We detect which thread is logging
      // by using the thread local, defaulting to the blocking publish.
      MoreObjects.firstNonNull(
          logEntryHandler.get(), this::blockingPublish).accept(builder.build());
    }

    /** Blocks caller till enough space exists to publish this log entry. */
    private void blockingPublish(BeamFnApi.LogEntry logEntry) {
      try {
        bufferedLogEntries.put(logEntry);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new RuntimeException(e);
      }
    }

    @Override
    public void run() {
      // Logging which occurs in this thread will attempt to publish log entries into the
      // above handler which should never block if the queue is full otherwise
      // this thread will get stuck.
      logEntryHandler.set(bufferedLogEntries::offer);
      List<BeamFnApi.LogEntry> additionalLogEntries =
          new ArrayList<>(MAX_BUFFERED_LOG_ENTRY_COUNT);
      try {
        BeamFnApi.LogEntry logEntry;
        while ((logEntry = bufferedLogEntries.take()) != POISON_PILL) {
          BeamFnApi.LogEntry.List.Builder builder =
              BeamFnApi.LogEntry.List.newBuilder().addLogEntries(logEntry);
          bufferedLogEntries.drainTo(additionalLogEntries);
          for (int i = 0; i < additionalLogEntries.size(); ++i) {
            if (additionalLogEntries.get(i) == POISON_PILL) {
              additionalLogEntries = additionalLogEntries.subList(0, i);
              break;
            }
          }
          builder.addAllLogEntries(additionalLogEntries);
          outboundObserver.onNext(builder.build());
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new IllegalStateException(e);
      }
    }

    @Override
    public void flush() {
    }

    @Override
    public void close() {
      synchronized (outboundObserver) {
        // If we are done, then a previous caller has already shutdown the queue processing thread
        // hence we don't need to do it again.
        if (!bufferedLogWriter.isDone()) {
          // We check to see if we were able to successfully insert the poison pill at the end of
          // the queue forcing the remainder of the elements to be processed or if the processing
          // thread is done.
          try {
            // The order of these checks is important because short circuiting will cause us to
            // insert into the queue first and only if it fails do we check that the thread is done.
            while (!bufferedLogEntries.offer(POISON_PILL, 60, TimeUnit.SECONDS)
                || !bufferedLogWriter.isDone()) {
            }
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
          }
          waitTillFinish();
        }
        outboundObserver.onCompleted();
      }
    }

    private void waitTillFinish() {
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

  private class LogControlObserver implements StreamObserver<BeamFnApi.LogControl> {
    @Override
    public void onNext(BeamFnApi.LogControl value) {
    }

    @Override
    public void onError(Throwable t) {
      inboundObserverCompletion.completeExceptionally(t);
    }

    @Override
    public void onCompleted() {
      inboundObserverCompletion.complete(null);
    }
  }
}
