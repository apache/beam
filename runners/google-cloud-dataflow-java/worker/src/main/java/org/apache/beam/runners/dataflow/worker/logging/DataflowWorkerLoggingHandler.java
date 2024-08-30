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
package org.apache.beam.runners.dataflow.worker.logging;

import static org.apache.beam.runners.dataflow.worker.logging.DataflowWorkerLoggingInitializer.LEVELS;

import com.fasterxml.jackson.core.JsonEncoding;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.util.MinimalPrettyPrinter;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.EnumMap;
import java.util.logging.ErrorManager;
import java.util.logging.Handler;
import java.util.logging.LogRecord;
import java.util.logging.SimpleFormatter;
import org.apache.beam.model.fnexecution.v1.BeamFnApi;
import org.apache.beam.runners.core.metrics.ExecutionStateTracker;
import org.apache.beam.runners.core.metrics.ExecutionStateTracker.ExecutionState;
import org.apache.beam.runners.dataflow.worker.DataflowOperationContext.DataflowExecutionState;
import org.apache.beam.runners.dataflow.worker.counters.NameContext;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.MoreObjects;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Supplier;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.io.CountingOutputStream;

/**
 * Formats {@link LogRecord} into JSON format for Cloud Logging. Any exception is represented using
 * {@link Throwable#printStackTrace()}.
 */
@SuppressWarnings({
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
public class DataflowWorkerLoggingHandler extends Handler {
  private static final EnumMap<BeamFnApi.LogEntry.Severity.Enum, String>
      BEAM_LOG_LEVEL_TO_CLOUD_LOG_LEVEL;

  static {
    BEAM_LOG_LEVEL_TO_CLOUD_LOG_LEVEL = new EnumMap<>(BeamFnApi.LogEntry.Severity.Enum.class);
    // Note that Google Cloud Logging only defines a fixed number of severities and maps "TRACE"
    // onto "DEBUG" as seen here: https://github.com/GoogleCloudPlatform/fluent-plugin-google-cloud
    // /blob/8a3ba9d085702c13b4f203812ee5dffdaf99572a/lib/fluent/plugin/out_google_cloud.rb#L865
    BEAM_LOG_LEVEL_TO_CLOUD_LOG_LEVEL.put(BeamFnApi.LogEntry.Severity.Enum.TRACE, "DEBUG");
    BEAM_LOG_LEVEL_TO_CLOUD_LOG_LEVEL.put(BeamFnApi.LogEntry.Severity.Enum.DEBUG, "DEBUG");
    BEAM_LOG_LEVEL_TO_CLOUD_LOG_LEVEL.put(BeamFnApi.LogEntry.Severity.Enum.INFO, "INFO");
    BEAM_LOG_LEVEL_TO_CLOUD_LOG_LEVEL.put(BeamFnApi.LogEntry.Severity.Enum.NOTICE, "NOTICE");
    // Note that Google Cloud Logging only defines a fixed number of severities and maps "WARN" onto
    // "WARNING" as seen here: https://github.com/GoogleCloudPlatform/fluent-plugin-google-cloud
    // /blob/8a3ba9d085702c13b4f203812ee5dffdaf99572a/lib/fluent/plugin/out_google_cloud.rb#L865
    BEAM_LOG_LEVEL_TO_CLOUD_LOG_LEVEL.put(BeamFnApi.LogEntry.Severity.Enum.WARN, "WARNING");
    BEAM_LOG_LEVEL_TO_CLOUD_LOG_LEVEL.put(BeamFnApi.LogEntry.Severity.Enum.ERROR, "ERROR");
    BEAM_LOG_LEVEL_TO_CLOUD_LOG_LEVEL.put(BeamFnApi.LogEntry.Severity.Enum.CRITICAL, "CRITICAL");
  }

  /**
   * Buffer size to use when writing logs. This matches <a
   * href="https://cloud.google.com/logging/quotas#log-limits">Logging usage limits</a> to avoid
   * spreading the same log entry across multiple disk flushes.
   */
  private static final int LOGGING_WRITER_BUFFER_SIZE = 262144; // 256kb

  /**
   * Formats the throwable as per {@link Throwable#printStackTrace()}.
   *
   * @param thrown The throwable to format.
   * @return A string containing the contents of {@link Throwable#printStackTrace()}.
   */
  public static String formatException(Throwable thrown) {
    if (thrown == null) {
      return null;
    }
    StringWriter sw = new StringWriter();
    try (PrintWriter pw = new PrintWriter(sw)) {
      thrown.printStackTrace(pw);
    }
    return sw.toString();
  }

  /** Constructs a handler that writes to a rotating set of files. */
  public DataflowWorkerLoggingHandler(String filename, long sizeLimit) throws IOException {
    this(new FileOutputStreamFactory(filename), sizeLimit);
  }

  /**
   * Constructs a handler that writes to arbitrary output streams. No rollover if sizeLimit is zero
   * or negative.
   */
  DataflowWorkerLoggingHandler(Supplier<OutputStream> factory, long sizeLimit) throws IOException {
    this.setFormatter(new SimpleFormatter());
    this.outputStreamFactory = factory;
    this.generatorFactory =
        new ObjectMapper()
            // Required to avoid flushing to the file in the middle of a log message and potentially
            // breaking its JSON formatting, if the first part is read from the file before the rest
            // of the message:
            .disable(SerializationFeature.FLUSH_AFTER_WRITE_VALUE)
            .getFactory();
    this.sizeLimit = sizeLimit < 1 ? Long.MAX_VALUE : sizeLimit;
    createOutputStream();
  }

  @Override
  public synchronized void publish(LogRecord record) {
    DataflowExecutionState currrentDataflowState = null;
    ExecutionState currrentState = ExecutionStateTracker.getCurrentExecutionState();
    if (currrentState instanceof DataflowExecutionState) {
      currrentDataflowState = (DataflowExecutionState) currrentState;
    }
    // It's okay to pass in the null state, publish() handles and tests this.
    publish(currrentDataflowState, record);
  }

  public synchronized void publish(DataflowExecutionState currentExecutionState, LogRecord record) {
    if (!isLoggable(record)) {
      return;
    }

    rolloverOutputStreamIfNeeded();

    try {
      // Generating a JSON map like:
      // {"timestamp": {"seconds": 1435835832, "nanos": 123456789}, ...  "message": "hello"}
      generator.writeStartObject();
      // Write the timestamp.
      generator.writeFieldName("timestamp");
      generator.writeStartObject();
      generator.writeNumberField("seconds", record.getMillis() / 1000);
      generator.writeNumberField("nanos", (record.getMillis() % 1000) * 1000000);
      generator.writeEndObject();
      // Write the severity.
      generator.writeObjectField(
          "severity",
          MoreObjects.firstNonNull(LEVELS.get(record.getLevel()), record.getLevel().getName()));
      // Write the other labels.
      writeIfNotEmpty("message", getFormatter().formatMessage(record));
      writeIfNotEmpty("thread", String.valueOf(record.getThreadID()));
      writeIfNotEmpty("job", DataflowWorkerLoggingMDC.getJobId());
      writeIfNotEmpty("stage", DataflowWorkerLoggingMDC.getStageName());

      if (currentExecutionState != null) {
        NameContext nameContext = currentExecutionState.getStepName();
        if (nameContext != null) {
          writeIfNotEmpty("step", nameContext.userName());
        }
      }
      writeIfNotEmpty("worker", DataflowWorkerLoggingMDC.getWorkerId());
      writeIfNotEmpty("work", DataflowWorkerLoggingMDC.getWorkId());
      writeIfNotEmpty("logger", record.getLoggerName());
      writeIfNotEmpty("exception", formatException(record.getThrown()));
      generator.writeEndObject();
      generator.writeRaw(System.lineSeparator());
    } catch (IOException | RuntimeException e) {
      reportFailure("Unable to publish", e, ErrorManager.WRITE_FAILURE);
    }

    // This implementation is based on that of java.util.logging.FileHandler, which flushes in a
    // synchronized context like this. Unfortunately the maximum throughput for generating log
    // entries will be the inverse of the flush latency. That could be as little as one hundred
    // log entries per second on some systems. For higher throughput this should be changed to
    // batch publish operations while writes and flushes are in flight on a different thread.
    flush();
  }

  public synchronized void publish(BeamFnApi.LogEntry logEntry) {
    if (generator == null || logEntry == null) {
      return;
    }

    rolloverOutputStreamIfNeeded();

    try {
      // Generating a JSON map like:
      // {"timestamp": {"seconds": 1435835832, "nanos": 123456789}, ...  "message": "hello"}
      generator.writeStartObject();
      // Write the timestamp.
      generator.writeFieldName("timestamp");
      generator.writeStartObject();
      generator.writeNumberField("seconds", logEntry.getTimestamp().getSeconds());
      generator.writeNumberField("nanos", logEntry.getTimestamp().getNanos());
      generator.writeEndObject();
      // Write the severity.
      generator.writeObjectField(
          "severity",
          MoreObjects.firstNonNull(
              BEAM_LOG_LEVEL_TO_CLOUD_LOG_LEVEL.get(logEntry.getSeverity()),
              logEntry.getSeverity().getValueDescriptor().getName()));
      // Write the other labels.
      writeIfNotEmpty("message", logEntry.getMessage());
      writeIfNotEmpty("thread", logEntry.getThread());
      writeIfNotEmpty("job", DataflowWorkerLoggingMDC.getJobId());
      // TODO: Write the stage execution information by translating the currently execution
      // instruction id to a stage.
      // writeIfNotNull("stage", ...);
      writeIfNotEmpty("step", logEntry.getTransformId());
      writeIfNotEmpty("worker", DataflowWorkerLoggingMDC.getWorkerId());
      // Id should match to id in //depot/google3/third_party/cloud/dataflow/worker/agent/sdk.go
      writeIfNotEmpty("portability_worker_id", DataflowWorkerLoggingMDC.getSdkHarnessId());
      writeIfNotEmpty("work", logEntry.getInstructionId());
      writeIfNotEmpty("logger", logEntry.getLogLocation());
      // TODO: Figure out a way to get exceptions transported across Beam Fn Logging API
      writeIfNotEmpty("exception", logEntry.getTrace());
      generator.writeEndObject();
      generator.writeRaw(System.lineSeparator());
    } catch (IOException | RuntimeException e) {
      reportFailure("Unable to publish", e, ErrorManager.WRITE_FAILURE);
    }

    // This implementation is based on that of java.util.logging.FileHandler, which flushes in a
    // synchronized context like this. Unfortunately the maximum throughput for generating log
    // entries will be the inverse of the flush latency. That could be as little as one hundred
    // log entries per second on some systems. For higher throughput this should be changed to
    // batch publish operations while writes and flushes are in flight on a different thread.
    flush();
  }

  /**
   * Check if a LogRecord will be logged.
   *
   * <p>This method checks if the <tt>LogRecord</tt> has an appropriate level and whether it
   * satisfies any <tt>Filter</tt>. It will also return false if the handler has been closed, or the
   * LogRecord is null.
   */
  @Override
  public boolean isLoggable(LogRecord record) {
    return generator != null && record != null && super.isLoggable(record);
  }

  @Override
  public synchronized void flush() {
    try {
      if (generator != null) {
        generator.flush();
      }
    } catch (IOException | RuntimeException e) {
      reportFailure("Unable to flush", e, ErrorManager.FLUSH_FAILURE);
    }
  }

  @Override
  public synchronized void close() {
    // Flush any in-flight content, though there should not actually be any because
    // the generator is currently flushed in the synchronized publish() method.
    flush();
    // Close the generator and log file.
    try {
      if (generator != null) {
        generator.close();
      }
    } catch (IOException | RuntimeException e) {
      reportFailure("Unable to close", e, ErrorManager.CLOSE_FAILURE);
    } finally {
      generator = null;
      counter = null;
    }
  }

  /** Unique file generator. Uses filenames with timestamp. */
  private static final class FileOutputStreamFactory implements Supplier<OutputStream> {
    private final String filepath;
    private final SimpleDateFormat formatter = new SimpleDateFormat("yyyy_MM_dd_hh_mm_ss_SSS");

    public FileOutputStreamFactory(String filepath) {
      this.filepath = filepath;
    }

    @Override
    public OutputStream get() {
      try {
        String filename = filepath + "." + formatter.format(new Date()) + ".log";
        return new BufferedOutputStream(
            new FileOutputStream(new File(filename), true /* append */),
            LOGGING_WRITER_BUFFER_SIZE);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }

  private void createOutputStream() throws IOException {
    CountingOutputStream stream = new CountingOutputStream(outputStreamFactory.get());
    generator = generatorFactory.createGenerator(stream, JsonEncoding.UTF8);
    counter = stream;

    // Avoid 1 space indent for every line. We already add a newline after each log record.
    generator.setPrettyPrinter(new MinimalPrettyPrinter(""));
  }

  /**
   * Rollover to a new output stream (log file) if we have reached the size limit. Ensure that the
   * rollover fails or succeeds atomically.
   */
  private void rolloverOutputStreamIfNeeded() {
    if (counter.getCount() < sizeLimit) {
      return;
    }

    try {
      JsonGenerator old = generator;
      createOutputStream();

      try {
        // Rollover successful. Attempt to close old stream, but ignore on failure.
        old.close();
      } catch (IOException | RuntimeException e) {
        reportFailure("Unable to close old log file", e, ErrorManager.CLOSE_FAILURE);
      }
    } catch (IOException | RuntimeException e) {
      reportFailure("Unable to create new log file", e, ErrorManager.OPEN_FAILURE);
    }
  }

  /** Appends a JSON key/value pair if the specified val is not null. */
  private void writeIfNotEmpty(String name, String val) throws IOException {
    if (val != null && !val.isEmpty()) {
      generator.writeStringField(name, val);
    }
  }

  /** Report logging failure to ErrorManager. Does not throw. */
  private void reportFailure(String message, Exception e, int code) {
    try {
      ErrorManager manager = getErrorManager();
      if (manager != null) {
        manager.error(message, e, code);
      }
    } catch (Throwable t) {
      // Failed to report logging failure. No meaningful action left.
    }
  }

  // Null after close().
  private JsonGenerator generator;
  private CountingOutputStream counter;

  private final long sizeLimit;
  private final Supplier<OutputStream> outputStreamFactory;
  private final JsonFactory generatorFactory;
}
