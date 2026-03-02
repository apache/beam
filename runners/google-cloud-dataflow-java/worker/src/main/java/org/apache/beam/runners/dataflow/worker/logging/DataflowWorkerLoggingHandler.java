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
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkNotNull;
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkState;

import com.fasterxml.jackson.core.JsonEncoding;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.util.MinimalPrettyPrinter;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.google.api.gax.batching.BatchingSettings;
import com.google.api.gax.batching.FlowControlSettings;
import com.google.api.gax.batching.FlowController;
import com.google.cloud.MonitoredResource;
import com.google.cloud.ServiceOptions;
import com.google.cloud.logging.LogEntry;
import com.google.cloud.logging.Logging;
import com.google.cloud.logging.LoggingLevel;
import com.google.cloud.logging.LoggingOptions;
import com.google.cloud.logging.Payload;
import com.google.cloud.logging.Severity;
import com.google.cloud.logging.Synchronicity;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.google.errorprone.annotations.concurrent.LazyInit;
import com.google.protobuf.Struct;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Date;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;
import java.util.MissingResourceException;
import java.util.ResourceBundle;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.logging.ErrorManager;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.SimpleFormatter;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;
import org.apache.beam.runners.core.metrics.ExecutionStateTracker;
import org.apache.beam.runners.core.metrics.ExecutionStateTracker.ExecutionState;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.runners.dataflow.options.DataflowWorkerHarnessOptions;
import org.apache.beam.runners.dataflow.options.DataflowWorkerLoggingOptions;
import org.apache.beam.runners.dataflow.worker.DataflowOperationContext.DataflowExecutionState;
import org.apache.beam.runners.dataflow.worker.counters.NameContext;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.sdk.extensions.gcp.util.GceMetadataUtil;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.MoreObjects;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Supplier;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.io.CountingOutputStream;
import org.slf4j.MDC;

/**
 * Formats {@link LogRecord} into JSON format for Cloud Logging. Any exception is represented using
 * {@link Throwable#printStackTrace()}.
 */
@Internal
@SuppressWarnings("method.invocation")
public class DataflowWorkerLoggingHandler extends Handler {
  /**
   * Buffer size to use when writing logs. This matches <a
   * href="https://cloud.google.com/logging/quotas#log-limits">Logging usage limits</a> to avoid
   * spreading the same log entry across multiple disk flushes.
   */
  private static final int LOGGING_WRITER_BUFFER_SIZE = 262144; // 256kb

  // Used as a side-channel for a Logger for which the configured non-direct logging level doesn't match the default
  // logging level and which is using direct logging.
  private static class DirectHintResourceBundle extends ResourceBundle {
    private static final String LEVEL_KEY = "NonDirectLogLevel";
    private final Level nonDirectLogLevel;

    DirectHintResourceBundle(Level directLevel) {
      this.nonDirectLogLevel = directLevel;
    }

    @Override
    public String getBaseBundleName() {
      return "DataflowWorkerLoggingHandler";
    }

    @Override
    protected Object handleGetObject(@Nonnull String s) {
      if (LEVEL_KEY.equals(s)) {
        return nonDirectLogLevel;
      }
      return new MissingResourceException(
          "The only valid key is " + LEVEL_KEY, this.getClass().getName(), s);
    }

    @Override
    public @Nonnull Enumeration<String> getKeys() {
      return Iterators.asEnumeration(Iterators.singletonIterator(LEVEL_KEY));
    }
  }
  // Since there are just a couple possible levels, we cache them.
  private static final ConcurrentHashMap<Level, ResourceBundle> resourceBundles =
      new ConcurrentHashMap<>();

  @VisibleForTesting
  static ResourceBundle resourceBundleForNonDirectLogLevelHint(Level nonDirectLogLevel) {
    return resourceBundles.computeIfAbsent(nonDirectLogLevel, DirectHintResourceBundle::new);
  }

  /** If true, add SLF4J MDC to custom_data of the log message. */
  @LazyInit private boolean logCustomMdc = false;

  // All of the direct logging related fields are only initialized if enableDirectLogging is called.  Afterwards they
  // are logically final.
  @LazyInit private @Nullable Logging directLogging = null;
  @LazyInit private boolean fallbackDirectErrorsToDisk = false;
  @LazyInit private Level defaultNonDirectLogLevel = Level.ALL;
  @LazyInit private Logging.WriteOption[] directWriteOptions = new Logging.WriteOption[0];
  @LazyInit private ImmutableMap<String, String> defaultResourceLabels = ImmutableMap.of();
  @LazyInit
  private MonitoredResource steplessMonitoredResource =
      MonitoredResource.newBuilder(RESOURCE_TYPE).build();
  @LazyInit private ImmutableMap<String, String> defaultLabels = ImmutableMap.of();
  @LazyInit private @Nullable Consumer<LogEntry> testDirectLogInterceptor;

  private static final String LOG_TYPE = "dataflow.googleapis.com%2Fworker";
  private static final String RESOURCE_TYPE = "dataflow_step";
  private static final String STEP_RESOURCE_LABEL = "step_id";
  private static final int FIELD_MAX_LENGTH = 100;
  private static final int LABEL_MAX_LENGTH = 100;
  private static final int MESSAGE_MAX_LENGTH = 30_000;
  // HACK: Detect stderr logs that originate from the LoggingImpl error handling. Hopefully this can
  // be removed if
  // error handling in LoggingImpl can be customized to use the ErrorHandler.
  private static final String LOGGING_IMPL_STDERR_PREFIX = "ERROR: onFailure exception:";

  // Null after close().
  @GuardedBy("this")
  private @Nullable JsonGenerator generator;

  @GuardedBy("this")
  private @Nullable CountingOutputStream counter;

  private final long sizeLimit;
  private final Supplier<OutputStream> outputStreamFactory;
  private final JsonFactory generatorFactory;

  /**
   * Formats the throwable as per {@link Throwable#printStackTrace()}.
   *
   * @param thrown The throwable to format.
   * @return A string containing the contents of {@link Throwable#printStackTrace()}.
   */
  public static @Nullable String formatException(@Nullable Throwable thrown) {
    if (thrown == null) {
      return null;
    }
    StringWriter sw = new StringWriter();
    try (PrintWriter pw = new PrintWriter(sw)) {
      thrown.printStackTrace(pw);
    }
    return sw.toString();
  }

  /**
   * Constructs a handler that writes to arbitrary output streams. No rollover if sizeLimit is zero
   * or negative.
   */
  public DataflowWorkerLoggingHandler(String filename, long sizeLimit) throws IOException {
    this(new FileOutputStreamFactory(filename), sizeLimit);
  }

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

  public void setLogMdc(boolean enabled) {
    logCustomMdc = enabled;
  }

  private void initializeLabelMaps(PipelineOptions options) {
    DataflowPipelineOptions dataflowOptions = options.as(DataflowPipelineOptions.class);
    DataflowWorkerHarnessOptions harnessOptions = options.as(DataflowWorkerHarnessOptions.class);
    @Nullable String jobId = harnessOptions.getJobId();
    if (jobId == null || jobId.isEmpty()) {
      jobId = GceMetadataUtil.fetchDataflowJobId();
    }
    jobId = middleCrop(jobId, LABEL_MAX_LENGTH);

    @Nullable String jobName = dataflowOptions.getJobName();
    if (jobName == null || jobName.isEmpty()) {
      jobName = GceMetadataUtil.fetchDataflowJobName();
    }
    jobName = middleCrop(jobName, LABEL_MAX_LENGTH);

    String region = dataflowOptions.getRegion();
    if (region.isEmpty()) {
      region = GceMetadataUtil.fetchDataflowRegion();
    }
    region = middleCrop(region, LABEL_MAX_LENGTH);

    // Note that the id in the options is a string name, not the numeric VM id.
    @Nullable String workerName = harnessOptions.getWorkerId();
    if (workerName == null || workerName.isEmpty()) {
      workerName = GceMetadataUtil.fetchDataflowWorkerName();
    }
    workerName = middleCrop(workerName, LABEL_MAX_LENGTH);

    String workerId = middleCrop(GceMetadataUtil.fetchDataflowWorkerId(), LABEL_MAX_LENGTH);

    @Nullable String projectId = harnessOptions.getProject();
    if (projectId == null || projectId.isEmpty()) {
      projectId = checkNotNull(ServiceOptions.getDefaultProjectId());
    }
    projectId = middleCrop(projectId, LABEL_MAX_LENGTH);

    ImmutableMap.Builder<String, String> defaultLabelsBuilder = new ImmutableMap.Builder<>();
    defaultLabelsBuilder.put("compute.googleapis.com/resource_type", "instance");
    defaultLabelsBuilder.put("compute.googleapis.com/resource_name", workerName);
    defaultLabelsBuilder.put("compute.googleapis.com/resource_id", workerId);
    defaultLabelsBuilder.put("dataflow.googleapis.com/region", region);
    defaultLabelsBuilder.put("dataflow.googleapis.com/job_name", jobName);
    defaultLabelsBuilder.put("dataflow.googleapis.com/job_id", jobId);
    defaultLabels = defaultLabelsBuilder.buildOrThrow();

    ImmutableMap.Builder<String, String> resourceLabelBuilder = new ImmutableMap.Builder<>();
    resourceLabelBuilder.put("job_id", jobId);
    resourceLabelBuilder.put("job_name", jobName);
    resourceLabelBuilder.put("project_id", projectId);
    resourceLabelBuilder.put("region", region);
    // We add the step when constructing the resource as it can change.
    defaultResourceLabels = resourceLabelBuilder.buildOrThrow();
    steplessMonitoredResource =
        MonitoredResource.newBuilder(RESOURCE_TYPE)
            .setLabels(defaultResourceLabels)
            .addLabel(STEP_RESOURCE_LABEL, "")
            .build();
  }

  private static String middleCrop(String value, int maxSize) {
    if (value.length() <= maxSize) {
      return value;
    }
    if (maxSize < 3) {
      return value.substring(0, maxSize);
    }
    int firstHalfSize = (maxSize - 2) / 2;
    int secondHalfSize = (maxSize - 3) / 2;
    return value.substring(0, firstHalfSize)
        + "..."
        + value.substring(value.length() - secondHalfSize);
  }

  private static Severity severityFor(Level level) {
    if (level instanceof LoggingLevel) {
      return ((LoggingLevel) level).getSeverity();
    }
    // Choose the severity based on Level range.
    // The assumption is that Level values below maintain same numeric value
    int value = level.intValue();
    if (value <= Level.FINE.intValue()) {
      return Severity.DEBUG;
    } else if (value <= Level.INFO.intValue()) {
      return Severity.INFO;
    } else if (value <= Level.WARNING.intValue()) {
      return Severity.WARNING;
    } else if (value <= Level.SEVERE.intValue()) {
      return Severity.ERROR;
    } else if (value == Level.OFF.intValue()) {
      return Severity.NONE;
    }
    return Severity.DEFAULT;
  }

  private void addLogField(
      Struct.Builder builder, String field, @Nullable String value, int maxSize) {
    if (value == null || value.isEmpty()) {
      return;
    }
    builder.putFieldsBuilderIfAbsent(field).setStringValue(middleCrop(value, maxSize));
  }

  @VisibleForTesting
  LogEntry constructDirectLogEntry(
      LogRecord record, @Nullable DataflowExecutionState executionState) {
    Struct.Builder payloadBuilder = Struct.newBuilder();
    addLogField(payloadBuilder, "logger", record.getLoggerName(), FIELD_MAX_LENGTH);
    addLogField(
        payloadBuilder, "message", getFormatter().formatMessage(record), MESSAGE_MAX_LENGTH);
    addLogField(
        payloadBuilder, "exception", formatException(record.getThrown()), MESSAGE_MAX_LENGTH);
    addLogField(payloadBuilder, "thread", String.valueOf(record.getThreadID()), FIELD_MAX_LENGTH);
    addLogField(payloadBuilder, "stage", DataflowWorkerLoggingMDC.getStageName(), FIELD_MAX_LENGTH);
    addLogField(payloadBuilder, "worker", DataflowWorkerLoggingMDC.getWorkerId(), FIELD_MAX_LENGTH);
    addLogField(payloadBuilder, "work", DataflowWorkerLoggingMDC.getWorkId(), FIELD_MAX_LENGTH);
    addLogField(payloadBuilder, "job", DataflowWorkerLoggingMDC.getJobId(), FIELD_MAX_LENGTH);
    if (logCustomMdc) {
      @Nullable Map<String, String> mdcMap = MDC.getCopyOfContextMap();
      if (mdcMap != null && !mdcMap.isEmpty()) {
        Struct.Builder customDataBuilder =
            payloadBuilder.putFieldsBuilderIfAbsent("custom_data").getStructValueBuilder();
        mdcMap.entrySet().stream()
            .sorted(Map.Entry.comparingByKey())
            .forEach(
                (entry) -> {
                  if (entry.getKey() != null) {
                    addLogField(
                      customDataBuilder, entry.getKey(), entry.getValue(), FIELD_MAX_LENGTH);
                  }
                });
      }
    }

    @Nullable String stepId = null;
    if (executionState != null) {
      @Nullable NameContext nameContext = executionState.getStepName();
      if (nameContext != null) {
        stepId = nameContext.userName();
      }
    }
    addLogField(payloadBuilder, "step", stepId, FIELD_MAX_LENGTH);

    LogEntry.Builder builder =
        LogEntry.newBuilder(Payload.JsonPayload.of(payloadBuilder.build()))
            .setTimestamp(Instant.ofEpochMilli(record.getMillis()))
            .setSeverity(severityFor(record.getLevel()));

    if (stepId != null) {
      builder.setResource(
          MonitoredResource.newBuilder(RESOURCE_TYPE)
              .setLabels(defaultResourceLabels)
              .addLabel(STEP_RESOURCE_LABEL, stepId)
              .build());
    }

    return builder.build();
  }

  /**
   * Enables logging of records directly to Cloud Logging instead of logging to the disk. Should
   * only be called once.
   *
   * @param pipelineOptions the pipelineOptions, used to configure buffers etc.
   * @param defaultNonDirectLogLevel the default level at which logs should be logged to disk.
   *     LogEntries that are below this level will be logged directly to Cloud Logging. The behavior
   *     for a specific LogEntry can be overridden by attaching a ResourceBundle obtained from
   *     resourceBundleForNonDirectLogLevelHint to it.
   */
  synchronized void enableDirectLogging(
      PipelineOptions pipelineOptions,
      Level defaultNonDirectLogLevel,
      @Nullable Consumer<LogEntry> testDirectLogInterceptor) {
    checkState(
        directLogging == null && this.testDirectLogInterceptor == null,
        "enableDirectLogging should only be called once on a DataflowWorkerLoggingHandler");
    initializeLabelMaps(pipelineOptions);

    DataflowWorkerLoggingOptions dfLoggingOptions =
        pipelineOptions.as(DataflowWorkerLoggingOptions.class);
    this.fallbackDirectErrorsToDisk =
        Boolean.TRUE.equals(dfLoggingOptions.getDirectLoggingFallbackToDiskOnErrors());
    directThrottler.setCooldownDuration(
        Duration.ofSeconds(dfLoggingOptions.getDirectLoggingCooldownSeconds()));

    if (testDirectLogInterceptor == null) {
      // Override some of the default settings.
      LoggingOptions cloudLoggingOptions =
          LoggingOptions.newBuilder()
              .setBatchingSettings(
                  BatchingSettings.newBuilder()
                      .setFlowControlSettings(
                          FlowControlSettings.newBuilder()
                              .setLimitExceededBehavior(
                                  FlowController.LimitExceededBehavior.ThrowException)
                              .setMaxOutstandingRequestBytes(
                                  dfLoggingOptions.getWorkerDirectLoggerBufferByteLimit())
                              .setMaxOutstandingElementCount(
                                  dfLoggingOptions.getWorkerDirectLoggerBufferElementLimit())
                              .build())
                      // These thresholds match the default settings in
                      // LoggingServiceV2StubSettings.java for writeLogEntries.  We must re-set them
                      // when we override the flow control settings because otherwise
                      // BatchingSettings defaults to no batching.
                      .setElementCountThreshold(
                          Math.min(
                              1000L,
                              Math.max(
                                  1L,
                                  dfLoggingOptions.getWorkerDirectLoggerBufferElementLimit() / 2)))
                      .setRequestByteThreshold(
                          Math.min(
                              1048576L,
                              Math.max(
                                  1, dfLoggingOptions.getWorkerDirectLoggerBufferByteLimit() / 2)))
                      .setDelayThresholdDuration(Duration.ofMillis(50L))
                      .build())
              .build();
      ArrayList<Logging.WriteOption> writeOptions = new ArrayList<>();
      writeOptions.add(Logging.WriteOption.labels(defaultLabels));
      writeOptions.add(Logging.WriteOption.logName(LOG_TYPE));
      writeOptions.add(Logging.WriteOption.resource(steplessMonitoredResource));
      this.directWriteOptions = Iterables.toArray(writeOptions, Logging.WriteOption.class);

      Logging directLogging = cloudLoggingOptions.getService();
      directLogging.setFlushSeverity(Severity.NONE);
      directLogging.setWriteSynchronicity(Synchronicity.ASYNC);

      this.directLogging = directLogging;
    } else {
      this.testDirectLogInterceptor = testDirectLogInterceptor;
    }
    this.defaultNonDirectLogLevel = defaultNonDirectLogLevel;
  }

  private static @Nullable DataflowExecutionState getCurrentDataflowExecutionState() {
    @Nullable ExecutionState currentState = ExecutionStateTracker.getCurrentExecutionState();
    if (!(currentState instanceof DataflowExecutionState)) {
      return null;
    }
    return (DataflowExecutionState) currentState;
  }

  @Override
  public void publish(@Nullable LogRecord record) {
    if (record == null) {
      return;
    }
    publish(getCurrentDataflowExecutionState(), record);
  }

  public void publish(@Nullable DataflowExecutionState executionState, LogRecord record) {
    boolean isDirectLog = isConfiguredDirectLog(record);
    if (isDirectLog) {
      if (directThrottler.shouldAttemptDirectLog()) {
        try {
          LogEntry logEntry = constructDirectLogEntry(record, executionState);
          if (testDirectLogInterceptor != null) {
            // The default labels are applied by write options generally bute we merge them in here
            // so they are visible to the test.
            HashMap<String, String> mergedLabels = new HashMap<>(defaultLabels);
            mergedLabels.putAll(logEntry.getLabels());
            logEntry = logEntry.toBuilder().setLabels(mergedLabels).build();
            if (logEntry.getResource() == null) {
              logEntry = logEntry.toBuilder().setResource(steplessMonitoredResource).build();
            }
            checkNotNull(testDirectLogInterceptor).accept(logEntry);
          } else {
            checkNotNull(directLogging).write(ImmutableList.of(logEntry), directWriteOptions);
          }
          directThrottler.noteDirectLoggingEnqueueSuccess();
          return;
        } catch (RuntimeException e) {
          @Nullable String log = directThrottler.noteDirectLoggingEnqueueFailure();
          if (log != null) {
            printErrorToDisk(log, e);
          }
        }
      }

      // Either we were throttled or encountered an error enqueuing the log.
      if (!fallbackDirectErrorsToDisk) {
        return;
      }
    }
    publishToDisk(executionState, record);
  }

  private void printErrorToDisk(String errorMsg, @Nullable Exception ex) {
    LogRecord record = new LogRecord(Level.SEVERE, errorMsg);
    record.setLoggerName(DataflowWorkerLoggingHandler.class.getName());
    if (ex != null) {
      record.setThrown(ex);
    }
    publishToDisk(null, record);
  }

  @SuppressWarnings({"nullness", "GuardedBy"})
  public synchronized void publishToDisk(
      @Nullable DataflowExecutionState currentExecutionState, LogRecord record) {

    rolloverOutputStreamIfNeeded();

    if (generator == null) {
      throw new IllegalStateException("rollover should have made generator");
    }
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
      writeIfNotEmpty(generator, "message", getFormatter().formatMessage(record));
      writeIfNotEmpty(generator, "thread", String.valueOf(record.getThreadID()));
      writeIfNotEmpty(generator, "job", DataflowWorkerLoggingMDC.getJobId());
      writeIfNotEmpty(generator, "stage", DataflowWorkerLoggingMDC.getStageName());

      if (currentExecutionState != null) {
        NameContext nameContext = currentExecutionState.getStepName();
        if (nameContext != null) {
          writeIfNotEmpty(generator, "step", nameContext.userName());
        }
      }
      writeIfNotEmpty(generator, "worker", DataflowWorkerLoggingMDC.getWorkerId());
      writeIfNotEmpty(generator, "work", DataflowWorkerLoggingMDC.getWorkId());
      writeIfNotEmpty(generator, "logger", record.getLoggerName());
      writeIfNotEmpty(generator, "exception", formatException(record.getThrown()));
      if (logCustomMdc) {
        @Nullable Map<String, String> mdcMap = MDC.getCopyOfContextMap();
        if (mdcMap != null && !mdcMap.isEmpty()) {
          generator.writeFieldName("custom_data");
          generator.writeStartObject();
          mdcMap.entrySet().stream()
              .sorted(Map.Entry.comparingByKey())
              .forEach(
                  (entry) -> {
                    try {
                      generator.writeStringField(entry.getKey(), entry.getValue());
                    } catch (IOException e) {
                      throw new RuntimeException(e);
                    }
                  });
          generator.writeEndObject();
        }
      }
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
    flushDisk();
  }

  @VisibleForTesting
  boolean isConfiguredDirectLog(LogRecord record) {
    if (directLogging == null && testDirectLogInterceptor == null) {
      return false;
    }
    @Nullable ResourceBundle resourceBundle = record.getResourceBundle();
    Level nonDirectLogLevel =
        (resourceBundle instanceof DirectHintResourceBundle)
            ? ((DirectHintResourceBundle) resourceBundle).nonDirectLogLevel
            : defaultNonDirectLogLevel;
    return nonDirectLogLevel.intValue() > record.getLevel().intValue();
  }

  @VisibleForTesting
  static final class DirectLoggingThrottler {
    private final AtomicBoolean rejectingRequests = new AtomicBoolean(false);
    private final Supplier<Instant> nowSupplier;

    DirectLoggingThrottler(Supplier<Instant> nowSupplier) {
      this.nowSupplier = nowSupplier;
    }

    @GuardedBy("this")
    private Duration cooldownDuration = Duration.ZERO;

    @GuardedBy("this")
    private Instant rejectRequestsUntil = Instant.MIN;

    @GuardedBy("this")
    private Instant failureStartTime = Instant.MAX;

    @GuardedBy("this")
    private Instant nextLogTime = Instant.MIN;

    @GuardedBy("this")
    private Instant nextSendErrorLogTime = Instant.MIN;

    synchronized void setCooldownDuration(Duration cooldownDuration) {
      this.cooldownDuration = cooldownDuration;
    }

    boolean shouldAttemptDirectLog() {
      if (!rejectingRequests.get()) {
        return true;
      }

      Instant now = nowSupplier.get();
      synchronized (this) {
        return !now.isBefore(rejectRequestsUntil);
      }
    }

    void noteDirectLoggingEnqueueSuccess() {
      if (!rejectingRequests.get()) {
        return;
      }

      synchronized (this) {
        rejectingRequests.set(false);
        failureStartTime = Instant.MAX;
      }
    }

    synchronized @Nullable String noteDirectLoggingEnqueueFailure() {
      rejectingRequests.set(true);
      if (failureStartTime.equals(Instant.MAX)) {
        Instant now = nowSupplier.get();
        failureStartTime = now;
        rejectRequestsUntil = now.plus(cooldownDuration);
        if (nextLogTime.isBefore(now)) {
          nextLogTime = now.plusSeconds(60);
          return String.format(
              "Failed to buffer log to send directly to cloud logging, falling back to normal logging path for %s. Consider increasing the buffer size with --workerDirectLoggerBufferByteLimit and --workerDirectLoggerBufferElementLimit.",
              cooldownDuration);
        }
      }
      return null;
    }

    synchronized @Nullable String noteDirectLoggingError() {
      rejectingRequests.set(true);
      Instant now = nowSupplier.get();
      if (failureStartTime.equals(Instant.MAX)) {
        failureStartTime = now;
      }
      rejectRequestsUntil = now.plus(cooldownDuration);
      if (nextSendErrorLogTime.isBefore(now)) {
        nextSendErrorLogTime = now.plusSeconds(60);
        return String.format(
            "Error occurred while handling direct logs, falling back to normal logging path for %s.",
            cooldownDuration);
      }
      return null;
    }
  }

  private final DirectLoggingThrottler directThrottler = new DirectLoggingThrottler(Instant::now);

  void publishStdErr(LogRecord record) {
    @Nullable String msg = record.getMessage();
    if (msg != null && msg.startsWith(LOGGING_IMPL_STDERR_PREFIX)) {
      @Nullable String throttleMsg = directThrottler.noteDirectLoggingError();
      if (throttleMsg != null) {
        printErrorToDisk(throttleMsg, null);
      }
    }
    publishToDisk(getCurrentDataflowExecutionState(), record);
  }

  void publishStdOut(LogRecord record) {
    publishToDisk(getCurrentDataflowExecutionState(), record);
  }

  private void flushDisk() {
    try {
      synchronized (this) {
        if (generator != null) {
          generator.flush();
        }
      }
    } catch (IOException | RuntimeException e) {
      reportFailure("Unable to flush", e, ErrorManager.FLUSH_FAILURE);
    }
  }

  @Override
  public void flush() {
    flushDisk();
    if (directLogging != null) {
      directLogging.flush();
    }
  }

  @Override
  public void close() {
    synchronized (this) {
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
    try {
      if (directLogging != null) {
        directLogging.close();
      }
    } catch (Exception e) {
      reportFailure("Unable to close", e, ErrorManager.CLOSE_FAILURE);
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

  private synchronized void createOutputStream() throws IOException {
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
  private synchronized void rolloverOutputStreamIfNeeded() {
    if (counter == null) {
      throw new IllegalStateException("");
    }
    if (counter.getCount() < sizeLimit) {
      return;
    }

    try {
      @Nullable JsonGenerator old = generator;
      createOutputStream();
      if (old != null) {
        try {
          // Rollover successful. Attempt to close old stream, but ignore on failure.
          old.close();
        } catch (IOException | RuntimeException e) {
          reportFailure("Unable to close old log file", e, ErrorManager.CLOSE_FAILURE);
        }
      }
    } catch (IOException | RuntimeException e) {
      reportFailure("Unable to create new log file", e, ErrorManager.OPEN_FAILURE);
    }
  }

  /** Appends a JSON key/value pair if the specified val is not null. */
  private static void writeIfNotEmpty(JsonGenerator generator, String name, String val)
      throws IOException {
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
}
