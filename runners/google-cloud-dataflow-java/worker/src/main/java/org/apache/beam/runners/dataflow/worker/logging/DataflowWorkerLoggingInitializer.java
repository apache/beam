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

import static org.apache.beam.runners.dataflow.options.DataflowWorkerLoggingOptions.Level.DEBUG;
import static org.apache.beam.runners.dataflow.options.DataflowWorkerLoggingOptions.Level.ERROR;
import static org.apache.beam.runners.dataflow.options.DataflowWorkerLoggingOptions.Level.INFO;
import static org.apache.beam.runners.dataflow.options.DataflowWorkerLoggingOptions.Level.OFF;
import static org.apache.beam.runners.dataflow.options.DataflowWorkerLoggingOptions.Level.TRACE;
import static org.apache.beam.runners.dataflow.options.DataflowWorkerLoggingOptions.Level.WARN;

import com.google.cloud.logging.LogEntry;
import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.List;
import java.util.function.Consumer;
import java.util.logging.ErrorManager;
import java.util.logging.Level;
import java.util.logging.LogManager;
import java.util.logging.Logger;
import javax.annotation.Nullable;
import org.apache.beam.runners.dataflow.options.DataflowWorkerLoggingOptions;
import org.apache.beam.sdk.options.SdkHarnessOptions;
import org.apache.beam.sdk.options.SdkHarnessOptions.LogLevel;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableBiMap;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Lists;
import org.slf4j.LoggerFactory;

/**
 * Sets up {@link java.util.logging} configuration on the Dataflow worker with a rotating file
 * logger. The file logger uses the {@link DataflowWorkerLoggingHandler} format. A user can override
 * the logging level by customizing the options found within {@link DataflowWorkerLoggingOptions}. A
 * user can override the location by specifying the Java system property
 * "dataflow.worker.logging.basepath" and the file size in MB before rolling over to a new file by
 * specifying the Java system property "dataflow.worker. loggging.filesize_mb". The default log
 * level is INFO, the default location is a file named dataflow-json.log within the system temporary
 * directory and the default file size is 1 GB.
 */
@SuppressWarnings({
  "nullness", // TODO(https://github.com/apache/beam/issues/20497)
  "ForbidDefaultCharset"
})
public class DataflowWorkerLoggingInitializer {
  private static final org.slf4j.Logger LOG =
      LoggerFactory.getLogger(DataflowWorkerLoggingInitializer.class);
  private static final String ROOT_LOGGER_NAME = "";

  @VisibleForTesting
  static final String DEFAULT_RUNNER_LOGGING_LOCATION =
      new File(System.getProperty("java.io.tmpdir"), "dataflow-json.log").getPath();

  @VisibleForTesting
  static final String DEFAULT_SDK_LOGGING_LOCATION =
      new File(System.getProperty("java.io.tmpdir"), "sdk-json.log").getPath();

  @VisibleForTesting
  public static final String RUNNER_FILEPATH_PROPERTY = "dataflow.worker.logging.filepath";

  @VisibleForTesting
  static final String SDK_FILEPATH_PROPERTY = "dataflow.worker.logging.sdkfilepath";

  private static final String FILESIZE_MB_PROPERTY = "dataflow.worker.logging.filesize_mb";

  static final String SYSTEM_OUT_LOG_NAME = "System.out";
  static final String SYSTEM_ERR_LOG_NAME = "System.err";

  static final ImmutableBiMap<Level, DataflowWorkerLoggingOptions.Level> LEVELS =
      ImmutableBiMap.<Level, DataflowWorkerLoggingOptions.Level>builder()
          .put(Level.OFF, OFF)
          .put(Level.SEVERE, ERROR)
          .put(Level.WARNING, WARN)
          .put(Level.INFO, INFO)
          .put(Level.FINE, DEBUG)
          .put(Level.FINEST, TRACE)
          .build();

  /**
   * This default log level is overridden by the log level found at {@code
   * DataflowWorkerLoggingOptions#getDefaultWorkerLogLevel()}.
   */
  private static final DataflowWorkerLoggingOptions.Level DEFAULT_LOG_LEVEL =
      LEVELS.get(Level.INFO);

  /* We need to store a reference to the configured loggers so that they are not
   * garbage collected. java.util.logging only has weak references to the loggers
   * so if they are garbage collection, our hierarchical configuration will be lost. */
  @SuppressWarnings("MismatchedQueryAndUpdateOfCollection")
  private static List<Logger> configuredLoggers = Lists.newArrayList();

  private static DataflowWorkerLoggingHandler loggingHandler;
  private static DataflowWorkerLoggingHandler sdkLoggingHandler;
  private static PrintStream originalStdOut;
  private static PrintStream originalStdErr = System.err;
  private static boolean initialized = false;
  @VisibleForTesting static @Nullable Consumer<LogEntry> testDirectLoggingInterceptor = null;

  // This is the same as ErrorManager except that it uses the provided
  // print stream.
  public static class PrintStreamErrorManager extends ErrorManager {
    public PrintStreamErrorManager(PrintStream stream) {
      this.stream = stream;
    }

    private PrintStream stream;
    private boolean reported = false;

    @Override
    public synchronized void error(String msg, Exception ex, int code) {
      if (reported) {
        // We only report the first error, to avoid clogging
        // the screen.
        return;
      }
      reported = true;
      String text = "java.util.logging.ErrorManager: " + code;
      if (msg != null) {
        text = text + ": " + msg;
      }
      stream.println(text);
      if (ex != null) {
        ex.printStackTrace(stream);
      }
    }
  };

  private static DataflowWorkerLoggingHandler makeLoggingHandler(
      String filepathProperty, String defaultFilePath) throws IOException {
    String filepath = System.getProperty(filepathProperty, defaultFilePath);
    int filesizeMb = Integer.parseInt(System.getProperty(FILESIZE_MB_PROPERTY, "1024"));
    DataflowWorkerLoggingHandler handler =
        new DataflowWorkerLoggingHandler(filepath, filesizeMb * 1024L * 1024L);
    handler.setLevel(Level.ALL);
    // To avoid potential deadlock between the handler and the System.err print stream, use the
    // original stderr print stream for errors. See BEAM-9399.
    handler.setErrorManager(new PrintStreamErrorManager(getOriginalStdErr()));
    return handler;
  }

  public static PrintStream getOriginalStdErr() {
    return originalStdErr;
  }

  /** Sets up the initial logging configuration. */
  public static synchronized void initialize() {
    if (initialized) {
      return;
    }

    try {
      loggingHandler =
          makeLoggingHandler(RUNNER_FILEPATH_PROPERTY, DEFAULT_RUNNER_LOGGING_LOCATION);
      Logger rootLogger = LogManager.getLogManager().getLogger(ROOT_LOGGER_NAME);

      Level logLevel = getJulLevel(DEFAULT_LOG_LEVEL);
      rootLogger.setLevel(logLevel);
      rootLogger.addHandler(loggingHandler);

      originalStdOut = System.out;
      originalStdErr = System.err;
      System.setOut(
          JulHandlerPrintStreamAdapterFactory.create(
              loggingHandler::publish, SYSTEM_OUT_LOG_NAME, Level.INFO, Charset.defaultCharset()));
      System.setErr(
          JulHandlerPrintStreamAdapterFactory.create(
              loggingHandler::publish,
              SYSTEM_ERR_LOG_NAME,
              Level.SEVERE,
              Charset.defaultCharset()));

      // Initialize the SDK Logging Handler, which will only be used for the LoggingService
      sdkLoggingHandler = makeLoggingHandler(SDK_FILEPATH_PROPERTY, DEFAULT_SDK_LOGGING_LOCATION);

      initialized = true;
    } catch (SecurityException | IOException | NumberFormatException e) {
      throw new ExceptionInInitializerError(e);
    }
  }

  private static class LevelOverrides {
    LevelOverrides(Level disk, Level direct) {
      this.direct = direct;
      this.disk = disk;
    }

    final Level disk;
    final Level direct;

    LevelOverrides merge(@Nullable Level disk, @Nullable Level direct) {
      return new LevelOverrides(
          disk == null ? this.disk : disk, direct == null ? this.direct : direct);
    }
  }

  private static void applyOverridesToLogger(
      Logger logger, LevelOverrides overrides, Level defaultNonDirectLogLevel) {
    if (overrides.direct.intValue() < overrides.disk.intValue()) {
      logger.setLevel(overrides.direct);
      // Only set the resource if the value is different from the default non-direct level.
      if (overrides.disk.intValue() != defaultNonDirectLogLevel.intValue()) {
        logger.setResourceBundle(
            DataflowWorkerLoggingHandler.resourceBundleForNonDirectLogLevelHint(overrides.disk));
      }
    } else {
      if (overrides.direct.intValue() != Level.OFF.intValue()) {
        LOG.warn(
            "Ignoring direct logging for {} as the configured disk logging level is of greater or equal value.",
            logger.getName());
      }
      logger.setLevel(overrides.disk);
    }
  }

  /** Reconfigures logging with the passed in options. */
  public static synchronized void configure(DataflowWorkerLoggingOptions options) {
    if (!initialized) {
      throw new RuntimeException("configure() called before initialize()");
    }

    // For compatibility reason, we do not call SdkHarnessOptions.getConfiguredLoggerFromOptions
    // to config the logging for legacy worker, instead replicate the config steps used for
    // DataflowWorkerLoggingOptions for default log level and log level overrides.
    SdkHarnessOptions harnessOptions = options.as(SdkHarnessOptions.class);
    boolean usedDeprecated = false;

    // default value for both DefaultSdkHarnessLogLevel and DefaultWorkerLogLevel are INFO for disk
    // logging and OFF
    // for direct logging.
    LevelOverrides defaultOverrides = new LevelOverrides(Level.INFO, Level.OFF);
    if (options.getDefaultWorkerLogLevel() != null && options.getDefaultWorkerLogLevel() != INFO) {
      defaultOverrides =
          defaultOverrides.merge(getJulLevel(options.getDefaultWorkerLogLevel()), null);
      usedDeprecated = true;
    } else {
      defaultOverrides =
          defaultOverrides.merge(getJulLevel(harnessOptions.getDefaultSdkHarnessLogLevel()), null);
    }
    if (options.getDefaultWorkerDirectLoggerLevel() != null
        && options.getDefaultWorkerDirectLoggerLevel() != OFF) {
      Level diskLevel = defaultOverrides.disk;
      Level directLevel = getJulLevel(options.getDefaultWorkerDirectLoggerLevel());
      LOG.info(
          "Enabling sending logs directly to Cloud Logging between severity {} and severity {}.",
          directLevel,
          diskLevel);
      try {
        loggingHandler.enableDirectLogging(options, diskLevel, testDirectLoggingInterceptor);
        LOG.info("Configuring sending to cloud logging directly succeeded.");
        defaultOverrides = defaultOverrides.merge(null, directLevel);
      } catch (RuntimeException e) {
        LOG.error("Unable to configure logs to be sent directly to cloud logging", e);
      }
    }

    final LevelOverrides finalDefaultOverrides = defaultOverrides;

    HashMap<String, LevelOverrides> loggerLevelOverrides = new HashMap<>();
    if (options.getWorkerLogLevelOverrides() != null) {
      options
          .getWorkerLogLevelOverrides()
          .forEach(
              (k, v) -> {
                loggerLevelOverrides.compute(
                    k,
                    (lk, lv) -> {
                      if (lv == null) {
                        lv = finalDefaultOverrides;
                      }
                      return lv.merge(getJulLevel(v), null);
                    });
              });
      usedDeprecated = true;
    } else if (harnessOptions.getSdkHarnessLogLevelOverrides() != null) {
      harnessOptions
          .getSdkHarnessLogLevelOverrides()
          .forEach(
              (k, v) -> {
                loggerLevelOverrides.compute(
                    k,
                    (lk, lv) -> {
                      if (lv == null) {
                        lv = finalDefaultOverrides;
                      }
                      return lv.merge(getJulLevel(v), null);
                    });
              });
    }
    if (options.getWorkerDirectLogLevelOverrides() != null) {
      options
          .getWorkerDirectLogLevelOverrides()
          .forEach(
              (k, v) -> {
                loggerLevelOverrides.compute(
                    k,
                    (lk, lv) -> {
                      if (lv == null) {
                        lv = finalDefaultOverrides;
                      }
                      return lv.merge(null, getJulLevel(v));
                    });
              });
    }

    applyOverridesToLogger(
        LogManager.getLogManager().getLogger(ROOT_LOGGER_NAME),
        defaultOverrides,
        finalDefaultOverrides.disk);
    loggerLevelOverrides.forEach(
        (loggerName, levelOverrides) -> {
          Logger logger = Logger.getLogger(loggerName);
          applyOverridesToLogger(logger, levelOverrides, finalDefaultOverrides.disk);
          configuredLoggers.add(logger);
        });

    // If the options specify a level for messages logged to System.out/err, we need to reconfigure
    // the corresponding stream adapter.
    if (options.getWorkerSystemOutMessageLevel() != null) {
      System.out.close();
      System.setOut(
          JulHandlerPrintStreamAdapterFactory.create(
              loggingHandler::publishStdOut,
              SYSTEM_OUT_LOG_NAME,
              getJulLevel(options.getWorkerSystemOutMessageLevel()),
              Charset.defaultCharset()));
    }

    if (options.getWorkerSystemErrMessageLevel() != null) {
      System.err.close();
      System.setErr(
          JulHandlerPrintStreamAdapterFactory.create(
              loggingHandler::publishStdErr,
              SYSTEM_ERR_LOG_NAME,
              getJulLevel(options.getWorkerSystemErrMessageLevel()),
              Charset.defaultCharset()));
    }

    if (harnessOptions.getLogMdc()) {
      LOG.info("Due to logMdc option, MDC fields will be added to custom_data field of logs.");
      loggingHandler.setLogMdc(true);
    }

    if (usedDeprecated) {
      LOG.warn(
          "Deprecated DataflowWorkerLoggingOptions are used for log level settings."
              + "Consider using options defined in SdkHarnessOptions for forward compatibility.");
    }
  }

  /**
   * Returns the underlying {@link DataflowWorkerLoggingHandler}.
   *
   * <p>Generally, code should just use logging interface.
   */
  public static DataflowWorkerLoggingHandler getLoggingHandler() {
    if (!initialized) {
      throw new RuntimeException("getLoggingHandler() called before initialize()");
    }
    return loggingHandler;
  }

  /**
   * Returns the underlying {@link DataflowWorkerLoggingHandler} for logs from the SDK.
   *
   * <p>Initializes Dataflow worker logging if not initialized already.
   */
  public static DataflowWorkerLoggingHandler getSdkLoggingHandler() {
    if (!initialized) {
      throw new RuntimeException("getSdkLoggingHandler() called before initialize()");
    }
    return sdkLoggingHandler;
  }

  private static Level getJulLevel(DataflowWorkerLoggingOptions.Level level) {
    return LEVELS.inverse().get(level);
  }

  private static Level getJulLevel(SdkHarnessOptions.LogLevel level) {
    return LogLevel.LEVEL_CONFIGURATION.get(level);
  }

  @VisibleForTesting
  public static synchronized void reset() {
    if (!initialized) {
      return;
    }
    configuredLoggers = Lists.newArrayList();
    System.setOut(originalStdOut);
    System.setErr(originalStdErr);
    JulHandlerPrintStreamAdapterFactory.reset();
    initialized = false;
  }
}
