/*
 * Copyright (C) 2015 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.google.cloud.dataflow.sdk.runners.worker.logging;

import static com.google.cloud.dataflow.sdk.options.DataflowWorkerLoggingOptions.Level.DEBUG;
import static com.google.cloud.dataflow.sdk.options.DataflowWorkerLoggingOptions.Level.ERROR;
import static com.google.cloud.dataflow.sdk.options.DataflowWorkerLoggingOptions.Level.INFO;
import static com.google.cloud.dataflow.sdk.options.DataflowWorkerLoggingOptions.Level.TRACE;
import static com.google.cloud.dataflow.sdk.options.DataflowWorkerLoggingOptions.Level.WARN;

import com.google.cloud.dataflow.sdk.options.DataflowWorkerLoggingOptions;
import com.google.common.collect.ImmutableBiMap;
import com.google.common.collect.Lists;

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.util.List;
import java.util.Map;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogManager;
import java.util.logging.Logger;

/**
 * Sets up {@link java.util.logging} configuration on the Dataflow worker with a rotating
 * file logger. The file logger uses the {@link DataflowWorkerLoggingHandler} format.
 * A user can override the logging level by customizing the options found within
 * {@link DataflowWorkerLoggingOptions}. A user can override the location by specifying the
 * Java system property "dataflow.worker.logging.basepath" and the file size in MB before
 * rolling over to a new file by specifying the Java system property "dataflow.worker.
 * loggging.filesize_mb". The default log level is INFO, the default location is a file
 * named dataflow-json.log within the system temporary directory and the default file size
 * is 1 GB.
 */
public class DataflowWorkerLoggingInitializer {
  private static final String ROOT_LOGGER_NAME = "";
  private static final String DEFAULT_LOGGING_LOCATION =
      new File(System.getProperty("java.io.tmpdir"), "dataflow-json.log").getPath();
  private static final String FILEPATH_PROPERTY = "dataflow.worker.logging.filepath";
  private static final String FILESIZE_MB_PROPERTY = "dataflow.worker.logging.filesize_mb";

  static final ImmutableBiMap<Level, DataflowWorkerLoggingOptions.Level> LEVELS =
      ImmutableBiMap.<Level, DataflowWorkerLoggingOptions.Level>builder()
          .put(Level.SEVERE, ERROR)
          .put(Level.WARNING, WARN)
          .put(Level.INFO, INFO)
          .put(Level.FINE, DEBUG)
          .put(Level.FINEST, TRACE)
          .build();

  /**
   * This default log level is overridden by the log level found at
   * {@code DataflowWorkerLoggingOptions#getDefaultWorkerLogLevel()}.
   */
  private static final DataflowWorkerLoggingOptions.Level DEFAULT_LOG_LEVEL =
      LEVELS.get(Level.INFO);

  /* We need to store a reference to the configured loggers so that they are not
   * garbage collected. java.util.logging only has weak references to the loggers
   * so if they are garbage collection, our hierarchical configuration will be lost. */
  private static List<Logger> configuredLoggers = Lists.newArrayList();
  private static PrintStream originalStdOut;
  private static PrintStream originalStdErr;
  private static boolean initialized = false;

  /**
   * Sets up the initial logging configuration.
   */
  public static synchronized void initialize() {
    if (initialized) {
      return;
    }

    try {
      String filepath = System.getProperty(FILEPATH_PROPERTY, DEFAULT_LOGGING_LOCATION);
      int filesizeMb = Integer.parseInt(System.getProperty(FILESIZE_MB_PROPERTY, "1024"));

      DataflowWorkerLoggingHandler loggingHandler =
          new DataflowWorkerLoggingHandler(filepath, filesizeMb * 1024 * 1024);
      loggingHandler.setLevel(Level.ALL);

      // Reset the global log manager, get the root logger and remove the default log handlers.
      LogManager logManager = LogManager.getLogManager();
      logManager.reset();
      Logger rootLogger = logManager.getLogger(ROOT_LOGGER_NAME);
      for (Handler handler : rootLogger.getHandlers()) {
        rootLogger.removeHandler(handler);
      }

      Level logLevel = LEVELS.inverse().get(DEFAULT_LOG_LEVEL);
      rootLogger.setLevel(logLevel);
      rootLogger.addHandler(loggingHandler);

      originalStdOut = System.out;
      originalStdErr = System.err;
      System.setOut(JulLoggerPrintStreamAdapterFactory.create("System.out", Level.INFO));
      System.setErr(JulLoggerPrintStreamAdapterFactory.create("System.err", Level.SEVERE));

      initialized = true;
    } catch (SecurityException | IOException | NumberFormatException e) {
      throw new ExceptionInInitializerError(e);
    }
  }

  /**
   * Reconfigures logging with the passed in options.
   */
  public static synchronized void configure(DataflowWorkerLoggingOptions options) {
    initialize();

    if (options.getDefaultWorkerLogLevel() != null) {
      LogManager.getLogManager().getLogger(ROOT_LOGGER_NAME).setLevel(
          LEVELS.inverse().get(options.getDefaultWorkerLogLevel()));
    }

    if (options.getWorkerLogLevelOverrides() != null) {
      for (Map.Entry<String, DataflowWorkerLoggingOptions.Level> loggerOverride :
          options.getWorkerLogLevelOverrides().entrySet()) {
        Logger logger = Logger.getLogger(loggerOverride.getKey());
        logger.setLevel(LEVELS.inverse().get(loggerOverride.getValue()));
        configuredLoggers.add(logger);
      }
    }
  }

  // Visible for testing
  static void reset() {
    configuredLoggers = Lists.newArrayList();
    System.setOut(originalStdOut);
    System.setErr(originalStdErr);
    initialized = false;
  }
}
