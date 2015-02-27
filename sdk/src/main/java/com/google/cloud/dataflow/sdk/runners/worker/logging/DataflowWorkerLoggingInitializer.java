/*
 * Copyright (C) 2014 Google Inc.
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

import com.google.api.client.util.Lists;
import com.google.cloud.dataflow.sdk.options.DataflowWorkerLoggingOptions;
import com.google.cloud.dataflow.sdk.options.DataflowWorkerLoggingOptions.WorkerLogLevelOverride;
import com.google.common.collect.ImmutableBiMap;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.logging.FileHandler;
import java.util.logging.Formatter;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogManager;
import java.util.logging.Logger;

/**
 * Sets up {@link java.util.logging} configuration on the Dataflow worker with a
 * file logger. The file logger uses the {@link DataflowWorkerLoggingFormatter} format.
 * A user can override the logging level by customizing the options found within
 * {@link DataflowWorkerLoggingOptions}. A user can override the location by specifying the
 * Java system property "dataflow.worker.logging.location". The default log level is INFO
 * and the default location is a file named dataflow-worker.log within the systems temporary
 * directory.
 */
public class DataflowWorkerLoggingInitializer {
  private static final String DEFAULT_LOGGING_LOCATION =
      new File(System.getProperty("java.io.tmpdir"), "dataflow-worker.log").getPath();
  private static final String ROOT_LOGGER_NAME = "";
  private static final String DATAFLOW_WORKER_LOGGING_LOCATION = "dataflow.worker.logging.location";
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
  private static FileHandler fileHandler;

  /**
   * Sets up the initial logging configuration.
   */
  public static synchronized void initialize() {
    if (fileHandler != null) {
      return;
    }
    try {
      Level logLevel = LEVELS.inverse().get(DEFAULT_LOG_LEVEL);
      Formatter formatter = new DataflowWorkerLoggingFormatter();

      fileHandler = new FileHandler(
          System.getProperty(DATAFLOW_WORKER_LOGGING_LOCATION, DEFAULT_LOGGING_LOCATION),
          true /* Append so that we don't squash existing logs */);
      fileHandler.setFormatter(formatter);
      fileHandler.setLevel(Level.ALL);

      // Reset the global log manager, get the root logger and remove the default log handlers.
      LogManager logManager = LogManager.getLogManager();
      logManager.reset();
      Logger rootLogger = logManager.getLogger(ROOT_LOGGER_NAME);
      for (Handler handler : rootLogger.getHandlers()) {
        rootLogger.removeHandler(handler);
      }

      rootLogger.setLevel(logLevel);
      rootLogger.addHandler(fileHandler);
    } catch (SecurityException | IOException e) {
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
    /* We store a reference to all the custom loggers the user configured.
     * To make sure that these custom levels override the default logger level,
     * we break the parent chain and have the logger directly pass log records
     * to the file handler. */
    if (options.getWorkerLogLevelOverrides() != null) {
      for (WorkerLogLevelOverride loggerOverride : options.getWorkerLogLevelOverrides()) {
        Logger logger = Logger.getLogger(loggerOverride.getName());
        logger.setUseParentHandlers(false);
        logger.setLevel(LEVELS.inverse().get(loggerOverride.getLevel()));
        logger.addHandler(fileHandler);
        configuredLoggers.add(logger);
      }
    }
  }

  // Visible for testing
  static void reset() {
    configuredLoggers = Lists.newArrayList();
    fileHandler = null;
  }
}
