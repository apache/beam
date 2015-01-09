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

import com.google.common.collect.ImmutableBiMap;

import java.io.File;
import java.io.IOException;
import java.util.logging.ConsoleHandler;
import java.util.logging.FileHandler;
import java.util.logging.Formatter;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogManager;
import java.util.logging.Logger;

/**
 * Sets up java.util.Logging configuration on the Dataflow Worker Harness with a
 * console and file logger. The console and file loggers use the
 * {@link DataflowWorkerLoggingFormatter} format. A user can override
 * the logging level and location by specifying the Java system properties
 * "dataflow.worker.logging.level" and "dataflow.worker.logging.location" respectively.
 * The default log level is INFO and the default location is a file named dataflow-worker.log
 * within the systems temporary directory.
 */
public class DataflowWorkerLoggingInitializer {
  private static final String DEFAULT_LOGGING_LOCATION =
      new File(System.getProperty("java.io.tmpdir"), "dataflow-worker.log").getPath();
  private static final String ROOT_LOGGER_NAME = "";
  public static final String DATAFLOW_WORKER_LOGGING_LEVEL = "dataflow.worker.logging.level";
  public static final String DATAFLOW_WORKER_LOGGING_LOCATION = "dataflow.worker.logging.location";
  public static final ImmutableBiMap<Level, String> LEVELS =
      ImmutableBiMap.<Level, String>builder()
      .put(Level.SEVERE, "ERROR")
      .put(Level.WARNING, "WARNING")
      .put(Level.INFO, "INFO")
      .put(Level.FINE, "DEBUG")
      .put(Level.FINEST, "TRACE")
      .build();
  private static final String DEFAULT_LOG_LEVEL = LEVELS.get(Level.INFO);

  public void initialize() {
    initialize(LogManager.getLogManager());
  }

  void initialize(LogManager logManager) {
    try {
      Level logLevel = LEVELS.inverse().get(
              System.getProperty(DATAFLOW_WORKER_LOGGING_LEVEL, DEFAULT_LOG_LEVEL));
      Formatter formatter = new DataflowWorkerLoggingFormatter();

      FileHandler fileHandler = new FileHandler(
          System.getProperty(DATAFLOW_WORKER_LOGGING_LOCATION, DEFAULT_LOGGING_LOCATION),
          true /* Append so that we don't squash existing logs */);
      fileHandler.setFormatter(formatter);
      fileHandler.setLevel(logLevel);

      ConsoleHandler consoleHandler = new ConsoleHandler();
      consoleHandler.setFormatter(formatter);
      consoleHandler.setLevel(logLevel);

      // Reset the global log manager, get the root logger and remove the default log handlers.
      logManager.reset();
      Logger rootLogger = logManager.getLogger(ROOT_LOGGER_NAME);
      for (Handler handler : rootLogger.getHandlers()) {
        rootLogger.removeHandler(handler);
      }

      rootLogger.setLevel(logLevel);
      rootLogger.addHandler(consoleHandler);
      rootLogger.addHandler(fileHandler);
    } catch (SecurityException | IOException e) {
      throw new ExceptionInInitializerError(e);
    }
  }
}
