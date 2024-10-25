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
package org.apache.beam.runners.dataflow.worker;

import java.io.PrintStream;
import java.lang.Thread.UncaughtExceptionHandler;
import org.apache.beam.runners.dataflow.worker.logging.DataflowWorkerLoggingInitializer;
import org.apache.beam.runners.dataflow.worker.util.common.worker.JvmRuntime;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;

/**
 * This uncaught exception handler logs the {@link Throwable} to the logger, {@link System#err} and
 * exits the application with status code 1.
 */
public class WorkerUncaughtExceptionHandler implements UncaughtExceptionHandler {
  private final JvmRuntime runtime;
  private final Logger logger;

  public WorkerUncaughtExceptionHandler(Logger logger) {
    this(JvmRuntime.INSTANCE, logger);
  }

  @VisibleForTesting
  WorkerUncaughtExceptionHandler(JvmRuntime runtime, Logger logger) {
    this.runtime = runtime;
    this.logger = logger;
  }

  @Override
  public void uncaughtException(Thread thread, Throwable e) {
    try {
      logger.error("Uncaught exception in main thread. Exiting with status code 1.", e);
      System.err.println("Uncaught exception in main thread. Exiting with status code 1.");
      e.printStackTrace();
    } catch (Throwable t) {
      PrintStream originalStdErr = DataflowWorkerLoggingInitializer.getOriginalStdErr();
      if (originalStdErr != null) {
        originalStdErr.println("Uncaught exception in main thread. Exiting with status code 1.");
        e.printStackTrace(originalStdErr);

        originalStdErr.println(
            "UncaughtExceptionHandler caused another exception to be thrown, as follows:");
        t.printStackTrace(originalStdErr);
      }
    } finally {
      runtime.halt(1);
    }
  }
}
