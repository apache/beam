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
package org.apache.beam.sdk.fn;

import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.beam.sdk.harness.JvmInitializer;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.util.common.ReflectHelpers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Helpers for executing {@link JvmInitializer} implementations. */
public class JvmInitializers {
  private static final AtomicBoolean initialized = new AtomicBoolean(false);

  /**
   * Finds all registered implementations of JvmInitializer and executes their {@code onStartup}
   * methods. Should be called in worker harness implementations at the very beginning of their main
   * method.
   */
  public static void runOnStartup() {
    for (JvmInitializer initializer : ReflectHelpers.loadServicesOrdered(JvmInitializer.class)) {
      // We write to standard out since logging has yet to be initialized.
      System.out.format("Running JvmInitializer#onStartup for %s%n", initializer);
      initializer.onStartup();
      System.out.format("Completed JvmInitializer#onStartup for %s%n", initializer);
    }
  }

  /**
   * Finds all registered implementations of JvmInitializer and executes their {@code
   * beforeProcessing} methods. Should be called in worker harness implementations after
   * initialization but before beginning to process any data.
   *
   * @param options The pipeline options passed to the worker.
   */
  public static void runBeforeProcessing(PipelineOptions options) {
    // We load the logger in the method to minimize the amount of class loading that happens
    // during class initialization.
    Logger logger = LoggerFactory.getLogger(JvmInitializers.class);

    try {
      for (JvmInitializer initializer : ReflectHelpers.loadServicesOrdered(JvmInitializer.class)) {
        logger.info("Running JvmInitializer#beforeProcessing for {}", initializer);
        initializer.beforeProcessing(options);
        logger.info("Completed JvmInitializer#beforeProcessing for {}", initializer);
      }
      initialized.compareAndSet(false, true);
    } catch (Error e) {
      if (initialized.get()) {
        logger.warn(
            "Error at JvmInitializer#beforeProcessing. This error is suppressed after "
                + "previous success runs. It is expected on Embedded environment",
            e);
      } else {
        throw e;
      }
    }
  }
}
