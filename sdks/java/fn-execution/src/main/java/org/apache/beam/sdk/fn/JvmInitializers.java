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

import org.apache.beam.sdk.harness.JvmInitializer;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.util.common.ReflectHelpers;

/** Helpers for executing {@link JvmInitializer} implementations. */
public class JvmInitializers {
  /**
   * Finds all registered implementations of JvmInitializer and executes their {@code onStartup}
   * methods. Should be called in worker harness implementations at the very beginning of their main
   * method.
   */
  public static void runOnStartup() {
    for (JvmInitializer initializer : ReflectHelpers.loadServicesOrdered(JvmInitializer.class)) {
      initializer.onStartup();
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
    for (JvmInitializer initializer : ReflectHelpers.loadServicesOrdered(JvmInitializer.class)) {
      initializer.beforeProcessing(options);
    }
  }
}
