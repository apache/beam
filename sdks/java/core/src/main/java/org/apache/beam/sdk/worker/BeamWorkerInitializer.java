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
package org.apache.beam.sdk.worker;

import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.options.PipelineOptions;

/**
 * A service interface for defining one-time initialization for Beam workers.
 *
 * <p>Beam workers will run every registered implementation's {@code onStartup} and {@code
 * beforeProcessing} functions at the appropriate stage of execution. Currently this is only
 * supported in the portable worker and legacy Dataflow worker.
 *
 * <p>{@link java.util.ServiceLoader} is used to discover implementations of {@link
 * BeamWorkerInitializer}, note that you will need to register your implementation with the
 * appropriate resources to ensure your code is executed. You can use a tool like {@link
 * com.google.auto.service.AutoService} to automate this.
 */
@Experimental
public abstract class BeamWorkerInitializer {

  /**
   * Implement onStartup to run some custom initialization immediately after the worker begins
   * running.
   *
   * <p>In general users should prefer to implement {@code beforeProcessing} to perform custom
   * initialization so that basic services such as logging can be initialized first, but {@code
   * onStartup} is also provided if initialization absolutely needs to be run immediately after
   * starting.
   */
  public void onStartup() {}

  /**
   * Implement beforeProcessing to run some custom initialization after the worker initializes
   * itself, but before it begins process data.
   *
   * @param options The pipeline options passed to the worker.
   */
  public void beforeProcessing(PipelineOptions options) {}
}
