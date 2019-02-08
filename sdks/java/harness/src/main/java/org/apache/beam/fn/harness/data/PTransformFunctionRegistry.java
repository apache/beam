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
package org.apache.beam.fn.harness.data;

import java.util.ArrayList;
import java.util.List;
import org.apache.beam.sdk.fn.function.ThrowingRunnable;

/**
 * A class to to register and retrieve functions for runner classes (i.e. the start, or finish
 * function). The purpose of this class is to wrap these functions with instrumentation i.e. for
 * metrics and other telemetry collection.
 *
 * <p>Usage: // Instantiate and use the registry for each class of functions. i.e. start. finish.
 *
 * <pre>
 * PTransformFunctionRegistry startFunctionRegistry;
 * PTransformFunctionRegistry finishFunctionRegistry;
 * startFunctionRegistry.register(myStartThrowingRunnable);
 * finishFunctionRegistry.register(myFinishThrowingRunnable);
 *
 * // Then invoke the functions by iterating over them, in your desired order: i.e.
 * for (ThrowingRunnable startFunction : startFunctionRegistry.getFunctions()) {
 *   startFunction.run();
 * }
 *
 * for (ThrowingRunnable finishFunction : Lists.reverse(finishFunctionRegistry.getFunctions())) {
 *   finishFunction.run();
 * }
 * // Note: this is used in ProcessBundleHandler.
 * </pre>
 */
public class PTransformFunctionRegistry {

  private List<ThrowingRunnable> runnables = new ArrayList<>();

  /**
   * Register the runnable to process the specific pTransformId and track its execution time.
   *
   * @param pTransformId
   * @param runnable
   */
  public void register(String pTransformId, ThrowingRunnable runnable) {
    runnables.add(runnable);
  }

  /**
   * @return A list of wrapper functions which will invoke the registered functions indirectly. The
   *     order of registry is maintained.
   */
  public List<ThrowingRunnable> getFunctions() {
    return runnables;
  }
}
