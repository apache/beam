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
package org.apache.beam.runners.direct;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult.State;
import org.apache.beam.sdk.runners.AppliedPTransform;
import org.apache.beam.sdk.transforms.PTransform;
import org.joda.time.Duration;

/**
 * An executor that schedules and executes {@link AppliedPTransform AppliedPTransforms} for both
 * source and intermediate {@link PTransform PTransforms}.
 */
interface PipelineExecutor {
  /**
   * Starts this executor on the provided graph. The {@link RootProviderRegistry} will be used to
   * create initial inputs for the provide {@link DirectGraph graph}.
   */
  void start(DirectGraph graph, RootProviderRegistry rootProviderRegistry);

  /**
   * Blocks until the job being executed enters a terminal state. A job is completed after all root
   * {@link AppliedPTransform AppliedPTransforms} have completed, and all {@link CommittedBundle
   * Bundles} have been consumed. Jobs may also terminate abnormally.
   *
   * <p>Waits for up to the provided duration, or forever if the provided duration is less than or
   * equal to zero.
   *
   * @return The terminal state of the Pipeline.
   * @throws Exception whenever an executor thread throws anything, transfers to the waiting thread
   *     and rethrows it
   */
  State waitUntilFinish(Duration duration) throws Exception;

  /** Gets the current state of the {@link Pipeline}. */
  State getPipelineState();

  /**
   * Shuts down the executor.
   *
   * <p>The executor may continue to run for a short time after this method returns.
   */
  void stop();
}
