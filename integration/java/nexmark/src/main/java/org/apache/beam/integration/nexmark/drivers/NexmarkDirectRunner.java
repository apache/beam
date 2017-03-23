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
package org.apache.beam.integration.nexmark.drivers;

import org.apache.beam.integration.nexmark.NexmarkRunner;

/**
 * Run a single query using the Direct Runner.
 */
class NexmarkDirectRunner extends NexmarkRunner<NexmarkDirectDriver.NexmarkDirectOptions> {
  public NexmarkDirectRunner(NexmarkDirectDriver.NexmarkDirectOptions options) {
    super(options);
  }

  @Override
  protected boolean isStreaming() {
    return options.isStreaming();
  }

  @Override
  protected int coresPerWorker() {
    return 4;
  }

  @Override
  protected int maxNumWorkers() {
    return 1;
  }

  @Override
  protected void invokeBuilderForPublishOnlyPipeline(PipelineBuilder builder) {
    throw new UnsupportedOperationException(
        "Cannot use --pubSubMode=COMBINED with DirectRunner");
  }

  /**
   * Monitor the progress of the publisher job. Return when it has been generating events for
   * at least {@code configuration.preloadSeconds}.
   */
  @Override
  protected void waitForPublisherPreload() {
    throw new UnsupportedOperationException(
        "Cannot use --pubSubMode=COMBINED with DirectRunner");
  }
}
