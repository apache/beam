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
package org.apache.beam.runners.prism;

import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkState;

import java.io.IOException;
import org.apache.beam.runners.portability.JobServicePipelineResult;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.metrics.MetricResults;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The {@link PipelineResult} of executing a {@link org.apache.beam.sdk.Pipeline} using the {@link
 * PrismRunner} and an internal {@link PipelineResult} delegate.
 */
class PrismPipelineResult implements PipelineResult {

  private static final Logger LOG = LoggerFactory.getLogger(PrismPipelineResult.class);

  private final JobServicePipelineResult delegate;
  private final Runnable closer;

  /**
   * Instantiate the {@link PipelineResult} from the {@param delegate} and {@link
   * PrismExecutor#stop()} provided as the {@param closer}.
   */
  PrismPipelineResult(JobServicePipelineResult delegate, Runnable closer) {
    this.delegate = delegate;
    this.closer = closer;
  }

  /**
   * Callback that invokes the stored {@link PrismExecutor#stop} method. Intended for use with a
   * {@link java.util.concurrent.CompletableFuture<org.apache.beam.sdk.PipelineResult.State>}.
   */
  void onJobStateComplete(State state, Throwable error) {
    checkState(state.isTerminal(), "onJobStateComplete called on non-terminal state: %s", state);
    LOG.info("received terminal state: {}", state);
    this.delegate.close();
    this.closer.run();
    if (error != null) {
      throw new RuntimeException(error);
    }
  }

  /** Forwards the result of the delegate {@link PipelineResult#getState}. */
  @Override
  public State getState() {
    return delegate.getState();
  }

  /** Forwards the result of the delegate {@link PipelineResult#cancel}. */
  @Override
  public State cancel() throws IOException {
    return delegate.cancel();
  }

  /** Forwards the result of the delegate {@link PipelineResult#waitUntilFinish(Duration)}. */
  @Override
  public State waitUntilFinish(Duration duration) {
    return delegate.waitUntilFinish(duration);
  }

  /** Forwards the result of the delegate {@link PipelineResult#waitUntilFinish}. */
  @Override
  public State waitUntilFinish() {
    return delegate.waitUntilFinish();
  }

  /** Forwards the result of the delegate {@link PipelineResult#metrics}. */
  @Override
  public MetricResults metrics() {
    return delegate.metrics();
  }
}
