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

import java.io.IOException;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.metrics.MetricResults;
import org.joda.time.Duration;

/**
 * The {@link PipelineResult} of executing a {@link org.apache.beam.sdk.Pipeline} using the {@link
 * PrismRunner} and an internal {@link PipelineResult} delegate.
 */
class PrismPipelineResult implements PipelineResult {

  private final PipelineResult delegate;
  private final Runnable cleanup;

  /**
   * Instantiate the {@link PipelineResult} from the {@param delegate} and a {@param cancel} to be
   * called when stopping the underlying executable Job management service.
   */
  PrismPipelineResult(PipelineResult delegate, Runnable cancel) {
    this.delegate = delegate;
    this.cleanup = cancel;
  }

  Runnable getCleanup() {
    return cleanup;
  }

  /** Forwards the result of the delegate {@link PipelineResult#getState}. */
  @Override
  public State getState() {
    return delegate.getState();
  }

  /**
   * Forwards the result of the delegate {@link PipelineResult#cancel}. Invokes {@link
   * PrismExecutor#stop()} before returning the resulting {@link
   * org.apache.beam.sdk.PipelineResult.State}.
   */
  @Override
  public State cancel() throws IOException {
    State state = delegate.cancel();
    this.cleanup.run();
    return state;
  }

  /**
   * Forwards the result of the delegate {@link PipelineResult#waitUntilFinish(Duration)}. Invokes
   * {@link PrismExecutor#stop()} before returning the resulting {@link
   * org.apache.beam.sdk.PipelineResult.State}.
   */
  @Override
  public State waitUntilFinish(Duration duration) {
    State state = delegate.waitUntilFinish(duration);
    this.cleanup.run();
    return state;
  }

  /**
   * Forwards the result of the delegate {@link PipelineResult#waitUntilFinish}. Invokes {@link
   * PrismExecutor#stop()} before returning the resulting {@link
   * org.apache.beam.sdk.PipelineResult.State}.
   */
  @Override
  public State waitUntilFinish() {
    State state = delegate.waitUntilFinish();
    this.cleanup.run();
    return state;
  }

  /** Forwards the result of the delegate {@link PipelineResult#metrics}. */
  @Override
  public MetricResults metrics() {
    return delegate.metrics();
  }
}
