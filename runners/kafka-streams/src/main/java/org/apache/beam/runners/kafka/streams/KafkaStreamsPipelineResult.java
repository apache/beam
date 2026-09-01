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
package org.apache.beam.runners.kafka.streams;

import java.io.IOException;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.metrics.MetricResults;
import org.joda.time.Duration;

/**
 * Forwards {@link PipelineResult} calls to a delegate and stops an embedded job server when the
 * pipeline reaches a terminal state.
 */
class KafkaStreamsPipelineResult implements PipelineResult {

  private final PipelineResult delegate;
  private final Runnable stopJobServer;

  KafkaStreamsPipelineResult(PipelineResult delegate, Runnable stopJobServer) {
    this.delegate = delegate;
    this.stopJobServer = stopJobServer;
  }

  @Override
  public State getState() {
    return delegate.getState();
  }

  @Override
  public State cancel() throws IOException {
    try {
      return delegate.cancel();
    } finally {
      stopJobServer.run();
    }
  }

  @Override
  public State waitUntilFinish(Duration duration) {
    State state = delegate.waitUntilFinish(duration);
    // A null/non-terminal state means the wait timed out and the pipeline is still running;
    // keep the job server alive so the caller can continue to interact with the job.
    if (state != null && state.isTerminal()) {
      stopJobServer.run();
    }
    return state;
  }

  @Override
  public State waitUntilFinish() {
    try {
      return delegate.waitUntilFinish();
    } finally {
      stopJobServer.run();
    }
  }

  @Override
  public MetricResults metrics() {
    return delegate.metrics();
  }
}
