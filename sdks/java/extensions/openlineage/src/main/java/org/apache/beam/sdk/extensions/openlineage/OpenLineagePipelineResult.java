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
package org.apache.beam.sdk.extensions.openlineage;

import java.io.IOException;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.metrics.MetricResults;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Duration;

/**
 * Delegating {@link PipelineResult} returned by {@link OpenLineageRunner} that emits the terminal
 * OpenLineage event deterministically when the caller observes job completion through {@link
 * #waitUntilFinish()} or {@link #cancel()} — instead of relying solely on the periodic {@link
 * OpenLineageJobTracker} poll, which could miss a fast batch job if the JVM exits before the next
 * tick.
 */
class OpenLineagePipelineResult implements PipelineResult {

  private final PipelineResult delegate;
  private final OpenLineageContext context;

  OpenLineagePipelineResult(PipelineResult delegate, OpenLineageContext context) {
    this.delegate = delegate;
    this.context = context;
  }

  @Override
  public State getState() {
    return delegate.getState();
  }

  @Override
  public State cancel() throws IOException {
    try {
      State state = delegate.cancel();
      afterTerminal(state == null ? State.CANCELLED : state);
      return state;
    } catch (IOException | RuntimeException e) {
      afterTerminal(State.CANCELLED);
      throw e;
    }
  }

  @Override
  public State waitUntilFinish(Duration duration) {
    State state = delegate.waitUntilFinish(duration);
    afterTerminal(state);
    return state;
  }

  @Override
  public State waitUntilFinish() {
    State state = delegate.waitUntilFinish();
    afterTerminal(state);
    return state;
  }

  @Override
  public MetricResults metrics() {
    return delegate.metrics();
  }

  private void afterTerminal(@Nullable State state) {
    if (state == null || !state.isTerminal()) {
      return;
    }
    context.sweepLineageMetrics(delegate);
    context.onJobFinished(OpenLineageJobTracker.terminalEventType(state), null);
  }
}
