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
package org.apache.beam.runners.flink;

import org.apache.beam.sdk.AggregatorRetrievalException;
import org.apache.beam.sdk.AggregatorValues;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.transforms.Aggregator;
import org.apache.commons.lang.NotImplementedException;
import org.joda.time.Duration;

import java.io.IOException;

/**
 * Result of executing a {@link org.apache.beam.sdk.Pipeline} with Flink. This
 * has methods to query to job runtime and the final values of
 * {@link Aggregator}s.
 */
public class FlinkDetachedRunnerResult implements PipelineResult {

  FlinkDetachedRunnerResult() {}

  @Override
  public State getState() {
    return State.UNKNOWN;
  }

  @Override
  public <T> AggregatorValues<T> getAggregatorValues(final Aggregator<?, T> aggregator)
      throws AggregatorRetrievalException {
    throw new AggregatorRetrievalException(
        "Accumulators can't be retrieved for detached Job executions.",
        new NotImplementedException());
  }

  @Override
  public State cancel() throws IOException {
    throw new UnsupportedOperationException("Cancelling is not yet supported.");
  }

  @Override
  public State waitUntilFinish() {
    return State.UNKNOWN;
  }

  @Override
  public State waitUntilFinish(Duration duration) {
    return State.UNKNOWN;
  }

  @Override
  public String toString() {
    return "FlinkDetachedRunnerResult{}";
  }
}
