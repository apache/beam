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
package org.apache.beam.runners.gearpump;

import java.io.IOException;

import org.apache.beam.sdk.AggregatorRetrievalException;
import org.apache.beam.sdk.AggregatorValues;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.metrics.MetricResults;
import org.apache.beam.sdk.transforms.Aggregator;

import org.joda.time.Duration;


/**
 * Result of executing a {@link Pipeline} with Gearpump.
 */
public class GearpumpPipelineResult implements PipelineResult {
  @Override
  public State getState() {
    return null;
  }

  @Override
  public State cancel() throws IOException {
    return null;
  }

  @Override
  public State waitUntilFinish(Duration duration) {
    return null;
  }

  @Override
  public State waitUntilFinish() {
    return null;
  }

  @Override
  public <T> AggregatorValues<T> getAggregatorValues(Aggregator<?, T> aggregator)
      throws AggregatorRetrievalException {
    throw new AggregatorRetrievalException(
        "PipelineResult getAggregatorValues not supported in Gearpump pipeline",
        new UnsupportedOperationException());
  }

  @Override
  public MetricResults metrics() {
    return null;
  }

}
