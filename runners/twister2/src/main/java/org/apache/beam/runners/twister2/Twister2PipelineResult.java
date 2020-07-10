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
package org.apache.beam.runners.twister2;

import java.io.IOException;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.metrics.MetricResults;
import org.joda.time.Duration;

/** Represents a Twister2 pipeline execution result. */
public class Twister2PipelineResult implements PipelineResult {

  PipelineResult.State state = State.RUNNING;

  @Override
  public State getState() {
    return state;
  }

  @Override
  public State cancel() throws IOException {
    throw new UnsupportedOperationException("Operation not supported");
  }

  @Override
  public State waitUntilFinish(Duration duration) {
    throw new UnsupportedOperationException("Operation not supported");
  }

  @Override
  public State waitUntilFinish() {
    throw new UnsupportedOperationException("Operation not supported");
  }

  @Override
  public MetricResults metrics() {
    throw new UnsupportedOperationException("Operation not supported");
  }

  public void setState(State state) {
    this.state = state;
  }
}
