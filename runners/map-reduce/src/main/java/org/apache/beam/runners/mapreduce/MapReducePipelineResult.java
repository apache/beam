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
package org.apache.beam.runners.mapreduce;

import static com.google.common.base.Preconditions.checkNotNull;

import java.io.IOException;
import java.util.List;
import org.apache.beam.runners.mapreduce.translation.MapReduceMetricResults;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.metrics.MetricResults;
import org.apache.hadoop.mapreduce.Job;
import org.joda.time.Duration;

public class MapReducePipelineResult implements PipelineResult {

  private final List<Job> jobs;
  public MapReducePipelineResult(List<Job> jobs) {
    this.jobs = checkNotNull(jobs, "jobs");
  }

  @Override
  public State getState() {
    return State.DONE;
  }

  @Override
  public State cancel() throws IOException {
    return State.DONE;
  }

  @Override
  public State waitUntilFinish(Duration duration) {
    return State.DONE;
  }

  @Override
  public State waitUntilFinish() {
    return State.DONE;
  }

  @Override
  public MetricResults metrics() {
    return new MapReduceMetricResults(jobs);
  }
}
