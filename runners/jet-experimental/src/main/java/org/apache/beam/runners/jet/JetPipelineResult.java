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
package org.apache.beam.runners.jet;

import com.hazelcast.jet.IMapJet;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.core.JobStatus;
import java.util.Objects;
import javax.annotation.concurrent.GuardedBy;
import org.apache.beam.runners.core.metrics.MetricUpdates;
import org.apache.beam.runners.jet.metrics.JetMetricResults;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.metrics.MetricResults;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Jet specific implementation of {@link PipelineResult}. */
public class JetPipelineResult implements PipelineResult {

  private static final Logger LOG = LoggerFactory.getLogger(JetRunner.class);

  private final IMapJet<String, MetricUpdates> metricsAccumulator;
  private final JetMetricResults metricResults = new JetMetricResults();

  @GuardedBy("this")
  private Job job = null;

  @GuardedBy("this")
  private State state = State.UNKNOWN;

  JetPipelineResult(IMapJet<String, MetricUpdates> metricsAccumulator) {
    this.metricsAccumulator = Objects.requireNonNull(metricsAccumulator);
    this.metricsAccumulator.addEntryListener(metricResults, true);
  }

  private static State getState(Job job) {
    JobStatus status = job.getStatus();
    switch (status) {
      case COMPLETED:
        return State.DONE;
      case COMPLETING:
      case RUNNING:
      case STARTING:
        return State.RUNNING;
      case FAILED:
        return State.FAILED;
      case NOT_RUNNING:
      case SUSPENDED:
        return State.STOPPED;
      default:
        LOG.warn("Unhandled " + JobStatus.class.getSimpleName() + ": " + status.name() + "!");
        return State.UNKNOWN;
    }
  }

  synchronized void setJob(Job job) {
    Job nonNullJob = job == null ? this.job : job;
    this.state = getState(nonNullJob);
    this.job = job;
  }

  @Override
  public synchronized State getState() {
    if (job != null) {
      state = getState(job);
    }
    return state;
  }

  @Override
  public synchronized State cancel() {
    if (job != null) {
      job.cancel();
      job = null;
      state = State.STOPPED;
    }
    return state;
  }

  @Override
  public State waitUntilFinish(Duration duration) {
    return waitUntilFinish(); // todo: how to time out?
  }

  @Override
  public synchronized State waitUntilFinish() {
    if (job != null) {
      try {
        job.join();
      } catch (Exception e) {
        e.printStackTrace(); // todo: what to do?
        return State.FAILED;
      }
      state = getState(job);
    }
    return state;
  }

  @Override
  public MetricResults metrics() {
    return metricResults;
  }
}
