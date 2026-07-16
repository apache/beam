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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.beam.model.jobmanagement.v1.JobApi;
import org.apache.beam.runners.core.metrics.MetricsContainerStepMap;
import org.apache.beam.runners.jobsubmission.PortablePipelineResult;
import org.apache.beam.sdk.metrics.MetricResults;
import org.apache.kafka.streams.KafkaStreams;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Result of executing a portable pipeline as a {@link KafkaStreams} application.
 *
 * <p>Translates the underlying {@link KafkaStreams.State} into Beam's {@link
 * org.apache.beam.sdk.PipelineResult.State} and forwards {@link #cancel()} / {@link
 * #waitUntilFinish()} to the {@code KafkaStreams} instance.
 */
class KafkaStreamsPortablePipelineResult implements PortablePipelineResult {

  private static final Logger LOG =
      LoggerFactory.getLogger(KafkaStreamsPortablePipelineResult.class);

  private final KafkaStreams kafkaStreams;
  // The job's metrics accumulator, shared by reference with the topology's stage processors, which
  // update it as the SDK harness reports bundle metrics.
  private final MetricsContainerStepMap metricsContainerStepMap;
  private final CountDownLatch terminated = new CountDownLatch(1);
  private volatile boolean cancelled = false;

  KafkaStreamsPortablePipelineResult(
      KafkaStreams kafkaStreams, MetricsContainerStepMap metricsContainerStepMap) {
    this.kafkaStreams = kafkaStreams;
    this.metricsContainerStepMap = metricsContainerStepMap;
    kafkaStreams.setStateListener(
        (newState, oldState) -> {
          if (newState == KafkaStreams.State.NOT_RUNNING || newState == KafkaStreams.State.ERROR) {
            terminated.countDown();
          }
        });
    // Guard against the race where the KafkaStreams instance transitions to a terminal state
    // (e.g. immediate startup failure) before the state listener is registered above. Without
    // this check, the latch would never be counted down and waitUntilFinish() would block forever.
    KafkaStreams.State current = kafkaStreams.state();
    if (current == KafkaStreams.State.NOT_RUNNING || current == KafkaStreams.State.ERROR) {
      terminated.countDown();
    }
  }

  @Override
  public State getState() {
    if (cancelled) {
      return State.CANCELLED;
    }
    return mapState(kafkaStreams.state());
  }

  @Override
  public State cancel() throws IOException {
    cancelled = true;
    kafkaStreams.close();
    terminated.countDown();
    return getState();
  }

  @Override
  public State waitUntilFinish(Duration duration) {
    try {
      boolean reachedTerminal = terminated.await(duration.getMillis(), TimeUnit.MILLISECONDS);
      if (!reachedTerminal) {
        return getState();
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      return State.UNKNOWN;
    }
    return getState();
  }

  @Override
  public State waitUntilFinish() {
    try {
      terminated.await();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      return State.UNKNOWN;
    }
    return getState();
  }

  @Override
  public MetricResults metrics() {
    // Attempted values only: the runner does not distinguish committed results yet (that needs
    // metrics to be folded into the exactly-once commit, which lands with the durability work).
    return MetricsContainerStepMap.asAttemptedOnlyMetricResults(metricsContainerStepMap);
  }

  @Override
  public JobApi.MetricResults portableMetrics() throws UnsupportedOperationException {
    LOG.debug("portableMetrics() not yet implemented in the Kafka Streams runner");
    return JobApi.MetricResults.newBuilder().build();
  }

  private static State mapState(KafkaStreams.State state) {
    switch (state) {
      case CREATED:
      case REBALANCING:
        return State.RUNNING;
      case RUNNING:
        return State.RUNNING;
      case PENDING_SHUTDOWN:
        return State.CANCELLED;
      case PENDING_ERROR:
      case ERROR:
        return State.FAILED;
      case NOT_RUNNING:
        return State.DONE;
      default:
        return State.UNKNOWN;
    }
  }
}
