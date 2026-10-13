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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasProperty;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.apache.beam.model.jobmanagement.v1.JobApi;
import org.apache.beam.runners.core.metrics.MetricsContainerImpl;
import org.apache.beam.runners.core.metrics.MetricsContainerStepMap;
import org.apache.beam.sdk.metrics.MetricName;
import org.apache.kafka.streams.KafkaStreams;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Tests for {@link KafkaStreamsPortablePipelineResult}, in particular the metrics an SDK other than
 * Java reads its results through.
 */
@RunWith(JUnit4.class)
public class KafkaStreamsPortablePipelineResultTest {

  private static final String STEP = "a-stage";
  private static final String NAMESPACE = "ns";
  private static final String COUNTER = "elements";

  private static KafkaStreams idleClient() {
    KafkaStreams kafkaStreams = mock(KafkaStreams.class);
    // The result registers a state listener and checks the current state, so it has to have one.
    when(kafkaStreams.state()).thenReturn(KafkaStreams.State.CREATED);
    return kafkaStreams;
  }

  @Test
  public void portableMetricsReportWhatTheHarnessMeasured() {
    MetricsContainerStepMap stepMap = new MetricsContainerStepMap();
    MetricsContainerImpl container = stepMap.getContainer(STEP);
    container.getCounter(MetricName.named(NAMESPACE, COUNTER)).inc(7);

    KafkaStreamsPortablePipelineResult result =
        new KafkaStreamsPortablePipelineResult(idleClient(), stepMap, () -> {});

    JobApi.MetricResults metrics = result.portableMetrics();

    // A pipeline from another SDK reads these over the job API; before they were reported the list
    // was empty and a Python pipeline saw no metrics at all.
    assertThat(
        metrics.getAttemptedList(),
        hasItem(hasProperty("urn", is("beam:metric:user:sum_int64:v1"))));
    assertThat(metrics.getAttemptedCount(), is(not(0)));
  }

  @Test
  public void portableMetricsAreNotReportedAsCommitted() {
    // The values are what the SDK harness reported per bundle, which is not tied to the commit of
    // the records that produced them, so claiming them as committed would be wrong.
    // See https://github.com/apache/beam/issues/39635.
    MetricsContainerStepMap stepMap = new MetricsContainerStepMap();
    stepMap.getContainer(STEP).getCounter(MetricName.named(NAMESPACE, COUNTER)).inc(1);

    KafkaStreamsPortablePipelineResult result =
        new KafkaStreamsPortablePipelineResult(idleClient(), stepMap, () -> {});

    assertThat(result.portableMetrics().getCommittedCount(), is(0));
  }

  @Test
  public void aPipelineThatMeasuredNothingReportsNothing() {
    KafkaStreamsPortablePipelineResult result =
        new KafkaStreamsPortablePipelineResult(
            idleClient(), new MetricsContainerStepMap(), () -> {});

    assertThat(result.portableMetrics().getAttemptedCount(), is(0));
  }
}
