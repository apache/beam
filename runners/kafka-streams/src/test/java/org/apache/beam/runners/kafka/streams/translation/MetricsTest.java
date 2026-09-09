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
package org.apache.beam.runners.kafka.streams.translation;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

import org.apache.beam.runners.kafka.streams.KafkaStreamsTestRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.MetricNameFilter;
import org.apache.beam.sdk.metrics.MetricQueryResults;
import org.apache.beam.sdk.metrics.MetricResult;
import org.apache.beam.sdk.metrics.MetricResults;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.metrics.MetricsFilter;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Iterables;
import org.junit.Test;

/**
 * End-to-end test that user metrics reported by a {@link DoFn} in the SDK harness surface through
 * the runner as {@link MetricResults}.
 *
 * <p>The harness reports metrics as Fn API MonitoringInfos with each bundle; the stage processor
 * folds them into the job's metrics step map, and the runner exposes them as attempted {@link
 * MetricResults}. This is the surface {@code PAssert} uses to verify its assertions actually ran,
 * so it is a prerequisite for the {@code @ValidatesRunner} suite.
 */
public class MetricsTest {

  private static final String NAMESPACE = "MetricsTest";
  private static final String COUNTER_NAME = "elements";

  /** Increments a user counter for every element the harness feeds it. */
  private static class CountingFn extends DoFn<Integer, Integer> {
    private final Counter counter = Metrics.counter(NAMESPACE, COUNTER_NAME);

    @ProcessElement
    public void processElement(@Element Integer input, OutputReceiver<Integer> out) {
      counter.inc();
      out.output(input);
    }
  }

  @Test
  public void userCounterFromHarnessSurfacesInMetricResults() {
    Pipeline pipeline = Pipeline.create(KafkaStreamsTestRunner.testOptions());
    pipeline.apply("create", Create.of(1, 2, 3)).apply("count", ParDo.of(new CountingFn()));

    MetricResults metrics = KafkaStreamsTestRunner.run(pipeline);

    MetricQueryResults query =
        metrics.queryMetrics(
            MetricsFilter.builder()
                .addNameFilter(MetricNameFilter.named(NAMESPACE, COUNTER_NAME))
                .build());
    MetricResult<Long> counter = Iterables.getOnlyElement(query.getCounters());
    // Create.of(1, 2, 3) feeds the DoFn exactly three elements.
    assertThat(counter.getAttempted(), is(3L));
  }
}
