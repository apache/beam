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

import org.apache.beam.runners.kafka.streams.KafkaStreamsPipelineOptions;
import org.apache.beam.runners.kafka.streams.KafkaStreamsTestRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.MetricNameFilter;
import org.apache.beam.sdk.metrics.MetricQueryResults;
import org.apache.beam.sdk.metrics.MetricResults;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.metrics.MetricsFilter;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Iterables;
import org.junit.Test;

/**
 * Checks that a user counter stays correct when the bundle size bound makes a stage run many small
 * bundles instead of one large one.
 *
 * <p>The runner folds the metrics the SDK harness reports into the job's step map as each bundle
 * completes, and those updates add rather than replace. That is right only if each report covers
 * its own bundle, so splitting the same input across more bundles must not change the total.
 */
public class MetricsAcrossBundlesTest {
  private static class CountingFn extends DoFn<Integer, Integer> {
    private final Counter counter = Metrics.counter("probe", "elements");

    @ProcessElement
    public void processElement(@Element Integer in, OutputReceiver<Integer> out) {
      counter.inc();
      out.output(in);
    }
  }

  private static long run(int maxBundleSize) {
    KafkaStreamsPipelineOptions options =
        KafkaStreamsTestRunner.testOptions().as(KafkaStreamsPipelineOptions.class);
    options.setMaxBundleSize(maxBundleSize);
    Pipeline p = Pipeline.create(options);
    p.apply(Create.of(1, 2, 3, 4, 5, 6)).apply(ParDo.of(new CountingFn()));
    MetricResults metrics = KafkaStreamsTestRunner.run(p);
    MetricQueryResults q =
        metrics.queryMetrics(
            MetricsFilter.builder()
                .addNameFilter(MetricNameFilter.named("probe", "elements"))
                .build());
    return Iterables.getOnlyElement(q.getCounters()).getAttempted();
  }

  @Test
  public void oneBundle() {
    assertThat(run(1000), is(6L));
  }

  @Test
  public void manyBundles() {
    assertThat(run(1), is(6L));
  }
}
