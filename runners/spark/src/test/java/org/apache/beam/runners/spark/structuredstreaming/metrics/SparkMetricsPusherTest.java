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
package org.apache.beam.runners.spark.structuredstreaming.metrics;

import org.apache.beam.runners.core.metrics.TestMetricsSink;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.metrics.MetricsOptions;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.UsesMetricsPusher;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

/**
 * TODO: add testInStreamingMode() once streaming support will be implemented.
 *
 * A test that verifies that metrics push system works in spark runner.
 */
public class SparkMetricsPusherTest {

  private static final Logger LOG = LoggerFactory.getLogger(
      SparkMetricsPusherTest.class);

  @Rule public final TestPipeline pipeline = TestPipeline.create();

  @Before
  public void init() {
    TestMetricsSink.clear();
    MetricsOptions options = pipeline.getOptions().as(MetricsOptions.class);
    options.setMetricsSink(TestMetricsSink.class);
  }

  private static class CountingDoFn extends DoFn<Integer, Integer> {
    private final Counter counter = Metrics.counter(
        SparkMetricsPusherTest.class, "counter");

    @ProcessElement
    public void processElement(ProcessContext context) {
      try {
        counter.inc();
        context.output(context.element());
      } catch (Exception e) {
        LOG.warn("Exception caught" + e);
      }
    }
  }

  @Category(UsesMetricsPusher.class)
  @Test
  public void testInSBatchMode() throws Exception {
    pipeline.apply(Create.of(1, 2, 3, 4, 5, 6)).apply(ParDo.of(new CountingDoFn()));

    pipeline.run();
    // give metrics pusher time to push
    Thread.sleep(
        (pipeline.getOptions().as(MetricsOptions.class).getMetricsPushPeriod() + 1L) * 1000);
    assertThat(TestMetricsSink.getCounterValue(), is(6L));
  }
}
