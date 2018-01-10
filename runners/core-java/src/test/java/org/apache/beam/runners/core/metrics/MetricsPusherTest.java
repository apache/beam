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

package org.apache.beam.runners.core.metrics;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

import org.apache.beam.sdk.io.GenerateSequence;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.UsesAttemptedMetrics;
import org.apache.beam.sdk.testing.UsesCounterMetrics;
import org.apache.beam.sdk.testing.ValidatesRunner;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.joda.time.Duration;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * A test that verifies that metrics push system works.
 */
public class MetricsPusherTest {

  private static final long NUM_ELEMENTS = 1000L;
  @Rule
  public final TestPipeline pipeline = TestPipeline.create();

  private static class CountingDoFn extends DoFn<Long, Long>{
    private final Counter counter = Metrics
        .counter(MetricsPusherTest.class, "counter");
    @ProcessElement
    public void processElement(ProcessContext context) {
      try {
        counter.inc();
        context.output(context.element());
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
  }

  @Category({ValidatesRunner.class, UsesAttemptedMetrics.class, UsesCounterMetrics.class})
  @Test
  public void test() throws Exception {
    assertThat(DummyMetricsSink.getCounterValue(), is(0L));
    pipeline.apply(
        // Use maxReadTime to force unbounded mode.
        GenerateSequence.from(0).to(NUM_ELEMENTS).withMaxReadTime(Duration.standardDays(1)))
        .apply(ParDo.of(new CountingDoFn()));
    pipeline.run();
    assertThat(DummyMetricsSink.getCounterValue(), is(NUM_ELEMENTS));
  }
}
