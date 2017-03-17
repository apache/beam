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

package org.apache.beam.sdk.metrics;

import static org.apache.beam.sdk.metrics.MetricMatchers.attemptedMetricsResult;
import static org.apache.beam.sdk.metrics.MetricMatchers.committedMetricsResult;
import static org.apache.beam.sdk.metrics.MetricMatchers.distributionAttemptedMinMax;
import static org.apache.beam.sdk.metrics.MetricMatchers.distributionCommittedMinMax;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;

import java.io.Serializable;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.testing.RunnableOnService;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.UsesAttemptedMetrics;
import org.apache.beam.sdk.testing.UsesCommittedMetrics;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.hamcrest.CoreMatchers;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Tests for {@link Metrics}.
 */
public class MetricsTest implements Serializable {

  private static final String NS = "test";
  private static final String NAME = "name";
  private static final MetricName METRIC_NAME = MetricName.named(NS, NAME);

  @Rule
  public final transient TestPipeline pipeline = TestPipeline.create();

  @After
  public void tearDown() {
    MetricsEnvironment.setCurrentContainer(null);
  }

  @Test
  public void distributionWithoutContainer() {
    assertNull(MetricsEnvironment.getCurrentContainer());
    // Should not fail even though there is no metrics container.
    Metrics.distribution(NS, NAME).update(5L);
  }

  @Test
  public void counterWithoutContainer() {
    assertNull(MetricsEnvironment.getCurrentContainer());
    // Should not fail even though there is no metrics container.
    Counter counter = Metrics.counter(NS, NAME);
    counter.inc();
    counter.inc(5L);
    counter.dec();
    counter.dec(5L);
  }

  @Test
  public void distributionToCell() {
    MetricsContainer container = new MetricsContainer("step");
    MetricsEnvironment.setCurrentContainer(container);

    Distribution distribution = Metrics.distribution(NS, NAME);

    distribution.update(5L);

    DistributionCell cell = container.getDistribution(METRIC_NAME);
    assertThat(cell.getCumulative(), equalTo(DistributionData.create(5, 1, 5, 5)));

    distribution.update(36L);
    assertThat(cell.getCumulative(), equalTo(DistributionData.create(41, 2, 5, 36)));

    distribution.update(1L);
    assertThat(cell.getCumulative(), equalTo(DistributionData.create(42, 3, 1, 36)));
  }

  @Test
  public void counterToCell() {
    MetricsContainer container = new MetricsContainer("step");
    MetricsEnvironment.setCurrentContainer(container);
    Counter counter = Metrics.counter(NS, NAME);
    CounterCell cell = container.getCounter(METRIC_NAME);
    counter.inc();
    assertThat(cell.getCumulative(), CoreMatchers.equalTo(1L));

    counter.inc(47L);
    assertThat(cell.getCumulative(), CoreMatchers.equalTo(48L));

    counter.dec(5L);
    assertThat(cell.getCumulative(), CoreMatchers.equalTo(43L));

    counter.dec();
    assertThat(cell.getCumulative(), CoreMatchers.equalTo(42L));
  }

  @Category({RunnableOnService.class, UsesCommittedMetrics.class})
  @Test
  public void committedMetricsReportToQuery() {
    PipelineResult result = runPipelineWithMetrics();

    MetricQueryResults metrics = result.metrics().queryMetrics(MetricsFilter.builder()
        .addNameFilter(MetricNameFilter.inNamespace(MetricsTest.class))
        .build());

    assertThat(metrics.counters(), hasItem(
        committedMetricsResult(MetricsTest.class.getName(), "count", "MyStep1", 3L)));
    assertThat(metrics.distributions(), hasItem(
        committedMetricsResult(MetricsTest.class.getName(), "input", "MyStep1",
            DistributionResult.create(26L, 3L, 5L, 13L))));

    assertThat(metrics.counters(), hasItem(
        committedMetricsResult(MetricsTest.class.getName(), "count", "MyStep2", 6L)));
    assertThat(metrics.distributions(), hasItem(
        committedMetricsResult(MetricsTest.class.getName(), "input", "MyStep2",
            DistributionResult.create(52L, 6L, 5L, 13L))));

    assertThat(metrics.distributions(), hasItem(
        distributionCommittedMinMax(MetricsTest.class.getName(), "bundle", "MyStep1", 10L, 40L)));
  }


  @Category({RunnableOnService.class, UsesAttemptedMetrics.class})
  @Test
  public void attemptedMetricsReportToQuery() {
    PipelineResult result = runPipelineWithMetrics();

    MetricQueryResults metrics = result.metrics().queryMetrics(MetricsFilter.builder()
        .addNameFilter(MetricNameFilter.inNamespace(MetricsTest.class))
        .build());

    // TODO: BEAM-1169: Metrics shouldn't verify the physical values tightly.
    assertThat(metrics.counters(), hasItem(
        attemptedMetricsResult(MetricsTest.class.getName(), "count", "MyStep1", 3L)));
    assertThat(metrics.distributions(), hasItem(
        attemptedMetricsResult(MetricsTest.class.getName(), "input", "MyStep1",
            DistributionResult.create(26L, 3L, 5L, 13L))));

    assertThat(metrics.counters(), hasItem(
        attemptedMetricsResult(MetricsTest.class.getName(), "count", "MyStep2", 6L)));
    assertThat(metrics.distributions(), hasItem(
        attemptedMetricsResult(MetricsTest.class.getName(), "input", "MyStep2",
            DistributionResult.create(52L, 6L, 5L, 13L))));

    assertThat(metrics.distributions(), hasItem(
        distributionAttemptedMinMax(MetricsTest.class.getName(), "bundle", "MyStep1", 10L, 40L)));
  }

  private PipelineResult runPipelineWithMetrics() {
    final Counter count = Metrics.counter(MetricsTest.class, "count");
    final TupleTag<Integer> output1 = new TupleTag<Integer>(){};
    final TupleTag<Integer> output2 = new TupleTag<Integer>(){};
    pipeline
        .apply(Create.of(5, 8, 13))
        .apply("MyStep1", ParDo.of(new DoFn<Integer, Integer>() {
          Distribution bundleDist = Metrics.distribution(MetricsTest.class, "bundle");

          @StartBundle
          public void startBundle(Context c) {
            bundleDist.update(10L);
          }

          @SuppressWarnings("unused")
          @ProcessElement
          public void processElement(ProcessContext c) {
            Distribution values = Metrics.distribution(MetricsTest.class, "input");
            count.inc();
            values.update(c.element());

            c.output(c.element());
            c.output(c.element());
          }

          @DoFn.FinishBundle
          public void finishBundle(Context c) {
            bundleDist.update(40L);
          }
        }))
        .apply("MyStep2", ParDo.withOutputTags(output1, TupleTagList.of(output2))
            .of(new DoFn<Integer, Integer>() {
              @SuppressWarnings("unused")
              @ProcessElement
              public void processElement(ProcessContext c) {
                Distribution values = Metrics.distribution(MetricsTest.class, "input");
                count.inc();
                values.update(c.element());
                c.output(c.element());
                c.sideOutput(output2, c.element());
              }
            }));
    PipelineResult result = pipeline.run();

    result.waitUntilFinish();
    return result;
  }
}
