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
import static org.apache.beam.sdk.metrics.MetricMatchers.distributionMinMax;
import static org.apache.beam.sdk.metrics.MetricMatchers.metricsResult;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;

import java.io.Serializable;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.GenerateSequence;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.UsesAttemptedMetrics;
import org.apache.beam.sdk.testing.UsesCommittedMetrics;
import org.apache.beam.sdk.testing.UsesCounterMetrics;
import org.apache.beam.sdk.testing.UsesDistributionMetrics;
import org.apache.beam.sdk.testing.UsesGaugeMetrics;
import org.apache.beam.sdk.testing.ValidatesRunner;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.hamcrest.CoreMatchers;
import org.joda.time.Duration;
import org.joda.time.Instant;
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
  private static final String NAMESPACE = MetricsTest.class.getName();
  private static final MetricName ELEMENTS_READ = SourceMetrics.elementsRead().getName();

  private static MetricQueryResults queryTestMetrics(PipelineResult result) {
    return result.metrics().queryMetrics(
        MetricsFilter.builder()
            .addNameFilter(MetricNameFilter.inNamespace(MetricsTest.class))
            .build());
  }

  @Rule
  public final transient TestPipeline pipeline = TestPipeline.create();

  @After
  public void tearDown() {
    MetricsEnvironment.setCurrentContainer(null);
  }

  @Test
  public void testDistributionWithoutContainer() {
    assertNull(MetricsEnvironment.getCurrentContainer());
    // Should not fail even though there is no metrics container.
    Metrics.distribution(NS, NAME).update(5L);
  }

  @Test
  public void testCounterWithoutContainer() {
    assertNull(MetricsEnvironment.getCurrentContainer());
    // Should not fail even though there is no metrics container.
    Counter counter = Metrics.counter(NS, NAME);
    counter.inc();
    counter.inc(5L);
    counter.dec();
    counter.dec(5L);
  }

  @Test
  public void testDistributionToCell() {
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
  public void testCounterToCell() {
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

  @Category({ValidatesRunner.class, UsesCommittedMetrics.class, UsesCounterMetrics.class,
      UsesDistributionMetrics.class, UsesGaugeMetrics.class})
  @Test
  public void testAllCommittedMetrics() {
    PipelineResult result = runPipelineWithMetrics();
    MetricQueryResults metrics = queryTestMetrics(result);

    assertAllMetrics(metrics, true);
  }

  @Category({ValidatesRunner.class, UsesAttemptedMetrics.class, UsesCounterMetrics.class,
      UsesDistributionMetrics.class, UsesGaugeMetrics.class})
  @Test
  public void testAllAttemptedMetrics() {
    PipelineResult result = runPipelineWithMetrics();
    MetricQueryResults metrics = queryTestMetrics(result);

    // TODO: BEAM-1169: Metrics shouldn't verify the physical values tightly.
    assertAllMetrics(metrics, false);
  }

  @Category({ValidatesRunner.class, UsesCommittedMetrics.class, UsesCounterMetrics.class})
  @Test
  public void testCommittedCounterMetrics() {
    PipelineResult result = runPipelineWithMetrics();
    MetricQueryResults metrics = queryTestMetrics(result);
    assertCounterMetrics(metrics, true);
  }

  @Category({ValidatesRunner.class, UsesAttemptedMetrics.class, UsesCounterMetrics.class})
  @Test
  public void testAttemptedCounterMetrics() {
    PipelineResult result = runPipelineWithMetrics();
    MetricQueryResults metrics = queryTestMetrics(result);
    assertCounterMetrics(metrics, false);
  }

  @Category({ValidatesRunner.class, UsesCommittedMetrics.class, UsesDistributionMetrics.class})
  @Test
  public void testCommittedDistributionMetrics() {
    PipelineResult result = runPipelineWithMetrics();
    MetricQueryResults metrics = queryTestMetrics(result);
    assertDistributionMetrics(metrics, true);
  }

  @Category({ValidatesRunner.class, UsesAttemptedMetrics.class, UsesDistributionMetrics.class})
  @Test
  public void testAttemptedDistributionMetrics() {
    PipelineResult result = runPipelineWithMetrics();
    MetricQueryResults metrics = queryTestMetrics(result);
    assertDistributionMetrics(metrics, false);
  }

  @Category({ValidatesRunner.class, UsesCommittedMetrics.class, UsesGaugeMetrics.class})
  @Test
  public void testCommittedGaugeMetrics() {
    PipelineResult result = runPipelineWithMetrics();
    MetricQueryResults metrics = queryTestMetrics(result);
    assertGaugeMetrics(metrics, true);
  }

  @Category({ValidatesRunner.class, UsesAttemptedMetrics.class, UsesGaugeMetrics.class})
  @Test
  public void testAttemptedGaugeMetrics() {
    PipelineResult result = runPipelineWithMetrics();
    MetricQueryResults metrics = queryTestMetrics(result);
    assertGaugeMetrics(metrics, false);
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
          public void startBundle() {
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
          public void finishBundle() {
            bundleDist.update(40L);
          }
        }))
        .apply("MyStep2", ParDo
            .of(new DoFn<Integer, Integer>() {
              @SuppressWarnings("unused")
              @ProcessElement
              public void processElement(ProcessContext c) {
                Distribution values = Metrics.distribution(MetricsTest.class, "input");
                Gauge gauge = Metrics.gauge(MetricsTest.class, "my-gauge");
                Integer element = c.element();
                count.inc();
                values.update(element);
                gauge.set(12L);
                c.output(element);
                c.output(output2, element);
              }
            })
            .withOutputTags(output1, TupleTagList.of(output2)));
    PipelineResult result = pipeline.run();

    result.waitUntilFinish();
    return result;
  }

  private static void assertCounterMetrics(MetricQueryResults metrics, boolean isCommitted) {
    assertThat(metrics.counters(), hasItem(
        metricsResult(NAMESPACE, "count", "MyStep1", 3L, isCommitted)));
    assertThat(metrics.counters(), hasItem(
        metricsResult(NAMESPACE, "count", "MyStep2", 6L, isCommitted)));
  }

  private static void assertGaugeMetrics(MetricQueryResults metrics, boolean isCommitted) {
    assertThat(metrics.gauges(), hasItem(
        metricsResult(NAMESPACE, "my-gauge", "MyStep2",
            GaugeResult.create(12L, Instant.now()), isCommitted)));
  }

  private static void assertDistributionMetrics(MetricQueryResults metrics, boolean isCommitted) {
    assertThat(metrics.distributions(), hasItem(
        metricsResult(NAMESPACE, "input", "MyStep1",
            DistributionResult.create(26L, 3L, 5L, 13L), isCommitted)));

    assertThat(metrics.distributions(), hasItem(
        metricsResult(NAMESPACE, "input", "MyStep2",
            DistributionResult.create(52L, 6L, 5L, 13L), isCommitted)));
    assertThat(metrics.distributions(), hasItem(
        distributionMinMax(NAMESPACE, "bundle", "MyStep1", 10L, 40L, isCommitted)));
  }

  private static void assertAllMetrics(MetricQueryResults metrics, boolean isCommitted) {
    assertCounterMetrics(metrics, isCommitted);
    assertDistributionMetrics(metrics, isCommitted);
    assertGaugeMetrics(metrics, isCommitted);
  }

  @Test
  @Category({ValidatesRunner.class, UsesAttemptedMetrics.class, UsesCounterMetrics.class})
  public void testBoundedSourceMetrics() {
    long numElements = 1000;

    pipeline.apply(GenerateSequence.from(0).to(numElements));

    PipelineResult pipelineResult = pipeline.run();

    MetricQueryResults metrics =
        pipelineResult
            .metrics()
            .queryMetrics(
                MetricsFilter.builder()
                    .addNameFilter(
                        MetricNameFilter.named(ELEMENTS_READ.namespace(), ELEMENTS_READ.name()))
                    .build());

    assertThat(metrics.counters(), hasItem(
        attemptedMetricsResult(
            ELEMENTS_READ.namespace(),
            ELEMENTS_READ.name(),
            "Read(BoundedCountingSource)",
            1000L)));
  }

  @Test
  @Category({ValidatesRunner.class, UsesAttemptedMetrics.class, UsesCounterMetrics.class})
  public void testUnboundedSourceMetrics() {
    long numElements = 1000;

    // Use withMaxReadTime to force unbounded mode.
    pipeline.apply(
        GenerateSequence.from(0).to(numElements).withMaxReadTime(Duration.standardDays(1)));

    PipelineResult pipelineResult = pipeline.run();

    MetricQueryResults metrics =
        pipelineResult
            .metrics()
            .queryMetrics(
                MetricsFilter.builder()
                    .addNameFilter(
                        MetricNameFilter.named(ELEMENTS_READ.namespace(), ELEMENTS_READ.name()))
                    .build());

    assertThat(metrics.counters(), hasItem(
        attemptedMetricsResult(
            ELEMENTS_READ.namespace(),
            ELEMENTS_READ.name(),
            "Read(UnboundedCountingSource)",
            1000L)));
  }
}
