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

import static org.apache.beam.sdk.metrics.MetricResultsMatchers.attemptedMetricsResult;
import static org.apache.beam.sdk.metrics.MetricResultsMatchers.distributionMinMax;
import static org.apache.beam.sdk.metrics.MetricResultsMatchers.metricsResult;
import static org.apache.beam.sdk.testing.SerializableMatchers.greaterThan;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.hasItem;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.io.Serializable;
import java.util.List;
import java.util.NoSuchElementException;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.io.GenerateSequence;
import org.apache.beam.sdk.io.Read;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.NeedsRunner;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.UsesAttemptedMetrics;
import org.apache.beam.sdk.testing.UsesCommittedMetrics;
import org.apache.beam.sdk.testing.UsesCounterMetrics;
import org.apache.beam.sdk.testing.UsesDistributionMetrics;
import org.apache.beam.sdk.testing.UsesGaugeMetrics;
import org.apache.beam.sdk.testing.UsesStringSetMetrics;
import org.apache.beam.sdk.testing.ValidatesRunner;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableSet;
import org.hamcrest.Matcher;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mockito;

/** Tests for {@link Metrics}. */
public class MetricsTest implements Serializable {

  private static final String NS = "test";
  private static final String NAME = "name";
  private static final MetricName METRIC_NAME = MetricName.named(NS, NAME);
  private static final String NAMESPACE = MetricsTest.class.getName();
  private static final MetricName ELEMENTS_READ = SourceMetrics.elementsRead().getName();

  private static MetricQueryResults queryTestMetrics(PipelineResult result) {
    return result
        .metrics()
        .queryMetrics(
            MetricsFilter.builder()
                .addNameFilter(MetricNameFilter.inNamespace(MetricsTest.class))
                .build());
  }

  /** Shared test helpers and setup/teardown. */
  public abstract static class SharedTestBase implements Serializable {
    @Rule public final transient ExpectedException thrown = ExpectedException.none();

    @Rule public final transient TestPipeline pipeline = TestPipeline.create();

    @After
    public void tearDown() {
      MetricsEnvironment.setCurrentContainer(null);
    }

    protected PipelineResult runPipelineWithMetrics() {
      final Counter count = Metrics.counter(MetricsTest.class, "count");
      StringSet sideinputs = Metrics.stringSet(MetricsTest.class, "sideinputs");
      final TupleTag<Integer> output1 = new TupleTag<Integer>() {};
      final TupleTag<Integer> output2 = new TupleTag<Integer>() {};
      pipeline
          .apply(Create.of(5, 8, 13))
          .apply(
              "MyStep1",
              ParDo.of(
                  new DoFn<Integer, Integer>() {
                    Distribution bundleDist = Metrics.distribution(MetricsTest.class, "bundle");

                    @StartBundle
                    public void startBundle() {
                      bundleDist.update(10L);
                    }

                    @SuppressWarnings("unused")
                    @ProcessElement
                    public void processElement(ProcessContext c) {
                      Distribution values = Metrics.distribution(MetricsTest.class, "input");
                      StringSet sources = Metrics.stringSet(MetricsTest.class, "sources");
                      count.inc();
                      values.update(c.element());

                      c.output(c.element());
                      c.output(c.element());
                      sources.add("gcs");
                      sources.add("gcs"); // repeated should appear once
                      sources.add("gcs", "gcs"); // repeated should appear once
                      sideinputs.add("bigtable", "spanner");
                    }

                    @DoFn.FinishBundle
                    public void finishBundle() {
                      bundleDist.update(40L);
                    }
                  }))
          .apply(
              "MyStep2",
              ParDo.of(
                      new DoFn<Integer, Integer>() {
                        @SuppressWarnings("unused")
                        @ProcessElement
                        public void processElement(ProcessContext c) {
                          Distribution values = Metrics.distribution(MetricsTest.class, "input");
                          Gauge gauge = Metrics.gauge(MetricsTest.class, "my-gauge");
                          StringSet sinks = Metrics.stringSet(MetricsTest.class, "sinks");
                          Integer element = c.element();
                          count.inc();
                          values.update(element);
                          gauge.set(12L);
                          c.output(element);
                          sinks.add("bq", "kafka", "kafka"); // repeated should appear once
                          sideinputs.add("bigtable", "sql");
                          c.output(output2, element);
                        }
                      })
                  .withOutputTags(output1, TupleTagList.of(output2)));
      PipelineResult result = pipeline.run();

      result.waitUntilFinish();
      return result;
    }
  }

  /** Tests validating basic metric scenarios. */
  @RunWith(JUnit4.class)
  public static class BasicTests extends SharedTestBase {
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
    public void testCounterWithEmptyName() {
      thrown.expect(IllegalArgumentException.class);
      Metrics.counter(NS, "");
    }

    @Test
    public void testCounterWithEmptyNamespace() {
      thrown.expect(IllegalArgumentException.class);
      Metrics.counter("", NAME);
    }

    @Test
    public void testDistributionWithEmptyName() {
      thrown.expect(IllegalArgumentException.class);
      Metrics.distribution(NS, "");
    }

    @Test
    public void testDistributionWithEmptyNamespace() {
      thrown.expect(IllegalArgumentException.class);
      Metrics.distribution("", NAME);
    }

    @Test
    public void testDistributionToCell() {
      MetricsContainer mockContainer = Mockito.mock(MetricsContainer.class);
      Distribution mockDistribution = Mockito.mock(Distribution.class);
      when(mockContainer.getDistribution(METRIC_NAME)).thenReturn(mockDistribution);

      Distribution distribution = Metrics.distribution(NS, NAME);

      MetricsEnvironment.setCurrentContainer(mockContainer);
      distribution.update(5L);

      verify(mockDistribution).update(5L);

      distribution.update(36L);
      distribution.update(1L);
      verify(mockDistribution).update(36L);
      verify(mockDistribution).update(1L);
    }

    @Test
    public void testCounterToCell() {
      MetricsContainer mockContainer = Mockito.mock(MetricsContainer.class);
      Counter mockCounter = Mockito.mock(Counter.class);
      when(mockContainer.getCounter(METRIC_NAME)).thenReturn(mockCounter);

      Counter counter = Metrics.counter(NS, NAME);

      MetricsEnvironment.setCurrentContainer(mockContainer);
      counter.inc();
      verify(mockCounter).inc(1);

      counter.inc(47L);
      verify(mockCounter).inc(47);

      counter.dec(5L);
      verify(mockCounter).inc(-5);
    }

    @Test
    public void testMetricsFlag() {
      Metrics.resetDefaultPipelineOptions();
      assertFalse(Metrics.MetricsFlag.counterDisabled());
      assertFalse(Metrics.MetricsFlag.stringSetDisabled());
      PipelineOptions options =
          PipelineOptionsFactory.fromArgs("--experiments=disableCounterMetrics").create();
      Metrics.setDefaultPipelineOptions(options);
      assertTrue(Metrics.MetricsFlag.counterDisabled());
      assertFalse(Metrics.MetricsFlag.stringSetDisabled());
      Metrics.resetDefaultPipelineOptions();
      options = PipelineOptionsFactory.fromArgs("--experiments=disableStringSetMetrics").create();
      Metrics.setDefaultPipelineOptions(options);
      assertFalse(Metrics.MetricsFlag.counterDisabled());
      assertTrue(Metrics.MetricsFlag.stringSetDisabled());
      Metrics.resetDefaultPipelineOptions();
    }
  }

  /** Tests for committed metrics. */
  @RunWith(JUnit4.class)
  public static class CommittedMetricTests extends SharedTestBase {
    @Category({
      ValidatesRunner.class,
      UsesCommittedMetrics.class,
      UsesCounterMetrics.class,
      UsesDistributionMetrics.class,
      UsesGaugeMetrics.class,
      UsesStringSetMetrics.class
    })
    @Test
    public void testAllCommittedMetrics() {
      PipelineResult result = runPipelineWithMetrics();
      MetricQueryResults metrics = queryTestMetrics(result);

      assertAllMetrics(metrics, true);
    }

    @Category({ValidatesRunner.class, UsesCommittedMetrics.class, UsesCounterMetrics.class})
    @Test
    public void testCommittedCounterMetrics() {
      PipelineResult result = runPipelineWithMetrics();
      MetricQueryResults metrics = queryTestMetrics(result);
      assertCounterMetrics(metrics, true);
    }

    @Category({ValidatesRunner.class, UsesCommittedMetrics.class, UsesDistributionMetrics.class})
    @Test
    public void testCommittedDistributionMetrics() {
      PipelineResult result = runPipelineWithMetrics();
      MetricQueryResults metrics = queryTestMetrics(result);
      assertDistributionMetrics(metrics, true);
    }

    @Category({ValidatesRunner.class, UsesCommittedMetrics.class, UsesGaugeMetrics.class})
    @Test
    public void testCommittedGaugeMetrics() {
      PipelineResult result = runPipelineWithMetrics();
      MetricQueryResults metrics = queryTestMetrics(result);
      assertGaugeMetrics(metrics, true);
    }

    @Category({ValidatesRunner.class, UsesCommittedMetrics.class, UsesStringSetMetrics.class})
    @Test
    public void testCommittedStringSetMetrics() {
      PipelineResult result = runPipelineWithMetrics();
      MetricQueryResults metrics = queryTestMetrics(result);
      assertStringSetMetrics(metrics, true);
    }

    @Test
    @Category({NeedsRunner.class, UsesAttemptedMetrics.class, UsesCounterMetrics.class})
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
                          MetricNameFilter.named(
                              ELEMENTS_READ.getNamespace(), ELEMENTS_READ.getName()))
                      .build());

      assertThat(
          metrics.getCounters(),
          anyOf(
              // Step names are different for portable and non-portable runners.
              hasItem(
                  attemptedMetricsResult(
                      ELEMENTS_READ.getNamespace(),
                      ELEMENTS_READ.getName(),
                      "Read(BoundedCountingSource)",
                      1000L)),
              hasItem(
                  attemptedMetricsResult(
                      ELEMENTS_READ.getNamespace(),
                      ELEMENTS_READ.getName(),
                      "Read-BoundedCountingSource-",
                      1000L))));
    }

    @Test
    @Category({NeedsRunner.class, UsesAttemptedMetrics.class, UsesCounterMetrics.class})
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
                          MetricNameFilter.named(
                              ELEMENTS_READ.getNamespace(), ELEMENTS_READ.getName()))
                      .build());

      assertThat(
          metrics.getCounters(),
          anyOf(
              // Step names are different for portable and non-portable runners.
              hasItem(
                  attemptedMetricsResult(
                      ELEMENTS_READ.getNamespace(),
                      ELEMENTS_READ.getName(),
                      "Read(UnboundedCountingSource)",
                      1000L)),
              hasItem(
                  attemptedMetricsResult(
                      ELEMENTS_READ.getNamespace(),
                      ELEMENTS_READ.getName(),
                      "Read-UnboundedCountingSource-",
                      1000L))));
    }
  }

  /** Tests for attempted metrics. */
  @RunWith(JUnit4.class)
  public static class AttemptedMetricTests extends SharedTestBase {
    @Category({
      ValidatesRunner.class,
      UsesAttemptedMetrics.class,
      UsesCounterMetrics.class,
      UsesDistributionMetrics.class,
      UsesGaugeMetrics.class,
      UsesStringSetMetrics.class
    })
    @Test
    public void testAllAttemptedMetrics() {
      PipelineResult result = runPipelineWithMetrics();
      MetricQueryResults metrics = queryTestMetrics(result);

      // TODO: BEAM-1169: Metrics shouldn't verify the physical values tightly.
      assertAllMetrics(metrics, false);
    }

    @Category({ValidatesRunner.class, UsesAttemptedMetrics.class, UsesCounterMetrics.class})
    @Test
    public void testAttemptedCounterMetrics() {
      PipelineResult result = runPipelineWithMetrics();
      MetricQueryResults metrics = queryTestMetrics(result);
      assertCounterMetrics(metrics, false);
    }

    @Category({ValidatesRunner.class, UsesAttemptedMetrics.class, UsesDistributionMetrics.class})
    @Test
    public void testAttemptedDistributionMetrics() {
      PipelineResult result = runPipelineWithMetrics();
      MetricQueryResults metrics = queryTestMetrics(result);
      assertDistributionMetrics(metrics, false);
    }

    @Category({ValidatesRunner.class, UsesAttemptedMetrics.class, UsesGaugeMetrics.class})
    @Test
    public void testAttemptedGaugeMetrics() {
      PipelineResult result = runPipelineWithMetrics();
      MetricQueryResults metrics = queryTestMetrics(result);
      assertGaugeMetrics(metrics, false);
    }

    @Category({ValidatesRunner.class, UsesAttemptedMetrics.class, UsesStringSetMetrics.class})
    @Test
    public void testAttemptedStringSetMetrics() {
      PipelineResult result = runPipelineWithMetrics();
      MetricQueryResults metrics = queryTestMetrics(result);
      assertStringSetMetrics(metrics, false);
    }

    @Test
    @Category({ValidatesRunner.class, UsesAttemptedMetrics.class, UsesCounterMetrics.class})
    public void testBoundedSourceMetricsInSplit() {
      pipeline.apply(Read.from(new CountingSourceWithMetrics(0, 10)));
      PipelineResult pipelineResult = pipeline.run();
      MetricQueryResults metrics =
          pipelineResult
              .metrics()
              .queryMetrics(
                  MetricsFilter.builder()
                      .addNameFilter(
                          MetricNameFilter.named(
                              CountingSourceWithMetrics.class,
                              CountingSourceWithMetrics.SPLIT_NAME))
                      .addNameFilter(
                          MetricNameFilter.named(
                              CountingSourceWithMetrics.class,
                              CountingSourceWithMetrics.ADVANCE_NAME))
                      .build());
      assertThat(
          metrics.getCounters(),
          hasItem(
              attemptedMetricsResult(
                  CountingSourceWithMetrics.class.getName(),
                  CountingSourceWithMetrics.ADVANCE_NAME,
                  null, // step name varies depending on the runner
                  10L)));
      assertThat(
          metrics.getCounters(),
          hasItem(
              metricsResult(
                  CountingSourceWithMetrics.class.getName(),
                  CountingSourceWithMetrics.SPLIT_NAME,
                  null, // step name varies depending on the runner
                  greaterThan(0L),
                  false)));
    }
  }

  public static class CountingSourceWithMetrics extends BoundedSource<Integer> {
    public static final String SPLIT_NAME = "num-split";
    public static final String ADVANCE_NAME = "num-advance";
    private static Counter splitCounter =
        Metrics.counter(CountingSourceWithMetrics.class, SPLIT_NAME);
    private static Counter advanceCounter =
        Metrics.counter(CountingSourceWithMetrics.class, ADVANCE_NAME);
    private final int start;
    private final int end;

    @Override
    public List<? extends BoundedSource<Integer>> split(
        long desiredBundleSizeBytes, PipelineOptions options) {
      splitCounter.inc();
      // simply split the current source into two
      if (end - start >= 2) {
        int mid = (start + end + 1) / 2;
        return ImmutableList.of(
            new CountingSourceWithMetrics(start, mid), new CountingSourceWithMetrics(mid, end));
      }
      return null;
    }

    @Override
    public long getEstimatedSizeBytes(PipelineOptions options) {
      return 0;
    }

    @Override
    public BoundedReader<Integer> createReader(PipelineOptions options) {
      return new CountingReader();
    }

    public CountingSourceWithMetrics(int start, int end) {
      this.start = start;
      this.end = end;
    }

    @Override
    public Coder<Integer> getOutputCoder() {
      return VarIntCoder.of();
    }

    public class CountingReader extends BoundedSource.BoundedReader<Integer> {
      private int current;

      @Override
      public boolean start() throws IOException {
        return current < end;
      }

      @Override
      public boolean advance() {
        ++current;
        advanceCounter.inc();
        return current < end;
      }

      @Override
      public Integer getCurrent() throws NoSuchElementException {
        return current;
      }

      @Override
      public void close() {}

      @Override
      public BoundedSource<Integer> getCurrentSource() {
        return null;
      }

      public CountingReader() {
        current = start;
      }
    }
  }

  private static <T> Matcher<MetricResult<T>> metricsResultPatchStep(
      final String name, final String step, final T value, final boolean isCommitted) {
    return anyOf(
        metricsResult(NAMESPACE, name, step, value, isCommitted),
        // portable runner adds a suffix for metrics initiated outside anonymous pardo
        metricsResult(NAMESPACE, name, step + "-ParMultiDo-Anonymous-", value, isCommitted));
  }

  private static void assertCounterMetrics(MetricQueryResults metrics, boolean isCommitted) {
    System.out.println(metrics.getCounters());
    assertThat(
        metrics.getCounters(),
        hasItem(metricsResultPatchStep("count", "MyStep1", 3L, isCommitted)));

    assertThat(
        metrics.getCounters(),
        hasItem(metricsResult(NAMESPACE, "count", "MyStep2", 6L, isCommitted)));
  }

  private static void assertGaugeMetrics(MetricQueryResults metrics, boolean isCommitted) {
    assertThat(
        metrics.getGauges(),
        hasItem(
            metricsResult(
                NAMESPACE,
                "my-gauge",
                "MyStep2",
                GaugeResult.create(12L, Instant.now()),
                isCommitted)));
  }

  private static void assertStringSetMetrics(MetricQueryResults metrics, boolean isCommitted) {
    // TODO(https://github.com/apache/beam/issues/32001) use containsInAnyOrder once portableMetrics
    //   duplicate metrics issue fixed
    assertThat(
        metrics.getStringSets(),
        hasItem(
            metricsResultPatchStep(
                "sources",
                "MyStep1",
                StringSetResult.create(ImmutableSet.of("gcs")),
                isCommitted)));
    assertThat(
        metrics.getStringSets(),
        hasItem(
            metricsResult(
                NAMESPACE,
                "sinks",
                "MyStep2",
                StringSetResult.create(ImmutableSet.of("kafka", "bq")),
                isCommitted)));
    assertThat(
        metrics.getStringSets(),
        hasItem(
            metricsResultPatchStep(
                "sideinputs",
                "MyStep1",
                StringSetResult.create(ImmutableSet.of("bigtable", "spanner")),
                isCommitted)));
    assertThat(
        metrics.getStringSets(),
        hasItem(
            metricsResult(
                NAMESPACE,
                "sideinputs",
                "MyStep2",
                StringSetResult.create(ImmutableSet.of("sql", "bigtable")),
                isCommitted)));
  }

  private static void assertDistributionMetrics(MetricQueryResults metrics, boolean isCommitted) {
    assertThat(
        metrics.getDistributions(),
        hasItem(
            metricsResultPatchStep(
                "input", "MyStep1", DistributionResult.create(26L, 3L, 5L, 13L), isCommitted)));

    assertThat(
        metrics.getDistributions(),
        hasItem(
            metricsResult(
                NAMESPACE,
                "input",
                "MyStep2",
                DistributionResult.create(52L, 6L, 5L, 13L),
                isCommitted)));
    assertThat(
        metrics.getDistributions(),
        anyOf(
            // Step names are different for portable and non-portable runners.
            hasItem(distributionMinMax(NAMESPACE, "bundle", "MyStep1", 10L, 40L, isCommitted)),
            hasItem(
                distributionMinMax(
                    NAMESPACE, "bundle", "MyStep1-ParMultiDo-Anonymous-", 10L, 40L, isCommitted))));
  }

  private static void assertAllMetrics(MetricQueryResults metrics, boolean isCommitted) {
    assertCounterMetrics(metrics, isCommitted);
    assertDistributionMetrics(metrics, isCommitted);
    assertGaugeMetrics(metrics, isCommitted);
    assertStringSetMetrics(metrics, isCommitted);
  }
}
