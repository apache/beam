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

package org.apache.beam.sdk.extensions.euphoria.beam;

import static java.time.temporal.ChronoUnit.MINUTES;
import static org.apache.beam.sdk.metrics.MetricResultsMatchers.metricsResult;
import static org.hamcrest.Matchers.hasItem;
import static org.junit.Assert.assertThat;

import java.time.Duration;
import java.util.Arrays;
import java.util.stream.Stream;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.extensions.euphoria.beam.BeamAccumulatorProvider.BeamMetricsCounter;
import org.apache.beam.sdk.extensions.euphoria.beam.BeamAccumulatorProvider.BeamMetricsHistogram;
import org.apache.beam.sdk.extensions.euphoria.beam.BeamAccumulatorProvider.BeamMetricsTimer;
import org.apache.beam.sdk.extensions.euphoria.beam.BeamExecutor.ExecutorResult;
import org.apache.beam.sdk.extensions.euphoria.core.client.dataset.Dataset;
import org.apache.beam.sdk.extensions.euphoria.core.client.flow.Flow;
import org.apache.beam.sdk.extensions.euphoria.core.client.io.Collector;
import org.apache.beam.sdk.extensions.euphoria.core.client.io.ListDataSink;
import org.apache.beam.sdk.extensions.euphoria.core.client.io.ListDataSource;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.MapElements;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.ReduceByKey;
import org.apache.beam.sdk.extensions.euphoria.core.client.util.Pair;
import org.apache.beam.sdk.extensions.euphoria.testing.DatasetAssert;
import org.apache.beam.sdk.metrics.DistributionResult;
import org.apache.beam.sdk.metrics.MetricNameFilter;
import org.apache.beam.sdk.metrics.MetricQueryResults;
import org.apache.beam.sdk.metrics.MetricsFilter;
import org.junit.Test;

/**
 * Testing translation of accumulators to Beam {@link org.apache.beam.sdk.metrics.Metrics}.
 */
public class BeamMetricsTranslationTest {

  @Test
  public void testBeamMetricsTranslation() {
    Flow flow = Flow.create();
    ListDataSource<Integer> source = ListDataSource.bounded(Arrays.asList(1, 2, 3, 4, 5));
    ListDataSink<Integer> sink = ListDataSink.get();
    Dataset<Integer> input = flow.createInput(source);
    final String counterName1 = "counter1";
    final String stepName1 = "step1";

    final Dataset<Pair<Integer, Integer>> pairedInput =
        ReduceByKey.named(stepName1)
            .of(input)
            .keyBy(e -> e)
            .reduceBy(
                (Stream<Integer> list, Collector<Integer> coll) ->
                    list.forEach(
                        i -> {
                          coll.getCounter(counterName1).increment();
                          coll.getHistogram(counterName1).add(i);
                          coll.getTimer(counterName1).add(Duration.of(i, MINUTES));
                          if (i % 2 == 0) {
                            coll.collect(i);
                          }
                        }))
            .output();

    final String stepName2 = "step2";
    final String counterName2 = "counter2";
    MapElements.named(stepName2)
        .of(pairedInput)
        .using(
            (pair, context) -> {
              final Integer value = pair.getSecond();
              context.getCounter(counterName2).increment();
              context.getHistogram(counterName2).add(value);
              context.getTimer(counterName2).add(Duration.of(value, MINUTES));
              return value;
            })
        .output()
        .persist(sink);

    BeamExecutor executor = TestUtils.createExecutor();
    PipelineResult result = ((ExecutorResult) executor.execute(flow)).getResult();

    final MetricQueryResults metricQueryResults =
        result.metrics()
            .queryMetrics(
                MetricsFilter.builder()
                    .addNameFilter(MetricNameFilter.inNamespace(BeamMetricsTimer.class))
                    .addNameFilter(MetricNameFilter.inNamespace(BeamMetricsHistogram.class))
                    .addNameFilter(MetricNameFilter.inNamespace(BeamMetricsCounter.class))
                    .build());

    testStep1Metrics(metricQueryResults, counterName1, stepName1);

    testStep2Metrics(metricQueryResults, counterName2, stepName2);

    DatasetAssert.unorderedEquals(sink.getOutputs(), 2, 4);
  }

  private void testStep1Metrics(MetricQueryResults metrics, String counterName1, String stepName1) {
    assertThat(
        metrics.getCounters(),
        hasItem(
            metricsResult(BeamMetricsCounter.class.getName(), counterName1, stepName1, 5L, true)));

    assertThat(
        metrics.getDistributions(),
        hasItem(
            metricsResult(
                BeamMetricsHistogram.class.getName(),
                counterName1,
                stepName1,
                DistributionResult.create(15, 5, 1, 5),
                true)));

    assertThat(
        metrics.getDistributions(),
        hasItem(
            metricsResult(
                BeamMetricsTimer.class.getName(),
                counterName1,
                stepName1,
                DistributionResult.create(900, 5, 60, 300),
                true)));
  }

  private void testStep2Metrics(MetricQueryResults metrics, String counterName2, String stepName2) {
    assertThat(
        metrics.getCounters(),
        hasItem(
            metricsResult(BeamMetricsCounter.class.getName(), counterName2, stepName2, 2L, true)));

    assertThat(
        metrics.getDistributions(),
        hasItem(
            metricsResult(
                BeamMetricsHistogram.class.getName(),
                counterName2,
                stepName2,
                DistributionResult.create(6, 2, 2, 4),
                true)));

    assertThat(
        metrics.getDistributions(),
        hasItem(
            metricsResult(
                BeamMetricsTimer.class.getName(),
                counterName2,
                stepName2,
                DistributionResult.create(360, 2, 120, 240),
                true)));
  }

}
