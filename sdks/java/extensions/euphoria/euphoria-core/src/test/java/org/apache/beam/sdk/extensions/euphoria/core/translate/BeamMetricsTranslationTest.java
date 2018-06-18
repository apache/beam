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

package org.apache.beam.sdk.extensions.euphoria.core.translate;

import static org.apache.beam.sdk.metrics.MetricResultsMatchers.metricsResult;
import static org.junit.Assert.assertThat;

import java.util.Arrays;
import java.util.stream.Stream;
import org.apache.beam.sdk.PipelineResult;
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
import org.hamcrest.Matchers;
import org.junit.Test;

/** Testing translation of accumulators to Beam {@link org.apache.beam.sdk.metrics.Metrics}. */
public class BeamMetricsTranslationTest {

  /**
   * Test metrics counters on {@link ReduceByKey} and {@link MapElements} operators
   * Flow:
   * 1.step RBK increment for all keys, add to histogram its value, collect even numbers.
   * 2.step MapElements increment for every element, add to histogram its value, map to integer.
   * 3.step test MapElements with default operator name, increment by value of its element,
   *   add to histogram 2 times value of its element.
   */
  @Test
  public void testBeamMetricsTranslation() {
    Flow flow = Flow.create();
    ListDataSource<Integer> source = ListDataSource.bounded(Arrays.asList(1, 2, 3, 4, 5));
    ListDataSink<Integer> sink = ListDataSink.get();
    Dataset<Integer> input = flow.createInput(source);
    final String counterName1 = "counter1";
    final String operatorName1 = "count_elements_and_save_even_numbers";

    final Dataset<Pair<Integer, Integer>> pairedInput =
        ReduceByKey.named(operatorName1)
            .of(input)
            .keyBy(e -> e)
            .reduceBy(
                (Stream<Integer> list, Collector<Integer> coll) ->
                    list.forEach(
                        i -> {
                          coll.getCounter(counterName1).increment();
                          coll.getHistogram(counterName1).add(i);
                          if (i % 2 == 0) {
                            coll.collect(i);
                          }
                        }))
            .output();


    final String counterName2 = "counter2";
    final String operatorName2 = "map_to_integer";

    final Dataset<Integer> mapElementsOutput =
        MapElements.named(operatorName2)
            .of(pairedInput) // pairedInput = [<2,2>, <4,4>]
            .using(
                (pair, context) -> {
                  final Integer value = pair.getSecond();
                  context.getCounter(counterName2).increment();
                  context.getHistogram(counterName2).add(value);
                  return value;
                })
            .output();

    final String defaultOperatorName3 = "MapElements";
    MapElements.of(mapElementsOutput) // mapElementsOutput = [2,4]
        .using(
            (value, context) -> {
              context.getCounter(counterName2).increment(value);
              context.getHistogram(counterName2).add(value, 2);
              return value;
            })
        .output()
        .persist(sink);

    BeamRunnerWrapper beamRunnerWrapper = BeamRunnerWrapper.ofDirect();
    PipelineResult result = beamRunnerWrapper.executeSync(flow).getResult();

    final MetricQueryResults metricQueryResults =
        result
            .metrics()
            .queryMetrics(
                MetricsFilter.builder()
                    .addNameFilter(MetricNameFilter.inNamespace(operatorName1))
                    .addNameFilter(MetricNameFilter.inNamespace(operatorName2))
                    .addNameFilter(MetricNameFilter.inNamespace(defaultOperatorName3))
                    .build());

    testStep1Metrics(metricQueryResults, counterName1, operatorName1);

    testStep2Metrics(metricQueryResults, counterName2, operatorName2);

    testStep3WithDefaultOperatorName(metricQueryResults, counterName2, defaultOperatorName3);

    DatasetAssert.unorderedEquals(sink.getOutputs(), 2, 4);
  }


  private void testStep1Metrics(MetricQueryResults metrics, String counterName1, String stepName1) {
    assertThat(
        metrics.getCounters(),
        Matchers.hasItem(
            metricsResult(stepName1, counterName1, stepName1, 5L, true)));

    assertThat(
        metrics.getDistributions(),
        Matchers.hasItem(
            metricsResult(
                stepName1, counterName1, stepName1, DistributionResult.create(15, 5, 1, 5), true)));
  }

  private void testStep2Metrics(MetricQueryResults metrics, String counterName2, String stepName2) {
    assertThat(
        metrics.getCounters(),
        Matchers.hasItem(
            metricsResult(stepName2, counterName2, stepName2, 2L, true)));

    assertThat(
        metrics.getDistributions(),
        Matchers.hasItem(
            metricsResult(
                stepName2, counterName2, stepName2, DistributionResult.create(6, 2, 2, 4), true)));
  }

  private void testStep3WithDefaultOperatorName(
      MetricQueryResults metrics, String counterName2, String stepName3) {
    assertThat(
        metrics.getCounters(),
        Matchers.hasItem(
            metricsResult(stepName3, counterName2, stepName3, 6L, true)));

    assertThat(
        metrics.getDistributions(),
        Matchers.hasItem(
            metricsResult(
                stepName3, counterName2, stepName3, DistributionResult.create(12, 4, 2, 4), true)));
  }
}
