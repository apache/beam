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

import java.util.stream.Stream;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.extensions.euphoria.core.client.io.Collector;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.MapElements;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.ReduceByKey;
import org.apache.beam.sdk.extensions.kryo.KryoCoder;
import org.apache.beam.sdk.metrics.DistributionResult;
import org.apache.beam.sdk.metrics.MetricNameFilter;
import org.apache.beam.sdk.metrics.MetricQueryResults;
import org.apache.beam.sdk.metrics.MetricsFilter;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Testing translation of accumulators to Beam {@link org.apache.beam.sdk.metrics.Metrics}. */
@RunWith(JUnit4.class)
public class BeamMetricsTranslationTest {

  @Rule public TestPipeline testPipeline = TestPipeline.create();

  @Before
  public void setup() {
    testPipeline
        .getCoderRegistry()
        .registerCoderForClass(Object.class, KryoCoder.of(testPipeline.getOptions()));
  }

  /**
   * Test metrics counters on {@link ReduceByKey} and {@link MapElements} operators Flow:
   *
   * <ol>
   *   <li>step RBK increment for all keys, add to histogram its value, collect even numbers.
   *   <li>step MapElements increment for every element, add to histogram its value, map to integer.
   *   <li>tep test MapElements with default operator name, increment by value of its element, add
   *       to histogram 2 times value of its element.
   * </ol>
   */
  @Test
  public void testBeamMetricsTranslation() {
    final PCollection<Integer> input =
        testPipeline.apply("input", Create.of(1, 2, 3, 4, 5).withType(TypeDescriptors.integers()));
    final String counterName1 = "counter1";
    final String operatorName1 = "count_elements_and_save_even_numbers";

    final PCollection<KV<Integer, Integer>> kvInput =
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
    final String operatorName3 = "map_elements";

    final PCollection<Integer> mapElementsOutput =
        MapElements.named(operatorName2)
            .of(kvInput) // kvInput = [<2,2>, <4,4>]
            .using(
                (kv, context) -> {
                  final Integer value = kv.getValue();
                  context.getCounter(counterName2).increment();
                  context.getHistogram(counterName2).add(value);
                  return value;
                })
            .output();

    final PCollection<Integer> output =
        MapElements.named(operatorName3)
            .of(mapElementsOutput) // mapElementsOutput = [2,4]
            .using(
                (value, context) -> {
                  context.getCounter(counterName2).increment(value);
                  context.getHistogram(counterName2).add(value, 2);
                  return value;
                })
            .output();

    PAssert.that(output).containsInAnyOrder(2, 4);

    final PipelineResult result = testPipeline.run();
    result.waitUntilFinish();

    final MetricQueryResults metricQueryResults =
        result
            .metrics()
            .queryMetrics(
                MetricsFilter.builder()
                    .addNameFilter(MetricNameFilter.inNamespace(operatorName1))
                    .addNameFilter(MetricNameFilter.inNamespace(operatorName2))
                    .addNameFilter(MetricNameFilter.inNamespace(operatorName3))
                    .build());

    testStep1Metrics(metricQueryResults, counterName1, operatorName1);
    testStep2Metrics(metricQueryResults, counterName2, operatorName2);
    testStep3WithDefaultOperatorName(metricQueryResults, counterName2, operatorName3);
  }

  private void testStep1Metrics(MetricQueryResults metrics, String counterName1, String stepName1) {
    MatcherAssert.assertThat(
        metrics.getCounters(),
        Matchers.hasItem(metricsResult(stepName1, counterName1, stepName1, 5L, true)));

    MatcherAssert.assertThat(
        metrics.getDistributions(),
        Matchers.hasItem(
            metricsResult(
                stepName1, counterName1, stepName1, DistributionResult.create(15, 5, 1, 5), true)));
  }

  private void testStep2Metrics(MetricQueryResults metrics, String counterName2, String stepName2) {
    MatcherAssert.assertThat(
        metrics.getCounters(),
        Matchers.hasItem(metricsResult(stepName2, counterName2, stepName2, 2L, true)));

    MatcherAssert.assertThat(
        metrics.getDistributions(),
        Matchers.hasItem(
            metricsResult(
                stepName2, counterName2, stepName2, DistributionResult.create(6, 2, 2, 4), true)));
  }

  private void testStep3WithDefaultOperatorName(
      MetricQueryResults metrics, String counterName2, String stepName3) {
    MatcherAssert.assertThat(
        metrics.getCounters(),
        Matchers.hasItem(metricsResult(stepName3, counterName2, stepName3, 6L, true)));

    MatcherAssert.assertThat(
        metrics.getDistributions(),
        Matchers.hasItem(
            metricsResult(
                stepName3, counterName2, stepName3, DistributionResult.create(12, 4, 2, 4), true)));
  }
}
