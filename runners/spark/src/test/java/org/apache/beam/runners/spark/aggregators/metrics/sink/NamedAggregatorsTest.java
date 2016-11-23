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

package org.apache.beam.runners.spark.aggregators.metrics.sink;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThat;

import com.google.common.collect.ImmutableSet;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import org.apache.beam.runners.spark.SparkPipelineOptions;
import org.apache.beam.runners.spark.examples.WordCount;
import org.apache.beam.runners.spark.translation.streaming.utils.SparkTestPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExternalResource;


/**
 * A test for the NamedAggregators mechanism.
 */
public class NamedAggregatorsTest {

  @Rule
  public ExternalResource inMemoryMetricsSink = new InMemoryMetricsSinkRule();

  @Rule
  public ClearAggregatorsRule clearAggregators = new ClearAggregatorsRule();

  @Rule
  public final SparkTestPipelineOptions pipelineOptions = new SparkTestPipelineOptions();

  private Pipeline createSparkPipeline() {
    SparkPipelineOptions options = pipelineOptions.getOptions();
    options.setEnableSparkMetricSinks(true);
    return Pipeline.create(options);
  }

  private void runPipeline() {

    final List<String> words =
        Arrays.asList("hi there", "hi", "hi sue bob", "hi sue", "", "bob hi");

    final Set<String> expectedCounts =
        ImmutableSet.of("hi: 5", "there: 1", "sue: 2", "bob: 2");

    final Pipeline pipeline = createSparkPipeline();

    final PCollection<String> output =
        pipeline
        .apply(Create.of(words).withCoder(StringUtf8Coder.of()))
        .apply(new WordCount.CountWords())
        .apply(MapElements.via(new WordCount.FormatAsTextFn()));

    PAssert.that(output).containsInAnyOrder(expectedCounts);

    pipeline.run();
  }

  @Test
  public void testNamedAggregators() throws Exception {

    // don't reuse context in this test, as is tends to mess up Spark's MetricsSystem thread-safety
    System.setProperty("beam.spark.test.reuseSparkContext", "false");

    assertThat(InMemoryMetrics.valueOf("emptyLines"), is(nullValue()));

    runPipeline();

    assertThat(InMemoryMetrics.<Double>valueOf("emptyLines"), is(1d));

  }
}
