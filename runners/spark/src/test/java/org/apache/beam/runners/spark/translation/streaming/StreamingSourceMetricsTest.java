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

package org.apache.beam.runners.spark.translation.streaming;

import static org.apache.beam.sdk.metrics.MetricMatchers.attemptedMetricsResult;
import static org.hamcrest.Matchers.hasItem;
import static org.junit.Assert.assertThat;

import java.io.Serializable;
import org.apache.beam.runners.spark.PipelineRule;
import org.apache.beam.runners.spark.TestSparkPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.CountingInput;
import org.apache.beam.sdk.io.Source;
import org.apache.beam.sdk.metrics.MetricNameFilter;
import org.apache.beam.sdk.metrics.MetricQueryResults;
import org.apache.beam.sdk.metrics.MetricsFilter;
import org.junit.Rule;
import org.junit.Test;


/**
 * Verify metrics support for {@link Source Sources} in streaming pipelines.
 */
public class StreamingSourceMetricsTest implements Serializable {

  // Force streaming pipeline using pipeline rule.
  @Rule
  public final transient PipelineRule pipelineRule = PipelineRule.streaming();

  @Test
  public void testUnboundedSourceMetrics() {
    TestSparkPipelineOptions options = pipelineRule.getOptions();

    Pipeline pipeline = Pipeline.create(options);

    final long numElements = 1000;

    pipeline.apply(CountingInput.unbounded().withMaxNumRecords(numElements));

    PipelineResult pipelineResult = pipeline.run();

    MetricQueryResults metrics =
        pipelineResult
            .metrics()
            .queryMetrics(
                MetricsFilter.builder()
                    .addNameFilter(MetricNameFilter.named("io", "elementsRead"))
                    .build());

    assertThat(metrics.counters(), hasItem(
        attemptedMetricsResult("io", "elementsRead", "Read(UnboundedCountingSource)", 1000L)));
  }
}
