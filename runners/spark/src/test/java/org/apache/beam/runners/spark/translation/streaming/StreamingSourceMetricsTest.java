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

import static org.apache.beam.sdk.metrics.MetricResultsMatchers.metricsResult;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

import java.io.Serializable;
import org.apache.beam.runners.spark.StreamingTest;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.GenerateSequence;
import org.apache.beam.sdk.io.Source;
import org.apache.beam.sdk.metrics.MetricName;
import org.apache.beam.sdk.metrics.MetricNameFilter;
import org.apache.beam.sdk.metrics.MetricQueryResults;
import org.apache.beam.sdk.metrics.MetricsFilter;
import org.apache.beam.sdk.metrics.SourceMetrics;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/** Verify metrics support for {@link Source Sources} in streaming pipelines. */
public class StreamingSourceMetricsTest implements Serializable {
  private static final MetricName ELEMENTS_READ = SourceMetrics.elementsRead().getName();

  // Force streaming pipeline using pipeline rule.
  @Rule public final transient TestPipeline pipeline = TestPipeline.create();

  @Test
  @Category(StreamingTest.class)
  public void testUnboundedSourceMetrics() {
    final long minElements = 1000;

    // Use a GenerateSequence for the UnboundedSequence, but push the watermark to infinity at
    // minElements to let the test pipeline cleanly shut it down.  Shutdown will occur shortly
    // afterwards, but at least minElements will be reported in the metrics.
    PCollection<Long> pc =
        pipeline.apply(
            GenerateSequence.from(1)
                .withRate(minElements / 10, Duration.millis(500L))
                .withTimestampFn(
                    t -> t < minElements ? Instant.now() : BoundedWindow.TIMESTAMP_MAX_VALUE));
    assertThat(pc.isBounded(), is(PCollection.IsBounded.UNBOUNDED));

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
        hasItem(
            metricsResult(
                ELEMENTS_READ.getNamespace(),
                ELEMENTS_READ.getName(),
                "GenerateSequence/Read(UnboundedCountingSource)",
                greaterThanOrEqualTo(minElements),
                false)));
  }
}
