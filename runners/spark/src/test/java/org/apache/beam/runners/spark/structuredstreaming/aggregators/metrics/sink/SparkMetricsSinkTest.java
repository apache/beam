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
package org.apache.beam.runners.spark.structuredstreaming.aggregators.metrics.sink;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThat;

import org.apache.beam.runners.spark.structuredstreaming.SparkStructuredStreamingPipelineOptions;
import org.apache.beam.runners.spark.structuredstreaming.SparkStructuredStreamingRunner;
import org.apache.beam.runners.spark.structuredstreaming.examples.WordCount;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableSet;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExternalResource;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * TODO: add testInStreamingMode() once streaming support will be implemented.
 *
 * <p>A test that verifies Beam metrics are reported to Spark's metrics sink in both batch and
 * streaming modes.
 */
@Ignore("Has been failing since at least c350188ef7a8704c7336f3c20a1ab2144abbcd4a")
@RunWith(JUnit4.class)
public class SparkMetricsSinkTest {
  @Rule public ExternalResource inMemoryMetricsSink = new InMemoryMetricsSinkRule();

  private static final ImmutableList<String> WORDS =
      ImmutableList.of("hi there", "hi", "hi sue bob", "hi sue", "", "bob hi");
  private static final ImmutableSet<String> EXPECTED_COUNTS =
      ImmutableSet.of("hi: 5", "there: 1", "sue: 2", "bob: 2");
  private static Pipeline pipeline;

  @BeforeClass
  public static void beforeClass() {
    SparkStructuredStreamingPipelineOptions options =
        PipelineOptionsFactory.create().as(SparkStructuredStreamingPipelineOptions.class);
    options.setRunner(SparkStructuredStreamingRunner.class);
    options.setTestMode(true);
    pipeline = Pipeline.create(options);
  }

  @Test
  public void testInBatchMode() throws Exception {
    assertThat(InMemoryMetrics.valueOf("emptyLines"), is(nullValue()));

    final PCollection<String> output =
        pipeline
            .apply(Create.of(WORDS).withCoder(StringUtf8Coder.of()))
            .apply(new WordCount.CountWords())
            .apply(MapElements.via(new WordCount.FormatAsTextFn()));
    PAssert.that(output).containsInAnyOrder(EXPECTED_COUNTS);
    pipeline.run();

    assertThat(InMemoryMetrics.<Double>valueOf("emptyLines"), is(1d));
  }
}
