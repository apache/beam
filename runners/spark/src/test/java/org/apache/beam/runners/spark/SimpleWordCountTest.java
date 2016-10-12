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

package org.apache.beam.runners.spark;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThat;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import java.io.File;
import java.util.Arrays;
import java.util.List;
import java.util.Set;

import org.apache.beam.runners.spark.aggregators.metrics.sink.InMemoryMetrics;
import org.apache.beam.runners.spark.examples.WordCount;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.PCollection;
import org.apache.commons.io.FileUtils;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExternalResource;
import org.junit.rules.TemporaryFolder;

/**
 * Simple word count test.
 */
public class SimpleWordCountTest {

  @Rule
  public ExternalResource inMemoryMetricsSink = new InMemoryMetricsSinkRule();

  @Rule
  public ClearAggregatorsRule clearAggregators = new ClearAggregatorsRule();

  private static final String[] WORDS_ARRAY = {
      "hi there", "hi", "hi sue bob",
      "hi sue", "", "bob hi"};
  private static final List<String> WORDS = Arrays.asList(WORDS_ARRAY);
  private static final Set<String> EXPECTED_COUNT_SET =
      ImmutableSet.of("hi: 5", "there: 1", "sue: 2", "bob: 2");

  @Test
  public void testInMem() throws Exception {
    assertThat(InMemoryMetrics.valueOf("emptyLines"), is(nullValue()));

    SparkPipelineOptions options = PipelineOptionsFactory.as(SparkPipelineOptions.class);
    options.setRunner(SparkRunner.class);
    Pipeline p = Pipeline.create(options);
    PCollection<String> inputWords = p.apply(Create.of(WORDS).withCoder(StringUtf8Coder
        .of()));
    PCollection<String> output = inputWords.apply(new WordCount.CountWords())
        .apply(MapElements.via(new WordCount.FormatAsTextFn()));

    PAssert.that(output).containsInAnyOrder(EXPECTED_COUNT_SET);

    p.run();

    assertThat(InMemoryMetrics.<Double>valueOf("emptyLines"), is(1d));
  }

  @Rule
  public TemporaryFolder testFolder = new TemporaryFolder();

  @Test
  public void testOutputFile() throws Exception {
    SparkPipelineOptions options = PipelineOptionsFactory.as(SparkPipelineOptions.class);
    options.setRunner(SparkRunner.class);
    Pipeline p = Pipeline.create(options);
    PCollection<String> inputWords = p.apply(Create.of(WORDS).withCoder(StringUtf8Coder
        .of()));
    PCollection<String> output = inputWords.apply(new WordCount.CountWords())
        .apply(MapElements.via(new WordCount.FormatAsTextFn()));

    File outputFile = testFolder.newFile();
    output.apply("WriteCounts", TextIO.Write.to(outputFile.getAbsolutePath()).withoutSharding());

    p.run();

    assertThat(Sets.newHashSet(FileUtils.readLines(outputFile)),
        containsInAnyOrder(EXPECTED_COUNT_SET.toArray()));
  }
}
