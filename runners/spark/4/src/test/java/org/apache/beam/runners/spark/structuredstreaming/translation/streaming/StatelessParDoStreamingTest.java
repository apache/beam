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
package org.apache.beam.runners.spark.structuredstreaming.translation.streaming;

import static org.junit.Assert.assertEquals;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Set;
import org.apache.beam.runners.spark.StreamingTest;
import org.apache.beam.runners.spark.structuredstreaming.SparkSessionRule;
import org.apache.beam.runners.spark.structuredstreaming.SparkStructuredStreamingPipelineOptions;
import org.apache.beam.runners.spark.structuredstreaming.io.streaming.TestUnboundedSource;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.Read;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Baseline streaming pipeline tests for stateless ParDo and Flatten. */
@RunWith(JUnit4.class)
@Category(StreamingTest.class)
public class StatelessParDoStreamingTest implements Serializable {

  @ClassRule public static final SparkSessionRule SESSION = new SparkSessionRule();

  @Rule public transient TemporaryFolder checkpointDir = new TemporaryFolder();

  private static class PassThroughFn extends DoFn<String, String> {
    @ProcessElement
    public void process(@Element String element, OutputReceiver<String> out) {
      out.output(element);
    }
  }

  @Test
  public void everyElementPassesThrough() throws Exception {
    String tag = "stateless-pardo";
    String collectorId = StreamingTestUtils.newCollectorId(tag);
    StreamingTestUtils.clear(collectorId);

    SparkStructuredStreamingPipelineOptions options =
        StreamingTestUtils.streamingOptions(checkpointDir);
    Pipeline pipeline = Pipeline.create(options);

    pipeline
        .apply("ReadUnbounded", Read.from(new TestUnboundedSource(tag, 1, 10)))
        .apply("PassThrough", ParDo.of(new PassThroughFn()))
        .apply("Collect", ParDo.of(new StreamingTestUtils.CollectDoFn<>(collectorId)));

    PipelineResult result = StreamingTestUtils.run(pipeline);

    Set<String> collected = StreamingTestUtils.collected(collectorId);
    Set<String> expected = TestUnboundedSource.elements(tag, 1, 10);
    assertEquals("pipeline state=" + result.getState(), expected, collected);
  }

  @Test
  public void flattenCombinesMultipleUnboundedSources() throws Exception {
    String tagA = "flatten-a";
    String tagB = "flatten-b";
    String collectorId = StreamingTestUtils.newCollectorId("flatten");
    StreamingTestUtils.clear(collectorId);

    SparkStructuredStreamingPipelineOptions options =
        StreamingTestUtils.streamingOptions(checkpointDir);
    Pipeline pipeline = Pipeline.create(options);

    PCollection<String> a = pipeline.apply("ReadA", Read.from(new TestUnboundedSource(tagA, 1, 5)));
    PCollection<String> b = pipeline.apply("ReadB", Read.from(new TestUnboundedSource(tagB, 1, 5)));

    PCollectionList.of(a)
        .and(b)
        .apply("Flatten", Flatten.pCollections())
        .apply("Collect", ParDo.of(new StreamingTestUtils.CollectDoFn<>(collectorId)));

    PipelineResult result = StreamingTestUtils.run(pipeline);

    Set<String> collected = StreamingTestUtils.collected(collectorId);
    Set<String> expected = new HashSet<>();
    expected.addAll(TestUnboundedSource.elements(tagA, 1, 5));
    expected.addAll(TestUnboundedSource.elements(tagB, 1, 5));
    assertEquals("pipeline state=" + result.getState(), expected, collected);
  }
}
