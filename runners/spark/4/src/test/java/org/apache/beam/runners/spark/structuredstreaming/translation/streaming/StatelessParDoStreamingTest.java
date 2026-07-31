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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.beam.runners.spark.StreamingTest;
import org.apache.beam.runners.spark.structuredstreaming.SparkSessionRule;
import org.apache.beam.runners.spark.structuredstreaming.SparkStructuredStreamingPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.io.Read;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TimestampedValue;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * The simplest possible streaming pipeline: an unbounded source feeding a plain, stateless {@code
 * ParDo}, with every element expected to pass straight through. This is the baseline the other
 * streaming tests in this package build on.
 */
@RunWith(JUnit4.class)
@Category(StreamingTest.class)
public class StatelessParDoStreamingTest implements Serializable {

  /**
   * This pipeline hosts no {@code transformWithState} operator of its own, but it still shares the
   * session with the rest of the streaming suite and is configured identically to it, so that the
   * baseline test differs from the stateful ones in the pipeline under test and nothing else. See
   * {@code BeamStatefulProcessorTest} for why the relaxation is needed at all.
   */
  @ClassRule
  public static final SparkSessionRule SESSION =
      new SparkSessionRule(KV.of("spark.kryo.registrationRequired", "false"));

  @Rule public transient TemporaryFolder checkpointDir = new TemporaryFolder();

  private static final Instant BASE = new Instant(0);

  /** Doubles the input, so the assertion can tell the ParDo actually ran, not just passed data. */
  private static class DoubleFn extends DoFn<Integer, Integer> {
    @ProcessElement
    public void process(@Element Integer element, OutputReceiver<Integer> out) {
      out.output(element * 2);
    }
  }

  @Test(timeout = 300_000)
  public void everyElementPassesThrough() throws Exception {
    String collectorId = StreamingTestUtils.newCollectorId("stateless-pardo");
    StreamingTestUtils.clear(collectorId);

    List<TimestampedValue<Integer>> elements = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      elements.add(TimestampedValue.of(i, BASE.plus(Duration.standardSeconds(i))));
    }

    SparkStructuredStreamingPipelineOptions options =
        StreamingTestUtils.streamingOptions(checkpointDir);
    SESSION.configure(options);
    Pipeline pipeline = Pipeline.create(options);

    pipeline
        .apply(
            "ReadUnbounded",
            Read.from(
                new StreamingTestUtils.ListBackedUnboundedSource<>(elements, VarIntCoder.of())))
        .apply("Double", ParDo.of(new DoubleFn()))
        .apply("Collect", ParDo.of(new StreamingTestUtils.CollectDoFn<>(collectorId)));

    PipelineResult result = pipeline.run();
    result.waitUntilFinish();

    // Nothing here is windowed or stateful, so every element is emitted as soon as its micro-batch
    // is processed and no watermark has to cross anything. Micro-batch boundaries make the order
    // arbitrary, hence the sort.
    List<Integer> collected =
        new ArrayList<>(StreamingTestUtils.<Integer>getCollected(collectorId));
    Collections.sort(collected);

    List<Integer> expected = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      expected.add(i * 2);
    }
    assertEquals("pipeline state=" + result.getState(), expected, collected);
  }
}
