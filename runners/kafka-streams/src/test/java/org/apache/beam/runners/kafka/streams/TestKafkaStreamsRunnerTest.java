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
package org.apache.beam.runners.kafka.streams;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertThrows;

import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Throwables;
import org.junit.Rule;
import org.junit.Test;

/**
 * End-to-end tests for {@link TestKafkaStreamsRunner}: a {@link TestPipeline} with {@link PAssert}
 * runs against the Kafka Streams runner, and {@code TestPipeline.verifyPAssertsSucceeded} confirms
 * through the runner's metrics that every assertion actually executed.
 *
 * <p>This is the full {@code @ValidatesRunner} mechanism exercised in-module: PAssert's {@code
 * containsInAnyOrder} materializes the actual PCollection through the {@code GBK + Flatten}
 * bootstrap path (no side inputs), the assertion DoFn runs in the EMBEDDED harness, its success
 * counter travels back through the runner's MetricResults, and a failing assertion surfaces as an
 * {@link AssertionError}.
 */
public class TestKafkaStreamsRunnerTest {

  private static PipelineOptions options() {
    PipelineOptions options = PipelineOptionsFactory.create();
    options.setRunner(TestKafkaStreamsRunner.class);
    return options;
  }

  @Rule public final transient TestPipeline pipeline = TestPipeline.fromOptions(options());

  @Test
  public void pAssertContainsInAnyOrderSucceeds() {
    PAssert.that(pipeline.apply("create", Create.of(1, 2, 3))).containsInAnyOrder(3, 2, 1);
    pipeline.run();
  }

  @Test
  public void pAssertOnGroupedResultSucceeds() {
    PAssert.that(
            pipeline.apply("create", Create.of("a", "b", "a")).apply("count", Count.perElement()))
        .containsInAnyOrder(KV.of("a", 2L), KV.of("b", 1L));
    pipeline.run();
  }

  @Test
  public void failingPAssertFailsTheRun() {
    // Over the Fn API a DoFn failure travels as an error string, not a Java AssertionError object,
    // so the guarantee for a portable runner is that a failing PAssert fails the pipeline run (and
    // its message carries the assertion text). A dropped assertion — one that never runs — would
    // instead be caught by TestPipeline.verifyPAssertsSucceeded via the runner's metrics.
    // Throwable rather than RuntimeException: if the exception chain ever preserves the DoFn's
    // AssertionError (an Error), the runner unwraps and rethrows it, which is also a pass here.
    PAssert.that(pipeline.apply("create", Create.of(1, 2, 3))).containsInAnyOrder(1, 2, 4);
    Throwable thrown = assertThrows(Throwable.class, pipeline::run);
    assertThat(Throwables.getStackTraceAsString(thrown), containsString("AssertionError"));
  }
}
