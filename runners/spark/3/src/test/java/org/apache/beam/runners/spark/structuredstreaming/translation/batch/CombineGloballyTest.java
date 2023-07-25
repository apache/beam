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
package org.apache.beam.runners.spark.structuredstreaming.translation.batch;

import java.io.Serializable;
import org.apache.beam.runners.spark.structuredstreaming.SparkSessionRule;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.Combine.BinaryCombineFn;
import org.apache.beam.sdk.transforms.CombineFnBase;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.Sum;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Sessions;
import org.apache.beam.sdk.transforms.windowing.SlidingWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TimestampedValue;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Test class for beam to spark {@link Combine#globally(CombineFnBase.GlobalCombineFn)} translation.
 */
@RunWith(JUnit4.class)
public class CombineGloballyTest implements Serializable {
  @ClassRule public static final SparkSessionRule SESSION = new SparkSessionRule();

  @Rule
  public transient TestPipeline pipeline =
      TestPipeline.fromOptions(SESSION.createPipelineOptions());

  @Test
  public void testCombineGlobally() {
    PCollection<Integer> input =
        pipeline.apply(Create.of(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)).apply(Sum.integersGlobally());
    PAssert.that(input).containsInAnyOrder(55);
    // uses combine per key
    pipeline.run();
  }

  @Test
  public void testCombineGloballyPreservesWindowing() {
    PCollection<Integer> input =
        pipeline
            .apply(
                Create.timestamped(
                    TimestampedValue.of(1, new Instant(1)),
                    TimestampedValue.of(2, new Instant(2)),
                    TimestampedValue.of(3, new Instant(11)),
                    TimestampedValue.of(4, new Instant(3)),
                    TimestampedValue.of(5, new Instant(11)),
                    TimestampedValue.of(6, new Instant(12))))
            .apply(Window.into(FixedWindows.of(Duration.millis(10))))
            .apply(Sum.integersGlobally().withoutDefaults());
    PAssert.that(input).containsInAnyOrder(7, 14);
    pipeline.run();
  }

  @Test
  public void testCombineGloballyWithSlidingWindows() {
    PCollection<Integer> input =
        pipeline
            .apply(
                Create.timestamped(
                    TimestampedValue.of(1, new Instant(1)),
                    TimestampedValue.of(3, new Instant(2)),
                    TimestampedValue.of(5, new Instant(3)),
                    TimestampedValue.of(2, new Instant(1)),
                    TimestampedValue.of(4, new Instant(2)),
                    TimestampedValue.of(6, new Instant(3))))
            .apply(Window.into(SlidingWindows.of(Duration.millis(3)).every(Duration.millis(1))))
            .apply(Sum.integersGlobally().withoutDefaults());
    PAssert.that(input)
        .containsInAnyOrder(1 + 2, 1 + 2 + 3 + 4, 1 + 3 + 5 + 2 + 4 + 6, 3 + 4 + 5 + 6, 5 + 6);
    pipeline.run();
  }

  @Test
  public void testCombineGloballyWithMergingWindows() {
    PCollection<Integer> input =
        pipeline
            .apply(
                Create.timestamped(
                    TimestampedValue.of(2, new Instant(5)),
                    TimestampedValue.of(4, new Instant(11)),
                    TimestampedValue.of(6, new Instant(12))))
            .apply(Window.into(Sessions.withGapDuration(Duration.millis(5))))
            .apply(Sum.integersGlobally().withoutDefaults());

    PAssert.that(input).containsInAnyOrder(2 /*window [5-10)*/, 10 /*window [11-17)*/);
    pipeline.run();
  }

  @Test
  public void testCountGloballyWithSlidingWindows() {
    PCollection<String> input =
        pipeline
            .apply(
                Create.timestamped(
                    TimestampedValue.of("a", new Instant(1)),
                    TimestampedValue.of("a", new Instant(2)),
                    TimestampedValue.of("a", new Instant(2))))
            .apply(Window.into(SlidingWindows.of(Duration.millis(2)).every(Duration.millis(1))));
    PCollection<Long> output =
        input.apply(Combine.globally(Count.<String>combineFn()).withoutDefaults());
    PAssert.that(output).containsInAnyOrder(1L, 3L, 2L);
    pipeline.run();
  }

  @Test
  public void testBinaryCombineWithSlidingWindows() {
    PCollection<Integer> input =
        pipeline
            .apply(
                Create.timestamped(
                    TimestampedValue.of(1, new Instant(1)),
                    TimestampedValue.of(3, new Instant(2)),
                    TimestampedValue.of(5, new Instant(3))))
            .apply(Window.into(SlidingWindows.of(Duration.millis(3)).every(Duration.millis(1))))
            .apply(
                Combine.globally(BinaryCombineFn.<Integer>of((i1, i2) -> i1 > i2 ? i1 : i2))
                    .withoutDefaults());
    PAssert.that(input).containsInAnyOrder(1, 3, 5, 5, 5);
    pipeline.run();
  }
}
