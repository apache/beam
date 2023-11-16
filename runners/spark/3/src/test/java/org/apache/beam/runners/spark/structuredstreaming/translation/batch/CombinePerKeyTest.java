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
import java.util.ArrayList;
import java.util.List;
import org.apache.beam.runners.spark.structuredstreaming.SparkSessionRule;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.CombineFnBase;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.Distinct;
import org.apache.beam.sdk.transforms.Sum;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Sessions;
import org.apache.beam.sdk.transforms.windowing.SlidingWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TimestampedValue;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Lists;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Test class for beam to spark {@link
 * org.apache.beam.sdk.transforms.Combine#perKey(CombineFnBase.GlobalCombineFn)} translation.
 */
@RunWith(JUnit4.class)
public class CombinePerKeyTest implements Serializable {
  @ClassRule public static final SparkSessionRule SESSION = new SparkSessionRule();

  @Rule
  public transient TestPipeline pipeline =
      TestPipeline.fromOptions(SESSION.createPipelineOptions());

  @Test
  public void testCombinePerKey() {
    List<KV<Integer, Integer>> elems = new ArrayList<>();
    elems.add(KV.of(1, 1));
    elems.add(KV.of(1, 3));
    elems.add(KV.of(1, 5));
    elems.add(KV.of(2, 2));
    elems.add(KV.of(2, 4));
    elems.add(KV.of(2, 6));

    PCollection<KV<Integer, Integer>> input =
        pipeline.apply(Create.of(elems)).apply(Sum.integersPerKey());
    PAssert.that(input).containsInAnyOrder(KV.of(1, 9), KV.of(2, 12));
    pipeline.run();
  }

  @Test
  public void testDistinctViaCombinePerKey() {
    List<Integer> elems = Lists.newArrayList(1, 2, 3, 3, 4, 4, 4, 4, 5, 5);

    // Distinct is implemented in terms of CombinePerKey
    PCollection<Integer> result = pipeline.apply(Create.of(elems)).apply(Distinct.create());

    PAssert.that(result).containsInAnyOrder(1, 2, 3, 4, 5);
    pipeline.run();
  }

  @Test
  public void testCombinePerKeyPreservesWindowing() {
    PCollection<KV<Integer, Integer>> input =
        pipeline
            .apply(
                Create.timestamped(
                    TimestampedValue.of(KV.of(1, 1), new Instant(1)),
                    TimestampedValue.of(KV.of(1, 3), new Instant(2)),
                    TimestampedValue.of(KV.of(1, 5), new Instant(11)),
                    TimestampedValue.of(KV.of(2, 2), new Instant(3)),
                    TimestampedValue.of(KV.of(2, 4), new Instant(11)),
                    TimestampedValue.of(KV.of(2, 6), new Instant(12))))
            .apply(Window.into(FixedWindows.of(Duration.millis(10))))
            .apply(Sum.integersPerKey());
    PAssert.that(input).containsInAnyOrder(KV.of(1, 4), KV.of(1, 5), KV.of(2, 2), KV.of(2, 10));
    pipeline.run();
  }

  @Test
  public void testCombinePerKeyWithSlidingWindows() {
    PCollection<KV<Integer, Integer>> input =
        pipeline
            .apply(
                Create.timestamped(
                    TimestampedValue.of(KV.of(1, 1), new Instant(1)),
                    TimestampedValue.of(KV.of(1, 3), new Instant(2)),
                    TimestampedValue.of(KV.of(1, 5), new Instant(3)),
                    TimestampedValue.of(KV.of(1, 2), new Instant(1)),
                    TimestampedValue.of(KV.of(1, 4), new Instant(2)),
                    TimestampedValue.of(KV.of(1, 6), new Instant(3))))
            .apply(Window.into(SlidingWindows.of(Duration.millis(3)).every(Duration.millis(1))))
            .apply(Sum.integersPerKey());
    PAssert.that(input)
        .containsInAnyOrder(
            KV.of(1, 1 + 2),
            KV.of(1, 1 + 2 + 3 + 4),
            KV.of(1, 1 + 3 + 5 + 2 + 4 + 6),
            KV.of(1, 3 + 4 + 5 + 6),
            KV.of(1, 5 + 6));
    pipeline.run();
  }

  @Test
  public void testCombineByKeyWithMergingWindows() {
    PCollection<KV<Integer, Integer>> input =
        pipeline
            .apply(
                Create.timestamped(
                    TimestampedValue.of(KV.of(1, 1), new Instant(5)),
                    TimestampedValue.of(KV.of(1, 3), new Instant(7)),
                    TimestampedValue.of(KV.of(1, 5), new Instant(11)),
                    TimestampedValue.of(KV.of(2, 2), new Instant(5)),
                    TimestampedValue.of(KV.of(2, 4), new Instant(11)),
                    TimestampedValue.of(KV.of(2, 6), new Instant(12))))
            .apply(Window.into(Sessions.withGapDuration(Duration.millis(5))))
            .apply(Sum.integersPerKey());

    PAssert.that(input)
        .containsInAnyOrder(
            KV.of(1, 9), // window [5-16)
            KV.of(2, 2), // window [5-10)
            KV.of(2, 10) // window [11-17)
            );
    pipeline.run();
  }

  @Test
  public void testCountPerElementWithSlidingWindows() {
    PCollection<String> input =
        pipeline
            .apply(
                Create.timestamped(
                    TimestampedValue.of("a", new Instant(1)),
                    TimestampedValue.of("a", new Instant(2)),
                    TimestampedValue.of("b", new Instant(3)),
                    TimestampedValue.of("b", new Instant(4))))
            .apply(Window.into(SlidingWindows.of(Duration.millis(2)).every(Duration.millis(1))));
    PCollection<KV<String, Long>> output = input.apply(Count.perElement());
    PAssert.that(output)
        .containsInAnyOrder(
            KV.of("a", 1L),
            KV.of("a", 2L),
            KV.of("a", 1L),
            KV.of("b", 1L),
            KV.of("b", 2L),
            KV.of("b", 1L));
    pipeline.run();
  }
}
