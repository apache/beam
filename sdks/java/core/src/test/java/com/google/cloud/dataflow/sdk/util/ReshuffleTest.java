/*
 * Copyright (C) 2015 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.google.cloud.dataflow.sdk.util;

import static org.junit.Assert.assertEquals;

import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.coders.KvCoder;
import com.google.cloud.dataflow.sdk.coders.StringUtf8Coder;
import com.google.cloud.dataflow.sdk.coders.VarIntCoder;
import com.google.cloud.dataflow.sdk.testing.DataflowAssert;
import com.google.cloud.dataflow.sdk.testing.RunnableOnService;
import com.google.cloud.dataflow.sdk.testing.TestPipeline;
import com.google.cloud.dataflow.sdk.transforms.Create;
import com.google.cloud.dataflow.sdk.transforms.GroupByKey;
import com.google.cloud.dataflow.sdk.transforms.windowing.FixedWindows;
import com.google.cloud.dataflow.sdk.transforms.windowing.Sessions;
import com.google.cloud.dataflow.sdk.transforms.windowing.Window;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.common.collect.ImmutableList;

import org.joda.time.Duration;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.util.List;

/**
 * Tests for {@link Reshuffle}.
 */
@RunWith(JUnit4.class)
public class ReshuffleTest {

  private static final List<KV<String, Integer>> ARBITRARY_KVS = ImmutableList.of(
        KV.of("k1", 3),
        KV.of("k5", Integer.MAX_VALUE),
        KV.of("k5", Integer.MIN_VALUE),
        KV.of("k2", 66),
        KV.of("k1", 4),
        KV.of("k2", -33),
        KV.of("k3", 0));

  // TODO: test with more than one value per key
  private static final List<KV<String, Integer>> GBK_TESTABLE_KVS = ImmutableList.of(
        KV.of("k1", 3),
        KV.of("k2", 4));

  private static final List<KV<String, Iterable<Integer>>> GROUPED_TESTABLE_KVS = ImmutableList.of(
        KV.of("k1", (Iterable<Integer>) ImmutableList.of(3)),
        KV.of("k2", (Iterable<Integer>) ImmutableList.of(4)));

  @Test
  @Category(RunnableOnService.class)
  public void testJustReshuffle() {
    Pipeline pipeline = TestPipeline.create();

    PCollection<KV<String, Integer>> input = pipeline
        .apply(Create.of(ARBITRARY_KVS)
            .withCoder(KvCoder.of(StringUtf8Coder.of(), VarIntCoder.of())));

    PCollection<KV<String, Integer>> output = input
        .apply(Reshuffle.<String, Integer>of());

    DataflowAssert.that(output).containsInAnyOrder(ARBITRARY_KVS);

    assertEquals(
        input.getWindowingStrategy(),
        output.getWindowingStrategy());

    pipeline.run();
  }

  @Test
  @Category(RunnableOnService.class)
  public void testReshuffleAfterSessionsAndGroupByKey() {
    Pipeline pipeline = TestPipeline.create();

    PCollection<KV<String, Iterable<Integer>>> input = pipeline
        .apply(Create.of(GBK_TESTABLE_KVS)
            .withCoder(KvCoder.of(StringUtf8Coder.of(), VarIntCoder.of())))
        .apply(Window.<KV<String, Integer>>into(
            Sessions.withGapDuration(Duration.standardMinutes(10))))
        .apply(GroupByKey.<String, Integer>create());

    PCollection<KV<String, Iterable<Integer>>> output = input
        .apply(Reshuffle.<String, Iterable<Integer>>of());

    DataflowAssert.that(output).containsInAnyOrder(GROUPED_TESTABLE_KVS);

    assertEquals(
        input.getWindowingStrategy(),
        output.getWindowingStrategy());

    pipeline.run();
  }

  @Test
  @Category(RunnableOnService.class)
  public void testReshuffleAfterFixedWindowsAndGroupByKey() {
    Pipeline pipeline = TestPipeline.create();

    PCollection<KV<String, Iterable<Integer>>> input = pipeline
        .apply(Create.of(GBK_TESTABLE_KVS)
            .withCoder(KvCoder.of(StringUtf8Coder.of(), VarIntCoder.of())))
        .apply(Window.<KV<String, Integer>>into(
            FixedWindows.of(Duration.standardMinutes(10L))))
        .apply(GroupByKey.<String, Integer>create());

    PCollection<KV<String, Iterable<Integer>>> output = input
        .apply(Reshuffle.<String, Iterable<Integer>>of());

    DataflowAssert.that(output).containsInAnyOrder(GROUPED_TESTABLE_KVS);

    assertEquals(
        input.getWindowingStrategy(),
        output.getWindowingStrategy());

    pipeline.run();
  }

  @Test
  @Category(RunnableOnService.class)
  public void testReshuffleAfterSlidingWindowsAndGroupByKey() {
    Pipeline pipeline = TestPipeline.create();

    PCollection<KV<String, Iterable<Integer>>> input = pipeline
        .apply(Create.of(GBK_TESTABLE_KVS)
            .withCoder(KvCoder.of(StringUtf8Coder.of(), VarIntCoder.of())))
        .apply(Window.<KV<String, Integer>>into(
            FixedWindows.of(Duration.standardMinutes(10L))))
        .apply(GroupByKey.<String, Integer>create());

    PCollection<KV<String, Iterable<Integer>>> output = input
        .apply(Reshuffle.<String, Iterable<Integer>>of());

    DataflowAssert.that(output).containsInAnyOrder(GROUPED_TESTABLE_KVS);

    assertEquals(
        input.getWindowingStrategy(),
        output.getWindowingStrategy());

    pipeline.run();
  }

  @Test
  @Category(RunnableOnService.class)
  public void testReshuffleAfterFixedWindows() {
    Pipeline pipeline = TestPipeline.create();

    PCollection<KV<String, Integer>> input = pipeline
        .apply(Create.of(ARBITRARY_KVS)
            .withCoder(KvCoder.of(StringUtf8Coder.of(), VarIntCoder.of())))
        .apply(Window.<KV<String, Integer>>into(
            FixedWindows.of(Duration.standardMinutes(10L))));

    PCollection<KV<String, Integer>> output = input
        .apply(Reshuffle.<String, Integer>of());

    DataflowAssert.that(output).containsInAnyOrder(ARBITRARY_KVS);

    assertEquals(
        input.getWindowingStrategy(),
        output.getWindowingStrategy());

    pipeline.run();
  }


  @Test
  @Category(RunnableOnService.class)
  public void testReshuffleAfterSlidingWindows() {
    Pipeline pipeline = TestPipeline.create();

    PCollection<KV<String, Integer>> input = pipeline
        .apply(Create.of(ARBITRARY_KVS)
            .withCoder(KvCoder.of(StringUtf8Coder.of(), VarIntCoder.of())))
        .apply(Window.<KV<String, Integer>>into(
            FixedWindows.of(Duration.standardMinutes(10L))));

    PCollection<KV<String, Integer>> output = input
        .apply(Reshuffle.<String, Integer>of());

    DataflowAssert.that(output).containsInAnyOrder(ARBITRARY_KVS);

    assertEquals(
        input.getWindowingStrategy(),
        output.getWindowingStrategy());

    pipeline.run();
  }
}
