/*
 * Copyright (C) 2014 Google Inc.
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

package com.google.cloud.dataflow.sdk.transforms;

import static com.google.cloud.dataflow.sdk.TestUtils.KvMatcher.isKv;
import static org.hamcrest.collection.IsIterableContainingInAnyOrder.containsInAnyOrder;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.coders.BigEndianIntegerCoder;
import com.google.cloud.dataflow.sdk.coders.KvCoder;
import com.google.cloud.dataflow.sdk.coders.MapCoder;
import com.google.cloud.dataflow.sdk.coders.StringUtf8Coder;
import com.google.cloud.dataflow.sdk.testing.DataflowAssert;
import com.google.cloud.dataflow.sdk.testing.TestPipeline;
import com.google.cloud.dataflow.sdk.transforms.windowing.FixedWindows;
import com.google.cloud.dataflow.sdk.transforms.windowing.InvalidWindows;
import com.google.cloud.dataflow.sdk.transforms.windowing.Sessions;
import com.google.cloud.dataflow.sdk.transforms.windowing.Window;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.PCollection;

import org.hamcrest.Matchers;
import org.joda.time.Duration;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * Tests for GroupByKey.
 */
@RunWith(JUnit4.class)
@SuppressWarnings({"rawtypes", "serial", "unchecked"})
public class GroupByKeyTest {

  @Rule
  public ExpectedException expectedEx = ExpectedException.none();

  @Test
  @Category(com.google.cloud.dataflow.sdk.testing.RunnableOnService.class)
  public void testGroupByKey() {
    List<KV<String, Integer>> ungroupedPairs = Arrays.asList(
        KV.of("k1", 3),
        KV.of("k5", Integer.MAX_VALUE),
        KV.of("k5", Integer.MIN_VALUE),
        KV.of("k2", 66),
        KV.of("k1", 4),
        KV.of("k2", -33),
        KV.of("k3", 0));

    Pipeline p = TestPipeline.create();

    PCollection<KV<String, Integer>> input =
        p.apply(Create.of(ungroupedPairs))
        .setCoder(KvCoder.of(StringUtf8Coder.of(), BigEndianIntegerCoder.of()));

    PCollection<KV<String, Iterable<Integer>>> output =
        input.apply(GroupByKey.<String, Integer>create());

    DataflowAssert.that(output)
        .satisfies(new AssertThatHasExpectedContentsForTestGroupByKey());

    p.run();
  }

  static class AssertThatHasExpectedContentsForTestGroupByKey
      implements SerializableFunction<Iterable<KV<String, Iterable<Integer>>>,
                                      Void> {
    @Override
    public Void apply(Iterable<KV<String, Iterable<Integer>>> actual) {
      assertThat(actual, containsInAnyOrder(
          isKv(is("k1"), containsInAnyOrder(3, 4)),
          isKv(is("k5"), containsInAnyOrder(Integer.MAX_VALUE,
                                            Integer.MIN_VALUE)),
          isKv(is("k2"), containsInAnyOrder(66, -33)),
          isKv(is("k3"), containsInAnyOrder(0))));
      return null;
    }
  }

  @Test
  @Category(com.google.cloud.dataflow.sdk.testing.RunnableOnService.class)
  public void testGroupByKeyAndWindows() {
    List<KV<String, Integer>> ungroupedPairs = Arrays.asList(
        KV.of("k1", 3),  // window [0, 5)
        KV.of("k5", Integer.MAX_VALUE), // window [0, 5)
        KV.of("k5", Integer.MIN_VALUE), // window [0, 5)
        KV.of("k2", 66), // window [0, 5)
        KV.of("k1", 4),  // window [5, 10)
        KV.of("k2", -33),  // window [5, 10)
        KV.of("k3", 0));  // window [5, 10)

    Pipeline p = TestPipeline.create();

    PCollection<KV<String, Integer>> input =
        p.apply(Create.timestamped(ungroupedPairs, Arrays.asList(1L, 2L, 3L, 4L, 5L, 6L, 7L)))
        .setCoder(KvCoder.of(StringUtf8Coder.of(), BigEndianIntegerCoder.of()));
    PCollection<KV<String, Iterable<Integer>>> output =
        input.apply(Window.<KV<String, Integer>>into(FixedWindows.of(new Duration(5))))
             .apply(GroupByKey.<String, Integer>create());

    DataflowAssert.that(output)
        .satisfies(new AssertThatHasExpectedContentsForTestGroupByKeyAndWindows());

    p.run();
  }

  static class AssertThatHasExpectedContentsForTestGroupByKeyAndWindows
      implements SerializableFunction<Iterable<KV<String, Iterable<Integer>>>,
                                      Void> {
    @Override
      public Void apply(Iterable<KV<String, Iterable<Integer>>> actual) {
      assertThat(actual, containsInAnyOrder(
          isKv(is("k1"), containsInAnyOrder(3)),
          isKv(is("k1"), containsInAnyOrder(4)),
          isKv(is("k5"), containsInAnyOrder(Integer.MAX_VALUE,
                                            Integer.MIN_VALUE)),
          isKv(is("k2"), containsInAnyOrder(66)),
          isKv(is("k2"), containsInAnyOrder(-33)),
          isKv(is("k3"), containsInAnyOrder(0))));
      return null;
    }
  }

  @Test
  @Category(com.google.cloud.dataflow.sdk.testing.RunnableOnService.class)
  public void testGroupByKeyEmpty() {
    List<KV<String, Integer>> ungroupedPairs = Arrays.asList();

    Pipeline p = TestPipeline.create();

    PCollection<KV<String, Integer>> input =
        p.apply(Create.of(ungroupedPairs))
        .setCoder(KvCoder.of(StringUtf8Coder.of(), BigEndianIntegerCoder.of()));

    PCollection<KV<String, Iterable<Integer>>> output =
        input.apply(GroupByKey.<String, Integer>create());

    DataflowAssert.that(output)
        .containsInAnyOrder();

    p.run();
  }

  @Test
  public void testGroupByKeyNonDeterministic() throws Exception {
    expectedEx.expect(IllegalStateException.class);
    expectedEx.expectMessage(Matchers.containsString("must be deterministic"));

    List<KV<Map<String, String>, Integer>> ungroupedPairs = Arrays.asList();

    Pipeline p = TestPipeline.create();

    PCollection<KV<Map<String, String>, Integer>> input =
        p.apply(Create.of(ungroupedPairs))
        .setCoder(
            KvCoder.of(MapCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of()),
                BigEndianIntegerCoder.of()));

    input.apply(GroupByKey.<Map<String, String>, Integer>create());

    p.run();
  }

  @Test
  public void testIdentityWindowFnPropagation() {
    Pipeline p = TestPipeline.create();

    List<KV<String, Integer>> ungroupedPairs = Arrays.asList();

    PCollection<KV<String, Integer>> input =
        p.apply(Create.of(ungroupedPairs))
        .setCoder(KvCoder.of(StringUtf8Coder.of(), BigEndianIntegerCoder.of()))
        .apply(Window.<KV<String, Integer>>into(FixedWindows.of(Duration.standardMinutes(1))));

    PCollection<KV<String, Iterable<Integer>>> output =
        input.apply(GroupByKey.<String, Integer>create());

    p.run();

    Assert.assertTrue(output.getWindowFn().isCompatible(
        FixedWindows.<KV<String, Integer>>of(Duration.standardMinutes(1))));

  }

  @Test
  public void testWindowFnInvalidation() {
    Pipeline p = TestPipeline.create();

    List<KV<String, Integer>> ungroupedPairs = Arrays.asList();

    PCollection<KV<String, Integer>> input =
        p.apply(Create.of(ungroupedPairs))
        .setCoder(KvCoder.of(StringUtf8Coder.of(), BigEndianIntegerCoder.of()))
        .apply(Window.<KV<String, Integer>>into(
            Sessions.withGapDuration(Duration.standardMinutes(1))));

    PCollection<KV<String, Iterable<Integer>>> output =
        input.apply(GroupByKey.<String, Integer>create());

    p.run();

    Assert.assertTrue(
        output.getWindowFn().isCompatible(
            new InvalidWindows(
                "Invalid",
                Sessions.<KV<String, Integer>>withGapDuration(
                    Duration.standardMinutes(1)))));
  }

  @Test
  public void testInvalidWindows() {
    Pipeline p = TestPipeline.create();

    List<KV<String, Integer>> ungroupedPairs = Arrays.asList();

    PCollection<KV<String, Integer>> input =
        p.apply(Create.of(ungroupedPairs))
        .setCoder(KvCoder.of(StringUtf8Coder.of(), BigEndianIntegerCoder.of()))
        .apply(Window.<KV<String, Integer>>into(
            Sessions.withGapDuration(Duration.standardMinutes(1))));

    try {
      PCollection<KV<String, Iterable<Iterable<Integer>>>> output = input
          .apply(GroupByKey.<String, Integer>create())
          .apply(GroupByKey.<String, Iterable<Integer>>create());
      Assert.fail("Exception should have been thrown");
    } catch (IllegalStateException e) {
      Assert.assertTrue(e.getMessage().startsWith(
          "GroupByKey must have a valid Window merge function."));
    }
  }

  @Test
  public void testRemerge() {
    Pipeline p = TestPipeline.create();

    List<KV<String, Integer>> ungroupedPairs = Arrays.asList();

    PCollection<KV<String, Integer>> input =
        p.apply(Create.of(ungroupedPairs))
        .setCoder(KvCoder.of(StringUtf8Coder.of(), BigEndianIntegerCoder.of()))
        .apply(Window.<KV<String, Integer>>into(
            Sessions.withGapDuration(Duration.standardMinutes(1))));

    PCollection<KV<String, Iterable<Iterable<Integer>>>> middle = input
        .apply(GroupByKey.<String, Integer>create())
        .apply(Window.<KV<String, Iterable<Integer>>>remerge())
        .apply(GroupByKey.<String, Iterable<Integer>>create())
        .apply(Window.<KV<String, Iterable<Iterable<Integer>>>>remerge());

    p.run();

    Assert.assertTrue(
        middle.getWindowFn().isCompatible(
            Sessions.withGapDuration(Duration.standardMinutes(1))));
  }
}
