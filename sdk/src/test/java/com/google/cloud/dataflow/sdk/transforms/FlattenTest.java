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

import static com.google.cloud.dataflow.sdk.TestUtils.LINES;
import static com.google.cloud.dataflow.sdk.TestUtils.LINES2;
import static com.google.cloud.dataflow.sdk.TestUtils.LINES_ARRAY;
import static com.google.cloud.dataflow.sdk.TestUtils.NO_LINES;
import static com.google.cloud.dataflow.sdk.TestUtils.NO_LINES_ARRAY;

import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.coders.Coder;
import com.google.cloud.dataflow.sdk.coders.IterableCoder;
import com.google.cloud.dataflow.sdk.coders.StringUtf8Coder;
import com.google.cloud.dataflow.sdk.testing.DataflowAssert;
import com.google.cloud.dataflow.sdk.testing.TestPipeline;
import com.google.cloud.dataflow.sdk.transforms.windowing.FixedWindows;
import com.google.cloud.dataflow.sdk.transforms.windowing.Sessions;
import com.google.cloud.dataflow.sdk.transforms.windowing.Window;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.cloud.dataflow.sdk.values.PCollectionList;

import org.joda.time.Duration;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.util.ArrayList;
import java.util.List;

/**
 * Tests for Flatten.
 */
@RunWith(JUnit4.class)
public class FlattenTest {

  @Test
  @Category(com.google.cloud.dataflow.sdk.testing.RunnableOnService.class)
  public void testFlattenPCollectionListUnordered() {
    Pipeline p = TestPipeline.create();

    List<String>[] inputs = new List[] {
      LINES, NO_LINES, LINES2, NO_LINES, LINES, NO_LINES };

    PCollection<String> output =
        makePCollectionListOfStrings(false /* not ordered */, p, inputs)
        .apply(Flatten.<String>pCollections());

    DataflowAssert.that(output).containsInAnyOrder(flatten(inputs));
    p.run();
  }

  // TODO: setOrdered(true) isn't supported yet by the Dataflow service.
  @Test
  public void testFlattenPCollectionListOrdered() {
    Pipeline p = TestPipeline.create();

    List<String>[] inputs = new List[] {
      LINES, NO_LINES, LINES2, NO_LINES, LINES, NO_LINES };

    PCollection<String> output =
        makePCollectionListOfStrings(true /* ordered */, p, inputs)
        .apply(Flatten.<String>pCollections()).setOrdered(true);

    DataflowAssert.that(output).containsInOrder(flatten(inputs));
    p.run();
  }

  @Test
  @Category(com.google.cloud.dataflow.sdk.testing.RunnableOnService.class)
  public void testFlattenPCollectionListEmpty() {
    Pipeline p = TestPipeline.create();

    PCollection<String> output =
        PCollectionList.<String>empty(p)
        .apply(Flatten.<String>pCollections()).setCoder(StringUtf8Coder.of());

    DataflowAssert.that(output).containsInAnyOrder();
    p.run();
  }

  @Test
  public void testWindowFnPropagationFailure() {
    Pipeline p = TestPipeline.create();

    PCollection<String> input1 =
        p.apply(Create.of("Input1"))
        .apply(Window.<String>into(FixedWindows.of(Duration.standardMinutes(1))));
    PCollection<String> input2 =
        p.apply(Create.of("Input2"))
        .apply(Window.<String>into(FixedWindows.of(Duration.standardMinutes(2))));

    try {
      PCollection<String> output =
          PCollectionList.of(input1).and(input2)
          .apply(Flatten.<String>create());
      Assert.fail("Exception should have been thrown");
    } catch (IllegalStateException e) {
      Assert.assertTrue(e.getMessage().startsWith(
          "Inputs to Flatten had incompatible window windowFns"));
    }
  }

  @Test
  public void testWindowFnPropagation() {
    Pipeline p = TestPipeline.create();

    PCollection<String> input1 =
        p.apply(Create.of("Input1"))
        .apply(Window.<String>into(FixedWindows.of(Duration.standardMinutes(1))));
    PCollection<String> input2 =
        p.apply(Create.of("Input2"))
        .apply(Window.<String>into(FixedWindows.of(Duration.standardMinutes(1))));

    PCollection<String> output =
        PCollectionList.of(input1).and(input2)
        .apply(Flatten.<String>create());

    p.run();

    Assert.assertTrue(output.getWindowFn().isCompatible(
        FixedWindows.<String>of(Duration.standardMinutes(1))));
  }

  @Test
  public void testEqualWindowFnPropagation() {
    Pipeline p = TestPipeline.create();

    PCollection<String> input1 =
        p.apply(Create.of("Input1"))
        .apply(Window.<String>into(Sessions.withGapDuration(Duration.standardMinutes(1))));
    PCollection<String> input2 =
        p.apply(Create.of("Input2"))
        .apply(Window.<String>into(Sessions.withGapDuration(Duration.standardMinutes(2))));

    PCollection<String> output =
        PCollectionList.of(input1).and(input2)
        .apply(Flatten.<String>create());

    p.run();

    Assert.assertTrue(output.getWindowFn().isCompatible(
        Sessions.<String>withGapDuration(Duration.standardMinutes(2))));
  }


  PCollectionList<String> makePCollectionListOfStrings(boolean ordered,
                                                       Pipeline p,
                                                       List<String>... lists) {
    return makePCollectionList(ordered, p, StringUtf8Coder.of(), lists);
  }

  <T> PCollectionList<T> makePCollectionList(boolean ordered,
                                             Pipeline p,
                                             Coder<T> coder,
                                             List<T>... lists) {
    List<PCollection<T>> pcs = new ArrayList<>();
    for (List<T> list : lists) {
      PCollection<T> pc = p.apply(Create.of(list)).setCoder(coder);
      if (ordered) {
        pc.setOrdered(true);
      }
      pcs.add(pc);
    }
    return PCollectionList.of(pcs);
  }

  <T> T[] flatten(List<T>... lists) {
    List<T> flattened = new ArrayList<>();
    for (List<T> list : lists) {
      flattened.addAll(list);
    }
    return flattened.toArray((T[]) new Object[flattened.size()]);
  }

  @Test
  @Category(com.google.cloud.dataflow.sdk.testing.RunnableOnService.class)
  public void testFlattenIterables() {
    Pipeline p = TestPipeline.create();

    PCollection<Iterable<String>> input = p
        .apply(Create.<Iterable<String>>of(LINES))
        .setCoder(IterableCoder.of(StringUtf8Coder.of()));

    PCollection<String> output =
        input.apply(Flatten.<String>iterables());

    DataflowAssert.that(output)
        .containsInAnyOrder(LINES_ARRAY);

    p.run();
  }

  @Test
  public void testFlattenIterablesOrdered() {
    Pipeline p = TestPipeline.create();

    PCollection<Iterable<String>> input = p
        .apply(Create.<Iterable<String>>of(LINES))
        .setCoder(IterableCoder.of(StringUtf8Coder.of()));

    PCollection<String> output =
        input.apply(Flatten.<String>iterables()).setOrdered(true);

    DataflowAssert.that(output)
        .containsInOrder(LINES_ARRAY);

    p.run();
  }

  @Test
  @Category(com.google.cloud.dataflow.sdk.testing.RunnableOnService.class)
  public void testFlattenIterablesEmpty() {
    Pipeline p = TestPipeline.create();

    PCollection<Iterable<String>> input = p
        .apply(Create.<Iterable<String>>of(NO_LINES))
        .setCoder(IterableCoder.of(StringUtf8Coder.of()));

    PCollection<String> output =
        input.apply(Flatten.<String>iterables());

    DataflowAssert.that(output)
        .containsInAnyOrder(NO_LINES_ARRAY);

    p.run();
  }
}
