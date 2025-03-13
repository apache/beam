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
package org.apache.beam.sdk.transforms;

import static org.apache.beam.sdk.transforms.display.DisplayDataMatchers.hasDisplayItem;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.BigEndianIntegerCoder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.testing.NeedsRunner;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.hamcrest.Matchers;
import org.joda.time.Duration;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for Top. */
@RunWith(JUnit4.class)
public class TopTest {

  @Rule public final TestPipeline p = TestPipeline.create();

  @Rule public ExpectedException expectedEx = ExpectedException.none();

  @SuppressWarnings("unchecked")
  static final String[] COLLECTION = new String[] {"a", "bb", "c", "c", "z"};

  @SuppressWarnings("unchecked")
  static final String[] EMPTY_COLLECTION = new String[] {};

  @SuppressWarnings({
    "rawtypes", // TODO(https://github.com/apache/beam/issues/20447)
    "unchecked"
  })
  static final KV<String, Integer>[] TABLE =
      new KV[] {
        KV.of("a", 1),
        KV.of("a", 2),
        KV.of("a", 3),
        KV.of("b", 1),
        KV.of("b", 10),
        KV.of("b", 10),
        KV.of("b", 100),
      };

  @SuppressWarnings({
    "rawtypes", // TODO(https://github.com/apache/beam/issues/20447)
    "unchecked"
  })
  static final KV<String, Integer>[] EMPTY_TABLE = new KV[] {};

  public PCollection<KV<String, Integer>> createInputTable(Pipeline p) {
    return p.apply(
        "CreateInputTable",
        Create.of(Arrays.asList(TABLE))
            .withCoder(KvCoder.of(StringUtf8Coder.of(), BigEndianIntegerCoder.of())));
  }

  public PCollection<KV<String, Integer>> createEmptyInputTable(Pipeline p) {
    return p.apply(
        "CreateEmptyInputTable",
        Create.of(Arrays.asList(EMPTY_TABLE))
            .withCoder(KvCoder.of(StringUtf8Coder.of(), BigEndianIntegerCoder.of())));
  }

  @Test
  @Category(NeedsRunner.class)
  @SuppressWarnings("unchecked")
  public void testTop() {
    PCollection<String> input =
        p.apply(Create.of(Arrays.asList(COLLECTION)).withCoder(StringUtf8Coder.of()));

    PCollection<List<String>> top1 = input.apply(Top.of(1, new OrderByLength()));
    PCollection<List<String>> top2 = input.apply(Top.largest(2));
    PCollection<List<String>> top3 = input.apply(Top.smallest(3));

    PCollection<KV<String, Integer>> inputTable = createInputTable(p);
    PCollection<KV<String, List<Integer>>> largestPerKey = inputTable.apply(Top.largestPerKey(2));
    PCollection<KV<String, List<Integer>>> smallestPerKey = inputTable.apply(Top.smallestPerKey(2));

    PAssert.thatSingletonIterable(top1).containsInAnyOrder(Arrays.asList("bb"));
    PAssert.thatSingletonIterable(top2).containsInAnyOrder("z", "c");
    PAssert.thatSingletonIterable(top3).containsInAnyOrder("a", "bb", "c");
    PAssert.that(largestPerKey)
        .containsInAnyOrder(KV.of("a", Arrays.asList(3, 2)), KV.of("b", Arrays.asList(100, 10)));
    PAssert.that(smallestPerKey)
        .containsInAnyOrder(KV.of("a", Arrays.asList(1, 2)), KV.of("b", Arrays.asList(1, 10)));

    p.run();
  }

  @Test
  @Category(NeedsRunner.class)
  @SuppressWarnings("unchecked")
  public void testTopEmpty() {
    PCollection<String> input =
        p.apply(Create.of(Arrays.asList(EMPTY_COLLECTION)).withCoder(StringUtf8Coder.of()));

    PCollection<List<String>> top1 = input.apply(Top.of(1, new OrderByLength()));
    PCollection<List<String>> top2 = input.apply(Top.largest(2));
    PCollection<List<String>> top3 = input.apply(Top.smallest(3));

    PCollection<KV<String, Integer>> inputTable = createEmptyInputTable(p);
    PCollection<KV<String, List<Integer>>> largestPerKey = inputTable.apply(Top.largestPerKey(2));
    PCollection<KV<String, List<Integer>>> smallestPerKey = inputTable.apply(Top.smallestPerKey(2));

    PAssert.thatSingletonIterable(top1).empty();
    PAssert.thatSingletonIterable(top2).empty();
    PAssert.thatSingletonIterable(top3).empty();
    PAssert.that(largestPerKey).empty();
    PAssert.that(smallestPerKey).empty();

    p.run();
  }

  @Test
  public void testTopEmptyWithIncompatibleWindows() {
    p.enableAbandonedNodeEnforcement(false);

    Window<String> windowingFn = Window.into(FixedWindows.of(Duration.standardDays(10L)));
    PCollection<String> input = p.apply(Create.empty(StringUtf8Coder.of())).apply(windowingFn);

    expectedEx.expect(IllegalStateException.class);
    expectedEx.expectMessage("Top");
    expectedEx.expectMessage("GlobalWindows");
    expectedEx.expectMessage("withoutDefaults");
    expectedEx.expectMessage("asSingletonView");

    input.apply(Top.of(1, new OrderByLength()));
  }

  @Test
  @Category(NeedsRunner.class)
  @SuppressWarnings("unchecked")
  public void testTopZero() {
    PCollection<String> input =
        p.apply(Create.of(Arrays.asList(COLLECTION)).withCoder(StringUtf8Coder.of()));

    PCollection<List<String>> top1 = input.apply(Top.of(0, new OrderByLength()));
    PCollection<List<String>> top2 = input.apply(Top.largest(0));
    PCollection<List<String>> top3 = input.apply(Top.smallest(0));

    PCollection<KV<String, Integer>> inputTable = createInputTable(p);
    PCollection<KV<String, List<Integer>>> largestPerKey = inputTable.apply(Top.largestPerKey(0));

    PCollection<KV<String, List<Integer>>> smallestPerKey = inputTable.apply(Top.smallestPerKey(0));

    PAssert.thatSingletonIterable(top1).empty();
    PAssert.thatSingletonIterable(top2).empty();
    PAssert.thatSingletonIterable(top3).empty();
    PAssert.that(largestPerKey)
        .containsInAnyOrder(KV.of("a", Arrays.asList()), KV.of("b", Arrays.asList()));
    PAssert.that(smallestPerKey)
        .containsInAnyOrder(KV.of("a", Arrays.asList()), KV.of("b", Arrays.asList()));

    p.run();
  }

  // This is a purely compile-time test.  If the code compiles, then it worked.
  @Test
  public void testPerKeySerializabilityRequirement() {
    p.enableAbandonedNodeEnforcement(false);

    p.apply(
        "CreateCollection", Create.of(Arrays.asList(COLLECTION)).withCoder(StringUtf8Coder.of()));

    PCollection<KV<String, Integer>> inputTable = createInputTable(p);
    inputTable.apply(Top.perKey(1, new IntegerComparator()));

    inputTable.apply("PerKey2", Top.perKey(1, new IntegerComparator2()));
  }

  @Test
  public void testCountConstraint() {
    p.enableAbandonedNodeEnforcement(false);

    PCollection<String> input =
        p.apply(Create.of(Arrays.asList(COLLECTION)).withCoder(StringUtf8Coder.of()));

    expectedEx.expect(IllegalArgumentException.class);
    expectedEx.expectMessage(Matchers.containsString(">= 0"));

    input.apply(Top.of(-1, new OrderByLength()));
  }

  @Test
  public void testTopGetNames() {
    assertEquals("Combine.globally(Top(OrderByLength))", Top.of(1, new OrderByLength()).getName());
    assertEquals("Combine.globally(Top(Reversed))", Top.smallest(1).getName());
    assertEquals("Combine.globally(Top(Natural))", Top.largest(2).getName());
    assertEquals(
        "Combine.perKey(Top(IntegerComparator))", Top.perKey(1, new IntegerComparator()).getName());
    assertEquals("Combine.perKey(Top(Reversed))", Top.<String, Integer>smallestPerKey(1).getName());
    assertEquals("Combine.perKey(Top(Natural))", Top.<String, Integer>largestPerKey(2).getName());
  }

  @Test
  public void testDisplayData() {
    Top.Natural<Integer> comparer = new Top.Natural<>();
    Combine.Globally<Integer, List<Integer>> top = Top.of(1234, comparer);
    DisplayData displayData = DisplayData.from(top);

    assertThat(displayData, hasDisplayItem("count", 1234));
    assertThat(displayData, hasDisplayItem("comparer", comparer.getClass()));
  }

  private static class OrderByLength implements Comparator<String>, Serializable {
    @Override
    public int compare(String a, String b) {
      if (a.length() != b.length()) {
        return a.length() - b.length();
      } else {
        return a.compareTo(b);
      }
    }
  }

  private static class IntegerComparator implements Comparator<Integer>, Serializable {
    @Override
    public int compare(Integer o1, Integer o2) {
      return o1.compareTo(o2);
    }
  }

  private static class IntegerComparator2 implements Comparator<Integer>, Serializable {
    @Override
    public int compare(Integer o1, Integer o2) {
      return o1.compareTo(o2);
    }
  }
}
