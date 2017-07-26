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

import static org.apache.beam.sdk.testing.CombineFnTester.testCombineFn;
import static org.apache.beam.sdk.transforms.display.DisplayDataMatchers.hasDisplayItem;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.collection.IsIterableContainingInOrder.contains;

import java.io.Serializable;
import java.util.ArrayList;
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
import org.apache.beam.sdk.transforms.ApproximateQuantiles.ApproximateQuantilesCombineFn;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.hamcrest.CoreMatchers;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeDiagnosingMatcher;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Tests for {@link ApproximateQuantiles}.
 */
@RunWith(JUnit4.class)
public class ApproximateQuantilesTest {

  static final List<KV<String, Integer>> TABLE = Arrays.asList(
      KV.of("a", 1),
      KV.of("a", 2),
      KV.of("a", 3),
      KV.of("b", 1),
      KV.of("b", 10),
      KV.of("b", 10),
      KV.of("b", 100)
  );

  @Rule
  public TestPipeline p = TestPipeline.create();

  public PCollection<KV<String, Integer>> createInputTable(Pipeline p) {
    return p.apply(Create.of(TABLE).withCoder(
        KvCoder.of(StringUtf8Coder.of(), BigEndianIntegerCoder.of())));
  }

  @Test
  @Category(NeedsRunner.class)
  public void testQuantilesGlobally() {
    PCollection<Integer> input = intRangeCollection(p, 101);
    PCollection<List<Integer>> quantiles =
        input.apply(ApproximateQuantiles.<Integer>globally(5));

    PAssert.that(quantiles)
        .containsInAnyOrder(Arrays.asList(0, 25, 50, 75, 100));
    p.run();
  }

  @Test
  @Category(NeedsRunner.class)
  public void testQuantilesGobally_comparable() {
    PCollection<Integer> input = intRangeCollection(p, 101);
    PCollection<List<Integer>> quantiles =
        input.apply(
            ApproximateQuantiles.globally(5, new DescendingIntComparator()));

    PAssert.that(quantiles)
        .containsInAnyOrder(Arrays.asList(100, 75, 50, 25, 0));
    p.run();
  }

  @Test
  @Category(NeedsRunner.class)
  public void testQuantilesPerKey() {
    PCollection<KV<String, Integer>> input = createInputTable(p);
    PCollection<KV<String, List<Integer>>> quantiles = input.apply(
        ApproximateQuantiles.<String, Integer>perKey(2));

    PAssert.that(quantiles)
        .containsInAnyOrder(
            KV.of("a", Arrays.asList(1, 3)),
            KV.of("b", Arrays.asList(1, 100)));
    p.run();

  }

  @Test
  @Category(NeedsRunner.class)
  public void testQuantilesPerKey_reversed() {
    PCollection<KV<String, Integer>> input = createInputTable(p);
    PCollection<KV<String, List<Integer>>> quantiles = input.apply(
        ApproximateQuantiles.<String, Integer, DescendingIntComparator>perKey(
            2, new DescendingIntComparator()));

    PAssert.that(quantiles)
        .containsInAnyOrder(
            KV.of("a", Arrays.asList(3, 1)),
            KV.of("b", Arrays.asList(100, 1)));
    p.run();
  }

  @Test
  public void testSingleton() {
    testCombineFn(
        ApproximateQuantilesCombineFn.<Integer>create(5),
        Arrays.asList(389),
        Arrays.asList(389, 389, 389, 389, 389));
  }

  @Test
  public void testSimpleQuantiles() {
    testCombineFn(
        ApproximateQuantilesCombineFn.<Integer>create(5),
        intRange(101),
        Arrays.asList(0, 25, 50, 75, 100));
  }

  @Test
  public void testUnevenQuantiles() {
    testCombineFn(
        ApproximateQuantilesCombineFn.<Integer>create(37),
        intRange(5000),
        quantileMatcher(5000, 37, 20 /* tolerance */));
  }

  @Test
  public void testLargerQuantiles() {
    testCombineFn(
        ApproximateQuantilesCombineFn.<Integer>create(50),
        intRange(10001),
        quantileMatcher(10001, 50, 20 /* tolerance */));
  }

  @Test
  public void testTightEpsilon() {
    testCombineFn(
        ApproximateQuantilesCombineFn.<Integer>create(10).withEpsilon(0.01),
        intRange(10001),
        quantileMatcher(10001, 10, 5 /* tolerance */));
  }

  @Test
  public void testDuplicates() {
    int size = 101;
    List<Integer> all = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      all.addAll(intRange(size));
    }
    testCombineFn(
        ApproximateQuantilesCombineFn.<Integer>create(5),
        all,
        Arrays.asList(0, 25, 50, 75, 100));
  }

  @Test
  public void testLotsOfDuplicates() {
    List<Integer> all = new ArrayList<>();
    all.add(1);
    for (int i = 1; i < 300; i++) {
      all.add(2);
    }
    for (int i = 300; i < 1000; i++) {
      all.add(3);
    }
    testCombineFn(
        ApproximateQuantilesCombineFn.<Integer>create(5),
        all,
        Arrays.asList(1, 2, 3, 3, 3));
  }

  @Test
  public void testLogDistribution() {
    List<Integer> all = new ArrayList<>();
    for (int i = 1; i < 1000; i++) {
      all.add((int) Math.log(i));
    }
    testCombineFn(
        ApproximateQuantilesCombineFn.<Integer>create(5),
        all,
        Arrays.asList(0, 5, 6, 6, 6));
  }

  @Test
  public void testZipfianDistribution() {
    List<Integer> all = new ArrayList<>();
    for (int i = 1; i < 1000; i++) {
      all.add(1000 / i);
    }
    testCombineFn(
        ApproximateQuantilesCombineFn.<Integer>create(5),
        all,
        Arrays.asList(1, 1, 2, 4, 1000));
  }

  @Test
  public void testAlternateComparator() {
    List<String> inputs = Arrays.asList(
        "aa", "aaa", "aaaa", "b", "ccccc", "dddd", "zz");
    testCombineFn(
        ApproximateQuantilesCombineFn.<String>create(3),
        inputs,
        Arrays.asList("aa", "b", "zz"));
    testCombineFn(
        ApproximateQuantilesCombineFn.create(3, new OrderByLength()),
        inputs,
        Arrays.asList("b", "aaa", "ccccc"));
  }

  @Test
  public void testDisplayData() {
    Top.Natural<Integer> comparer = new Top.Natural<Integer>();
    PTransform<?, ?> approxQuanitiles = ApproximateQuantiles.globally(20, comparer);
    DisplayData displayData = DisplayData.from(approxQuanitiles);

    assertThat(displayData, hasDisplayItem("numQuantiles", 20));
    assertThat(displayData, hasDisplayItem("comparer", comparer.getClass()));
  }

  private Matcher<Iterable<? extends Integer>> quantileMatcher(
      int size, int numQuantiles, int absoluteError) {
    List<Matcher<? super Integer>> quantiles = new ArrayList<>();
    quantiles.add(CoreMatchers.is(0));
    for (int k = 1; k < numQuantiles - 1; k++) {
      int expected = (int) (((double) (size - 1)) * k / (numQuantiles - 1));
      quantiles.add(new Between<>(
          expected - absoluteError, expected + absoluteError));
    }
    quantiles.add(CoreMatchers.is(size - 1));
    return contains(quantiles);
  }

  private static class Between<T extends Comparable<T>>
      extends TypeSafeDiagnosingMatcher<T> {
    private final T min;
    private final T max;
    private Between(T min, T max) {
      this.min = min;
      this.max = max;
    }
    @Override
    public void describeTo(Description description) {
      description.appendText("is between " + min + " and " + max);
    }

    @Override
    protected boolean matchesSafely(T item, Description mismatchDescription) {
      return min.compareTo(item) <= 0 && item.compareTo(max) <= 0;
    }
  }

  private static class DescendingIntComparator implements
      SerializableComparator<Integer> {
    @Override
    public int compare(Integer o1, Integer o2) {
      return o2.compareTo(o1);
    }
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


  private PCollection<Integer> intRangeCollection(Pipeline p, int size) {
    return p.apply("CreateIntsUpTo(" + size + ")", Create.of(intRange(size)));
  }

  private List<Integer> intRange(int size) {
    List<Integer> all = new ArrayList<>(size);
    for (int i = 0; i < size; i++) {
      all.add(i);
    }
    return all;
  }
}
