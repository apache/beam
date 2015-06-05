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

package com.google.cloud.dataflow.sdk.transforms;

import static org.junit.Assert.assertThat;

import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.coders.BigEndianIntegerCoder;
import com.google.cloud.dataflow.sdk.coders.KvCoder;
import com.google.cloud.dataflow.sdk.coders.StringUtf8Coder;
import com.google.cloud.dataflow.sdk.runners.RecordingPipelineVisitor;
import com.google.cloud.dataflow.sdk.testing.DataflowAssert;
import com.google.cloud.dataflow.sdk.testing.TestPipeline;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.PCollection;

import org.hamcrest.Matchers;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

/** Tests for Top. */
@RunWith(JUnit4.class)
@SuppressWarnings("serial")
public class TopTest {

  @Rule
  public ExpectedException expectedEx = ExpectedException.none();

  @SuppressWarnings("unchecked")
  static final String[] COLLECTION = new String[] {
    "a", "bb", "c", "c", "z"
  };

  @SuppressWarnings("unchecked")
  static final String[] EMPTY_COLLECTION = new String[] {
  };

  @SuppressWarnings({"rawtypes", "unchecked"})
  static final KV<String, Integer>[] TABLE = new KV[] {
    KV.of("a", 1),
    KV.of("a", 2),
    KV.of("a", 3),
    KV.of("b", 1),
    KV.of("b", 10),
    KV.of("b", 10),
    KV.of("b", 100),
  };

  @SuppressWarnings({"rawtypes", "unchecked"})
  static final KV<String, Integer>[] EMPTY_TABLE = new KV[] {
  };

  public PCollection<KV<String, Integer>> createInputTable(Pipeline p) {
    return p.apply("CreateInputTable", Create.of(Arrays.asList(TABLE)).withCoder(
        KvCoder.of(StringUtf8Coder.of(), BigEndianIntegerCoder.of())));
  }

  public PCollection<KV<String, Integer>> createEmptyInputTable(Pipeline p) {
    return p.apply("CreateEmptyInputTable", Create.of(Arrays.asList(EMPTY_TABLE)).withCoder(
        KvCoder.of(StringUtf8Coder.of(), BigEndianIntegerCoder.of())));
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testTop() {
    Pipeline p = TestPipeline.create();
    PCollection<String> input =
        p.apply(Create.of(Arrays.asList(COLLECTION))
                 .withCoder(StringUtf8Coder.of()));

    PCollection<List<String>> top1 = input.apply(Top.of(1, new OrderByLength()));
    PCollection<List<String>> top2 = input.apply(Top.<String>largest(2));
    PCollection<List<String>> top3 = input.apply(Top.<String>smallest(3));

    PCollection<KV<String, Integer>> inputTable = createInputTable(p);
    PCollection<KV<String, List<Integer>>> largestPerKey = inputTable
        .apply(Top.<String, Integer>largestPerKey(2));
    PCollection<KV<String, List<Integer>>> smallestPerKey = inputTable
        .apply(Top.<String, Integer>smallestPerKey(2));

    DataflowAssert.thatSingletonIterable(top1).containsInAnyOrder(Arrays.asList("bb"));
    DataflowAssert.thatSingletonIterable(top2).containsInAnyOrder("z", "c");
    DataflowAssert.thatSingletonIterable(top3).containsInAnyOrder("a", "bb", "c");
    DataflowAssert.that(largestPerKey).containsInAnyOrder(
        KV.of("a", Arrays.asList(3, 2)),
        KV.of("b", Arrays.asList(100, 10)));
    DataflowAssert.that(smallestPerKey).containsInAnyOrder(
        KV.of("a", Arrays.asList(1, 2)),
        KV.of("b", Arrays.asList(1, 10)));

    p.run();
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testTopEmpty() {
    Pipeline p = TestPipeline.create();
    PCollection<String> input =
        p.apply(Create.of(Arrays.asList(EMPTY_COLLECTION))
                 .withCoder(StringUtf8Coder.of()));

    PCollection<List<String>> top1 = input.apply(Top.of(1, new OrderByLength()));
    PCollection<List<String>> top2 = input.apply(Top.<String>largest(2));
    PCollection<List<String>> top3 = input.apply(Top.<String>smallest(3));

    PCollection<KV<String, Integer>> inputTable = createEmptyInputTable(p);
    PCollection<KV<String, List<Integer>>> largestPerKey = inputTable
        .apply(Top.<String, Integer>largestPerKey(2));
    PCollection<KV<String, List<Integer>>> smallestPerKey = inputTable
        .apply(Top.<String, Integer>smallestPerKey(2));

    DataflowAssert.thatSingletonIterable(top1).containsInAnyOrder();
    DataflowAssert.thatSingletonIterable(top2).containsInAnyOrder();
    DataflowAssert.thatSingletonIterable(top3).containsInAnyOrder();
    DataflowAssert.that(largestPerKey).containsInAnyOrder();
    DataflowAssert.that(smallestPerKey).containsInAnyOrder();

    p.run();
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testTopZero() {
    Pipeline p = TestPipeline.create();
    PCollection<String> input =
        p.apply(Create.of(Arrays.asList(COLLECTION))
                 .withCoder(StringUtf8Coder.of()));

    PCollection<List<String>> top1 = input.apply(Top.of(0, new OrderByLength()));
    PCollection<List<String>> top2 = input.apply(Top.<String>largest(0));
    PCollection<List<String>> top3 = input.apply(Top.<String>smallest(0));

    PCollection<KV<String, Integer>> inputTable = createInputTable(p);
    PCollection<KV<String, List<Integer>>> largestPerKey = inputTable
        .apply(Top.<String, Integer>largestPerKey(0));

    PCollection<KV<String, List<Integer>>> smallestPerKey = inputTable
        .apply(Top.<String, Integer>smallestPerKey(0));

    DataflowAssert.thatSingletonIterable(top1).containsInAnyOrder();
    DataflowAssert.thatSingletonIterable(top2).containsInAnyOrder();
    DataflowAssert.thatSingletonIterable(top3).containsInAnyOrder();
    DataflowAssert.that(largestPerKey).containsInAnyOrder(
        KV.of("a", Arrays.<Integer>asList()),
        KV.of("b", Arrays.<Integer>asList()));
    DataflowAssert.that(smallestPerKey).containsInAnyOrder(
        KV.of("a", Arrays.<Integer>asList()),
        KV.of("b", Arrays.<Integer>asList()));

    p.run();
  }

  // This is a purely compile-time test.  If the code compiles, then it worked.
  @Test
  public void testPerKeySerializabilityRequirement() {
    Pipeline p = TestPipeline.create();
    p.apply("CreateCollection", Create.of(Arrays.asList(COLLECTION))
        .withCoder(StringUtf8Coder.of()));

    PCollection<KV<String, Integer>> inputTable = createInputTable(p);
    inputTable
        .apply(Top.<String, Integer, IntegerComparator>perKey(1,
            new IntegerComparator()));

    inputTable
        .apply("PerKey2", Top.<String, Integer, IntegerComparator2>perKey(1,
            new IntegerComparator2()));
  }

  @Test
  public void testCountConstraint() {
    Pipeline p = TestPipeline.create();
    PCollection<String> input =
        p.apply(Create.of(Arrays.asList(COLLECTION))
            .withCoder(StringUtf8Coder.of()));

    expectedEx.expect(IllegalArgumentException.class);
    expectedEx.expectMessage(Matchers.containsString(">= 0"));

    input.apply(Top.of(-1, new OrderByLength()));
  }

  @Test
  public void testTransformName() {
    Pipeline p = TestPipeline.create();
    PCollection<String> input =
        p.apply(Create.of(Arrays.asList(COLLECTION))
            .withCoder(StringUtf8Coder.of()));

    PTransform<PCollection<String>, PCollection<List<String>>> top = Top
        .of(10, new OrderByLength());
    input.apply(top);

    p.traverseTopologically(new RecordingPipelineVisitor());
    // Check that the transform is named "Top" rather than "Combine".
    assertThat(p.getFullNameForTesting(top), Matchers.startsWith("Top"));
  }

  static class OrderByLength implements Comparator<String>, Serializable {
    @Override
    public int compare(String a, String b) {
      if (a.length() != b.length()) {
        return a.length() - b.length();
      } else {
        return a.compareTo(b);
      }
    }
  }

  static class IntegerComparator implements Comparator<Integer>, Serializable {
    @Override
    public int compare(Integer o1, Integer o2) {
      return o1.compareTo(o2);
    }
  }

  static class IntegerComparator2 implements SerializableComparator<Integer> {
    @Override
    public int compare(Integer o1, Integer o2) {
      return o1.compareTo(o2);
    }
  }

}
