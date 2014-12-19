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

import static org.hamcrest.collection.IsIterableContainingInAnyOrder.containsInAnyOrder;
import static org.hamcrest.collection.IsIterableContainingInOrder.contains;
import static org.junit.Assert.assertThat;

import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.coders.BigEndianIntegerCoder;
import com.google.cloud.dataflow.sdk.coders.KvCoder;
import com.google.cloud.dataflow.sdk.coders.StringUtf8Coder;
import com.google.cloud.dataflow.sdk.runners.DirectPipeline;
import com.google.cloud.dataflow.sdk.runners.DirectPipelineRunner.EvaluationResults;
import com.google.cloud.dataflow.sdk.runners.RecordingPipelineVisitor;
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
    return p.apply(Create.of(Arrays.asList(TABLE))).setCoder(
        KvCoder.of(StringUtf8Coder.of(), BigEndianIntegerCoder.of()));
  }

  public PCollection<KV<String, Integer>> createEmptyInputTable(Pipeline p) {
    return p.apply(Create.of(Arrays.asList(EMPTY_TABLE))).setCoder(
        KvCoder.of(StringUtf8Coder.of(), BigEndianIntegerCoder.of()));
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testTop() {
    DirectPipeline p = DirectPipeline.createForTest();
    PCollection<String> input =
        p.apply(Create.of(Arrays.asList(COLLECTION)))
                 .setCoder(StringUtf8Coder.of());

    PCollection<List<String>> top1 = input.apply(Top.of(1, new OrderByLength()));
    PCollection<List<String>> top2 = input.apply(Top.<String>largest(2));
    PCollection<List<String>> top3 = input.apply(Top.<String>smallest(3));

    PCollection<KV<String, List<Integer>>> largestPerKey = createInputTable(p)
        .apply(Top.<String, Integer>largestPerKey(2));
    PCollection<KV<String, List<Integer>>> smallestPerKey = createInputTable(p)
        .apply(Top.<String, Integer>smallestPerKey(2));

    EvaluationResults results = p.run();

    assertThat(results.getPCollection(top1).get(0), contains("bb"));
    assertThat(results.getPCollection(top2).get(0), contains("z", "c"));
    assertThat(results.getPCollection(top3).get(0), contains("a", "bb", "c"));
    assertThat(results.getPCollection(largestPerKey), containsInAnyOrder(
        KV.of("a", Arrays.asList(3, 2)),
        KV.of("b", Arrays.asList(100, 10))));
    assertThat(results.getPCollection(smallestPerKey), containsInAnyOrder(
        KV.of("a", Arrays.asList(1, 2)),
        KV.of("b", Arrays.asList(1, 10))));
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testTopEmpty() {
    DirectPipeline p = DirectPipeline.createForTest();
    PCollection<String> input =
        p.apply(Create.of(Arrays.asList(EMPTY_COLLECTION)))
                 .setCoder(StringUtf8Coder.of());

    PCollection<List<String>> top1 = input.apply(Top.of(1, new OrderByLength()));
    PCollection<List<String>> top2 = input.apply(Top.<String>largest(2));
    PCollection<List<String>> top3 = input.apply(Top.<String>smallest(3));

    PCollection<KV<String, List<Integer>>> largestPerKey = createEmptyInputTable(p)
        .apply(Top.<String, Integer>largestPerKey(2));
    PCollection<KV<String, List<Integer>>> smallestPerKey = createEmptyInputTable(p)
        .apply(Top.<String, Integer>smallestPerKey(2));

    EvaluationResults results = p.run();

    assertThat(results.getPCollection(top1).get(0), containsInAnyOrder());
    assertThat(results.getPCollection(top2).get(0), containsInAnyOrder());
    assertThat(results.getPCollection(top3).get(0), containsInAnyOrder());
    assertThat(results.getPCollection(largestPerKey), containsInAnyOrder());
    assertThat(results.getPCollection(smallestPerKey), containsInAnyOrder());
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testTopZero() {
    DirectPipeline p = DirectPipeline.createForTest();
    PCollection<String> input =
        p.apply(Create.of(Arrays.asList(COLLECTION)))
                 .setCoder(StringUtf8Coder.of());

    PCollection<List<String>> top1 = input.apply(Top.of(0, new OrderByLength()));
    PCollection<List<String>> top2 = input.apply(Top.<String>largest(0));
    PCollection<List<String>> top3 = input.apply(Top.<String>smallest(0));

    PCollection<KV<String, List<Integer>>> largestPerKey = createInputTable(p)
        .apply(Top.<String, Integer>largestPerKey(0));

    PCollection<KV<String, List<Integer>>> smallestPerKey = createInputTable(p)
        .apply(Top.<String, Integer>smallestPerKey(0));

    EvaluationResults results = p.run();

    assertThat(results.getPCollection(top1).get(0), containsInAnyOrder());
    assertThat(results.getPCollection(top2).get(0), containsInAnyOrder());
    assertThat(results.getPCollection(top3).get(0), containsInAnyOrder());
    assertThat(results.getPCollection(largestPerKey), containsInAnyOrder(
        KV.of("a", Arrays.<Integer>asList()),
        KV.of("b", Arrays.<Integer>asList())));
    assertThat(results.getPCollection(smallestPerKey), containsInAnyOrder(
        KV.of("a", Arrays.<Integer>asList()),
        KV.of("b", Arrays.<Integer>asList())));
  }

  // This is a purely compile-time test.  If the code compiles, then it worked.
  @Test
  public void testPerKeySerializabilityRequirement() {
    DirectPipeline p = DirectPipeline.createForTest();
    p.apply(Create.of(Arrays.asList(COLLECTION)))
            .setCoder(StringUtf8Coder.of());

    createInputTable(p)
        .apply(Top.<String, Integer, IntegerComparator>perKey(1,
            new IntegerComparator()));

    createInputTable(p)
        .apply(Top.<String, Integer, IntegerComparator2>perKey(1,
            new IntegerComparator2()));
  }

  @Test
  public void testCountConstraint() {
    DirectPipeline p = DirectPipeline.createForTest();
    PCollection<String> input =
        p.apply(Create.of(Arrays.asList(COLLECTION)))
            .setCoder(StringUtf8Coder.of());

    expectedEx.expect(IllegalArgumentException.class);
    expectedEx.expectMessage(Matchers.containsString(">= 0"));

    input.apply(Top.of(-1, new OrderByLength()));
  }

  @Test
  public void testTransformName() {
    DirectPipeline p = DirectPipeline.createForTest();
    PCollection<String> input =
        p.apply(Create.of(Arrays.asList(COLLECTION)))
            .setCoder(StringUtf8Coder.of());

    PTransform<PCollection<String>, PCollection<List<String>>> top = Top
        .of(10, new OrderByLength());
    input.apply(top);

    p.traverseTopologically(new RecordingPipelineVisitor());
    // Check that the transform is named "Top" rather than "Combine".
    assertThat(p.getFullName(top), Matchers.startsWith("Top"));
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
