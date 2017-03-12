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
package org.apache.beam.sdk.values;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

import com.google.common.collect.ImmutableList;
import com.google.common.testing.EqualsTester;
import java.util.Collections;
import java.util.List;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.CountingInput;
import org.apache.beam.sdk.io.CountingInput.BoundedCountingInput;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.hamcrest.Matchers;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Tests for PCollectionLists.
 */
@RunWith(JUnit4.class)
public class PCollectionListTest {
  @Test
  public void testEmptyListFailure() {
    try {
      PCollectionList.of(Collections.<PCollection<String>>emptyList());
      fail("should have failed");
    } catch (IllegalArgumentException exn) {
      assertThat(
          exn.toString(),
          containsString(
              "must either have a non-empty list of PCollections, "
              + "or must first call empty(Pipeline)"));
    }
  }

  @Test
  public void testIterationOrder() {
    Pipeline p = TestPipeline.create();
    PCollection<Long> createOne = p.apply("CreateOne", Create.of(1L, 2L, 3L));
    PCollection<Long> boundedCount = p.apply("CountBounded", CountingInput.upTo(23L));
    PCollection<Long> unboundedCount = p.apply("CountUnbounded", CountingInput.unbounded());
    PCollection<Long> createTwo = p.apply("CreateTwo", Create.of(-1L, -2L));
    PCollection<Long> maxRecordsCount =
        p.apply("CountLimited", CountingInput.unbounded().withMaxNumRecords(22L));

    ImmutableList<PCollection<Long>> counts =
        ImmutableList.of(boundedCount, maxRecordsCount, unboundedCount);
    // Build a PCollectionList from a list. This should have the same order as the input list.
    PCollectionList<Long> pcList = PCollectionList.of(counts);
    // Contains is the order-dependent matcher
    assertThat(
        pcList.getAll(),
        contains(boundedCount, maxRecordsCount, unboundedCount));

    // A list that is expanded with builder methods has the added value at the end
    PCollectionList<Long> withOneCreate = pcList.and(createTwo);
    assertThat(
        withOneCreate.getAll(), contains(boundedCount, maxRecordsCount, unboundedCount, createTwo));

    // Lists that are built entirely from the builder return outputs in the order they were added
    PCollectionList<Long> fromEmpty =
        PCollectionList.<Long>empty(p)
            .and(unboundedCount)
            .and(createOne)
            .and(ImmutableList.of(boundedCount, maxRecordsCount));
    assertThat(
        fromEmpty.getAll(), contains(unboundedCount, createOne, boundedCount, maxRecordsCount));

    List<TaggedPValue> expansion = fromEmpty.expand();
    // TaggedPValues are stable between expansions
    assertThat(expansion, equalTo(fromEmpty.expand()));
    // TaggedPValues are equivalent between equivalent lists
    assertThat(
        expansion,
        equalTo(
            PCollectionList.of(unboundedCount)
                .and(createOne)
                .and(boundedCount)
                .and(maxRecordsCount)
                .expand()));

    List<PCollection<Long>> expectedList =
        ImmutableList.of(unboundedCount, createOne, boundedCount, maxRecordsCount);
    for (int i = 0; i < expansion.size(); i++) {
      assertThat(
          "Index " + i + " should have equal PValue",
          expansion.get(i).getValue(),
          Matchers.<PValue>equalTo(expectedList.get(i)));
    }
  }

  @Test
  public void testEquals() {
    Pipeline p = TestPipeline.create();
    PCollection<String> first = p.apply("Meta", Create.of("foo", "bar"));
    PCollection<String> second = p.apply("Pythonic", Create.of("spam, ham"));
    PCollection<String> third = p.apply("Syntactic", Create.of("eggs", "baz"));

    EqualsTester tester = new EqualsTester();
    tester.addEqualityGroup(PCollectionList.empty(p), PCollectionList.empty(p));
    tester.addEqualityGroup(PCollectionList.of(first).and(second));
    // Constructors should all produce equivalent
    tester.addEqualityGroup(
        PCollectionList.of(first).and(second).and(third),
        PCollectionList.of(first).and(second).and(third),
        PCollectionList.<String>empty(p).and(first).and(second).and(third),
        PCollectionList.of(ImmutableList.of(first, second, third)),
        PCollectionList.of(first).and(ImmutableList.of(second, third)),
        PCollectionList.of(ImmutableList.of(first, second)).and(third));
    // Order is considered
    tester.addEqualityGroup(PCollectionList.of(first).and(third).and(second));
    tester.addEqualityGroup(PCollectionList.empty(TestPipeline.create()));

    tester.testEquals();
  }

  @Test
  public void testExpansionOrderWithDuplicates() {
    TestPipeline p = TestPipeline.create();
    BoundedCountingInput count = CountingInput.upTo(10L);
    PCollection<Long> firstCount = p.apply("CountFirst", count);
    PCollection<Long> secondCount = p.apply("CountSecond", count);

    PCollectionList<Long> counts =
        PCollectionList.of(firstCount).and(secondCount).and(firstCount).and(firstCount);

    ImmutableList<PCollection<Long>> expectedOrder =
        ImmutableList.of(firstCount, secondCount, firstCount, firstCount);
    PCollectionList<Long> reconstructed = PCollectionList.empty(p);
    assertThat(counts.expand(), hasSize(4));
    for (int i = 0; i < 4; i++) {
      PValue value = counts.expand().get(i).getValue();
      assertThat(
          "Index " + i + " should be equal", value,
          Matchers.<PValue>equalTo(expectedOrder.get(i)));
      reconstructed = reconstructed.and((PCollection<Long>) value);
    }
    assertThat(reconstructed, equalTo(counts));
  }
}
