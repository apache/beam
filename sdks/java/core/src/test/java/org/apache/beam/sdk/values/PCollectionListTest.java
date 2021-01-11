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
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

import com.google.common.testing.EqualsTester;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.GenerateSequence;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.joda.time.Duration;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for PCollectionLists. */
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
    PCollection<Long> boundedCount = p.apply("CountBounded", GenerateSequence.from(0).to(23));
    PCollection<Long> unboundedCount = p.apply("CountUnbounded", GenerateSequence.from(0));
    PCollection<Long> createTwo = p.apply("CreateTwo", Create.of(-1L, -2L));
    PCollection<Long> maxReadTimeCount =
        p.apply(
            "CountLimited", GenerateSequence.from(0).withMaxReadTime(Duration.standardSeconds(5)));

    ImmutableList<PCollection<Long>> counts =
        ImmutableList.of(boundedCount, maxReadTimeCount, unboundedCount);
    // Build a PCollectionList from a list. This should have the same order as the input list.
    PCollectionList<Long> pcList = PCollectionList.of(counts);
    // Contains is the order-dependent matcher
    assertThat(pcList.getAll(), contains(boundedCount, maxReadTimeCount, unboundedCount));

    // A list that is expanded with builder methods has the added value at the end
    PCollectionList<Long> withOneCreate = pcList.and(createTwo);
    assertThat(
        withOneCreate.getAll(),
        contains(boundedCount, maxReadTimeCount, unboundedCount, createTwo));

    // Lists that are built entirely from the builder return outputs in the order they were added
    PCollectionList<Long> fromEmpty =
        PCollectionList.<Long>empty(p)
            .and(unboundedCount)
            .and(createOne)
            .and(ImmutableList.of(boundedCount, maxReadTimeCount));
    assertThat(
        fromEmpty.getAll(), contains(unboundedCount, createOne, boundedCount, maxReadTimeCount));

    Map<TupleTag<?>, PValue> expansion = fromEmpty.expand();
    // Tag->PValue mappings are stable between expansions. They don't need to be stable across
    // different list instances, though
    assertThat(expansion, equalTo(fromEmpty.expand()));

    List<PCollection<Long>> expectedList =
        ImmutableList.of(unboundedCount, createOne, boundedCount, maxReadTimeCount);
    assertThat(expansion.values(), containsInAnyOrder(expectedList.toArray()));
  }

  @Test
  public void testExpandWithDuplicates() {
    Pipeline p = TestPipeline.create();
    PCollection<Long> createOne = p.apply("CreateOne", Create.of(1L, 2L, 3L));

    PCollectionList<Long> list = PCollectionList.of(createOne).and(createOne).and(createOne);
    assertThat(list.expand().values(), containsInAnyOrder(createOne, createOne, createOne));
  }

  @Test
  public void testEquals() {
    Pipeline p = TestPipeline.create();
    PCollection<String> first = p.apply("Meta", Create.of("foo", "bar"));
    PCollection<String> second = p.apply("Pythonic", Create.of("spam, ham"));
    PCollection<String> third = p.apply("Syntactic", Create.of("eggs", "baz"));

    EqualsTester tester = new EqualsTester();
    //    tester.addEqualityGroup(PCollectionList.empty(p), PCollectionList.empty(p));
    //    tester.addEqualityGroup(PCollectionList.of(first).and(second));
    // Constructors should all produce equivalent
    tester.addEqualityGroup(
        PCollectionList.of(first).and(second).and(third),
        PCollectionList.of(first).and(second).and(third),
        //        PCollectionList.<String>empty(p).and(first).and(second).and(third),
        //        PCollectionList.of(ImmutableList.of(first, second, third)),
        //        PCollectionList.of(first).and(ImmutableList.of(second, third)),
        PCollectionList.of(ImmutableList.of(first, second)).and(third));
    // Order is considered
    tester.addEqualityGroup(PCollectionList.of(first).and(third).and(second));
    tester.addEqualityGroup(PCollectionList.empty(TestPipeline.create()));

    tester.testEquals();
  }

  @Test
  public void testTagNames() {
    Pipeline p = TestPipeline.create();
    PCollection<String> first = p.apply("first", Create.of("1"));
    PCollection<String> second = p.apply("second", Create.of("2"));
    PCollection<String> third = p.apply("third", Create.of("3"));

    PCollectionList<String> list = PCollectionList.of(first).and(second).and(third);
    assertThat(list.pcollections.get(0).getTag().id, equalTo("0"));
    assertThat(list.pcollections.get(1).getTag().id, equalTo("1"));
    assertThat(list.pcollections.get(2).getTag().id, equalTo("2"));
  }
}
