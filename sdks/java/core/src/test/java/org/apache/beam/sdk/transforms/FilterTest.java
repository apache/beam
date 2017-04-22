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

import java.io.Serializable;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.ValidatesRunner;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Tests for {@link Filter}.
 */
@RunWith(JUnit4.class)
public class FilterTest implements Serializable {

  static class TrivialFn implements SerializableFunction<Integer, Boolean> {
    private final Boolean returnVal;

    TrivialFn(Boolean returnVal) {
      this.returnVal = returnVal;
    }

    @Override
    public Boolean apply(Integer elem) {
      return this.returnVal;
    }
  }

  static class EvenFn implements SerializableFunction<Integer, Boolean> {
    @Override
    public Boolean apply(Integer elem) {
      return elem % 2 == 0;
    }
  }

  @Rule
  public final TestPipeline p = TestPipeline.create();

  @Test
  @Category(ValidatesRunner.class)
  public void testIdentityFilterByPredicate() {
    PCollection<Integer> output = p
        .apply(Create.of(591, 11789, 1257, 24578, 24799, 307))
        .apply(Filter.by(new TrivialFn(true)));

    PAssert.that(output).containsInAnyOrder(591, 11789, 1257, 24578, 24799, 307);
    p.run();
  }

  @Test
  @Category(ValidatesRunner.class)
  public void testNoFilterByPredicate() {
    PCollection<Integer> output = p
        .apply(Create.of(1, 2, 4, 5))
        .apply(Filter.by(new TrivialFn(false)));

    PAssert.that(output).empty();
    p.run();
  }

  @Test
  @Category(ValidatesRunner.class)
  public void testFilterByPredicate() {
    PCollection<Integer> output = p
        .apply(Create.of(1, 2, 3, 4, 5, 6, 7))
        .apply(Filter.by(new EvenFn()));

    PAssert.that(output).containsInAnyOrder(2, 4, 6);
    p.run();
  }

  @Test
  @Category(ValidatesRunner.class)
  public void testFilterLessThan() {
    PCollection<Integer> output = p
        .apply(Create.of(1, 2, 3, 4, 5, 6, 7))
        .apply(Filter.lessThan(4));

    PAssert.that(output).containsInAnyOrder(1, 2, 3);
    p.run();
  }

  @Test
  @Category(ValidatesRunner.class)
  public void testFilterGreaterThan() {
    PCollection<Integer> output = p
        .apply(Create.of(1, 2, 3, 4, 5, 6, 7))
        .apply(Filter.greaterThan(4));

    PAssert.that(output).containsInAnyOrder(5, 6, 7);
    p.run();
  }

  @Test
  @Category(ValidatesRunner.class)
  public void testFilterLessThanEq() {
    PCollection<Integer> output = p
        .apply(Create.of(1, 2, 3, 4, 5, 6, 7))
        .apply(Filter.lessThanEq(4));

    PAssert.that(output).containsInAnyOrder(1, 2, 3, 4);
    p.run();
  }

  @Test
  @Category(ValidatesRunner.class)
  public void testFilterGreaterThanEq() {
    PCollection<Integer> output = p
        .apply(Create.of(1, 2, 3, 4, 5, 6, 7))
        .apply(Filter.greaterThanEq(4));

    PAssert.that(output).containsInAnyOrder(4, 5, 6, 7);
    p.run();
  }

  @Test
  public void testDisplayData() {
    assertThat(DisplayData.from(Filter.lessThan(123)), hasDisplayItem("predicate", "x < 123"));

    assertThat(DisplayData.from(Filter.lessThanEq(234)), hasDisplayItem("predicate", "x ≤ 234"));

    assertThat(DisplayData.from(Filter.greaterThan(345)), hasDisplayItem("predicate", "x > 345"));

    assertThat(DisplayData.from(Filter.greaterThanEq(456)), hasDisplayItem("predicate", "x ≥ 456"));
  }
}
