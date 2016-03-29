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

import com.google.cloud.dataflow.sdk.testing.DataflowAssert;
import com.google.cloud.dataflow.sdk.testing.RunnableOnService;
import com.google.cloud.dataflow.sdk.testing.TestPipeline;
import com.google.cloud.dataflow.sdk.values.PCollection;

import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.io.Serializable;

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

  @Deprecated
  @Test
  @Category(RunnableOnService.class)
  public void testIdentityFilterBy() {
    TestPipeline p = TestPipeline.create();

    PCollection<Integer> output = p
        .apply(Create.of(591, 11789, 1257, 24578, 24799, 307))
        .apply(Filter.by(new TrivialFn(true)));

    DataflowAssert.that(output).containsInAnyOrder(591, 11789, 1257, 24578, 24799, 307);
    p.run();
  }

  @Deprecated
  @Test
  public void testNoFilter() {
    TestPipeline p = TestPipeline.create();

    PCollection<Integer> output = p
        .apply(Create.of(1, 2, 4, 5))
        .apply(Filter.by(new TrivialFn(false)));

    DataflowAssert.that(output).empty();
    p.run();
  }

  @Deprecated
  @Test
  @Category(RunnableOnService.class)
  public void testFilterBy() {
    TestPipeline p = TestPipeline.create();

    PCollection<Integer> output = p
        .apply(Create.of(1, 2, 3, 4, 5, 6, 7))
        .apply(Filter.by(new EvenFn()));

    DataflowAssert.that(output).containsInAnyOrder(2, 4, 6);
    p.run();
  }

  @Test
  @Category(RunnableOnService.class)
  public void testIdentityFilterByPredicate() {
    TestPipeline p = TestPipeline.create();

    PCollection<Integer> output = p
        .apply(Create.of(591, 11789, 1257, 24578, 24799, 307))
        .apply(Filter.byPredicate(new TrivialFn(true)));

    DataflowAssert.that(output).containsInAnyOrder(591, 11789, 1257, 24578, 24799, 307);
    p.run();
  }

  @Test
  public void testNoFilterByPredicate() {
    TestPipeline p = TestPipeline.create();

    PCollection<Integer> output = p
        .apply(Create.of(1, 2, 4, 5))
        .apply(Filter.byPredicate(new TrivialFn(false)));

    DataflowAssert.that(output).empty();
    p.run();
  }

  @Test
  @Category(RunnableOnService.class)
  public void testFilterByPredicate() {
    TestPipeline p = TestPipeline.create();

    PCollection<Integer> output = p
        .apply(Create.of(1, 2, 3, 4, 5, 6, 7))
        .apply(Filter.byPredicate(new EvenFn()));

    DataflowAssert.that(output).containsInAnyOrder(2, 4, 6);
    p.run();
  }

  @Test
  @Category(RunnableOnService.class)
  public void testFilterLessThan() {
    TestPipeline p = TestPipeline.create();

    PCollection<Integer> output = p
        .apply(Create.of(1, 2, 3, 4, 5, 6, 7))
        .apply(Filter.lessThan(4));

    DataflowAssert.that(output).containsInAnyOrder(1, 2, 3);
    p.run();
  }

  @Test
  public void testFilterGreaterThan() {
    TestPipeline p = TestPipeline.create();

    PCollection<Integer> output = p
        .apply(Create.of(1, 2, 3, 4, 5, 6, 7))
        .apply(Filter.greaterThan(4));

    DataflowAssert.that(output).containsInAnyOrder(5, 6, 7);
    p.run();
  }
}
