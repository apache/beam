/*
 * Copyright (C) 2016 Google Inc.
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

package com.google.cloud.dataflow.sdk.io;

import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.io.CountingInput.UnboundedCountingInput;
import com.google.cloud.dataflow.sdk.testing.DataflowAssert;
import com.google.cloud.dataflow.sdk.testing.RunnableOnService;
import com.google.cloud.dataflow.sdk.testing.TestPipeline;
import com.google.cloud.dataflow.sdk.transforms.Count;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.Max;
import com.google.cloud.dataflow.sdk.transforms.Min;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.transforms.RemoveDuplicates;
import com.google.cloud.dataflow.sdk.transforms.SerializableFunction;
import com.google.cloud.dataflow.sdk.values.PCollection;

import org.joda.time.Instant;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Tests for {@link CountingInput}.
 */
@RunWith(JUnit4.class)
public class CountingInputTest {
  public static void addCountingAsserts(PCollection<Long> input, long numElements) {
    // Count == numElements
    DataflowAssert.thatSingleton(input.apply("Count", Count.<Long>globally()))
        .isEqualTo(numElements);
    // Unique count == numElements
    DataflowAssert.thatSingleton(
            input
                .apply(RemoveDuplicates.<Long>create())
                .apply("UniqueCount", Count.<Long>globally()))
        .isEqualTo(numElements);
    // Min == 0
    DataflowAssert.thatSingleton(input.apply("Min", Min.<Long>globally())).isEqualTo(0L);
    // Max == numElements-1
    DataflowAssert.thatSingleton(input.apply("Max", Max.<Long>globally()))
        .isEqualTo(numElements - 1);
  }

  @Test
  @Category(RunnableOnService.class)
  public void testBoundedInput() {
    Pipeline p = TestPipeline.create();
    long numElements = 1000;
    PCollection<Long> input = p.apply(CountingInput.upTo(numElements));

    addCountingAsserts(input, numElements);
    p.run();
  }

  @Test
  @Category(RunnableOnService.class)
  public void testUnboundedInput() {
    Pipeline p = TestPipeline.create();
    long numElements = 1000;

    PCollection<Long> input = p.apply(CountingInput.unbounded().withMaxNumRecords(numElements));

    addCountingAsserts(input, numElements);
    p.run();
  }

  private static class ElementValueDiff extends DoFn<Long, Long> {
    @Override
    public void processElement(ProcessContext c) throws Exception {
      c.output(c.element() - c.timestamp().getMillis());
    }
  }

  @Test
  @Category(RunnableOnService.class)
  public void testUnboundedInputTimestamps() {
    Pipeline p = TestPipeline.create();
    long numElements = 1000;

    PCollection<Long> input =
        p.apply(
            CountingInput.unbounded()
                .withTimestampFn(new ValueAsTimestampFn())
                .withMaxNumRecords(numElements));
    addCountingAsserts(input, numElements);

    PCollection<Long> diffs =
        input
            .apply("TimestampDiff", ParDo.of(new ElementValueDiff()))
            .apply("RemoveDuplicateTimestamps", RemoveDuplicates.<Long>create());
    // This assert also confirms that diffs only has one unique value.
    DataflowAssert.thatSingleton(diffs).isEqualTo(0L);

    p.run();
  }

  /**
   * A timestamp function that uses the given value as the timestamp. Because the input values will
   * not wrap, this function is non-decreasing and meets the timestamp function criteria laid out
   * in {@link UnboundedCountingInput#withTimestampFn(SerializableFunction)}.
   */
  private static class ValueAsTimestampFn implements SerializableFunction<Long, Instant> {
    @Override
    public Instant apply(Long input) {
      return new Instant(input);
    }
  }
}
