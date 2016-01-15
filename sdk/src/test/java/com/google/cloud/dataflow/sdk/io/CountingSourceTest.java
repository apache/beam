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

package com.google.cloud.dataflow.sdk.io;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.io.CountingSource.CounterMark;
import com.google.cloud.dataflow.sdk.io.UnboundedSource.UnboundedReader;
import com.google.cloud.dataflow.sdk.testing.DataflowAssert;
import com.google.cloud.dataflow.sdk.testing.RunnableOnService;
import com.google.cloud.dataflow.sdk.testing.TestPipeline;
import com.google.cloud.dataflow.sdk.transforms.Count;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.Flatten;
import com.google.cloud.dataflow.sdk.transforms.Max;
import com.google.cloud.dataflow.sdk.transforms.Min;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.transforms.RemoveDuplicates;
import com.google.cloud.dataflow.sdk.transforms.SerializableFunction;
import com.google.cloud.dataflow.sdk.util.CoderUtils;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.cloud.dataflow.sdk.values.PCollectionList;

import org.joda.time.Instant;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.util.List;

/**
 * Tests of {@link CountingSource}.
 */
@RunWith(JUnit4.class)
public class CountingSourceTest {

  public static void addCountingAsserts(PCollection<Long> input, long numElements) {
    // Count == numElements
    DataflowAssert
      .thatSingleton(input.apply("Count", Count.<Long>globally()))
      .isEqualTo(numElements);
    // Unique count == numElements
    DataflowAssert
      .thatSingleton(input.apply(RemoveDuplicates.<Long>create())
                          .apply("UniqueCount", Count.<Long>globally()))
      .isEqualTo(numElements);
    // Min == 0
    DataflowAssert
      .thatSingleton(input.apply("Min", Min.<Long>globally()))
      .isEqualTo(0L);
    // Max == numElements-1
    DataflowAssert
      .thatSingleton(input.apply("Max", Max.<Long>globally()))
      .isEqualTo(numElements - 1);
  }

  @Test
  @Category(RunnableOnService.class)
  public void testBoundedSource() {
    Pipeline p = TestPipeline.create();
    long numElements = 1000;
    PCollection<Long> input = p.apply(Read.from(CountingSource.upTo(numElements)));

    addCountingAsserts(input, numElements);
    p.run();
  }

  @Test
  @Category(RunnableOnService.class)
  public void testBoundedSourceSplits() throws Exception {
    Pipeline p = TestPipeline.create();
    long numElements = 1000;
    long numSplits = 10;
    long splitSizeBytes = numElements * 8 / numSplits;  // 8 bytes per long element.

    BoundedSource<Long> initial = CountingSource.upTo(numElements);
    List<? extends BoundedSource<Long>> splits =
        initial.splitIntoBundles(splitSizeBytes, p.getOptions());
    assertEquals("Expected exact splitting", numSplits, splits.size());

    // Assemble all the splits into one flattened PCollection, also verify their sizes.
    PCollectionList<Long> pcollections = PCollectionList.empty(p);
    for (int i = 0; i < splits.size(); ++i) {
      BoundedSource<Long> split = splits.get(i);
      pcollections = pcollections.and(p.apply("split" + i, Read.from(split)));
      assertEquals("Expected even splitting",
          splitSizeBytes, split.getEstimatedSizeBytes(p.getOptions()));
    }
    PCollection<Long> input = pcollections.apply(Flatten.<Long>pCollections());

    addCountingAsserts(input, numElements);
    p.run();
  }

  @Test
  @Category(RunnableOnService.class)
  public void testUnboundedSource() {
    Pipeline p = TestPipeline.create();
    long numElements = 1000;

    PCollection<Long> input = p
        .apply(Read.from(CountingSource.unbounded()).withMaxNumRecords(numElements));

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
  public void testUnboundedSourceTimestamps() {
    Pipeline p = TestPipeline.create();
    long numElements = 1000;

    PCollection<Long> input = p.apply(
        Read.from(CountingSource.unboundedWithTimestampFn(new ValueAsTimestampFn()))
            .withMaxNumRecords(numElements));
    addCountingAsserts(input, numElements);

    PCollection<Long> diffs = input
        .apply("TimestampDiff", ParDo.of(new ElementValueDiff()))
        .apply("RemoveDuplicateTimestamps", RemoveDuplicates.<Long>create());
    // This assert also confirms that diffs only has one unique value.
    DataflowAssert.thatSingleton(diffs).isEqualTo(0L);

    p.run();
  }

  @Test
  @Category(RunnableOnService.class)
  public void testUnboundedSourceSplits() throws Exception {
    Pipeline p = TestPipeline.create();
    long numElements = 1000;
    int numSplits = 10;

    UnboundedSource<Long, ?> initial = CountingSource.unbounded();
    List<? extends UnboundedSource<Long, ?>> splits =
        initial.generateInitialSplits(numSplits, p.getOptions());
    assertEquals("Expected exact splitting", numSplits, splits.size());

    long elementsPerSplit = numElements / numSplits;
    assertEquals("Expected even splits", numElements, elementsPerSplit * numSplits);
    PCollectionList<Long> pcollections = PCollectionList.empty(p);
    for (int i = 0; i < splits.size(); ++i) {
      pcollections = pcollections.and(
          p.apply("split" + i, Read.from(splits.get(i)).withMaxNumRecords(elementsPerSplit)));
    }
    PCollection<Long> input = pcollections.apply(Flatten.<Long>pCollections());

    addCountingAsserts(input, numElements);
    p.run();
  }

  /**
   * A timestamp function that uses the given value as the timestamp. Because the input values will
   * not wrap, this function is non-decreasing and meets the timestamp function criteria laid out
   * in {@link CountingSource#unboundedWithTimestampFn(SerializableFunction)}.
   */
  private static class ValueAsTimestampFn implements SerializableFunction<Long, Instant> {
    @Override
    public Instant apply(Long input) {
      return new Instant(input);
    }
  }

  @Test
  public void testUnboundedSourceCheckpointMark() throws Exception {
    UnboundedSource<Long, CounterMark> source =
        CountingSource.unboundedWithTimestampFn(new ValueAsTimestampFn());
    UnboundedReader<Long> reader = source.createReader(null, null);
    final long numToSkip = 3;
    assertTrue(reader.start());

    // Advance the source numToSkip elements and manually save state.
    for (long l = 0; l < numToSkip; ++l) {
      reader.advance();
    }

    // Confirm that we get the expected element in sequence before checkpointing.
    assertEquals(numToSkip, (long) reader.getCurrent());
    assertEquals(numToSkip, reader.getCurrentTimestamp().getMillis());

    // Checkpoint and restart, and confirm that the source continues correctly.
    CounterMark mark = CoderUtils.clone(
        source.getCheckpointMarkCoder(), (CounterMark) reader.getCheckpointMark());
    reader = source.createReader(null, mark);
    assertTrue(reader.start());

    // Confirm that we get the next element in sequence.
    assertEquals(numToSkip + 1, (long) reader.getCurrent());
    assertEquals(numToSkip + 1, reader.getCurrentTimestamp().getMillis());
  }
}
